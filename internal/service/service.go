// Package service is the production Pegasus process: it wires config,
// the database pool, the Costguard-backed pipeline, the live-window
// flusher, restart recovery, live intake sources and the health server
// together, and owns the order they start and stop in. cmd/pegasus is a
// thin wrapper around Run.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	"github.com/marcoantonios1/Pegasus/db"
	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Options carries what Run cannot read from Config: the live sources to
// start, plus hooks tests use to observe and fake parts of the process.
// The zero value is the production configuration (no sources yet).
type Options struct {
	// Sources are started after the recovery snapshot and stopped first on
	// shutdown. Empty until the Whatsmeow source exists.
	Sources []LiveSource

	// Triager/Extractor replace the Costguard-backed ones (tests only).
	Triager   api.Triager
	Extractor api.Extractor

	// Logf defaults to log.Printf.
	Logf func(format string, args ...any)

	// OnEvent is called synchronously at each lifecycle milestone (see the
	// Event* constants). Tests use it to assert ordering.
	OnEvent func(event string)

	// OnPool is called once with the pool right after it connects.
	OnPool func(*pgxpool.Pool)

	// OnListening is called with the health server's bound address
	// (useful with a ":0" HTTPAddr).
	OnListening func(addr string)
}

// Lifecycle events passed to Options.OnEvent.
const (
	EventReady           = "ready"
	EventHTTPStopped     = "http_stopped"
	EventSourceStopped   = "source_stopped:" // + source name
	EventIntakeDrained   = "intake_drained"
	EventRecoveryStopped = "recovery_stopped"
	EventFlushDone       = "flush_done"
	EventPoolClosed      = "pool_closed"
)

// ErrShutdownIncomplete means at least one shutdown step ran out of its
// time budget. Any message whose extraction didn't finish is still stored
// with processed=false and is picked up by the next start's recovery.
var ErrShutdownIncomplete = errors.New("graceful shutdown did not complete within PEGASUS_SHUTDOWN_TIMEOUT")

// httpShutdownBudget is short: health requests are tiny, and Shutdown otherwise
// waits on freshly-opened idle keep-alive connections. After it, Close.
const httpShutdownBudget = 2 * time.Second

// Run starts the service and blocks until ctx is cancelled (SIGINT/SIGTERM
// in production), then shuts down in order and returns. It returns nil on
// a clean shutdown, a *ConfigError-free runtime error if startup fails,
// or ErrShutdownIncomplete if shutdown overran its budget.
func Run(ctx context.Context, cfg *Config, opts Options) error {
	logf := opts.Logf
	if logf == nil {
		logf = log.Printf
	}
	emit := func(e string) {
		if opts.OnEvent != nil {
			opts.OnEvent(e)
		}
	}

	logf("pegasus: starting with effective configuration (secrets redacted):")
	for _, line := range cfg.Redacted() {
		logf("pegasus:   %s", line)
	}
	if len(cfg.SelfExternalIDs) == 0 {
		logf("pegasus: WARNING: PEGASUS_SELF_EXTERNAL_IDS is empty. Harmless while no live source is enabled, " +
			"but once one is, Marco's own outgoing messages would be attributed to a contact entity instead of to him. " +
			"Set it before enabling any live source.")
	}

	wantVersion, err := db.LatestMigrationVersion()
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	// --- Database pool ---
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		// Don't wrap err: pgx parse errors can echo the connection string.
		return fmt.Errorf("build database config for %s: invalid connection settings", dbTarget(cfg))
	}
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, cfg.DBConnectTimeout)
	pool, err := pgxpool.NewWithConfig(connectCtx, poolCfg)
	if err == nil {
		err = pool.Ping(connectCtx)
	}
	cancelConnect()
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		return fmt.Errorf("connect to postgres at %s: %w", dbTarget(cfg), err)
	}
	if opts.OnPool != nil {
		opts.OnPool(pool)
	}
	logf("pegasus: connected to postgres at %s", dbTarget(cfg))

	// Everything from here on must go through shutdown() so the pool (and
	// whatever else has started) is released in order, even on a startup
	// failure.
	sd := &shutdownState{cfg: cfg, logf: logf, emit: emit, pool: pool}

	// --- Migration check (validate only; never applies) ---
	migCtx, cancelMig := context.WithTimeout(ctx, cfg.DBConnectTimeout)
	migErr := checkMigrations(migCtx, pool, wantVersion)
	cancelMig()
	if migErr != nil {
		sd.run()
		return migErr
	}
	logf("pegasus: schema is at the expected migration version %d", wantVersion)

	// --- Costguard client and pipeline ---
	client := extraction.NewCostguardClient(cfg.CostguardURL)
	client.Agent = cfg.CostguardAgent
	client.HTTPClient = &http.Client{Timeout: cfg.CostguardTimeout}
	client.TranscriptionRetryBackoff = cfg.CostguardTranscriptionRetryBackoff

	probe := costguardProber(cfg.CostguardURL)
	probeCtx, cancelProbe := context.WithTimeout(ctx, costguardProbeTimeout)
	if err := probe(probeCtx); err != nil {
		logf("pegasus: Costguard at %s is not reachable right now (%v). Continuing: messages are still stored, and extraction that fails is recovered later.", cfg.CostguardURL, err)
	} else {
		logf("pegasus: Costguard at %s is reachable", cfg.CostguardURL)
	}
	cancelProbe()

	triager := opts.Triager
	if triager == nil {
		triager = extraction.NewTriager(client)
	}
	extractor := opts.Extractor
	if extractor == nil {
		ex := extraction.NewExtractor(client)
		// The default logger quotes the rejected triple's subject/object,
		// which can be verbatim message content. Keep only the fact.
		ex.Logger = func(string, ...any) {
			logf("extraction: rejected a malformed triple (details suppressed: may contain message content)")
		}
		extractor = ex
	}

	// TODO(reflection): no consolidation orchestrator exists yet
	// (internal/reflection has the stages and Trigger, but nothing that
	// sequences dedup/decay/stats as Trigger's onConsolidate callback).
	// Once it does: build reflection.NewTrigger(cfg, orchestrator.Run),
	// Start it here with its own context, pass it as ActivityRecorder,
	// and stop it in shutdown step 4.
	logf("pegasus: reflection engine: not wired (no consolidation orchestrator yet)")

	// TODO(live-audio): no live AudioFetcher exists; it needs the live
	// source's media download (Whatsmeow). Until a source supplies one,
	// live voice notes are stored untranscribed.
	logf("pegasus: live voice transcription is disabled until a live source supplies an AudioFetcher")

	a := api.New(
		memory.NewEdgeStore(pool),
		memory.NewMessageStore(pool),
		memory.NewEntityStore(pool),
		memory.NewEmbeddingStore(pool),
		memory.NewRelationshipStatsStore(pool),
		client,
	).WithPipeline(api.PipelineDeps{
		Triager:          triager,
		Extractor:        extractor,
		ActivityRecorder: nil, // see TODO(reflection) above
		SelfExternalIDs:  cfg.SelfExternalIDs,
		Logger:           logf,
		Transcriber:      client,
		AudioFetcher:     nil, // see TODO(live-audio) above
		TranscriptionConfig: api.TranscriptionConfig{
			ConfidenceThreshold: cfg.TranscriptionConfidenceThreshold,
			OpenAIProviderHint:  cfg.TranscriptionOpenAIProviderHint,
		},
		// HonorCorrectionsOnImport deliberately unset: this process never imports.
		LiveWindowFlushAfter:        cfg.LiveWindowFlushAfter,
		LiveWindowFlushPollInterval: cfg.LiveWindowFlushPollInterval,
	})

	// --- Recovery snapshot: synchronously, BEFORE any intake starts, so a
	// live message can never be both in the snapshot and in a live window.
	var snapshot []*memory.Message
	if cfg.RecoverOnStart {
		// One indexed query; generously bounded so a wedged DB fails startup
		// instead of hanging it.
		snapCtx, cancelSnap := context.WithTimeout(ctx, cfg.DBConnectTimeout*4)
		snapshot, err = a.SnapshotUnprocessedLive(snapCtx)
		cancelSnap()
		if err != nil {
			sd.run()
			return fmt.Errorf("recovery snapshot: %w", err)
		}
	}

	// --- Live-window flusher: its own context, cancelled explicitly in
	// shutdown step 3 (never the signal context).
	flusherCtx, cancelFlusher := context.WithCancel(context.Background())
	sd.cancelFlusher = cancelFlusher
	sd.flushDone = a.StartLiveWindowFlusher(flusherCtx)

	// --- Health server ---
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		sd.run()
		return fmt.Errorf("health server: listen on %s: %w", cfg.HTTPAddr, err)
	}
	srv := newHTTPServer(cfg.HTTPAddr, newHealthHandler(&healthState{
		ping:           pool.Ping,
		migrationErr:   migErr,
		probeCostguard: probe,
	}))
	sd.srv = srv
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("pegasus: health server stopped unexpectedly: %v", err)
		}
	}()
	logf("pegasus: health server listening on %s (/healthz, /readyz)", ln.Addr())
	if opts.OnListening != nil {
		opts.OnListening(ln.Addr().String())
	}

	// --- Background recovery of the snapshot ---
	if cfg.RecoverOnStart {
		logf("pegasus: recovery: %d unprocessed message(s) with text in the startup snapshot; processing in the background", len(snapshot))
		recCtx, cancelRec := context.WithCancel(context.Background())
		recDone := make(chan struct{})
		sd.cancelRecovery, sd.recoveryDone = cancelRec, recDone
		go func() {
			defer close(recDone)
			stats, err := a.ProcessRecoveredMessages(recCtx, snapshot)
			if err != nil {
				logf("pegasus: recovery: %v", err)
				return
			}
			logf("pegasus: recovery finished: %d message(s) in %d window(s) recovered, %d error(s), %d without text skipped",
				stats.MessagesRecovered, stats.WindowsProcessed, stats.Errors, stats.SkippedNoText)
		}()
	} else {
		logf("pegasus: recovery on start is disabled (PEGASUS_RECOVER_ON_START=false)")
	}

	// --- Intake sources ---
	workCtx, cancelWork := context.WithCancel(context.Background())
	in := &intake{workCtx: workCtx, process: a.ProcessLiveMessage}
	sd.intake, sd.cancelWork = in, cancelWork
	if len(opts.Sources) == 0 {
		logf("pegasus: no live sources configured")
	}
	for _, src := range opts.Sources {
		g := in.newGate()
		sd.sources = append(sd.sources, startedSource{src: src, gate: g})
		if err := src.Start(workCtx, g.sink); err != nil {
			g.close()
			sd.sources = sd.sources[:len(sd.sources)-1]
			sd.run()
			return fmt.Errorf("start live source %q: %w", src.Name(), err)
		}
		logf("pegasus: live source %q started", src.Name())
	}

	logf("pegasus: ready")
	emit(EventReady)

	<-ctx.Done()
	logf("pegasus: shutdown requested, stopping (budget %s)", cfg.ShutdownTimeout)
	if err := sd.run(); err != nil {
		return err
	}
	logf("pegasus: shutdown complete")
	return nil
}

type startedSource struct {
	src  LiveSource
	gate *gate
}

// shutdownState holds whatever has been started so far; run() tears it
// down in the required order, skipping anything that never started.
type shutdownState struct {
	cfg  *Config
	logf func(string, ...any)
	emit func(string)

	srv            *http.Server
	sources        []startedSource
	intake         *intake
	cancelWork     context.CancelFunc
	cancelRecovery context.CancelFunc
	recoveryDone   <-chan struct{}
	cancelFlusher  context.CancelFunc
	flushDone      <-chan struct{}
	pool           *pgxpool.Pool
}

// run executes the shutdown order:
//
//  1. stop intake: health server Shutdown, then each source's Stop (and
//     close its sink gate right after, so late deliveries are rejected);
//  2. wait (bounded) for in-flight sink calls to drain; cancel the
//     recovery goroutine and wait for it;
//  3. cancel the flusher and WAIT for its final flush to finish;
//  4. stop the reflection trigger (not wired yet: nothing to do);
//  5. close the pool.
//
// The whole sequence is bounded by cfg.ShutdownTimeout. The final flush
// has its own internal 30s bound (api.ShutdownFlushTimeout), so the
// earlier steps get whatever remains after reserving that plus a small
// margin for the pool close.
func (s *shutdownState) run() error {
	deadline := time.Now().Add(s.cfg.ShutdownTimeout)
	overall, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	incomplete := false

	// Steps 1-2 may use everything except what step 3 (+ margin) needs.
	reserve := api.ShutdownFlushTimeout + 5*time.Second
	earlyDeadline := deadline.Add(-reserve)
	if time.Until(earlyDeadline) < time.Second {
		earlyDeadline = time.Now().Add(time.Second)
	}
	early, cancelEarly := context.WithDeadline(overall, earlyDeadline)
	defer cancelEarly()

	// 1. Stop intake.
	if s.srv != nil {
		hctx, hcancel := context.WithTimeout(early, httpShutdownBudget)
		if err := s.srv.Shutdown(hctx); err != nil {
			s.logf("pegasus: shutdown: health server: %v", err)
			_ = s.srv.Close()
		}
		hcancel()
		s.logf("pegasus: shutdown: health server stopped")
		s.emit(EventHTTPStopped)
	}
	for i := len(s.sources) - 1; i >= 0; i-- {
		ss := s.sources[i]
		if err := ss.src.Stop(early); err != nil {
			s.logf("pegasus: shutdown: stop live source %q: %v", ss.src.Name(), err)
			incomplete = incomplete || errors.Is(err, context.DeadlineExceeded)
		}
		ss.gate.close()
		s.logf("pegasus: shutdown: live source %q stopped", ss.src.Name())
		s.emit(EventSourceStopped + ss.src.Name())
	}

	// 2. Drain in-flight sink calls, then stop recovery.
	if s.intake != nil {
		if err := s.intake.drain(early); err != nil {
			incomplete = true
			s.logf("pegasus: shutdown: in-flight live messages did not finish in time; cancelling them. " +
				"Messages already stored stay processed=false and are recovered on next start; a message cancelled before it was stored must be redelivered by its source.")
			s.cancelWork()
			wctx, wcancel := context.WithTimeout(overall, 2*time.Second)
			_ = s.intake.drain(wctx)
			wcancel()
		}
		s.logf("pegasus: shutdown: live intake drained")
		s.emit(EventIntakeDrained)
	}
	if s.cancelWork != nil {
		defer s.cancelWork()
	}
	if s.cancelRecovery != nil {
		s.cancelRecovery()
		select {
		case <-s.recoveryDone:
		case <-early.Done():
			incomplete = true
			s.logf("pegasus: shutdown: recovery goroutine did not stop in time")
		}
		s.logf("pegasus: shutdown: recovery stopped (anything it did not finish stays processed=false for next start)")
		s.emit(EventRecoveryStopped)
	}

	// 3. Final live-window flush, and wait for it.
	if s.cancelFlusher != nil {
		s.logf("pegasus: shutdown: flushing open live windows (bounded by %s)", api.ShutdownFlushTimeout)
		s.cancelFlusher()
		select {
		case <-s.flushDone:
			s.logf("pegasus: shutdown: final live-window flush finished")
			s.emit(EventFlushDone)
		case <-overall.Done():
			incomplete = true
			s.logf("pegasus: shutdown: final live-window flush did not finish within the shutdown timeout; " +
				"its messages stay processed=false and are recovered on next start")
		}
	}

	// 4. Reflection trigger: not wired yet (see TODO(reflection) in Run).

	// 5. Close the pool. pgxpool.Close blocks until every acquired
	// connection is released, so bound it in case something above is
	// still holding one.
	if s.pool != nil {
		closed := make(chan struct{})
		go func() {
			s.pool.Close()
			close(closed)
		}()
		select {
		case <-closed:
			s.logf("pegasus: shutdown: database pool closed")
			s.emit(EventPoolClosed)
		case <-overall.Done():
			incomplete = true
			s.logf("pegasus: shutdown: database pool did not close within the shutdown timeout")
		}
	}

	if incomplete {
		return ErrShutdownIncomplete
	}
	return nil
}

func dbTarget(cfg *Config) string {
	return net.JoinHostPort(cfg.DBHost, strconv.Itoa(cfg.DBPort)) + "/" + cfg.DBName
}
