package service

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// testConfig builds a Config pointing at the test database, following
// this repo's DB-test convention (internal/api/api_test.go's testDB):
// PEGASUS_TEST_DATABASE_URL, else a localhost DSN from POSTGRES_PASSWORD;
// skip when nothing is configured or reachable, but FAIL on an
// authentication error so a rotated password can't silently skip the
// suite. The database must already be migrated (`docker compose up -d
// postgres migrate`); these tests never alter the schema.
//
// Every config uses RecoverOnStart=false: the dev database holds other
// tests' unprocessed rows, which this suite must not touch.
func testConfig(t *testing.T) *Config {
	t.Helper()
	dsn := os.Getenv("PEGASUS_TEST_DATABASE_URL")
	if dsn == "" {
		if pw := os.Getenv("POSTGRES_PASSWORD"); pw != "" {
			dsn = "postgres://pegasus:" + pw + "@localhost:5432/pegasus?sslmode=disable"
		}
	}
	if dsn == "" {
		t.Skip("no test database configured — set PEGASUS_TEST_DATABASE_URL, or POSTGRES_PASSWORD (`set -a; source .env; set +a`), to enable this test")
	}

	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, pc)
	if err != nil {
		if isAuthFailure(err) {
			t.Fatalf("authentication failed connecting to the test database (%v) — the Postgres password was likely rotated; set PEGASUS_TEST_DATABASE_URL to the current DSN, or re-export POSTGRES_PASSWORD (`set -a; source .env; set +a`) before running go test", err)
		}
		t.Skipf("no test database available (%v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}
	conn.Close(ctx)

	sslmode := "disable"
	if u, err := url.Parse(dsn); err == nil && u.Query().Get("sslmode") != "" {
		sslmode = u.Query().Get("sslmode")
	}

	return &Config{
		DBHost: pc.Host, DBPort: int(pc.Port), DBName: pc.Database, DBUser: pc.User, DBPassword: pc.Password,
		DBSSLMode: sslmode, DBConnectTimeout: 5 * time.Second,

		// Nothing listens on port 1: the probe fails fast, and any
		// accidental real model call fails instead of reaching Costguard.
		CostguardURL: "http://127.0.0.1:1", CostguardAgent: "pegasus-test",
		CostguardTimeout: 2 * time.Second, CostguardTranscriptionRetryBackoff: time.Millisecond,

		TranscriptionConfidenceThreshold: -0.7, TranscriptionOpenAIProviderHint: "openai_primary",

		LiveWindowFlushAfter: time.Hour, LiveWindowFlushPollInterval: time.Hour,
		RecoverOnStart:  false,
		HTTPAddr:        "127.0.0.1:0",
		ShutdownTimeout: 45 * time.Second,
	}
}

func isAuthFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "28P01" {
		return true
	}
	return strings.Contains(err.Error(), "password authentication failed")
}

func waitStatus(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			last = resp.StatusCode
			resp.Body.Close()
			if last == want {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("GET %s: wanted %d within %s, last status %d", url, want, timeout, last)
}

// TestRun_Smoke: start Run in-process with no live source, wait for
// /readyz and /healthz, cancel, and check Run returns nil in time and has
// closed its pool.
func TestRun_Smoke(t *testing.T) {
	cfg := testConfig(t)

	addrCh := make(chan string, 1)
	var pool *pgxpool.Pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cfg, Options{
			Logf:        t.Logf,
			OnPool:      func(p *pgxpool.Pool) { pool = p },
			OnListening: func(a string) { addrCh <- a },
		})
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case err := <-errCh:
		t.Fatalf("Run exited during startup: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("health server never started")
	}

	waitStatus(t, "http://"+addr+"/readyz", 200, 5*time.Second)
	waitStatus(t, "http://"+addr+"/healthz", 200, 5*time.Second)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(cfg.ShutdownTimeout):
		t.Fatalf("Run did not return within the shutdown timeout %s", cfg.ShutdownTimeout)
	}

	if pool == nil {
		t.Fatal("OnPool was never called")
	}
	pingCtx, pcancel := context.WithTimeout(context.Background(), time.Second)
	defer pcancel()
	if err := pool.Ping(pingCtx); err == nil {
		t.Error("expected the pool to be closed after Run returned")
	}
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Error("expected the health server to be stopped after Run returned")
	}
}

// --- shutdown ordering ---

type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func indexOf(events []string, e string) int {
	for i, x := range events {
		if x == e {
			return i
		}
	}
	return -1
}

func lastIndexOf(events []string, e string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i] == e {
			return i
		}
	}
	return -1
}

type fakeSource struct {
	rec     *recorder
	sink    ingestion.Sink
	started chan struct{}
}

func (s *fakeSource) Name() string { return "fake" }
func (s *fakeSource) Start(_ context.Context, sink ingestion.Sink) error {
	s.sink = sink
	s.rec.add("source_start")
	close(s.started)
	return nil
}
func (s *fakeSource) Stop(context.Context) error {
	s.rec.add("source_stop")
	return nil
}

// slowTriager approves everything; the "slow" message blocks long enough
// that it is still in flight when shutdown starts.
type slowTriager struct {
	rec     *recorder
	entered chan struct{}
}

func (f *slowTriager) IsCandidate(_ context.Context, text string) (bool, error) {
	if text == "slow" {
		close(f.entered)
		time.Sleep(300 * time.Millisecond)
		f.rec.add("slow_triage_done")
	}
	return true, nil
}
func (f *slowTriager) IsWindowCandidate(context.Context, extraction.Window) (bool, error) {
	return true, nil
}

type recordingExtractor struct{ rec *recorder }

func (f *recordingExtractor) ExtractWindow(context.Context, extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.rec.add("extract")
	return nil, nil
}

// TestRun_ShutdownOrdering drives messages through a fake live source and
// checks the shutdown sequence: source Stop -> in-flight message drained
// -> final flush (extraction) -> flush done -> pool closed; that every
// accepted message ends processed=true; and that a delivery after Stop
// is rejected and never stored.
func TestRun_ShutdownOrdering(t *testing.T) {
	cfg := testConfig(t)
	rec := &recorder{}
	src := &fakeSource{rec: rec, started: make(chan struct{})}
	triager := &slowTriager{rec: rec, entered: make(chan struct{})}

	run := uuid.NewString()
	sender := "ordering-sender-" + run
	mkMsg := func(conv, text string) ingestion.RawMessage {
		return ingestion.RawMessage{
			ExternalID: "ordering-" + uuid.NewString(), Platform: "whatsapp",
			ConversationID: conv + "-" + run, SenderExternalID: sender,
			Timestamp: time.Now(), MediaType: ingestion.MediaTypeText, Text: &text,
		}
	}
	accepted := []ingestion.RawMessage{mkMsg("conv-a", "hello"), mkMsg("conv-a", "again"), mkMsg("conv-b", "hi")}
	slow := mkMsg("conv-b", "slow")
	late := mkMsg("conv-a", "too late")
	var lateErr error

	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cfg, Options{
			Sources:   []LiveSource{src},
			Triager:   triager,
			Extractor: &recordingExtractor{rec: rec},
			Logf:      t.Logf,
			OnEvent: func(e string) {
				rec.add(e)
				switch e {
				case EventReady:
					close(ready)
				case EventSourceStopped + "fake":
					// The source misbehaves and keeps delivering after Stop.
					lateErr = src.sink(late)
				}
			},
		})
	}()

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("Run exited during startup: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("service never became ready")
	}

	for _, m := range accepted {
		if err := src.sink(m); err != nil {
			t.Fatalf("sink: %v", err)
		}
	}
	slowErr := make(chan error, 1)
	go func() { slowErr <- src.sink(slow) }()
	<-triager.entered // the slow message is now in flight
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(cfg.ShutdownTimeout):
		t.Fatal("Run did not return within the shutdown timeout")
	}
	if err := <-slowErr; err != nil {
		t.Errorf("in-flight delivery should have completed, got %v", err)
	}
	if !errors.Is(lateErr, ErrIntakeClosed) {
		t.Errorf("delivery after Stop: got %v, want ErrIntakeClosed", lateErr)
	}

	ev := rec.snapshot()
	t.Logf("events: %v", ev)
	stop := indexOf(ev, "source_stop")
	slowDone := indexOf(ev, "slow_triage_done")
	drained := indexOf(ev, EventIntakeDrained)
	firstExtract := indexOf(ev, "extract")
	lastExtract := lastIndexOf(ev, "extract")
	flushDone := indexOf(ev, EventFlushDone)
	poolClosed := indexOf(ev, EventPoolClosed)
	for name, i := range map[string]int{"source_stop": stop, "slow_triage_done": slowDone, "intake_drained": drained, "extract": firstExtract, "flush_done": flushDone, "pool_closed": poolClosed} {
		if i < 0 {
			t.Fatalf("event %s never happened: %v", name, ev)
		}
	}
	if !(stop < drained && slowDone < drained) {
		t.Errorf("in-flight delivery must finish before intake is reported drained: %v", ev)
	}
	if !(drained < firstExtract) {
		t.Errorf("final flush must start only after Stop and drain: %v", ev)
	}
	if !(lastExtract < flushDone && flushDone < poolClosed) {
		t.Errorf("flush must finish before the pool closes: %v", ev)
	}
	if n := strings.Count(strings.Join(ev, ","), "extract"); n != 2 {
		t.Errorf("expected one final-flush extraction per conversation (2), got %d: %v", n, ev)
	}

	// Check with a separate connection: Run's pool is closed.
	conn, err := pgx.Connect(context.Background(), cfg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	for _, m := range append(accepted, slow) {
		var processed bool
		if err := conn.QueryRow(context.Background(), `SELECT processed FROM messages WHERE external_id = $1`, m.ExternalID).Scan(&processed); err != nil {
			t.Fatalf("message %s not found: %v", m.ExternalID, err)
		}
		if !processed {
			t.Errorf("message %s: processed=false after shutdown, want true", m.ExternalID)
		}
	}
	var lateCount int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM messages WHERE external_id = $1`, late.ExternalID).Scan(&lateCount); err != nil {
		t.Fatal(err)
	}
	if lateCount != 0 {
		t.Errorf("message delivered after Stop was stored (%d rows)", lateCount)
	}
}
