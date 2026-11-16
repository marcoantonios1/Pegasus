package wasource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
)

// Source states, reported in /readyz.
const (
	StateIdle         = "idle"
	StateConnecting   = "connecting"
	StateConnected    = "connected"
	StateDisconnected = "disconnected"
	StateFailed       = "failed"
	StateStopped      = "stopped"
)

// Config configures a Source. Zero durations/counts use the defaults below.
type Config struct {
	SessionPath          string
	LogLevel             string // whatsmeow's own log level: DEBUG, INFO, WARN, ERROR
	ReconnectBackoffBase time.Duration
	ReconnectBackoffMax  time.Duration

	// ReconnectGrace is how long whatsmeow's built-in auto-reconnect gets
	// after a disconnect before the supervisor starts its own attempts.
	ReconnectGrace time.Duration
	// SinkAttempts is how many times one message is offered to the sink
	// before it is counted sink_failed (ingestion is idempotent on
	// external_id, so retrying is safe).
	SinkAttempts     int
	SinkRetryBackoff time.Duration
	// SummaryInterval: how often a stats summary is logged.
	SummaryInterval time.Duration
	// LongDisconnectWarn: while disconnected longer than this, warn this often.
	LongDisconnectWarn time.Duration

	Logf func(format string, args ...any)
}

const (
	DefaultReconnectBackoffBase = 2 * time.Second
	DefaultReconnectBackoffMax  = 5 * time.Minute
	DefaultLogLevel             = "INFO"
	defaultReconnectGrace       = 30 * time.Second
	defaultSinkAttempts         = 3
	defaultSinkRetryBackoff     = time.Second
	defaultSummaryInterval      = 15 * time.Minute
	defaultLongDisconnectWarn   = 10 * time.Minute
)

func (c Config) withDefaults() Config {
	if c.ReconnectBackoffBase <= 0 {
		c.ReconnectBackoffBase = DefaultReconnectBackoffBase
	}
	if c.ReconnectBackoffMax <= 0 {
		c.ReconnectBackoffMax = DefaultReconnectBackoffMax
	}
	if c.ReconnectGrace <= 0 {
		c.ReconnectGrace = defaultReconnectGrace
	}
	if c.SinkAttempts <= 0 {
		c.SinkAttempts = defaultSinkAttempts
	}
	if c.SinkRetryBackoff <= 0 {
		c.SinkRetryBackoff = defaultSinkRetryBackoff
	}
	if c.SummaryInterval <= 0 {
		c.SummaryInterval = defaultSummaryInterval
	}
	if c.LongDisconnectWarn <= 0 {
		c.LongDisconnectWarn = defaultLongDisconnectWarn
	}
	if c.LogLevel == "" {
		c.LogLevel = DefaultLogLevel
	}
	if c.Logf == nil {
		c.Logf = log.Printf
	}
	return c
}

// errSinkFailed is what deliver returns to the EventHandler once retries
// are exhausted: deliberately content-free (the underlying error can carry
// a sender JID), so the handler's own error line can't leak one.
var errSinkFailed = errors.New("live sink failed after retries (see the sink_failed log line)")

// Source is the WhatsApp LiveSource (internal/service.LiveSource).
type Source struct {
	cfg     Config
	client  Client
	store   io.Closer // session store; nil when built with a fake client
	stats   *Stats
	canon   *canonicalizer
	handler *whatsapp.EventHandler
	logf    func(string, ...any)

	// after and jitter are swappable for tests.
	after  func(time.Duration) <-chan time.Time
	jitter func() float64
	now    func() time.Time

	sink    ingestion.Sink
	workCtx context.Context

	tracker tracker
	kick    chan struct{}

	bgCancel context.CancelFunc
	bgDone   sync.WaitGroup

	mu                sync.Mutex
	state             string
	failReason        string
	connectedSince    time.Time
	disconnectedSince time.Time
	everConnected     bool
	reconnects        int64 // successful re-connections after the first connect
	connectAttempts   int64 // supervisor-initiated Connect calls
}

// Open opens the session store at cfg.SessionPath, loads the paired device
// and builds a whatsmeow client. It does not register handlers or
// connect; Start does. Returns an error wrapping ErrNotPaired if the store
// has no paired device: the service never pairs.
func Open(ctx context.Context, cfg Config) (*Source, error) {
	cfg = cfg.withDefaults()
	path, err := ResolveSessionPath(cfg.SessionPath)
	if err != nil {
		return nil, err
	}
	wlog := waLog.Stdout("whatsmeow", cfg.LogLevel, false)
	container, err := OpenStore(ctx, path, wlog.Sub("Database"))
	if err != nil {
		return nil, err
	}
	dev, err := pairedDevice(ctx, container)
	if err != nil {
		_ = container.Close()
		if errors.Is(err, ErrNotPaired) {
			return nil, fmt.Errorf("%w (session %s): %s", ErrNotPaired, path, PairCommandHint)
		}
		return nil, err
	}
	cli := whatsmeow.NewClient(dev, wlog.Sub("Client"))
	cli.EnableAutoReconnect = true
	s := NewWithClient(whatsmeowClient{cli}, cfg)
	s.store = container
	return s, nil
}

// NewWithClient builds a Source around any Client (tests pass a fake).
func NewWithClient(c Client, cfg Config) *Source {
	cfg = cfg.withDefaults()
	stats := newStats()
	s := &Source{
		cfg:    cfg,
		client: c,
		stats:  stats,
		logf:   cfg.Logf,
		after:  time.After,
		jitter: rand.Float64,
		now:    time.Now,
		kick:   make(chan struct{}, 1),
		state:  StateIdle,
	}
	s.canon = &canonicalizer{self: c.OwnJID().ToNonAD(), pnFor: c.PNForLID, stats: stats}
	return s
}

func (s *Source) Name() string { return "whatsapp" }

// Stats exposes the counters (tests and health).
func (s *Source) Stats() StatsSnapshot { return s.stats.Snapshot() }

// SelfExternalIDs returns the account's own canonical JID. The service
// merges it into the pipeline's SelfExternalIDs so the IsFromMe sender
// rewrite in canonicalizer.apply matches resolveSender exactly.
func (s *Source) SelfExternalIDs() []string {
	if s.canon.self.IsEmpty() {
		return nil
	}
	return []string{s.canon.self.String()}
}

// Start registers the event handler, then connects. A failed initial
// connect is treated as transient (the supervisor keeps retrying); only a
// fatal state stops intake.
func (s *Source) Start(ctx context.Context, sink ingestion.Sink) error {
	s.sink = sink
	s.workCtx = ctx
	s.handler = whatsapp.NewEventHandler(s.deliver)
	s.handler.Logger = s.logf

	bgCtx, cancel := context.WithCancel(context.Background())
	s.bgCancel = cancel
	s.bgDone.Add(2)
	go func() { defer s.bgDone.Done(); s.supervise(bgCtx) }()
	go func() { defer s.bgDone.Done(); s.summarize(bgCtx) }()

	s.client.AddEventHandler(s.onEvent)
	s.setState(StateConnecting)
	s.logf("whatsapp: connecting as %s", whatsapp.RedactJID(s.canon.self))
	if err := s.client.Connect(); err != nil {
		s.logf("whatsapp: initial connect failed (%v); will keep retrying", err)
		s.markDisconnected()
	}
	return nil
}

// Stop disconnects, then waits for every in-flight handler call (and so
// every in-flight sink call) to finish, bounded by ctx. Events arriving
// after that are dropped and counted.
func (s *Source) Stop(ctx context.Context) error {
	if s.bgCancel != nil {
		s.bgCancel()
		s.bgDone.Wait()
	}
	s.client.Disconnect()
	// Drain, close to new calls, then drain again: anything that slipped in
	// between the first wait and close() still finishes before Stop returns.
	err := s.tracker.wait(ctx)
	s.tracker.close()
	if err == nil {
		err = s.tracker.wait(ctx)
	}

	s.mu.Lock()
	if s.state != StateFailed {
		s.state = StateStopped
	}
	s.mu.Unlock()
	s.logSummary("shutdown")
	if err != nil {
		return fmt.Errorf("whatsapp: in-flight handler calls did not drain: %w", err)
	}
	return nil
}

// Close releases the session store. Call after Stop.
func (s *Source) Close() error {
	if s.store == nil {
		return nil
	}
	return s.store.Close()
}

// Health reports readiness for /readyz: only a fatal state makes the
// source (and so the service) unready; transient disconnects never do.
func (s *Source) Health() (ready bool, reason string, info any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := map[string]any{
		"state":            s.state,
		"reconnects":       s.reconnects,
		"connect_attempts": s.connectAttempts,
		"stats":            s.stats.Snapshot(),
	}
	if s.state == StateConnected {
		h["connected_since"] = s.connectedSince.UTC().Format(time.RFC3339)
	}
	if s.state == StateFailed {
		h["failure"] = s.failReason
		return false, "whatsapp: " + s.failReason, h
	}
	return true, "", h
}

// --- events ---

func (s *Source) onEvent(evt any) {
	if !s.tracker.enter() {
		if _, ok := evt.(*events.Message); ok {
			s.stats.drop(DropAfterStop)
		}
		return
	}
	defer s.tracker.leave()

	switch e := evt.(type) {
	case *events.Message:
		s.handleMessage(e)
		return
	case *events.HistorySync:
		s.stats.drop(DropHistorySync)
		return
	}

	s.stats.otherEvent(eventTypeName(evt))
	switch e := evt.(type) {
	case *events.Connected:
		s.onConnected()
	case *events.Disconnected:
		s.markDisconnected()
	case *events.LoggedOut:
		s.fail("logged_out", fmt.Sprintf("the linked device was logged out or revoked (%v). Delete the session file and re-pair: %s", e.Reason, PairCommandHint))
	case *events.StreamReplaced:
		s.fail("stream_replaced", "another client connected with this same session (two processes share one session file, e.g. a `pair` run while the service was up). Stop the other process, then restart the service")
	case *events.ClientOutdated:
		s.fail("client_outdated", "WhatsApp rejected this whatsmeow version as outdated. Update go.mau.fi/whatsmeow, rebuild and restart")
	case *events.TemporaryBan:
		s.fail("temporary_ban", fmt.Sprintf("the account is temporarily banned (%v). Wait for it to expire, then restart the service", e.Code))
	case *events.ConnectFailure:
		if isFatalConnectFailure(e.Reason) {
			s.fail("connect_failure", fmt.Sprintf("WhatsApp refused the connection (%v). Re-pair if this persists: %s", e.Reason, PairCommandHint))
		} else {
			s.logf("whatsapp: transient connect failure (%v)", e.Reason)
		}
	}
}

func eventTypeName(evt any) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", evt), "*events.")
}

// isFatalConnectFailure: authentication/account-level refusals. Anything
// else (generic, 5xx, ...) is left to reconnect.
func isFatalConnectFailure(r events.ConnectFailureReason) bool {
	if r.IsLoggedOut() {
		return true
	}
	switch r {
	case events.ConnectFailureTempBanned, events.ConnectFailureClientOutdated, events.ConnectFailureBadUserAgent,
		events.ConnectFailureCATExpired, events.ConnectFailureCATInvalid:
		return true
	}
	return false
}

// dropReason returns why evt is deliberately not forwarded, or "".
func dropReason(evt *events.Message) string {
	chat := evt.Info.Chat
	switch {
	case chat.Server == types.GroupServer:
		return DropGroup
	case chat.Server == types.BroadcastServer && chat.User == types.StatusBroadcastJID.User:
		return DropStatusBroadcast
	case chat.Server == types.BroadcastServer:
		return DropBroadcastList
	case chat.Server == types.NewsletterServer:
		return DropNewsletter
	case evt.IsEdit:
		return DropEdit
	}
	mediaType, ok := whatsapp.MediaTypeOf(evt.Message)
	if !ok {
		return DropUnsupportedType
	}
	if mediaType == ingestion.MediaTypeVoice {
		// TODO(live-voice): transcribing live voice needs an AudioFetcher
		// built on client.Download, keyed by the original message (the
		// RawMessage only carries the encrypted media URL). Until that
		// follow-up exists, forwarding voice would leave rows that can
		// never be processed, so drop it.
		return DropVoiceLive
	}
	return ""
}

func (s *Source) handleMessage(evt *events.Message) {
	if reason := dropReason(evt); reason != "" {
		s.stats.drop(reason)
		return
	}
	s.handler.HandleMessage(s.canon.apply(s.workCtx, evt))
}

// deliver is the EventHandler's sink: it calls the service sink (which
// runs api.ProcessLiveMessage synchronously with a per-call timeout)
// and retries a bounded number of times. There is no in-process queue:
// a message is durable only once ProcessLiveMessage has stored it.
func (s *Source) deliver(raw ingestion.RawMessage) error {
	attempts := s.cfg.SinkAttempts
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		last = s.sink(raw)
		if last == nil {
			s.stats.add(&s.stats.forwarded)
			return nil
		}
		if errors.Is(last, ingestion.ErrSinkClosed) || attempt == attempts {
			break
		}
		s.stats.add(&s.stats.sinkRetries)
		if !s.wait(s.workCtx, s.cfg.SinkRetryBackoff<<(attempt-1)) {
			break
		}
	}
	s.stats.add(&s.stats.sinkFailed)
	kind := "error"
	switch {
	case errors.Is(last, ingestion.ErrSinkClosed):
		kind = "intake closed"
	case errors.Is(last, context.DeadlineExceeded):
		kind = "timeout"
	}
	// Message ID only: the error itself can carry a sender JID.
	s.logf("whatsapp: message id=%s not ingested (%s; sink_failed)", raw.ExternalID, kind)
	return errSinkFailed
}

// --- connection state and supervision ---

func (s *Source) setState(st string) {
	s.mu.Lock()
	if s.state != StateFailed {
		s.state = st
	}
	s.mu.Unlock()
}

func (s *Source) onConnected() {
	s.mu.Lock()
	if s.state == StateFailed || s.state == StateStopped {
		s.mu.Unlock()
		return
	}
	reconnect := s.everConnected
	if reconnect {
		s.reconnects++
	}
	var downFor time.Duration
	if !s.disconnectedSince.IsZero() {
		downFor = s.now().Sub(s.disconnectedSince)
	}
	s.everConnected = true
	s.state = StateConnected
	s.connectedSince = s.now()
	s.disconnectedSince = time.Time{}
	s.mu.Unlock()
	if reconnect {
		s.logf("whatsapp: reconnected after %s", downFor.Round(time.Second))
	} else {
		s.logf("whatsapp: connected")
	}
}

func (s *Source) markDisconnected() {
	s.mu.Lock()
	if s.state == StateFailed || s.state == StateStopped {
		s.mu.Unlock()
		return
	}
	s.state = StateDisconnected
	if s.disconnectedSince.IsZero() {
		s.disconnectedSince = s.now()
	}
	s.mu.Unlock()
	s.logf("whatsapp: disconnected; waiting %s for auto-reconnect before supervising", s.cfg.ReconnectGrace)
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// fail records a fatal state once, stops all reconnecting and logs one
// loud error. The process stays up; /readyz turns 503.
func (s *Source) fail(state, action string) {
	s.mu.Lock()
	if s.state == StateFailed {
		s.mu.Unlock()
		return
	}
	s.state = StateFailed
	s.failReason = state
	s.mu.Unlock()
	s.client.DisableAutoReconnect()
	s.logf("whatsapp: ERROR: fatal state %q: %s. Live intake has stopped; the service stays up (open windows still flush, recovery completes) but /readyz reports not ready until this is fixed and the service restarted.", state, action)
}

func (s *Source) needsReconnect() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == StateDisconnected || s.state == StateConnecting
}

// wait sleeps d unless ctx ends first; false means ctx ended.
func (s *Source) wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-s.after(d):
		return true
	}
}

// backoff is capped exponential with ±20% jitter.
func (s *Source) backoff(attempt int) time.Duration {
	d := s.cfg.ReconnectBackoffBase
	for i := 0; i < attempt && d < s.cfg.ReconnectBackoffMax; i++ {
		d *= 2
	}
	if d > s.cfg.ReconnectBackoffMax {
		d = s.cfg.ReconnectBackoffMax
	}
	return time.Duration(float64(d) * (0.8 + 0.4*s.jitter()))
}

// supervise backs up whatsmeow's own auto-reconnect: after each disconnect
// it waits ReconnectGrace, then (if still disconnected and not failed)
// calls Connect with capped exponential backoff, indefinitely.
func (s *Source) supervise(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.kick:
		}
		if !s.wait(ctx, s.cfg.ReconnectGrace) {
			return
		}
		var lastWarn time.Time
		for attempt := 0; s.needsReconnect(); attempt++ {
			s.mu.Lock()
			s.connectAttempts++
			n := s.connectAttempts
			since := s.disconnectedSince
			s.mu.Unlock()

			if down := s.now().Sub(since); !since.IsZero() && down >= s.cfg.LongDisconnectWarn && s.now().Sub(lastWarn) >= s.cfg.LongDisconnectWarn {
				lastWarn = s.now()
				s.logf("whatsapp: WARNING: disconnected for %s; still retrying", down.Round(time.Second))
			}
			s.logf("whatsapp: reconnect attempt %d", n)
			if err := s.client.Connect(); err != nil && !errors.Is(err, whatsmeow.ErrAlreadyConnected) {
				s.logf("whatsapp: reconnect attempt %d failed: %v", n, err)
			}
			if !s.wait(ctx, s.backoff(attempt)) {
				return
			}
		}
	}
}

func (s *Source) summarize(ctx context.Context) {
	for {
		if !s.wait(ctx, s.cfg.SummaryInterval) {
			return
		}
		s.logSummary("periodic")
	}
}

func (s *Source) logSummary(kind string) {
	s.mu.Lock()
	state, reconnects, attempts := s.state, s.reconnects, s.connectAttempts
	s.mu.Unlock()
	s.logf("whatsapp: %s summary: state=%s reconnects=%d connect_attempts=%d %s", kind, state, reconnects, attempts, s.stats.Snapshot())
}

// tracker counts in-flight handler calls and can be closed to new ones.
type tracker struct {
	mu     sync.Mutex
	closed bool
	n      int
	idle   chan struct{}
}

func (t *tracker) enter() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.n++
	return true
}

func (t *tracker) leave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n--
	if t.n == 0 && t.idle != nil {
		close(t.idle)
		t.idle = nil
	}
}

func (t *tracker) wait(ctx context.Context) error {
	t.mu.Lock()
	if t.n == 0 {
		t.mu.Unlock()
		return nil
	}
	if t.idle == nil {
		t.idle = make(chan struct{})
	}
	ch := t.idle
	t.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *tracker) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}
