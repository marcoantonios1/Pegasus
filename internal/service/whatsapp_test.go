package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp/wasource"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp/wasource/wasourcetest"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// These tests drive the real WhatsApp source (canonicalization, drops,
// EventHandler, sink) with a fake whatsmeow client against real Postgres,
// with fake triage/extraction: no network.

type waFixture struct {
	own     types.JID // device-suffixed own JID
	ownPN   types.JID
	contact types.JID
}

func newWAFixture() waFixture {
	digits := func() string { return fmt.Sprintf("1555%07d", rand.IntN(10_000_000)) }
	own := types.NewADJID(digits(), 0, 3)
	return waFixture{
		own:     own,
		ownPN:   own.ToNonAD(),
		contact: types.NewJID(digits(), types.DefaultUserServer),
	}
}

var textSeq int64
var textSeqMu sync.Mutex

func waText(chat, sender types.JID, fromMe bool, text string) *events.Message {
	textSeqMu.Lock()
	textSeq++
	n := textSeq
	textSeqMu.Unlock()
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: fromMe},
			ID:            types.MessageID(fmt.Sprintf("WA%s-%d", strings.ReplaceAll(uuid.NewString(), "-", "")[:12], n)),
			Timestamp:     time.Now(),
		},
		Message: &waE2E.Message{Conversation: proto.String(text)},
	}
}

// tripleExtractor returns one literal triple per window and records calls.
type tripleExtractor struct {
	rec     *recorder
	subject string
}

func (f *tripleExtractor) ExtractWindow(context.Context, extraction.Window) ([]extraction.ExtractedTriple, error) {
	if f.rec != nil {
		f.rec.add("extract")
	}
	return []extraction.ExtractedTriple{{Subject: f.subject, Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8}}, nil
}

type yesTriager struct{}

func (yesTriager) IsCandidate(context.Context, string) (bool, error) { return true, nil }
func (yesTriager) IsWindowCandidate(context.Context, extraction.Window) (bool, error) {
	return true, nil
}

func testWAConfig(t *testing.T) wasource.Config {
	return wasource.Config{
		Logf: t.Logf, ReconnectGrace: time.Millisecond,
		ReconnectBackoffBase: time.Millisecond, ReconnectBackoffMax: 5 * time.Millisecond,
		SinkRetryBackoff: time.Millisecond,
	}
}

type runHandle struct {
	addr   string
	cancel context.CancelFunc
	errCh  chan error
}

func startRun(t *testing.T, cfg *Config, opts Options) *runHandle {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrCh := make(chan string, 1)
	ready := make(chan struct{})
	prevEvent := opts.OnEvent
	opts.OnEvent = func(e string) {
		if prevEvent != nil {
			prevEvent(e)
		}
		if e == EventReady {
			close(ready)
		}
	}
	opts.OnListening = func(a string) { addrCh <- a }
	if opts.Logf == nil {
		opts.Logf = t.Logf
	}
	h := &runHandle{cancel: cancel, errCh: make(chan error, 1)}
	go func() { h.errCh <- Run(ctx, cfg, opts) }()
	select {
	case <-ready:
		h.addr = <-addrCh
	case err := <-h.errCh:
		cancel()
		t.Fatalf("Run exited during startup: %v", err)
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("service never became ready")
	}
	t.Cleanup(cancel)
	return h
}

func (h *runHandle) stop(t *testing.T, timeout time.Duration) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(timeout):
		t.Fatal("Run did not return within the shutdown timeout")
	}
}

func dbConn(t *testing.T, cfg *Config) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), cfg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func testPool(t *testing.T, cfg *Config) *pgxpool.Pool {
	t.Helper()
	pc, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		t.Fatal(err)
	}
	pc.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { return pgxvec.RegisterTypes(ctx, c) }
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type msgRow struct {
	id        uuid.UUID
	processed bool
	isSelf    bool
	sender    string
}

func loadMsg(t *testing.T, conn *pgx.Conn, externalID string) msgRow {
	t.Helper()
	var r msgRow
	err := conn.QueryRow(context.Background(), `
		SELECT m.id, m.processed, e.is_self, e.canonical_name
		FROM messages m JOIN entities e ON e.id = m.sender_id
		WHERE m.external_id = $1`, externalID).Scan(&r.id, &r.processed, &r.isSelf, &r.sender)
	if err != nil {
		t.Fatalf("message %s: %v", externalID, err)
	}
	return r
}

func edgesFor(t *testing.T, conn *pgx.Conn, ids []uuid.UUID) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM edges WHERE source_message_ids && $1`, ids).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestWhatsApp_MessagesInEdgesOut (18) plus the startup/shutdown ordering
// assertions (20) and the graceful-shutdown variant of (19).
func TestWhatsApp_MessagesInEdgesOut_Ordering(t *testing.T) {
	cfg := testConfig(t)
	cfg.RecoverOnStart = true
	fx := newWAFixture()
	rec := &recorder{}
	fc := wasourcetest.New(fx.own)
	fc.Record = func(e string) { rec.add("client_" + e) }
	src := wasource.NewWithClient(fc, testWAConfig(t))

	h := startRun(t, cfg, Options{
		Sources:        []LiveSource{src},
		Triager:        yesTriager{},
		Extractor:      &tripleExtractor{rec: rec, subject: "WA edge subject " + uuid.NewString()},
		OnEvent:        rec.add,
		RecoveryFilter: func(*memory.Message) bool { return false }, // never touch other tests' rows
	})

	contactDev := types.NewADJID(fx.contact.User, 0, 5)
	in1 := waText(fx.contact, contactDev, false, "hi from contact")
	mine := waText(fx.contact, types.NewADJID(fx.own.User, 0, 9), true, "hi back from me")
	in2 := waText(fx.contact, fx.contact, false, "how are you")
	for _, e := range []*events.Message{in1, mine, in2} {
		fc.Emit(e)
	}
	if got := src.Stats().Forwarded; got != 3 {
		t.Fatalf("forwarded %d, want 3 (%+v)", got, src.Stats())
	}

	h.stop(t, cfg.ShutdownTimeout)

	ev := rec.snapshot()
	t.Logf("events: %v", ev)
	order := func(a, b string) {
		t.Helper()
		ia, ib := indexOf(ev, a), indexOf(ev, b)
		if ia < 0 || ib < 0 || ia >= ib {
			t.Errorf("want %s before %s: %v", a, b, ev)
		}
	}
	order(EventSnapshotTaken, EventFlusherStarted)
	order(EventFlusherStarted, EventRecoveryStarted)
	order(EventRecoveryStarted, "client_add_handler")
	order("client_add_handler", "client_connect")
	order("client_disconnect", EventSourceStopped+"whatsapp")
	order(EventSourceStopped+"whatsapp", "extract")
	if lastIndexOf(ev, "extract") > indexOf(ev, EventFlushDone) {
		t.Errorf("extraction after flush_done: %v", ev)
	}
	order(EventFlushDone, EventPoolClosed)

	conn := dbConn(t, cfg)
	var ids []uuid.UUID
	for _, e := range []*events.Message{in1, mine, in2} {
		r := loadMsg(t, conn, string(e.Info.ID))
		ids = append(ids, r.id)
		if !r.processed {
			t.Errorf("message %s processed=false after graceful shutdown", e.Info.ID)
		}
	}
	if r := loadMsg(t, conn, string(mine.Info.ID)); !r.isSelf {
		t.Errorf("own message attributed to %q (is_self=false), want the self entity", r.sender)
	}
	for _, e := range []*events.Message{in1, in2} {
		if r := loadMsg(t, conn, string(e.Info.ID)); r.isSelf || r.sender != fx.contact.String() {
			t.Errorf("incoming message sender = %q (is_self=%v), want canonical %s", r.sender, r.isSelf, fx.contact)
		}
	}
	if n := edgesFor(t, conn, ids); n == 0 {
		t.Error("no edges produced from the live messages")
	}
}

// TestWhatsApp_RestartMidWindow (19): two messages leave a window open;
// the process "crashes" (no shutdown flush); a fresh API recovers them.
func TestWhatsApp_RestartMidWindow(t *testing.T) {
	cfg := testConfig(t)
	pool := testPool(t, cfg)
	fx := newWAFixture()
	newAPI := func() *api.API {
		return api.New(memory.NewEdgeStore(pool), memory.NewMessageStore(pool), memory.NewEntityStore(pool),
			memory.NewEmbeddingStore(pool), memory.NewRelationshipStatsStore(pool), nil).
			WithPipeline(api.PipelineDeps{
				Triager: yesTriager{}, Extractor: &tripleExtractor{subject: "WA restart subject " + uuid.NewString()},
				SelfExternalIDs: []string{fx.ownPN.String()}, Logger: t.Logf,
				LiveWindowFlushAfter: time.Hour, LiveWindowFlushPollInterval: time.Hour,
			})
	}

	a1 := newAPI()
	fc := wasourcetest.New(fx.own)
	src := wasource.NewWithClient(fc, testWAConfig(t))
	ctx := context.Background()
	if err := src.Start(ctx, func(m ingestion.RawMessage) error { return a1.ProcessLiveMessage(ctx, m) }); err != nil {
		t.Fatal(err)
	}
	m1 := waText(fx.contact, fx.contact, false, "first")
	m2 := waText(fx.contact, fx.own, true, "second")
	fc.Emit(m1)
	fc.Emit(m2)

	conn := dbConn(t, cfg)
	for _, e := range []*events.Message{m1, m2} {
		if loadMsg(t, conn, string(e.Info.ID)).processed {
			t.Fatal("window should still be open (message already processed)")
		}
	}
	// Crash: a1 and src are abandoned without Stop or a shutdown flush.

	a2 := newAPI()
	snap, err := a2.SnapshotUnprocessedLive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r1, r2 := loadMsg(t, conn, string(m1.Info.ID)), loadMsg(t, conn, string(m2.Info.ID))
	var ours []*memory.Message
	for _, m := range snap {
		if m.ID == r1.id || m.ID == r2.id {
			ours = append(ours, m)
		}
	}
	if len(ours) != 2 {
		t.Fatalf("snapshot holds %d of our 2 messages", len(ours))
	}
	stats, err := a2.ProcessRecoveredMessages(ctx, ours)
	if err != nil || stats.MessagesRecovered != 2 {
		t.Fatalf("recovery: %+v, %v", stats, err)
	}
	for _, e := range []*events.Message{m1, m2} {
		if !loadMsg(t, conn, string(e.Info.ID)).processed {
			t.Errorf("message %s not processed after recovery", e.Info.ID)
		}
	}
	if edgesFor(t, conn, []uuid.UUID{r1.id, r2.id}) == 0 {
		t.Error("no edge after recovery")
	}
}

// TestWhatsApp_FatalStateFailsReadiness (23).
func TestWhatsApp_FatalStateFailsReadiness(t *testing.T) {
	for name, evt := range map[string]any{
		"logged_out":      &events.LoggedOut{Reason: events.ConnectFailureLoggedOut},
		"stream_replaced": &events.StreamReplaced{},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			fx := newWAFixture()
			fc := wasourcetest.New(fx.own)
			src := wasource.NewWithClient(fc, testWAConfig(t))
			h := startRun(t, cfg, Options{Sources: []LiveSource{src}, Triager: yesTriager{}, Extractor: &tripleExtractor{subject: "x"}})

			code, body := getJSON(t, "http://"+h.addr+"/readyz")
			if code != 200 {
				t.Fatalf("readyz before fatal: %d %s", code, body)
			}

			fc.Emit(evt)
			fc.Emit(&events.Disconnected{})
			time.Sleep(50 * time.Millisecond)

			code, body = getJSON(t, "http://"+h.addr+"/readyz")
			if code != 503 || !strings.Contains(body, name) {
				t.Errorf("readyz after %s: %d %s", name, code, body)
			}
			var parsed readyResponse
			_ = json.Unmarshal([]byte(body), &parsed)
			if parsed.Checks["whatsapp"].Status != "fail" || parsed.Checks["database"].Status != "ok" {
				t.Errorf("checks: %+v", parsed.Checks)
			}
			if code, _ := getJSON(t, "http://"+h.addr+"/healthz"); code != 200 {
				t.Errorf("healthz = %d: the process must stay up", code)
			}
			select {
			case err := <-h.errCh:
				t.Fatalf("Run exited after a fatal source state: %v", err)
			default:
			}
			if n := fc.ConnectCount(); n != 1 {
				t.Errorf("reconnect attempts after fatal state: connects=%d", n)
			}
			h.stop(t, cfg.ShutdownTimeout)
		})
	}
}

func getJSON(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestWhatsApp_SinkRetryIdempotent (24): the first attempt stores the
// message and then fails; the retry must not create a second row.
func TestWhatsApp_SinkRetryIdempotent(t *testing.T) {
	cfg := testConfig(t)
	pool := testPool(t, cfg)
	fx := newWAFixture()
	a := api.New(memory.NewEdgeStore(pool), memory.NewMessageStore(pool), memory.NewEntityStore(pool),
		memory.NewEmbeddingStore(pool), memory.NewRelationshipStatsStore(pool), nil).
		WithPipeline(api.PipelineDeps{Triager: yesTriager{}, Extractor: &tripleExtractor{subject: "x"}, Logger: t.Logf,
			LiveWindowFlushAfter: time.Hour, LiveWindowFlushPollInterval: time.Hour})

	fc := wasourcetest.New(fx.own)
	src := wasource.NewWithClient(fc, testWAConfig(t))
	ctx := context.Background()
	calls := 0
	if err := src.Start(ctx, func(m ingestion.RawMessage) error {
		calls++
		err := a.ProcessLiveMessage(ctx, m)
		if calls == 1 {
			return errors.New("transient failure after storing")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e := waText(fx.contact, fx.contact, false, "retry me")
	fc.Emit(e)

	conn := dbConn(t, cfg)
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM messages WHERE external_id = $1`, string(e.Info.ID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || n != 1 {
		t.Errorf("sink calls %d (want 2), rows %d (want exactly 1)", calls, n)
	}
	if st := src.Stats(); st.Forwarded != 1 || st.SinkRetries != 1 || st.SinkFailed != 0 {
		t.Errorf("stats %+v", st)
	}
	_ = src.Stop(ctx)
}
