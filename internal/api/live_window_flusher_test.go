package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// --- Live-window idle flush ---
//
// fakeClock mirrors reflection.Trigger's own test clock (trigger_test.go)
// exactly — the real ticker inside StartLiveWindowFlusher still runs on
// real time (configured with a short real PollInterval in these tests),
// but what FlushIdleWindows COMPUTES as "idle for" comes from this,
// which the test controls directly via WithClock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// assertProcessedFlags queries every message in conversationID (ordered
// by timestamp) and fails the test if any of their processed flags
// don't match want.
func assertProcessedFlags(t *testing.T, ctx context.Context, ts *testStores, conversationID uuid.UUID, want bool) {
	t.Helper()
	rows, err := ts.pool.Query(ctx, `SELECT processed FROM messages WHERE conversation_id = $1`, conversationID)
	if err != nil {
		t.Fatalf("query processed flags: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var p bool
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan: %v", err)
		}
		n++
		if p != want {
			t.Errorf("expected processed=%v for a message in conversation %s, got %v", want, conversationID, p)
		}
	}
	if n == 0 {
		t.Fatalf("expected at least one message in conversation %s, found none", conversationID)
	}
}

func liveWindowExists(a *API, conversationID uuid.UUID) bool {
	a.liveWindowsMu.Lock()
	defer a.liveWindowsMu.Unlock()
	_, exists := a.liveWindows[conversationID]
	return exists
}

// sendLiveMessage sends one message through the live path and returns its
// external_id, so a test can later look up that exact row's processed flag
// (conversation-wide checks don't work once a conversation can hold a mix
// of flushed-and-processed and still-pending messages at once).
func sendLiveMessage(t *testing.T, ctx context.Context, a *API, conversationExternalID, senderExternalID string, ts time.Time) string {
	t.Helper()
	externalID := "msg-" + uuid.New().String()
	raw := ingestion.RawMessage{
		ExternalID: externalID, Platform: "whatsapp",
		ConversationID: conversationExternalID, SenderExternalID: senderExternalID,
		Timestamp: ts, MediaType: ingestion.MediaTypeText, Text: textPtr("message"),
	}
	if err := a.ProcessLiveMessage(ctx, raw); err != nil {
		t.Fatalf("ProcessLiveMessage: %v", err)
	}
	return externalID
}

// messageProcessed looks up a single message's processed flag by its
// external_id.
func messageProcessed(t *testing.T, ctx context.Context, ts *testStores, externalID string) bool {
	t.Helper()
	var processed bool
	if err := ts.pool.QueryRow(ctx, `SELECT processed FROM messages WHERE external_id = $1`, externalID).Scan(&processed); err != nil {
		t.Fatalf("query processed for external_id=%s: %v", externalID, err)
	}
	return processed
}

// conversationProcessedState reports whether every message currently
// stored for conversationID has processed=true, and how many rows exist.
func conversationProcessedState(ctx context.Context, ts *testStores, conversationID uuid.UUID) (allProcessed bool, count int, err error) {
	rows, err := ts.pool.Query(ctx, `SELECT processed FROM messages WHERE conversation_id = $1`, conversationID)
	if err != nil {
		return false, 0, err
	}
	defer rows.Close()
	allProcessed = true
	for rows.Next() {
		var p bool
		if err := rows.Scan(&p); err != nil {
			return false, 0, err
		}
		count++
		if !p {
			allProcessed = false
		}
	}
	return allProcessed, count, rows.Err()
}

// waitForAllProcessed polls until every message in conversationID has
// processed=true. Needed anywhere a goroutine-driven flush (the real
// StartLiveWindowFlusher, its shutdown path) is involved: a signaling
// extractor's channel send happens mid-pipeline (inside ExtractWindow),
// strictly BEFORE storeExtractedTriples/MarkProcessed run, so receiving
// from that channel is not proof the write has landed yet — asserting on
// the database immediately after would race the background goroutine
// that's still writing to it (and, worse, race the test's own
// t.Cleanup-scheduled pool.Close once the test function returns).
func waitForAllProcessed(t *testing.T, ctx context.Context, ts *testStores, conversationID uuid.UUID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		done, n, err := conversationProcessedState(ctx, ts, conversationID)
		if err != nil {
			t.Fatalf("query processed flags: %v", err)
		}
		if n > 0 && done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for every message in conversation %s to be processed", conversationID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestFlushIdleWindows_FlushesAfterThreshold covers requirement 12: two
// messages under both of Add's own bounds leave the window open
// (extractor not called, rows processed=false); advancing the clock
// past LiveWindowFlushAfter and calling FlushIdleWindows closes it —
// extractor called once with exactly that window, the edge stored,
// messages marked processed=true, and the map entry removed.
func TestFlushIdleWindows_FlushesAfterThreshold(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Flush Threshold Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "flush-conv-" + uuid.New().String()
	senderExternalID := "flush-sender-" + uuid.New().String()
	conversationID := resolveConversationID("whatsapp", conversationExternalID)

	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	if extractor.calls != 0 {
		t.Fatalf("expected extractor not called yet (window still open), got %d calls", extractor.calls)
	}
	assertProcessedFlags(t, ctx, ts, conversationID, false)
	if !liveWindowExists(a, conversationID) {
		t.Fatal("expected an open window for this conversation before any flush")
	}

	clock.Advance(6 * time.Minute)

	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}

	if extractor.calls != 1 {
		t.Errorf("expected extraction called exactly once, got %d", extractor.calls)
	}
	if len(extractor.lastWindow.Messages) != 2 {
		t.Errorf("expected the flushed window to carry exactly the 2 pending messages, got %d", len(extractor.lastWindow.Messages))
	}
	assertProcessedFlags(t, ctx, ts, conversationID, true)
	if liveWindowExists(a, conversationID) {
		t.Error("expected the map entry removed after flush")
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, "Flush Threshold Contact")
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d (err=%v)", len(edges), err)
	}
}

// TestFlushIdleWindows_NotFlushedBeforeThreshold covers requirement 14's
// first half: advancing the clock LESS than LiveWindowFlushAfter must
// not flush anything.
func TestFlushIdleWindows_NotFlushedBeforeThreshold(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "not-yet-conv-" + uuid.New().String()
	senderExternalID := "not-yet-sender-" + uuid.New().String()

	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	clock.Advance(4 * time.Minute) // under the 5-minute threshold

	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 0 {
		t.Errorf("expected no flush before the threshold, got %d extraction calls", extractor.calls)
	}

	conversationID := resolveConversationID("whatsapp", conversationExternalID)
	if !liveWindowExists(a, conversationID) {
		t.Error("expected the window to remain open")
	}
}

// TestFlushIdleWindows_NewMessageResetsIdleClock covers requirement 14's
// second half: a new message arriving resets lastArrival, so the
// ORIGINAL deadline (computed from the first message's own arrival)
// does not flush the window — only a fresh deadline computed from the
// SECOND message's arrival does.
func TestFlushIdleWindows_NewMessageResetsIdleClock(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Reset Idle Clock Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "reset-conv-" + uuid.New().String()
	senderExternalID := "reset-sender-" + uuid.New().String()

	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now()) // t0

	clock.Advance(4 * time.Minute) // t0+4min, still under threshold
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now()) // arrival resets lastArrival to t0+4min

	clock.Advance(4 * time.Minute) // "now" = t0+8min: past the ORIGINAL t0+5min deadline, but only 4min since the reset
	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 0 {
		t.Fatalf("expected no flush yet (only 4 minutes since the reset arrival), got %d calls", extractor.calls)
	}

	clock.Advance(2 * time.Minute) // now 6 minutes since the reset arrival -> past threshold
	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 1 {
		t.Errorf("expected exactly 1 flush once genuinely idle since the LAST arrival, got %d calls", extractor.calls)
	}
}

// TestFlushIdleWindows_ConversationsAreIndependent covers requirement
// 15: two conversations, only the genuinely idle one flushes.
func TestFlushIdleWindows_ConversationsAreIndependent(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Independent Conv Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	convAExternal := "indep-conv-a-" + uuid.New().String()
	convBExternal := "indep-conv-b-" + uuid.New().String()
	senderA := "indep-sender-a-" + uuid.New().String()
	senderB := "indep-sender-b-" + uuid.New().String()

	sendLiveMessage(t, ctx, a, convAExternal, senderA, clock.Now()) // A's last arrival: t0

	clock.Advance(4 * time.Minute)
	sendLiveMessage(t, ctx, a, convBExternal, senderB, clock.Now()) // B's last arrival: t0+4min

	clock.Advance(2 * time.Minute) // now = t0+6min: A idle 6min (past threshold), B idle 2min (not)
	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 1 {
		t.Fatalf("expected exactly 1 flush (conversation A only), got %d", extractor.calls)
	}

	convAID := resolveConversationID("whatsapp", convAExternal)
	convBID := resolveConversationID("whatsapp", convBExternal)
	if liveWindowExists(a, convAID) {
		t.Error("expected conversation A's window flushed (removed)")
	}
	if !liveWindowExists(a, convBID) {
		t.Error("expected conversation B's window to remain open (not yet idle)")
	}
}

// TestFlushIdleWindows_HistorySyncBurstNotFlushedPrematurely covers
// requirement 16: a burst of messages timestamped a year old, arriving
// within seconds of real/fake-clock time, must NOT be flushed just
// because their CONTENT timestamps look "expired" — idleness is judged
// purely by arrival time (clock.Now(), via lastArrival), never message
// Timestamp. Once genuinely idle (by arrival time), it flushes
// correctly.
func TestFlushIdleWindows_HistorySyncBurstNotFlushedPrematurely(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "History Sync Burst Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "history-burst-conv-" + uuid.New().String()
	senderExternalID := "history-burst-sender-" + uuid.New().String()

	// Message CONTENT timestamps are a year old, a minute apart —
	// within extraction.LiveWindower.Add's own 5-minute SPAN rule, so
	// Add itself never force-closes this window either. Real/fake
	// ARRIVAL time barely advances between sends (a few seconds), as a
	// real history-sync burst would deliver them.
	contentBase := time.Now().Add(-365 * 24 * time.Hour)
	for i := 0; i < 3; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "hist-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: senderExternalID,
			Timestamp: contentBase.Add(time.Duration(i) * time.Minute),
			MediaType: ingestion.MediaTypeText, Text: textPtr("old message"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
		clock.Advance(2 * time.Second) // arrival time barely moves
	}

	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 0 {
		t.Fatalf("expected the ancient-content burst NOT flushed prematurely (arrival time is still fresh), got %d calls", extractor.calls)
	}

	clock.Advance(6 * time.Minute) // NOW genuinely idle by arrival time
	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}
	if extractor.calls != 1 {
		t.Errorf("expected exactly 1 flush once genuinely idle by arrival time, got %d", extractor.calls)
	}
	if len(extractor.lastWindow.Messages) != 3 {
		t.Errorf("expected all 3 burst messages in the one flushed window, got %d", len(extractor.lastWindow.Messages))
	}
}

// signalingExtractor wraps a fakeExtractor-equivalent with a channel send
// on every call — used so goroutine-based tests (the real
// StartLiveWindowFlusher, shutdown) can wait for a specific extraction
// to have happened instead of polling or sleeping blindly.
type signalingExtractor struct {
	mu      sync.Mutex
	calls   int
	triples []extraction.ExtractedTriple
	signal  chan extraction.Window
}

func (f *signalingExtractor) ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	f.signal <- w
	return f.triples, nil
}

func (f *signalingExtractor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func waitForWindow(t *testing.T, ch <-chan extraction.Window, timeout time.Duration, msg string) extraction.Window {
	t.Helper()
	select {
	case w := <-ch:
		return w
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for: %s", msg)
		return extraction.Window{}
	}
}

// TestStartLiveWindowFlusher_FlushesViaRealGoroutine covers requirement
// 13: the production wiring entrypoint itself (a real ticker goroutine),
// not just the synchronous FlushIdleWindows method — a short real
// PollInterval, the fake clock advanced past the threshold, and a
// signaling extractor proving the flush actually happened through the
// goroutine.
func TestStartLiveWindowFlusher_FlushesViaRealGoroutine(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &signalingExtractor{
		triples: []extraction.ExtractedTriple{{Subject: "Real Flusher Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8}},
		signal:  make(chan extraction.Window, 1),
	}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{
			Triager: triager, Extractor: extractor,
			LiveWindowFlushAfter: 5 * time.Minute, LiveWindowFlushPollInterval: 10 * time.Millisecond,
		}).
		WithClock(clock.Now)

	conversationExternalID := "real-flusher-conv-" + uuid.New().String()
	senderExternalID := "real-flusher-sender-" + uuid.New().String()
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	flusherCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.StartLiveWindowFlusher(flusherCtx)

	clock.Advance(6 * time.Minute)

	w := waitForWindow(t, extractor.signal, 2*time.Second, "the real flusher goroutine to flush the idle window")
	if len(w.Messages) != 2 {
		t.Errorf("expected the flushed window to carry 2 messages, got %d", len(w.Messages))
	}
	if extractor.callCount() != 1 {
		t.Errorf("expected exactly 1 flush, got %d", extractor.callCount())
	}

	conversationID := resolveConversationID("whatsapp", conversationExternalID)
	waitForAllProcessed(t, ctx, ts, conversationID, 2*time.Second)
}

// TestStartLiveWindowFlusher_ShutdownFlushesAllOpenWindows covers
// requirement 18: cancelling the flusher's context must flush and
// process every still-open window immediately, regardless of idle time
// (LiveWindowFlushAfter is set deliberately long here, so nothing would
// idle-flush within the test's own real time budget — only the shutdown
// path can be what closes these windows).
func TestStartLiveWindowFlusher_ShutdownFlushesAllOpenWindows(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &signalingExtractor{
		triples: []extraction.ExtractedTriple{{Subject: "Shutdown Flush Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8}},
		signal:  make(chan extraction.Window, 2),
	}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{
			Triager: triager, Extractor: extractor,
			LiveWindowFlushAfter: time.Hour, LiveWindowFlushPollInterval: 10 * time.Millisecond,
		}).
		WithClock(clock.Now)

	convAExternal := "shutdown-conv-a-" + uuid.New().String()
	convBExternal := "shutdown-conv-b-" + uuid.New().String()
	senderExternalID := "shutdown-sender-" + uuid.New().String()
	sendLiveMessage(t, ctx, a, convAExternal, senderExternalID, clock.Now())
	sendLiveMessage(t, ctx, a, convBExternal, senderExternalID, clock.Now())

	flusherCtx, cancel := context.WithCancel(context.Background())
	a.StartLiveWindowFlusher(flusherCtx)

	// Neither window is remotely idle (LiveWindowFlushAfter = 1 hour) —
	// cancellation is the only thing that can close them here.
	cancel()

	waitForWindow(t, extractor.signal, 2*time.Second, "shutdown to flush the first window")
	waitForWindow(t, extractor.signal, 2*time.Second, "shutdown to flush the second window")

	if extractor.callCount() != 2 {
		t.Errorf("expected exactly 2 flushes (one per conversation) on shutdown, got %d", extractor.callCount())
	}

	convAID := resolveConversationID("whatsapp", convAExternal)
	convBID := resolveConversationID("whatsapp", convBExternal)
	waitForAllProcessed(t, ctx, ts, convAID, 2*time.Second)
	waitForAllProcessed(t, ctx, ts, convBID, 2*time.Second)
}

// TestFlushIdleWindows_ConcurrentArrivalDuringFlush covers requirement
// 17 (run with -race): a message arriving for the SAME conversation
// while a just-flushed window's extraction is still in flight must be
// neither lost nor double-processed. collectWindowsToFlush already
// deleted the old map entry (under lock) before the slow extraction
// call even starts, so a concurrent ProcessLiveMessage call safely
// starts a brand-new, independent window for that conversation — this
// test proves that holds under real concurrent execution, not just by
// reading the code.
func TestFlushIdleWindows_ConcurrentArrivalDuringFlush(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	release := make(chan struct{})
	blocking := &blockingExtractor{
		triples: []extraction.ExtractedTriple{{Subject: "Concurrent Flush Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8}},
		release: release,
	}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: blocking, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "concurrent-flush-conv-" + uuid.New().String()
	senderExternalID := "concurrent-flush-sender-" + uuid.New().String()
	conversationID := resolveConversationID("whatsapp", conversationExternalID)

	oldExternalID1 := sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	oldExternalID2 := sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	clock.Advance(6 * time.Minute)

	flushDone := make(chan error, 1)
	go func() { flushDone <- a.FlushIdleWindows(ctx) }()

	// Wait until the flush's extraction call has actually started
	// (blocking on release) before sending the concurrent message — this
	// is the window during which the old map entry is already gone (per
	// collectWindowsToFlush's own discipline: delete-then-release-lock)
	// but the old window's extraction hasn't finished.
	blocking.waitForCall(t, 2*time.Second)

	// Lost or double-processed are the two failure modes requirement 17
	// rules out. Neither is possible here by construction: the old map
	// entry is already gone, so this call can only start a brand-new,
	// independent window for the conversation — it can't be merged into
	// the in-flight flush, and it can't be dropped either.
	newExternalID := sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	close(release)
	if err := <-flushDone; err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}

	if !messageProcessed(t, ctx, ts, oldExternalID1) {
		t.Error("expected the first old message processed=true once the in-flight flush completed")
	}
	if !messageProcessed(t, ctx, ts, oldExternalID2) {
		t.Error("expected the second old message processed=true once the in-flight flush completed")
	}
	if messageProcessed(t, ctx, ts, newExternalID) {
		t.Error("expected the concurrently-arrived message to remain processed=false — it started a new, still-open window, not part of the window being flushed")
	}
	if !liveWindowExists(a, conversationID) {
		t.Error("expected a new open window for the conversation, started by the concurrently-arrived message")
	}
	if blocking.callCount() != 1 {
		t.Errorf("expected exactly 1 extraction call — the concurrently-arrived message's own window must not have been flushed too, got %d", blocking.callCount())
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, "Concurrent Flush Contact")
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge (not double-processed), got %d (err=%v)", len(edges), err)
	}
}

// blockingExtractor blocks inside ExtractWindow until release is closed
// — used to deterministically widen the window during which a flushed
// window's extraction is "in flight" for TestFlushIdleWindows_
// ConcurrentArrivalDuringFlush.
type blockingExtractor struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	triples []extraction.ExtractedTriple
	release chan struct{}
}

func (f *blockingExtractor) ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.mu.Lock()
	f.calls++
	if f.started == nil {
		f.started = make(chan struct{})
	}
	close(f.started)
	f.mu.Unlock()
	<-f.release
	return f.triples, nil
}

func (f *blockingExtractor) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *blockingExtractor) waitForCall(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		f.mu.Lock()
		started := f.started
		f.mu.Unlock()
		if started != nil {
			select {
			case <-started:
				return
			case <-deadline:
				t.Fatal("timed out waiting for the blocking extractor to be called")
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the blocking extractor to be called")
		case <-time.After(time.Millisecond):
		}
	}
}

// TestFlushIdleWindows_NeverCallsRecordActivity covers requirement 21's
// flusher half: FlushIdleWindows is housekeeping, not live activity —
// requirement 4. ProcessLiveMessage itself legitimately calls
// RecordActivity once per message (unrelated, expected); this asserts
// FlushIdleWindows adds nothing further to that count.
func TestFlushIdleWindows_NeverCallsRecordActivity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "No Activity Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	recorder := &fakeActivityRecorder{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{
			Triager: triager, Extractor: extractor, ActivityRecorder: recorder,
			LiveWindowFlushAfter: 5 * time.Minute,
		}).
		WithClock(clock.Now)

	conversationExternalID := "no-activity-conv-" + uuid.New().String()
	senderExternalID := "no-activity-sender-" + uuid.New().String()
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	callsBeforeFlush := recorder.calls
	if callsBeforeFlush != 2 {
		t.Fatalf("expected RecordActivity called twice by ProcessLiveMessage itself, got %d", callsBeforeFlush)
	}

	clock.Advance(6 * time.Minute)
	if err := a.FlushIdleWindows(ctx); err != nil {
		t.Fatalf("FlushIdleWindows: %v", err)
	}

	if recorder.calls != callsBeforeFlush {
		t.Errorf("expected FlushIdleWindows to never call RecordActivity, but count changed from %d to %d", callsBeforeFlush, recorder.calls)
	}
}
