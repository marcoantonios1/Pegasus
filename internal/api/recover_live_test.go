package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// failingExtractor always fails ExtractWindow — used to simulate a
// flush (or a recovery pass) that could not complete extraction, so
// messages stay processed=false for a later, successful recovery to
// pick up.
type failingExtractor struct {
	calls int
	err   error
}

func (f *failingExtractor) ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.calls++
	return nil, f.err
}

// TestFlushIdleWindows_FailingExtractorThenRecovery covers requirement
// 19: a flush whose extractor fails must leave the messages
// processed=false (error logged, no in-process retry — requirement 6),
// and a later RecoverUnprocessedLive run (with a working extractor) must
// still be able to recover and process them.
func TestFlushIdleWindows_FailingExtractorThenRecovery(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	clock := newFakeClock(time.Now())

	triager := &fakeTriager{candidate: true}
	failing := &failingExtractor{err: errors.New("extraction backend unavailable")}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: failing, LiveWindowFlushAfter: 5 * time.Minute}).
		WithClock(clock.Now)

	conversationExternalID := "failing-flush-conv-" + uuid.New().String()
	senderExternalID := "failing-flush-sender-" + uuid.New().String()
	conversationID := resolveConversationID("whatsapp", conversationExternalID)

	externalID1 := sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())
	externalID2 := sendLiveMessage(t, ctx, a, conversationExternalID, senderExternalID, clock.Now())

	clock.Advance(6 * time.Minute)
	if err := a.FlushIdleWindows(ctx); err == nil {
		t.Fatal("expected FlushIdleWindows to return the extractor's error")
	}

	if failing.calls != 1 {
		t.Errorf("expected exactly 1 extraction attempt, got %d", failing.calls)
	}
	if messageProcessed(t, ctx, ts, externalID1) {
		t.Error("expected the first message to remain processed=false after the failed flush")
	}
	if messageProcessed(t, ctx, ts, externalID2) {
		t.Error("expected the second message to remain processed=false after the failed flush")
	}
	// The window is gone from memory regardless of extraction failure
	// (requirement 6: "the window is gone from memory but its messages
	// remain processed=false and will be picked up by recovery").
	if liveWindowExists(a, conversationID) {
		t.Error("expected the in-memory window to be gone after the flush attempt, success or not")
	}

	// Now swap in a working extractor (WithPipeline is callable again to
	// replace dependencies) and recover.
	working := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Failing Extractor Recovery Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a.WithPipeline(PipelineDeps{Triager: triager, Extractor: working, LiveWindowFlushAfter: 5 * time.Minute})

	stats, err := a.RecoverUnprocessedLive(ctx)
	if err != nil {
		t.Fatalf("RecoverUnprocessedLive: %v", err)
	}
	if stats.MessagesRecovered != 2 {
		t.Errorf("expected 2 messages recovered, got %d", stats.MessagesRecovered)
	}
	if stats.WindowsProcessed != 1 {
		t.Errorf("expected 1 window processed, got %d", stats.WindowsProcessed)
	}
	if stats.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", stats.Errors)
	}

	if !messageProcessed(t, ctx, ts, externalID1) {
		t.Error("expected the first message processed=true after recovery")
	}
	if !messageProcessed(t, ctx, ts, externalID2) {
		t.Error("expected the second message processed=true after recovery")
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, "Failing Extractor Recovery Contact")
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d (err=%v)", len(edges), err)
	}
}

// TestRecoverUnprocessedLive covers requirement 20: seeded unprocessed
// text messages across two conversations, plus a processed message, a
// processed image row, and an unprocessed voice row with no transcript.
// Asserts: windows respect the time/size bounds, tails are flushed, rows
// get marked processed, the transcript-less voice row is left untouched
// and counted, a second call is a no-op, and a message that arrives
// AFTER the snapshot isn't touched by that same recovery run.
func TestRecoverUnprocessedLive(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Recovery Sweep Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor})

	senderA := createTestEntity(t, ctx, ts, "person", "Recovery Sender A "+uuid.New().String())
	senderB := createTestEntity(t, ctx, ts, "person", "Recovery Sender B "+uuid.New().String())

	convAID := uuid.New()
	convBID := uuid.New()

	base := time.Now().Add(-time.Hour)

	// Conversation A: 12 unprocessed text messages, one second apart —
	// MaxLiveWindowMessages=10 means this must close into 2 windows (10
	// + 2), both flushed by the unconditional tail-flush (recovery never
	// waits for idle — see recoverOneConversation's own doc comment).
	var convAExternalIDs []string
	for i := 0; i < 12; i++ {
		m := &memory.Message{
			ConversationID: convAID, SenderID: senderA.ID, MediaType: ingestion.MediaTypeText,
			RawText: textPtr("hello"), Timestamp: base.Add(time.Duration(i) * time.Second),
			ExternalID: textPtr("recov-a-" + uuid.New().String()),
		}
		if err := ts.messages.Create(ctx, m); err != nil {
			t.Fatalf("create conversation A message %d: %v", i, err)
		}
		convAExternalIDs = append(convAExternalIDs, *m.ExternalID)
	}

	// Conversation B: 2 unprocessed text messages — one window, flushed
	// as a tail.
	var convBExternalIDs []string
	for i := 0; i < 2; i++ {
		m := &memory.Message{
			ConversationID: convBID, SenderID: senderB.ID, MediaType: ingestion.MediaTypeText,
			RawText: textPtr("hi"), Timestamp: base.Add(time.Duration(i) * time.Second),
			ExternalID: textPtr("recov-b-" + uuid.New().String()),
		}
		if err := ts.messages.Create(ctx, m); err != nil {
			t.Fatalf("create conversation B message %d: %v", i, err)
		}
		convBExternalIDs = append(convBExternalIDs, *m.ExternalID)
	}

	// Already-processed text message in conversation B — must be
	// invisible to recovery (ListUnprocessedWithText filters on
	// processed=false) and untouched by it.
	processedExternalID := "recov-b-processed-" + uuid.New().String()
	processedMsg := &memory.Message{
		ConversationID: convBID, SenderID: senderB.ID, MediaType: ingestion.MediaTypeText,
		RawText: textPtr("already done"), Timestamp: base.Add(10 * time.Second), Processed: true,
		ExternalID: &processedExternalID,
	}
	if err := ts.messages.Create(ctx, processedMsg); err != nil {
		t.Fatalf("create already-processed message: %v", err)
	}

	// A processed=true image row — also invisible to recovery, same
	// reason.
	processedImageExternalID := "recov-b-image-" + uuid.New().String()
	processedImage := &memory.Message{
		ConversationID: convBID, SenderID: senderB.ID, MediaType: ingestion.MediaTypeImage,
		MediaRef: textPtr("s3://bucket/image.jpg"), Timestamp: base.Add(11 * time.Second), Processed: true,
		ExternalID: &processedImageExternalID,
	}
	if err := ts.messages.Create(ctx, processedImage); err != nil {
		t.Fatalf("create processed image message: %v", err)
	}

	// An unprocessed voice message with NO transcript (transcription
	// failed) — requirement 10: left untouched, counted in
	// RecoveryStats.SkippedNoText, never fed through a window.
	noTranscriptExternalID := "recov-b-voice-" + uuid.New().String()
	noTranscriptVoice := &memory.Message{
		ConversationID: convBID, SenderID: senderB.ID, MediaType: ingestion.MediaTypeVoice,
		MediaRef: textPtr("s3://bucket/voice.ogg"), Timestamp: base.Add(12 * time.Second), Processed: false,
		ExternalID: &noTranscriptExternalID,
	}
	if err := ts.messages.Create(ctx, noTranscriptVoice); err != nil {
		t.Fatalf("create no-transcript voice message: %v", err)
	}

	stats, err := a.RecoverUnprocessedLive(ctx)
	if err != nil {
		t.Fatalf("RecoverUnprocessedLive: %v", err)
	}

	if stats.MessagesRecovered != 14 {
		t.Errorf("expected 14 messages recovered (12 in conversation A + 2 in conversation B), got %d", stats.MessagesRecovered)
	}
	if stats.WindowsProcessed != 3 {
		t.Errorf("expected 3 windows processed (2 for conversation A's 12 messages + 1 for conversation B's 2), got %d", stats.WindowsProcessed)
	}
	if stats.SkippedNoText != 1 {
		t.Errorf("expected 1 message skipped for having no usable text, got %d", stats.SkippedNoText)
	}
	if stats.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", stats.Errors)
	}
	if extractor.calls != 3 {
		t.Errorf("expected extraction called 3 times (once per window), got %d", extractor.calls)
	}

	for i, externalID := range convAExternalIDs {
		if !messageProcessed(t, ctx, ts, externalID) {
			t.Errorf("expected conversation A message %d processed=true after recovery", i)
		}
	}
	for i, externalID := range convBExternalIDs {
		if !messageProcessed(t, ctx, ts, externalID) {
			t.Errorf("expected conversation B message %d processed=true after recovery", i)
		}
	}
	if !messageProcessed(t, ctx, ts, processedExternalID) {
		t.Error("expected the already-processed message to remain processed=true (untouched)")
	}
	if !messageProcessed(t, ctx, ts, processedImageExternalID) {
		t.Error("expected the processed image row to remain processed=true (untouched)")
	}
	if messageProcessed(t, ctx, ts, noTranscriptExternalID) {
		t.Error("expected the no-transcript voice message to remain processed=false (untouched, not retried)")
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, "Recovery Sweep Contact")
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil {
		t.Fatalf("GetBySubjectAndPredicate: %v", err)
	}
	if len(edges) != 3 {
		t.Fatalf("expected exactly 3 edges (one per window processed), got %d", len(edges))
	}

	// A message that "arrives" (is written to the table) after the first
	// recovery run's own snapshot was already taken must NOT be touched
	// by that run — it belongs to the NEXT recovery sweep, same as a live
	// message arriving during a production recovery's async processing
	// phase (requirement 9).
	lateExternalID := "recov-late-" + uuid.New().String()
	lateMsg := &memory.Message{
		ConversationID: convBID, SenderID: senderB.ID, MediaType: ingestion.MediaTypeText,
		RawText: textPtr("I arrived late"), Timestamp: base.Add(time.Hour), ExternalID: &lateExternalID,
	}
	if err := ts.messages.Create(ctx, lateMsg); err != nil {
		t.Fatalf("create late-arriving message: %v", err)
	}

	// Idempotency: a second call finds nothing from the FIRST batch (all
	// already processed=true) — only the late-arriving message is new
	// work.
	stats2, err := a.RecoverUnprocessedLive(ctx)
	if err != nil {
		t.Fatalf("second RecoverUnprocessedLive: %v", err)
	}
	if stats2.MessagesRecovered != 1 {
		t.Errorf("expected the second call to recover only the late-arriving message, got %d", stats2.MessagesRecovered)
	}
	if stats2.WindowsProcessed != 1 {
		t.Errorf("expected 1 window processed on the second call, got %d", stats2.WindowsProcessed)
	}
	if !messageProcessed(t, ctx, ts, lateExternalID) {
		t.Error("expected the late-arriving message processed=true after the second recovery call")
	}

	// A third call: truly nothing left anywhere.
	stats3, err := a.RecoverUnprocessedLive(ctx)
	if err != nil {
		t.Fatalf("third RecoverUnprocessedLive: %v", err)
	}
	if stats3.MessagesRecovered != 0 || stats3.WindowsProcessed != 0 {
		t.Errorf("expected the third call to be a complete no-op, got %+v", stats3)
	}
}

// TestRecoverUnprocessedLive_NeverCallsRecordActivity covers requirement
// 21's recovery half: recovery is housekeeping over historical rows, not
// live activity, and must never feed the Reflection Engine's idle
// detection.
func TestRecoverUnprocessedLive_NeverCallsRecordActivity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Recovery No Activity Contact", Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}}
	recorder := &fakeActivityRecorder{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).
		WithPipeline(PipelineDeps{Triager: triager, Extractor: extractor, ActivityRecorder: recorder})

	sender := createTestEntity(t, ctx, ts, "person", "Recovery No Activity Sender "+uuid.New().String())
	convID := uuid.New()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		m := &memory.Message{
			ConversationID: convID, SenderID: sender.ID, MediaType: ingestion.MediaTypeText,
			RawText: textPtr("hello"), Timestamp: base.Add(time.Duration(i) * time.Second),
			ExternalID: textPtr("recov-no-activity-" + uuid.New().String()),
		}
		if err := ts.messages.Create(ctx, m); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
	}

	if _, err := a.RecoverUnprocessedLive(ctx); err != nil {
		t.Fatalf("RecoverUnprocessedLive: %v", err)
	}

	if recorder.calls != 0 {
		t.Errorf("expected RecoverUnprocessedLive to never call RecordActivity, got %d calls", recorder.calls)
	}
}
