package api

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// fakeTriager returns a fixed verdict for both IsCandidate and
// IsWindowCandidate, regardless of input — lets tests control triage
// outcomes deterministically without a live Costguard/model call.
type fakeTriager struct {
	candidate bool
}

func (f *fakeTriager) IsCandidate(ctx context.Context, text string) (bool, error) {
	return f.candidate, nil
}
func (f *fakeTriager) IsWindowCandidate(ctx context.Context, w extraction.Window) (bool, error) {
	return f.candidate, nil
}

// fakeExtractor returns a fixed set of triples for ExtractWindow
// regardless of window content, and counts how many times it was called
// — lets tests assert exactly when/how often a window closed and was
// extracted, without depending on real model output.
type fakeExtractor struct {
	triples []extraction.ExtractedTriple
	calls   int
}

func (f *fakeExtractor) ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.calls++
	return f.triples, nil
}

// fakeActivityRecorder counts RecordActivity calls.
type fakeActivityRecorder struct {
	calls int
}

func (f *fakeActivityRecorder) RecordActivity() { f.calls++ }

func textPtr(s string) *string { return &s }

// TestProcessLiveMessage_WindowTriggersExtractionWithCorrectSourceWeight
// covers requirement 6: a sequence of messages through the full live
// path, asserting a window's worth of messages triggers extraction
// exactly once (not per-message), produces an edge with source_weight
// correctly applied, and that RecordActivity fires once per message.
func TestProcessLiveMessage_WindowTriggersExtractionWithCorrectSourceWeight(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Live Window Contact", Predicate: "likes", Object: "hiking", ObjectType: "literal", Confidence: 0.8},
	}}
	recorder := &fakeActivityRecorder{}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, ActivityRecorder: recorder,
	})

	conversationExternalID := "conv-" + uuid.New().String()
	senderExternalID := "sender-" + uuid.New().String()
	base := time.Now()

	// MaxLiveWindowMessages is 10 — the 11th message is what closes the
	// window (see extraction.LiveWindower.Add's own contract: the
	// triggering message starts the NEXT window, isn't part of the
	// completed one), so extraction should fire exactly once after the
	// 11th call, not before and not more than once.
	for i := 0; i < 11; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "msg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: senderExternalID,
			Timestamp: base.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeText, Text: textPtr("message"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}

	if extractor.calls != 1 {
		t.Errorf("expected extraction called exactly once for an 11-message burst, got %d calls", extractor.calls)
	}
	if recorder.calls != 11 {
		t.Errorf("expected RecordActivity called once per message (11), got %d", recorder.calls)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, "Live Window Contact")
	if err != nil || subject == nil {
		t.Fatalf("expected subject entity to have been created, err=%v subject=%v", err, subject)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil {
		t.Fatalf("GetBySubjectAndPredicate: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d", len(edges))
	}
	edge := edges[0]
	wantConfidence := 0.8 * memory.SourceWeights[memory.SourceTypeWhatsAppText]
	if edge.Confidence != wantConfidence {
		t.Errorf("expected confidence %v (0.8 * source_weight), got %v", wantConfidence, edge.Confidence)
	}
	if edge.SourceType != memory.SourceTypeWhatsAppText {
		t.Errorf("expected source_type %q, got %q", memory.SourceTypeWhatsAppText, edge.SourceType)
	}
	if edge.SourceWeight != memory.SourceWeights[memory.SourceTypeWhatsAppText] {
		t.Errorf("expected source_weight %v, got %v", memory.SourceWeights[memory.SourceTypeWhatsAppText], edge.SourceWeight)
	}
	if edge.Importance != DefaultExtractedImportance {
		t.Errorf("expected importance %v, got %v", DefaultExtractedImportance, edge.Importance)
	}

	// The first 10 messages' window closed and was marked processed; the
	// 11th started a new, still-open window and must NOT be marked
	// processed yet.
	convID := resolveConversationID("whatsapp", conversationExternalID)
	rows, err := ts.pool.Query(ctx, `SELECT processed FROM messages WHERE conversation_id = $1 ORDER BY timestamp ASC`, convID)
	if err != nil {
		t.Fatalf("query processed flags: %v", err)
	}
	defer rows.Close()
	var flags []bool
	for rows.Next() {
		var p bool
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("scan: %v", err)
		}
		flags = append(flags, p)
	}
	if len(flags) != 11 {
		t.Fatalf("expected 11 stored messages, got %d", len(flags))
	}
	for i := 0; i < 10; i++ {
		if !flags[i] {
			t.Errorf("expected message %d (in the closed window) to be processed=true", i)
		}
	}
	if flags[10] {
		t.Error("expected message 10 (the window-closing trigger, starting a new window) to still be processed=false")
	}
}

// TestImportWhatsApp_MatchesLivePathOutput covers requirement 7: bulk
// import and the live path, fed equivalent content through the SAME
// storeExtractedTriples function (different Subject names so the two
// runs don't collide via reinforcement, but otherwise identical triple
// shape), must produce edges with identical Confidence/SourceType/
// SourceWeight/Importance — proving by construction, not by comment, that
// both call the same underlying extraction/storage path.
func TestImportWhatsApp_MatchesLivePathOutput(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	tripleTemplate := func(subject string) extraction.ExtractedTriple {
		return extraction.ExtractedTriple{Subject: subject, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.77}
	}

	// Unique per test run, same reasoning as randomBasisIndex (api_test.go):
	// this suite runs against a real, persistent, shared local dev
	// database with no per-test reset. A fixed subject name would collide
	// with a leftover entity/edge from an earlier run of this same test,
	// turning "created" into "reinforced" and breaking the exact
	// Action assertions below.
	runID := uuid.New().String()
	bulkSubjectName := "Bulk Contact " + runID
	liveSubjectName := "Live Contact " + runID

	// --- Bulk path ---
	bulkExtractor := &fakeExtractor{triples: []extraction.ExtractedTriple{tripleTemplate(bulkSubjectName)}}
	bulkTriager := &fakeTriager{candidate: true}
	bulkAPI := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: bulkTriager, Extractor: bulkExtractor,
	})

	dir := t.TempDir()
	exportPath := filepath.Join(dir, "BulkConv.txt")
	var export string
	for i := 0; i < extraction.DefaultHistoricalWindowSize; i++ {
		export += "1/1/26, 9:0" + string(rune('0'+i)) + " AM - John: message " + string(rune('0'+i)) + "\n"
	}
	if err := os.WriteFile(exportPath, []byte(export), 0o644); err != nil {
		t.Fatalf("write export file: %v", err)
	}

	importResult, err := bulkAPI.ImportWhatsApp(ctx, exportPath)
	if err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}
	if bulkExtractor.calls != 1 {
		t.Fatalf("expected bulk extraction called exactly once for one 8-message window, got %d", bulkExtractor.calls)
	}
	if importResult.MessagesParsed != extraction.DefaultHistoricalWindowSize {
		t.Errorf("expected MessagesParsed=%d, got %d", extraction.DefaultHistoricalWindowSize, importResult.MessagesParsed)
	}
	if len(importResult.Windows) != 1 || !importResult.Windows[0].IsCandidate {
		t.Fatalf("expected exactly 1 candidate window in the result, got %+v", importResult.Windows)
	}
	if len(importResult.Windows[0].Outcomes) != 1 || importResult.Windows[0].Outcomes[0].Action != TripleActionCreated {
		t.Errorf("expected 1 created-triple outcome, got %+v", importResult.Windows[0].Outcomes)
	}

	// --- Live path, equivalent content ---
	liveExtractor := &fakeExtractor{triples: []extraction.ExtractedTriple{tripleTemplate(liveSubjectName)}}
	liveTriager := &fakeTriager{candidate: true}
	liveAPI := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: liveTriager, Extractor: liveExtractor,
	})

	// Live windowing closes on a DIFFERENT bound than historical
	// (extraction.MaxLiveWindowMessages = 10 messages or 5 minutes,
	// vs. historical's fixed extraction.DefaultHistoricalWindowSize = 8
	// — see extraction.LiveWindower's own doc comment for why live and
	// historical windowing deliberately differ). This test is comparing
	// the two paths' RESULTING EDGE shape, not their window sizes, so it
	// sends however many live messages actually close a live window
	// (11, same as TestProcessLiveMessage_WindowTriggersExtractionWithCorrectSourceWeight),
	// not extraction.DefaultHistoricalWindowSize.
	conversationExternalID := "live-equiv-" + uuid.New().String()
	base := time.Now()
	for i := 0; i < extraction.MaxLiveWindowMessages+1; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "msg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: "John",
			Timestamp: base.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeText, Text: textPtr("message"),
		}
		if err := liveAPI.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}
	if liveExtractor.calls != 1 {
		t.Fatalf("expected live extraction called exactly once for one 8-message window, got %d", liveExtractor.calls)
	}

	// --- Compare ---
	bulkSubject, err := ts.entities.GetByCanonicalName(ctx, bulkSubjectName)
	if err != nil || bulkSubject == nil {
		t.Fatalf("bulk subject entity not found: err=%v", err)
	}
	liveSubject, err := ts.entities.GetByCanonicalName(ctx, liveSubjectName)
	if err != nil || liveSubject == nil {
		t.Fatalf("live subject entity not found: err=%v", err)
	}

	bulkEdges, err := ts.edges.GetBySubjectAndPredicate(ctx, bulkSubject.ID, "likes")
	if err != nil || len(bulkEdges) != 1 {
		t.Fatalf("expected exactly 1 bulk edge, got %d (err=%v)", len(bulkEdges), err)
	}
	liveEdges, err := ts.edges.GetBySubjectAndPredicate(ctx, liveSubject.ID, "likes")
	if err != nil || len(liveEdges) != 1 {
		t.Fatalf("expected exactly 1 live edge, got %d (err=%v)", len(liveEdges), err)
	}

	bulkEdge, liveEdge := bulkEdges[0], liveEdges[0]
	if bulkEdge.Confidence != liveEdge.Confidence {
		t.Errorf("bulk confidence %v != live confidence %v", bulkEdge.Confidence, liveEdge.Confidence)
	}
	if bulkEdge.SourceType != liveEdge.SourceType {
		t.Errorf("bulk source_type %q != live source_type %q", bulkEdge.SourceType, liveEdge.SourceType)
	}
	if bulkEdge.SourceWeight != liveEdge.SourceWeight {
		t.Errorf("bulk source_weight %v != live source_weight %v", bulkEdge.SourceWeight, liveEdge.SourceWeight)
	}
	if bulkEdge.Importance != liveEdge.Importance {
		t.Errorf("bulk importance %v != live importance %v", bulkEdge.Importance, liveEdge.Importance)
	}
	if *bulkEdge.ObjectLiteral != *liveEdge.ObjectLiteral {
		t.Errorf("bulk object %q != live object %q", *bulkEdge.ObjectLiteral, *liveEdge.ObjectLiteral)
	}
}

func TestStoreMemory_RejectsInvalidPredicate(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Store Memory Subject")
	lit := "something"

	_, err := a.StoreMemory(ctx, memory.Edge{
		SubjectID: subject.ID, Predicate: "not_a_real_predicate", ObjectLiteral: &lit,
		Confidence: 0.5, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	})
	if err == nil {
		t.Fatal("expected an error for an invalid predicate, got nil")
	}
}

func TestStoreMemory_Succeeds(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Store Memory Success Subject")
	lit := "olives"

	edge, err := a.StoreMemory(ctx, memory.Edge{
		SubjectID: subject.ID, Predicate: "dislikes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	})
	if err != nil {
		t.Fatalf("StoreMemory: %v", err)
	}
	if edge.ID == uuid.Nil {
		t.Error("expected a non-zero edge ID after StoreMemory")
	}

	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if reloaded.Predicate != "dislikes" {
		t.Errorf("expected predicate %q, got %q", "dislikes", reloaded.Predicate)
	}
}

func TestUpdateMemory_UpdatesOnlyImportance(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Update Memory Subject")
	lit := "chess"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.87, Importance: 0.2, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	newImportance := 0.95
	updated, err := a.UpdateMemory(ctx, edge.ID, UpdateMemoryFields{Importance: &newImportance})
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	if updated.Importance != 0.95 {
		t.Errorf("expected importance 0.95, got %v", updated.Importance)
	}

	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if reloaded.Importance != 0.95 {
		t.Errorf("expected persisted importance 0.95, got %v", reloaded.Importance)
	}
	// Everything else must be untouched — UpdateMemory is importance-only
	// by construction (UpdateMemoryFields has no other settable field).
	if reloaded.Confidence != 0.87 {
		t.Errorf("expected confidence unchanged at 0.87, got %v", reloaded.Confidence)
	}
	if reloaded.Predicate != "likes" {
		t.Errorf("expected predicate unchanged, got %q", reloaded.Predicate)
	}
	if *reloaded.ObjectLiteral != "chess" {
		t.Errorf("expected object_literal unchanged, got %q", *reloaded.ObjectLiteral)
	}
}

// TestDeleteMemory_SoftDeleteBehavior covers requirement 9: the deleted
// edge remains queryable directly (GetByID, both old and tombstone), but
// is excluded from GetRelevantContext's default results.
func TestDeleteMemory_SoftDeleteBehavior(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Delete Memory Subject")
	lit := "an outdated fact"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.7, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	// Still directly queryable, and its own fields untouched beyond
	// superseded_by.
	oldReloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	if oldReloaded.SupersededBy == nil {
		t.Fatal("expected the deleted edge to have superseded_by set")
	}
	if oldReloaded.Confidence != 0.9 || oldReloaded.Importance != 0.7 {
		t.Errorf("expected old edge's own fields untouched, got confidence=%v importance=%v", oldReloaded.Confidence, oldReloaded.Importance)
	}

	tombstone, err := ts.edges.GetByID(ctx, *oldReloaded.SupersededBy)
	if err != nil {
		t.Fatalf("GetByID(tombstone): %v", err)
	}
	if tombstone.Confidence != 0 || tombstone.Importance != 0 {
		t.Errorf("expected tombstone confidence=0 importance=0, got confidence=%v importance=%v", tombstone.Confidence, tombstone.Importance)
	}

	// Excluded from GetRelevantContext's default results — both the old
	// (superseded) edge and the zero-value tombstone.
	results, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &subject.ID}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	assertContainsEdge(t, results, edge.ID, false)
	assertContainsEdge(t, results, tombstone.ID, false)
}

func TestDeleteMemory_AlreadyDeletedReturnsError(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Double Delete Subject")
	lit := "fact"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.7, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("first DeleteMemory: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err == nil {
		t.Error("expected an error deleting an already-deleted edge, got nil")
	}
}

// --- Embeddings wiring (closes the gap cmd/phase0_dryrun/NOTES.md flagged) ---

// TestStoreExtractedTriples_WritesEmbeddingForSourceMessage covers
// requirement 6: storeExtractedTriples must write exactly one embeddings
// row for its source message, with the correct message_id and vector.
func TestStoreExtractedTriples_WritesEmbeddingForSourceMessage(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Embed Test Subject "+uuid.New().String())
	sender := createTestEntity(t, ctx, ts, "person", "Embed Test Sender")
	text := "I love playing chess"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create test message: %v", err)
	}

	wantVector := basisVector(randomBasisIndex(), 1)
	embedder := &fakeEmbedder{vector: wantVector}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.9},
	}
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected 1 created outcome, got %+v", outcomes)
	}

	emb, err := ts.embeds.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil {
		t.Fatal("expected an embedding row for the source message, got none")
	}
	if emb.MessageID != msg.ID {
		t.Errorf("expected message_id %s, got %s", msg.ID, emb.MessageID)
	}
	if len(emb.Vector) != len(wantVector) || emb.Vector[0] != wantVector[0] {
		t.Errorf("expected vector matching what the embedder returned, got a %d-dim vector", len(emb.Vector))
	}
}

// TestStoreExtractedTriples_EmbedsMessageEvenWithZeroTriples covers
// requirement 1's own explicitly-named scenario: a message in a window
// triage flagged as a candidate, but extraction produced nothing from —
// a legitimate, normal outcome (§8.2's "[] if nothing extractable"), not
// an error. The message must still get embedded: semantic search over
// raw content doesn't care whether structured extraction found anything.
func TestStoreExtractedTriples_EmbedsMessageEvenWithZeroTriples(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Zero Triples Sender "+uuid.New().String())
	text := "something triage thought was worth a look, but extraction found nothing in"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create test message: %v", err)
	}

	embedder := &fakeEmbedder{vector: basisVector(randomBasisIndex(), 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	// The exact scenario named in requirement 1: ExtractWindow returned
	// an empty slice (extraction found nothing), not that
	// storeExtractedTriples was never called at all.
	outcomes, err := a.storeExtractedTriples(ctx, nil, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 0 {
		t.Fatalf("expected no triple outcomes for an empty triples slice, got %+v", outcomes)
	}

	emb, err := ts.embeds.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil {
		t.Error("expected the message to still be embedded despite zero extracted triples, got none")
	}
}

// TestStoreExtractedTriples_EmbeddingIsIdempotent covers requirement 7:
// calling storeExtractedTriples twice with an overlapping source message
// (simulating reinforcement/reprocessing) must not produce a second
// embeddings row, and must not call Embed a second time either.
func TestStoreExtractedTriples_EmbeddingIsIdempotent(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Idempotent Embed Subject "+uuid.New().String())
	sender := createTestEntity(t, ctx, ts, "person", "Idempotent Embed Sender")
	text := "I love playing chess"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create test message: %v", err)
	}

	embedder := &fakeEmbedder{vector: basisVector(randomBasisIndex(), 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.9},
	}

	if _, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now()); err != nil {
		t.Fatalf("first storeExtractedTriples: %v", err)
	}
	// Second call: same message, same triple — the triple itself will
	// reinforce (not create) the existing edge, but the point under test
	// is the embedding, which must not be duplicated either way.
	if _, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now()); err != nil {
		t.Fatalf("second storeExtractedTriples: %v", err)
	}

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, msg.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 embeddings row after two overlapping calls, got %d", rowCount)
	}
}

// TestStoreExtractedTriples_EmbeddingFailureLogsAndContinues covers
// requirement 8: the chosen failure behavior (log-and-continue, see
// embedMessages' own doc comment) — an Embedder that errors must NOT
// fail storeExtractedTriples or block the edges it already wrote, and
// the failure must be surfaced via the Logger hook, not silently
// dropped.
func TestStoreExtractedTriples_EmbeddingFailureLogsAndContinues(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Embed Failure Subject "+uuid.New().String())
	sender := createTestEntity(t, ctx, ts, "person", "Embed Failure Sender")
	text := "I love playing chess"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create test message: %v", err)
	}

	embedder := &fakeEmbedder{err: fmt.Errorf("simulated costguard embeddings outage")}
	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.9},
	}
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("expected storeExtractedTriples to succeed despite the embedding failure, got: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCreated || outcomes[0].Edge == nil {
		t.Fatalf("expected the triple's edge to still be created, got %+v", outcomes)
	}

	// The edge must be genuinely persisted and queryable, not just
	// present in the in-memory outcome.
	reloaded, err := ts.edges.GetByID(ctx, outcomes[0].Edge.ID)
	if err != nil {
		t.Fatalf("expected the edge to be persisted despite the embedding failure: %v", err)
	}
	if reloaded.Predicate != "likes" {
		t.Errorf("expected persisted edge predicate %q, got %q", "likes", reloaded.Predicate)
	}

	emb, err := ts.embeds.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb != nil {
		t.Error("expected no embedding row after a failed Embed call, got one")
	}

	found := false
	for _, line := range logLines {
		if strings.Contains(line, msg.ID.String()) && strings.Contains(line, "embed") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected the embedding failure to be logged with the message ID, got log lines: %v", logLines)
	}
}

// TestEmbeddingPipeline_WrittenEmbeddingIsRetrievable covers requirement
// 9: a message that goes through the REAL write path (ProcessLiveMessage,
// not hand-seeded fixture data) must produce an embedding that
// SearchSemantic and GetRelevantContext can actually retrieve — closing
// the loop cmd/phase0_dryrun/NOTES.md identified, where vector search was
// only ever exercised against pre-seeded rows, never pipeline output.
func TestEmbeddingPipeline_WrittenEmbeddingIsRetrievable(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	// Jittered value, not a bare basisVector(queryIndex, 1): this test
	// suite has run many times against this same persistent, shared dev
	// DB (see randomBasisIndex's own doc comment on why a fixed vector
	// value collides with leftover rows from earlier runs) — over enough
	// runs, an exact-1.0-valued vector at some index eventually gets
	// reused, and an exact-distance-0 tie with a leftover row could push
	// this test's own target message out of SearchSemantic's top N. A
	// small random jitter on the value makes an exact tie with any past
	// run's vector vanishingly unlikely while leaving this vector's
	// relationship to a genuinely different query vector unchanged.
	queryIndex := randomBasisIndex()
	queryValue := float32(1 - rand.Float64()*0.001)
	embedder := &fakeEmbedder{vector: basisVector(queryIndex, queryValue)}

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Retrieval Loop Contact", Predicate: "likes", Object: "sailing", ObjectType: "literal", Confidence: 0.9},
	}}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	conversationExternalID := "conv-" + uuid.New().String()
	senderExternalID := "sender-" + uuid.New().String()
	base := time.Now()
	for i := 0; i < 11; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "msg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: senderExternalID,
			Timestamp: base.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeText, Text: textPtr("a real message about sailing"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}

	// The window closes on the 11th call (index 10), taking messages 0-9
	// with it (see extraction.LiveWindower.Add's contract) — that's when
	// storeExtractedTriples (and its embedding step) actually runs, so
	// the message IDs are only meaningfully queryable AFTER the full
	// loop, not mid-loop. Message index 9 (the last one in the closed
	// window) is what gets embedded; message 10 starts a new, still-open
	// window and is not embedded yet.
	convID := resolveConversationID("whatsapp", conversationExternalID)
	rows, err := ts.pool.Query(ctx, `SELECT id FROM messages WHERE conversation_id = $1 ORDER BY timestamp ASC LIMIT 10`, convID)
	if err != nil {
		t.Fatalf("query message ids: %v", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 10 {
		t.Fatalf("expected 10 messages in the closed window, got %d", len(ids))
	}
	lastMsgID := ids[len(ids)-1]
	emb, err := ts.embeds.GetByMessageID(ctx, lastMsgID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil {
		t.Fatal("expected the real write path to have produced an embedding for this message, got none")
	}

	// SearchSemantic: the pipeline-written embedding must be findable by
	// meaning, not just present as a row. topN=20, not a smaller number
	// like 5: fakeEmbedder returns the identical vector regardless of
	// input text, so all 10 messages in this window's embeddings tie at
	// distance 0 to the query — a tight LIMIT would let Postgres'
	// arbitrary tie-break exclude lastMsgID even though it's a perfect
	// match, which isn't what this test is trying to prove.
	results, err := a.SearchSemantic(ctx, "anything (fakeEmbedder ignores query text)", 20)
	if err != nil {
		t.Fatalf("SearchSemantic: %v", err)
	}
	foundMsg := false
	for _, m := range results {
		if m.ID == lastMsgID {
			foundMsg = true
		}
	}
	if !foundMsg {
		t.Errorf("expected SearchSemantic to retrieve the pipeline-written message %s, got %d other results", lastMsgID, len(results))
	}

	// GetRelevantContext: the edge produced by the same pipeline run must
	// rank via REAL vector similarity through its source message's
	// pipeline-written embedding, not just appear via a SubjectID filter.
	subject, err := ts.entities.GetByCanonicalName(ctx, "Retrieval Loop Contact")
	if err != nil || subject == nil {
		t.Fatalf("expected subject entity to exist, err=%v", err)
	}
	relevant, err := a.GetRelevantContext(ctx, "a query about sailing", Filters{SubjectID: &subject.ID}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	if len(relevant) == 0 {
		t.Fatal("expected GetRelevantContext to return the edge produced by the real write path, got none")
	}
}
