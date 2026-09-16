package api

import (
	"context"
	"os"
	"path/filepath"
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

	// --- Bulk path ---
	bulkExtractor := &fakeExtractor{triples: []extraction.ExtractedTriple{tripleTemplate("Bulk Contact")}}
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

	if err := bulkAPI.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}
	if bulkExtractor.calls != 1 {
		t.Fatalf("expected bulk extraction called exactly once for one 8-message window, got %d", bulkExtractor.calls)
	}

	// --- Live path, equivalent content ---
	liveExtractor := &fakeExtractor{triples: []extraction.ExtractedTriple{tripleTemplate("Live Contact")}}
	liveTriager := &fakeTriager{candidate: true}
	liveAPI := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: liveTriager, Extractor: liveExtractor,
	})

	conversationExternalID := "live-equiv-" + uuid.New().String()
	base := time.Now()
	for i := 0; i < extraction.DefaultHistoricalWindowSize; i++ {
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
	bulkSubject, err := ts.entities.GetByCanonicalName(ctx, "Bulk Contact")
	if err != nil || bulkSubject == nil {
		t.Fatalf("bulk subject entity not found: err=%v", err)
	}
	liveSubject, err := ts.entities.GetByCanonicalName(ctx, "Live Contact")
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
