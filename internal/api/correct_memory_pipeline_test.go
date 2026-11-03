package api

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Issue 32: CorrectMemory never applying from the extraction pipeline ---
//
// These tests exercise selectCorrectionTarget (internal/api/pipeline.go)
// through the real pipeline (storeExtractedTriples, ProcessLiveMessage,
// ImportWhatsApp) against real Postgres — the path that shipped with NO
// pipeline-level coverage at all, which is how the original bug (every
// correction silently skipped) went unnoticed. Only the extractor is
// faked throughout; everything from storeExtractedTriples down is real.

// resolveTestSelfEntity resolves (or creates, idempotently) the shared
// is_self entity via the real production path (resolveSender), the same
// pattern correction_gate_test.go already established for this exact
// constraint (entities_single_self_idx: at most one is_self=true row,
// ever, in this suite's real, persistent, shared dev database).
func resolveTestSelfEntity(t *testing.T, ctx context.Context, ts *testStores, selfExternalID string) *memory.Entity {
	t.Helper()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		SelfExternalIDs: []string{selfExternalID},
	})
	self, err := a.resolveSender(ctx, selfExternalID)
	if err != nil {
		t.Fatalf("resolve self entity: %v", err)
	}
	return self
}

// TestProcessLiveMessage_CorrectionEndToEnd covers requirement 7: a real
// live window containing a genuine self correction ("Dubai" -> "Beirut")
// must actually supersede the old edge, not silently skip it (Issue
// 32's bug). Asserts the full real-world shape: superseded_by linkage,
// the new edge's hardcoded correction fields, the TripleOutcome, and
// both read paths (GetRelevantContext excludes the old edge,
// GetRelationshipHistory includes both).
func TestProcessLiveMessage_CorrectionEndToEnd(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "live-correction-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	existingLit := "Dubai"
	existingEdge := &memory.Edge{
		SubjectID: self.ID, Predicate: "event_present", ObjectLiteral: &existingLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, existingEdge); err != nil {
		t.Fatalf("create existing edge: %v", err)
	}

	triager := &fakeTriager{candidate: true}
	oldValue := "Dubai"
	correctionIdx := 9 // last message in the window that closes (see below)
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "event_present", Object: "Beirut", ObjectType: "literal",
			Confidence: 0.95, IsCorrection: true, CorrectionSourceIndex: &correctionIdx, ReplacesObject: &oldValue},
	}}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, SelfExternalIDs: []string{selfExternalID},
	})

	conversationExternalID := "live-correction-conv-" + uuid.New().String()
	base := time.Now()
	for i := 0; i < 11; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "lcmsg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: selfExternalID,
			Timestamp: base.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeText, Text: textPtr("actually I moved to Beirut, not Dubai"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}

	if extractor.calls != 1 {
		t.Fatalf("expected extraction called exactly once, got %d", extractor.calls)
	}

	reloadedExisting, err := ts.edges.GetByID(ctx, existingEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(existingEdge): %v", err)
	}
	if reloadedExisting.SupersededBy == nil {
		t.Fatal("expected the old Dubai edge to be superseded — Issue 32's bug would leave this nil")
	}

	newEdge, err := ts.edges.GetByID(ctx, *reloadedExisting.SupersededBy)
	if err != nil {
		t.Fatalf("GetByID(new edge): %v", err)
	}
	if newEdge.ObjectLiteral == nil || *newEdge.ObjectLiteral != "Beirut" {
		t.Errorf("expected the new edge's object to be %q, got %v", "Beirut", newEdge.ObjectLiteral)
	}
	if newEdge.Confidence != 1.0 {
		t.Errorf("expected confidence=1.0, got %v", newEdge.Confidence)
	}
	if !newEdge.DecayLocked {
		t.Error("expected decay_locked=true")
	}
	if !newEdge.IsCorrection {
		t.Error("expected is_correction=true")
	}
	if newEdge.SourceType != memory.SourceTypeUserCorrection {
		t.Errorf("expected source_type=%q, got %q", memory.SourceTypeUserCorrection, newEdge.SourceType)
	}

	// Default retrieval: the new edge, not the old.
	relevant, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &self.ID}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	assertContainsEdge(t, relevant, newEdge.ID, true)
	assertContainsEdge(t, relevant, existingEdge.ID, false)

	// History: both.
	history, err := a.GetRelationshipHistory(ctx, self.ID)
	if err != nil {
		t.Fatalf("GetRelationshipHistory: %v", err)
	}
	assertContainsEdge(t, history, newEdge.ID, true)
	assertContainsEdge(t, history, existingEdge.ID, true)
}

// TestStoreExtractedTriples_MultiValuedDisambiguation covers requirement
// 8: two live event_present edges exist for the same subject (a
// multi-valued predicate) — one naming the old value, one unrelated. A
// correction naming the old value via ReplacesObject must supersede ONLY
// the matching edge; the unrelated one is untouched.
func TestStoreExtractedTriples_MultiValuedDisambiguation(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "disambig-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	dubaiLit := "living in Dubai"
	dubaiEdge := &memory.Edge{
		SubjectID: self.ID, Predicate: "event_present", ObjectLiteral: &dubaiLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, dubaiEdge); err != nil {
		t.Fatalf("create dubai edge: %v", err)
	}
	unrelatedLit := "working on a new project"
	unrelatedEdge := &memory.Edge{
		SubjectID: self.ID, Predicate: "event_present", ObjectLiteral: &unrelatedLit,
		Confidence: 0.7, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, unrelatedEdge); err != nil {
		t.Fatalf("create unrelated edge: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	oldValue := "living in Dubai"
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "event_present", Object: "living in Beirut", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &oldValue},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCorrected {
		t.Fatalf("expected the disambiguated correction to supersede, got %+v", outcomes)
	}

	reloadedDubai, err := ts.edges.GetByID(ctx, dubaiEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(dubaiEdge): %v", err)
	}
	if reloadedDubai.SupersededBy == nil {
		t.Error("expected the Dubai edge specifically to be superseded")
	}

	reloadedUnrelated, err := ts.edges.GetByID(ctx, unrelatedEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(unrelatedEdge): %v", err)
	}
	if reloadedUnrelated.SupersededBy != nil {
		t.Error("expected the unrelated edge to remain untouched, but it was superseded")
	}
}

// TestStoreExtractedTriples_AmbiguousWithoutReplacesObject covers
// requirement 9's first half: two live candidates, no ReplacesObject —
// nothing superseded, no new edge, TripleActionSkippedAmbiguousCorrectionTarget,
// candidate IDs logged.
func TestStoreExtractedTriples_AmbiguousWithoutReplacesObject(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "ambiguous-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	lit1, lit2 := "option A", "option B"
	edge1 := &memory.Edge{SubjectID: self.ID, Predicate: "event_present", ObjectLiteral: &lit1,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	edge2 := &memory.Edge{SubjectID: self.ID, Predicate: "event_present", ObjectLiteral: &lit2,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, edge1); err != nil {
		t.Fatalf("create edge1: %v", err)
	}
	if err := ts.edges.Create(ctx, edge2); err != nil {
		t.Fatalf("create edge2: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "event_present", Object: "option C", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx}, // no ReplacesObject
	}

	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedAmbiguousCorrectionTarget {
		t.Fatalf("expected an ambiguous skip, got %+v", outcomes)
	}
	if outcomes[0].Edge != nil {
		t.Error("expected no edge on an ambiguous skip")
	}

	for _, e := range []*memory.Edge{edge1, edge2} {
		reloaded, err := ts.edges.GetByID(ctx, e.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if reloaded.SupersededBy != nil {
			t.Errorf("expected edge %s to remain untouched on ambiguity, but it was superseded", e.ID)
		}
	}

	found := false
	for _, l := range logLines {
		if strings.Contains(l, "ambiguous") && (strings.Contains(l, edge1.ID.String()) || strings.Contains(l, edge2.ID.String())) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a log line naming the ambiguous candidate edge IDs, got: %v", logLines)
	}
}

// TestStoreExtractedTriples_ReplacesObjectMatchingNoneIsSkipped covers
// requirement 9's second half: ReplacesObject is stated but matches
// none of the live candidates — skipped, not a guess.
func TestStoreExtractedTriples_ReplacesObjectMatchingNoneIsSkipped(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "no-match-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	existingLit := "April"
	existingEdge := &memory.Edge{SubjectID: self.ID, Predicate: "has_birthday", ObjectLiteral: &existingLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, existingEdge); err != nil {
		t.Fatalf("create existing edge: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	wrongOld := "December" // doesn't match the existing edge's "April"
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "March", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &wrongOld},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedNoCorrectionTarget {
		t.Fatalf("expected a skip when replaces_object matches nothing, got %+v", outcomes)
	}

	reloaded, err := ts.edges.GetByID(ctx, existingEdge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if reloaded.SupersededBy != nil {
		t.Error("expected the existing edge to remain untouched")
	}
}

// TestStoreExtractedTriples_ZeroLiveCandidatesIsSkipped covers
// requirement 10: no live edges at all for this subject+predicate
// (including the case where the only edge is already superseded) —
// skipped, not an error.
func TestStoreExtractedTriples_ZeroLiveCandidatesIsSkipped(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "zero-candidates-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)
	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())

	idx := 0
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "March", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedNoCorrectionTarget {
		t.Fatalf("expected a skip with zero live candidates, got %+v", outcomes)
	}
}

// TestStoreExtractedTriples_ZeroLiveCandidatesWhenOnlyEdgeAlreadySuperseded
// covers requirement 10's explicit second case: the only edge for this
// subject+predicate exists but is already superseded — must behave
// exactly like zero candidates, not find it via FindLiveEdgesBySubjectAndPredicate.
func TestStoreExtractedTriples_ZeroLiveCandidatesWhenOnlyEdgeAlreadySuperseded(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "already-superseded-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	oldLit := "April"
	oldEdge := &memory.Edge{SubjectID: self.ID, Predicate: "has_birthday", ObjectLiteral: &oldLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, oldEdge); err != nil {
		t.Fatalf("create old edge: %v", err)
	}
	// Supersede it directly, so the ONLY edge for this subject+predicate
	// is already non-live.
	if _, err := ts.edges.CorrectMemory(ctx, oldEdge.ID, memory.CorrectionInput{
		SubjectID: self.ID, Predicate: "has_birthday", ObjectLiteral: strPtrLocal("March"),
	}); err != nil {
		t.Fatalf("pre-supersede old edge: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "May", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: strPtrLocal("March")},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	// March edge IS live (it's the one that superseded oldEdge) — this
	// should actually find and supersede it, proving the query correctly
	// EXCLUDES the already-superseded "April" edge from consideration
	// rather than tripping over it.
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCorrected {
		t.Fatalf("expected the live March edge to be found and superseded (April correctly excluded as non-live), got %+v", outcomes)
	}
}

func strPtrLocal(s string) *string { return &s }

// TestStoreExtractedTriples_LiveEdgeAlreadyHoldingValueIsReinforced
// covers requirement 11: the live edge already holds the triple's new
// value — reinforced, not duplicated, not superseded.
func TestStoreExtractedTriples_LiveEdgeAlreadyHoldingValueIsReinforced(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "already-correct-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	existingLit := "March"
	existingEdge := &memory.Edge{SubjectID: self.ID, Predicate: "has_birthday", ObjectLiteral: &existingLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, existingEdge); err != nil {
		t.Fatalf("create existing edge: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	triples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "March", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx}, // already-true "correction", no replaces_object needed
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionReinforced {
		t.Fatalf("expected the already-correct edge to be reinforced, got %+v", outcomes)
	}
	if outcomes[0].Edge == nil || outcomes[0].Edge.ID != existingEdge.ID {
		t.Errorf("expected the reinforced edge to be the existing one, got %+v", outcomes[0].Edge)
	}

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM edges WHERE subject_id = $1 AND predicate = 'has_birthday'`, self.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count edges: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 edge (reinforced, not duplicated), got %d", rowCount)
	}
}

// TestFindMatchingEdge_IncludesSupersededEdges pins the behavior
// requirement 12 requires: after a correction supersedes "Dubai",
// re-extracting "Dubai" through the NORMAL (non-correction) path must
// reinforce the superseded edge, NOT create a second, live "Dubai" edge
// that would contradict the correction. This is CURRENTLY load-bearing
// in FindMatchingEdge (see its own doc comment) — any future change to
// FindMatchingEdge must preserve this deliberately, not accidentally
// break it while "fixing" something else.
func TestFindMatchingEdge_IncludesSupersededEdges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Pin Test Subject "+uuid.New().String())
	dubaiLit := "Dubai"
	dubaiEdge := &memory.Edge{SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: &dubaiLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, dubaiEdge); err != nil {
		t.Fatalf("create dubai edge: %v", err)
	}
	if _, err := ts.edges.CorrectMemory(ctx, dubaiEdge.ID, memory.CorrectionInput{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: strPtrLocal("Beirut"),
	}); err != nil {
		t.Fatalf("supersede dubai edge: %v", err)
	}

	// Re-extract "Dubai" through the NORMAL (non-correction) path.
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", time.Now())
	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: "event_present", Object: "Dubai", ObjectType: "literal", Confidence: 0.7},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionReinforced {
		t.Fatalf("expected re-extracting the OLD value to reinforce the superseded edge, got %+v — if this is now Created, FindMatchingEdge's superseded-inclusion broke", outcomes)
	}
	if outcomes[0].Edge.ID != dubaiEdge.ID {
		t.Errorf("expected the reinforced edge to be the original superseded Dubai edge %s, got %s", dubaiEdge.ID, outcomes[0].Edge.ID)
	}

	var liveDubaiCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM edges
		WHERE subject_id = $1 AND predicate = 'event_present' AND object_literal = 'Dubai' AND superseded_by IS NULL
	`, subject.ID).Scan(&liveDubaiCount); err != nil {
		t.Fatalf("count live dubai edges: %v", err)
	}
	if liveDubaiCount != 0 {
		t.Errorf("expected NO live Dubai edge to exist (it should still be superseded, not resurrected as a new live edge), got %d", liveDubaiCount)
	}
}

// TestStoreExtractedTriples_ConcurrentCorrectionRaceIsSkippedNotFatal
// covers requirement 13: two genuinely concurrent storeExtractedTriples
// calls targeting the SAME live edge for correction — Postgres's own
// row lock (CorrectMemory's SELECT ... FOR UPDATE) serializes them
// deterministically, so exactly one must win (TripleActionCorrected) and
// the other must lose to ErrEdgeAlreadySuperseded, logged and skipped
// rather than erroring the whole call out — and each call's OTHER,
// unrelated triple in the same batch must still store regardless of
// which way its own correction triple went.
func TestStoreExtractedTriples_ConcurrentCorrectionRaceIsSkippedNotFatal(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "race-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)

	existingLit := "April"
	existingEdge := &memory.Edge{SubjectID: self.ID, Predicate: "has_birthday", ObjectLiteral: &existingLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, existingEdge); err != nil {
		t.Fatalf("create existing edge: %v", err)
	}

	msgA := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	msgB := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())

	unrelatedSubjectA := "Race Unrelated A " + uuid.New().String()
	unrelatedSubjectB := "Race Unrelated B " + uuid.New().String()

	idx := 0
	oldValue := "April"
	triplesA := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "March", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &oldValue},
		{Subject: unrelatedSubjectA, Predicate: "likes", Object: "coffee", ObjectType: "literal", Confidence: 0.8},
	}
	triplesB := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: "has_birthday", Object: "May", ObjectType: "literal",
			Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &oldValue},
		{Subject: unrelatedSubjectB, Predicate: "likes", Object: "tea", ObjectType: "literal", Confidence: 0.8},
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	var wg sync.WaitGroup
	var outcomesA, outcomesB []TripleOutcome
	var errA, errB error
	wg.Add(2)
	go func() {
		defer wg.Done()
		outcomesA, errA = a.storeExtractedTriples(ctx, triplesA, memory.SourceTypeWhatsAppText, []uuid.UUID{msgA.ID}, time.Now())
	}()
	go func() {
		defer wg.Done()
		outcomesB, errB = a.storeExtractedTriples(ctx, triplesB, memory.SourceTypeWhatsAppText, []uuid.UUID{msgB.ID}, time.Now())
	}()
	wg.Wait()

	if errA != nil {
		t.Fatalf("storeExtractedTriples (A): %v", errA)
	}
	if errB != nil {
		t.Fatalf("storeExtractedTriples (B): %v", errB)
	}
	if len(outcomesA) != 2 || len(outcomesB) != 2 {
		t.Fatalf("expected 2 outcomes per call (correction + unrelated), got A=%d B=%d", len(outcomesA), len(outcomesB))
	}

	// The unrelated triple in EACH call must have stored regardless of
	// how the race went for the correction triple in the same batch.
	if outcomesA[1].Action != TripleActionCreated {
		t.Errorf("expected A's unrelated triple to be created, got %s", outcomesA[1].Action)
	}
	if outcomesB[1].Action != TripleActionCreated {
		t.Errorf("expected B's unrelated triple to be created, got %s", outcomesB[1].Action)
	}

	// Exactly one of the two correction triples won the race.
	correctedCount := 0
	skippedCount := 0
	for _, action := range []TripleAction{outcomesA[0].Action, outcomesB[0].Action} {
		switch action {
		case TripleActionCorrected:
			correctedCount++
		case TripleActionSkippedNoCorrectionTarget:
			skippedCount++
		default:
			t.Errorf("unexpected correction outcome action: %s", action)
		}
	}
	if correctedCount != 1 || skippedCount != 1 {
		t.Fatalf("expected exactly 1 corrected and 1 skipped (the race loser), got corrected=%d skipped=%d (A=%s B=%s)",
			correctedCount, skippedCount, outcomesA[0].Action, outcomesB[0].Action)
	}

	reloadedExisting, err := ts.edges.GetByID(ctx, existingEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(existingEdge): %v", err)
	}
	if reloadedExisting.SupersededBy == nil {
		t.Error("expected the existing edge to have been superseded by the race winner")
	}
}
