package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Step 0: reproduce the bug before fixing anything ---
//
// Root cause (confirmed by reading, not assumed): SoftDelete
// (internal/memory/delete_memory.go) leaves a LIVE tombstone edge
// (superseded_by IS NULL, same subject/predicate/object as the deleted
// edge, confidence=0, importance=0, source_type=user_deletion) and
// supersedes the old edge. SearchByText (internal/memory/search.go)
// excludes superseded edges but has no deletion check at all, and
// deliberately skips IsPruneCandidate (its own doc comment: an explicit
// keyword lookup shouldn't hide what was asked for) — so the tombstone's
// object_literal/predicate still matches the search term and comes back
// as a "result" for a fact the user explicitly deleted.

// TestSearchMemory_RegressionReturnsDeletedFact is this issue's own
// regression test, written BEFORE the fix (Step 0, requirement 1): after
// DeleteMemory, SearchMemory by a keyword from the object must return
// NEITHER the original (now-superseded) edge NOR the tombstone. Run
// before the fix, this fails — it finds the tombstone.
func TestSearchMemory_RegressionReturnsDeletedFact(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Tombstone Search Subject")
	lit := "likes olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	if reloaded.SupersededBy == nil {
		t.Fatal("expected the deleted edge to be superseded")
	}
	tombstoneID := *reloaded.SupersededBy

	results, err := a.SearchMemory(ctx, "olives", Filters{SubjectID: &subject.ID})
	if err != nil {
		t.Fatalf("SearchMemory: %v", err)
	}
	assertContainsEdge(t, results, edge.ID, false)
	assertContainsEdge(t, results, tombstoneID, false)
}

// TestSearchMemory_DeletedFactNotFoundBySubjectOrPredicate covers
// requirement 10's remaining match paths: a deleted fact must not come
// back whether the keyword matches the subject's canonical_name, the
// predicate, or the object — and whether or not a SubjectID filter is
// applied (SearchByText's WHERE clause ANDs the filter in; it must not
// change whether the tombstone exclusion itself applies).
func TestSearchMemory_DeletedFactNotFoundBySubjectOrPredicate(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Deleted Match Subject")
	lit := "olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "dislikes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	for _, tc := range []struct {
		name   string
		query  string
		filter Filters
	}{
		{"by subject name, no filter", "Deleted Match Subject", Filters{}},
		{"by subject name, with SubjectID filter", "Deleted Match Subject", Filters{SubjectID: &subject.ID}},
		{"by predicate, no filter", "dislikes", Filters{}},
		{"by predicate, with SubjectID filter", "dislikes", Filters{SubjectID: &subject.ID}},
		{"by object, no filter", "olives", Filters{}},
		{"by object, with SubjectID filter", "olives", Filters{SubjectID: &subject.ID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results, err := a.SearchMemory(ctx, tc.query, tc.filter)
			if err != nil {
				t.Fatalf("SearchMemory(%q): %v", tc.query, err)
			}
			assertContainsEdge(t, results, edge.ID, false)
		})
	}
}

// TestGetRelevantContext_ExcludesTombstoneEvenWithPruningDisabled covers
// requirement 12: proving the tombstone exclusion is EXPLICIT, not an
// incidental side effect of the confidence/importance prune floors.
//
// IncludePruned: true, not PruneConfig{ConfidenceFloor: 0,
// ImportanceFloor: 0}, is the actual way to disable pruning here —
// PruneConfig.withDefaults() treats any floor <= 0 as "unset" and
// silently substitutes the real defaults (0.15/0.1), so literally
// passing zero floors would just re-run with ordinary defaults and
// prove nothing new (a confidence-0 tombstone is already below BOTH
// non-zero defaults anyway). IncludePruned skips IsPruneCandidate
// entirely (rankAndFilter's own code, relevant_context.go) — if
// tombstone exclusion were only ever the incidental "confidence/
// importance below the floor" side effect this issue fixes, turning
// that whole gate off here would let the tombstone back in. It doesn't,
// because SearchRelevant (memory.searchFiltersWhere) already excluded
// it before rankAndFilter ever got a candidate list to apply
// IsPruneCandidate to in the first place.
func TestGetRelevantContext_ExcludesTombstoneEvenWithPruningDisabled(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Prune Disabled Tombstone Subject")
	lit := "likes olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	tombstoneID := *reloaded.SupersededBy

	results, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &subject.ID, IncludePruned: true}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	assertContainsEdge(t, results, edge.ID, false) // superseded, excluded either way
	assertContainsEdge(t, results, tombstoneID, false)
}

// TestGetContact_TopEdgesExcludesTombstone covers requirement 12's
// GetContact half: GetContact's top-edges query is GetRelevantContext
// under the hood (contact.go's own doc comment), so it inherits the same
// explicit exclusion automatically — pinned here as its own test so a
// future change decoupling the two wouldn't silently lose this.
func TestGetContact_TopEdgesExcludesTombstone(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "GetContact Tombstone Subject")
	lit := "likes olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	tombstoneID := *reloaded.SupersededBy

	summary, err := a.GetContact(ctx, subject.ID)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	assertContainsEdge(t, summary.TopEdges, edge.ID, false)
	assertContainsEdge(t, summary.TopEdges, tombstoneID, false)
}

// TestGetByID_ReturnsBothOldEdgeAndTombstone pins Part 3's deliberate
// "direct lookup is unfiltered" rule (requirement 13): EdgeStore.GetByID
// must keep returning both the original (now-superseded) edge and its
// tombstone — a direct-by-ID lookup is never curated default retrieval.
func TestGetByID_ReturnsBothOldEdgeAndTombstone(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "GetByID Tombstone Subject")
	lit := "likes olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	oldReloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	if oldReloaded == nil || oldReloaded.ID != edge.ID {
		t.Fatalf("expected GetByID to return the old edge, got %+v", oldReloaded)
	}

	tombstone, err := ts.edges.GetByID(ctx, *oldReloaded.SupersededBy)
	if err != nil {
		t.Fatalf("GetByID(tombstone): %v", err)
	}
	if !memory.IsTombstone(*tombstone) {
		t.Errorf("expected the superseding edge to be a tombstone, got source_type=%q", tombstone.SourceType)
	}
}

// TestGetRelationshipHistory_IncludesTombstone pins Part 3's other
// deliberate exception (requirement 13): GetRelationshipHistory must
// keep including a tombstone in the timeline — a deletion is part of a
// relationship's history, not noise to hide from it.
func TestGetRelationshipHistory_IncludesTombstone(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	contact := createTestEntity(t, ctx, ts, "person", "History Tombstone Contact")
	lit := "likes olives"
	edge := &memory.Edge{
		SubjectID: contact.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	tombstoneID := *reloaded.SupersededBy

	history, err := a.GetRelationshipHistory(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetRelationshipHistory: %v", err)
	}
	assertContainsEdge(t, history, edge.ID, true)
	assertContainsEdge(t, history, tombstoneID, true)
}

// TestSearchMemory_DoesNotHideLowConfidenceNonDeletedFact pins the
// DELIBERATE decision requirement 11 asks for: a faded, decayed, but
// NEVER-deleted edge must still be returned by SearchMemory. This is the
// opposite rule from tombstone exclusion, by design — SearchMemory's own
// doc comment: an explicit keyword lookup shouldn't hide what was asked
// for, and deletion (unconditional) is a different, separate rule from
// pruning (which SearchMemory never applies at all).
func TestSearchMemory_DoesNotHideLowConfidenceNonDeletedFact(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Faded Fact Subject")
	lit := "collecting stamps"
	longAgo := time.Now().Add(-5 * 365 * 24 * time.Hour)
	faded := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.05, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, FirstSeen: longAgo, LastReinforced: longAgo,
	}
	if err := ts.edges.Create(ctx, faded); err != nil {
		t.Fatalf("create faded edge: %v", err)
	}

	results, err := a.SearchMemory(ctx, "stamps", Filters{SubjectID: &subject.ID})
	if err != nil {
		t.Fatalf("SearchMemory: %v", err)
	}
	assertContainsEdge(t, results, faded.ID, true)
}

// --- Part 2: pipeline paths after a delete (requirement 14) ---

// TestStoreExtractedTriples_ReassertionAfterDeleteCreatesNewLiveEdge
// covers requirement 14(a): after deleting "likes olives", a LATER
// message reasserting it must create a fresh live edge via the
// stale-resurrection guard (isStaleResurrection) — not reinforce the
// tombstone (FindMatchingEdge now excludes it, see edge_store.go) — and
// the tombstone itself must be completely untouched (its last_reinforced
// unchanged).
func TestStoreExtractedTriples_ReassertionAfterDeleteCreatesNewLiveEdge(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Reassert After Delete Subject "+uuid.New().String())
	predicate := "reassert_after_delete_predicate_" + uuid.New().String()

	lit := "olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	reloadedOld, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	tombstoneID := *reloadedOld.SupersededBy
	tombstoneBefore, err := ts.edges.GetByID(ctx, tombstoneID)
	if err != nil {
		t.Fatalf("GetByID(tombstone): %v", err)
	}

	later := tombstoneBefore.FirstSeen.Add(1 * time.Hour)
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", later)
	triple := extraction.ExtractedTriple{Subject: subject.CanonicalName, Predicate: predicate, Object: "olives", ObjectType: "literal", Confidence: 0.9}

	outcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{triple}, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, later)
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected a fresh Created edge, got %+v", outcomes)
	}
	newEdge := outcomes[0].Edge
	if newEdge.ID == edge.ID || newEdge.ID == tombstoneID {
		t.Errorf("expected a brand-new edge distinct from both the old edge and the tombstone, got %s", newEdge.ID)
	}

	tombstoneAfter, err := ts.edges.GetByID(ctx, tombstoneID)
	if err != nil {
		t.Fatalf("GetByID(tombstone, after): %v", err)
	}
	if !tombstoneAfter.LastReinforced.Equal(tombstoneBefore.LastReinforced) {
		t.Errorf("expected the tombstone's last_reinforced untouched, got %v want %v", tombstoneAfter.LastReinforced, tombstoneBefore.LastReinforced)
	}
	if tombstoneAfter.SupersededBy != nil {
		t.Errorf("expected the tombstone to remain live (not superseded), got superseded_by=%v", tombstoneAfter.SupersededBy)
	}
}

// TestStoreExtractedTriples_EarlierAssertionAfterDeleteIsSkippedStale
// covers requirement 14(b): a message dated BEFORE the deletion
// reasserting the same fact is a stale, out-of-order replay — skipped,
// not resurrected.
func TestStoreExtractedTriples_EarlierAssertionAfterDeleteIsSkippedStale(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Stale After Delete Subject "+uuid.New().String())
	predicate := "stale_after_delete_predicate_" + uuid.New().String()

	lit := "olives"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}
	reloadedOld, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	tombstoneID := *reloadedOld.SupersededBy
	tombstoneBefore, err := ts.edges.GetByID(ctx, tombstoneID)
	if err != nil {
		t.Fatalf("GetByID(tombstone): %v", err)
	}

	earlier := tombstoneBefore.FirstSeen.Add(-1 * time.Hour)
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", earlier)
	triple := extraction.ExtractedTriple{Subject: subject.CanonicalName, Predicate: predicate, Object: "olives", ObjectType: "literal", Confidence: 0.9}

	outcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{triple}, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, earlier)
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedStale {
		t.Fatalf("expected a stale skip, got %+v", outcomes)
	}

	var liveCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM edges
		WHERE subject_id = $1 AND predicate = $2 AND object_literal = 'olives' AND superseded_by IS NULL
	`, subject.ID, predicate).Scan(&liveCount); err != nil {
		t.Fatalf("count live edges: %v", err)
	}
	if liveCount != 1 {
		t.Errorf("expected exactly 1 live edge (the tombstone itself, untouched — no new edge created), got %d", liveCount)
	}
}

// TestStoreExtractedTriples_TombstoneNotACorrectionTargetCandidate
// covers requirement 14(c): after deleting a fact, a correction triple
// naming the same subject+predicate must find ZERO live candidates (the
// tombstone is excluded) — never pick the tombstone as a correction
// target.
func TestStoreExtractedTriples_TombstoneNotACorrectionTargetCandidate(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "delete-correction-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)
	predicate := "tombstone_correction_predicate_" + uuid.New().String()

	lit := "olives"
	edge := &memory.Edge{
		SubjectID: self.ID, Predicate: predicate, ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}
	if err := a.DeleteMemory(ctx, edge.ID); err != nil {
		t.Fatalf("DeleteMemory: %v", err)
	}

	msg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", time.Now())
	idx := 0
	correctionTriple := extraction.ExtractedTriple{
		Subject: self.CanonicalName, Predicate: predicate, Object: "cilantro", ObjectType: "literal",
		Confidence: 0.9, IsCorrection: true, CorrectionSourceIndex: &idx,
	}

	outcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{correctionTriple}, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, time.Now())
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedNoCorrectionTarget {
		t.Fatalf("expected a no-correction-target skip (the tombstone must never be picked as a target), got %+v", outcomes)
	}
}
