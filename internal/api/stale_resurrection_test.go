package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- FindMatchingEdge stale-resurrection fix ---
//
// FindMatchingEdge used to deliberately include superseded edges (see its
// git history and the now-deleted TestFindMatchingEdge_IncludesSupersededEdges
// in correct_memory_pipeline_test.go) — that was found to be a real bug:
// a corrected-away value, re-extracted from an older, out-of-order
// message (a retried voice-note transcription, a re-import), would
// silently reinforce the dead edge instead of being evaluated as the new
// claim it actually is. FindMatchingEdge is now live-only, and
// isStaleResurrection (internal/api/pipeline.go) is what actually decides
// whether a no-live-match triple that happens to match a SUPERSEDED edge
// is a stale replay (skip) or a genuine fresh reassertion (create a new
// live edge) — see its own doc comment for the full reasoning. These
// tests exercise that decision through the real pipeline against real
// Postgres.

// TestStoreExtractedTriples_ReassertionAfterCorrectionCreatesNewLiveEdge
// covers requirement 9: seed live "works at Acme", correct it to
// "Globex" (a self message), then process a LATER message reasserting
// Acme through the normal (non-correction) path. The old Acme edge must
// be left exactly as the correction left it (still superseded,
// last_reinforced unchanged), and a brand-new, distinct live Acme edge
// must exist — this is NOT the dead edge coming back to life, it's a
// fresh claim that happens to repeat an old value.
func TestStoreExtractedTriples_ReassertionAfterCorrectionCreatesNewLiveEdge(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "reassert-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)
	predicate := "reassert_predicate_" + uuid.New().String()

	acmeLit := "Acme"
	acmeEdge := &memory.Edge{SubjectID: self.ID, Predicate: predicate, ObjectLiteral: &acmeLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, acmeEdge); err != nil {
		t.Fatalf("create acme edge: %v", err)
	}
	originalLastReinforced := acmeEdge.LastReinforced

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	// The correction: Acme -> Globex, said at T1.
	t1 := time.Now()
	correctionMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t1)
	idx := 0
	globexLit := "Globex"
	correctionTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: globexLit, ObjectType: "literal",
			Confidence: 0.95, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &acmeLit},
	}
	correctionOutcomes, err := a.storeExtractedTriples(ctx, correctionTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{correctionMsg.ID}, t1)
	if err != nil {
		t.Fatalf("storeExtractedTriples (correction): %v", err)
	}
	if len(correctionOutcomes) != 1 || correctionOutcomes[0].Action != TripleActionCorrected {
		t.Fatalf("expected the correction to apply, got %+v", correctionOutcomes)
	}
	globexEdge := correctionOutcomes[0].Edge

	// A LATER message (T2 > T1) reasserting Acme through the normal path.
	t2 := t1.Add(1 * time.Hour)
	reassertMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t2)
	reassertTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: acmeLit, ObjectType: "literal", Confidence: 0.9},
	}
	reassertOutcomes, err := a.storeExtractedTriples(ctx, reassertTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{reassertMsg.ID}, t2)
	if err != nil {
		t.Fatalf("storeExtractedTriples (reassertion): %v", err)
	}
	if len(reassertOutcomes) != 1 || reassertOutcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected a fresh reassertion to be Created, got %+v", reassertOutcomes)
	}
	newAcmeEdge := reassertOutcomes[0].Edge
	if newAcmeEdge == nil || newAcmeEdge.ObjectLiteral == nil || *newAcmeEdge.ObjectLiteral != acmeLit {
		t.Fatalf("expected a new Acme edge, got %+v", newAcmeEdge)
	}
	if newAcmeEdge.ID == acmeEdge.ID {
		t.Error("expected a NEW edge ID, not the original dead Acme edge's ID")
	}

	// The old Acme edge is untouched: still superseded (by Globex),
	// last_reinforced unchanged.
	reloadedOldAcme, err := ts.edges.GetByID(ctx, acmeEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(acmeEdge): %v", err)
	}
	if reloadedOldAcme.SupersededBy == nil || *reloadedOldAcme.SupersededBy != globexEdge.ID {
		t.Errorf("expected the old Acme edge to still be superseded by the Globex edge, got %+v", reloadedOldAcme.SupersededBy)
	}
	if !reloadedOldAcme.LastReinforced.Equal(originalLastReinforced) {
		t.Errorf("expected the old Acme edge's last_reinforced to be untouched, got %v want %v", reloadedOldAcme.LastReinforced, originalLastReinforced)
	}
}

// TestStoreExtractedTriples_StaleResurrectionIsSkipped covers
// requirement 10: after the same Acme -> Globex correction, a message
// timestamped BEFORE the correction asserting Acme must be skipped as
// stale — no new edge, the dead Acme edge left untouched, outcome
// TripleActionSkippedStale, and the log line names the triple, the
// superseded edge, and both timestamps (requirement 6).
func TestStoreExtractedTriples_StaleResurrectionIsSkipped(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "stale-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)
	predicate := "stale_predicate_" + uuid.New().String()

	acmeLit := "Acme"
	acmeEdge := &memory.Edge{SubjectID: self.ID, Predicate: predicate, ObjectLiteral: &acmeLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, acmeEdge); err != nil {
		t.Fatalf("create acme edge: %v", err)
	}
	originalLastReinforced := acmeEdge.LastReinforced

	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	t1 := time.Now()
	correctionMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t1)
	idx := 0
	globexLit := "Globex"
	correctionTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: globexLit, ObjectType: "literal",
			Confidence: 0.95, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &acmeLit},
	}
	correctionOutcomes, err := a.storeExtractedTriples(ctx, correctionTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{correctionMsg.ID}, t1)
	if err != nil {
		t.Fatalf("storeExtractedTriples (correction): %v", err)
	}
	if len(correctionOutcomes) != 1 || correctionOutcomes[0].Action != TripleActionCorrected {
		t.Fatalf("expected the correction to apply, got %+v", correctionOutcomes)
	}

	// An OLDER message (T0 < T1) reasserting Acme — a retried/re-imported
	// message that was actually said before the correction.
	t0 := t1.Add(-1 * time.Hour)
	staleMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t0)
	staleTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: acmeLit, ObjectType: "literal", Confidence: 0.9},
	}
	staleOutcomes, err := a.storeExtractedTriples(ctx, staleTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{staleMsg.ID}, t0)
	if err != nil {
		t.Fatalf("storeExtractedTriples (stale): %v", err)
	}
	if len(staleOutcomes) != 1 || staleOutcomes[0].Action != TripleActionSkippedStale {
		t.Fatalf("expected a stale skip, got %+v", staleOutcomes)
	}
	if staleOutcomes[0].Edge != nil {
		t.Error("expected no edge on a stale skip")
	}

	var liveAcmeCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM edges
		WHERE subject_id = $1 AND predicate = $2 AND object_literal = $3 AND superseded_by IS NULL
	`, self.ID, predicate, acmeLit).Scan(&liveAcmeCount); err != nil {
		t.Fatalf("count live acme edges: %v", err)
	}
	if liveAcmeCount != 0 {
		t.Errorf("expected NO live Acme edge to have been created, got %d", liveAcmeCount)
	}

	reloadedOldAcme, err := ts.edges.GetByID(ctx, acmeEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(acmeEdge): %v", err)
	}
	if !reloadedOldAcme.LastReinforced.Equal(originalLastReinforced) {
		t.Errorf("expected the dead Acme edge to be untouched, got last_reinforced %v want %v", reloadedOldAcme.LastReinforced, originalLastReinforced)
	}

	found := false
	for _, l := range logLines {
		if strings.Contains(l, "stale") && strings.Contains(l, acmeEdge.ID.String()) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a log line naming the stale triple's superseded edge, got: %v", logLines)
	}
}

// TestStoreExtractedTriples_StaleResurrection_EqualTimestampIsAlsoStale
// covers requirement 10's boundary case: the incoming message's
// timestamp is EXACTLY EQUAL to the correction's own timestamp —
// isStaleResurrection's doc comment is explicit this counts as stale
// (<=, not <), since a message processed again at the exact instant it
// was originally said carries no new information.
func TestStoreExtractedTriples_StaleResurrection_EqualTimestampIsAlsoStale(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "stale-equal-self-" + uuid.New().String()
	self := resolveTestSelfEntity(t, ctx, ts, selfExternalID)
	predicate := "stale_equal_predicate_" + uuid.New().String()

	acmeLit := "Acme"
	acmeEdge := &memory.Edge{SubjectID: self.ID, Predicate: predicate, ObjectLiteral: &acmeLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, acmeEdge); err != nil {
		t.Fatalf("create acme edge: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	t1 := time.Now()
	correctionMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t1)
	idx := 0
	globexLit := "Globex"
	correctionTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: globexLit, ObjectType: "literal",
			Confidence: 0.95, IsCorrection: true, CorrectionSourceIndex: &idx, ReplacesObject: &acmeLit},
	}
	if _, err := a.storeExtractedTriples(ctx, correctionTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{correctionMsg.ID}, t1); err != nil {
		t.Fatalf("storeExtractedTriples (correction): %v", err)
	}

	// A different message, but stamped at EXACTLY t1 — the same instant
	// as the correction itself.
	equalMsg := createTestMessage(t, ctx, ts, uuid.New(), self.ID, "text", t1)
	equalTriples := []extraction.ExtractedTriple{
		{Subject: self.CanonicalName, Predicate: predicate, Object: acmeLit, ObjectType: "literal", Confidence: 0.9},
	}
	outcomes, err := a.storeExtractedTriples(ctx, equalTriples, memory.SourceTypeWhatsAppText, []uuid.UUID{equalMsg.ID}, t1)
	if err != nil {
		t.Fatalf("storeExtractedTriples (equal timestamp): %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedStale {
		t.Fatalf("expected an equal timestamp to count as stale, got %+v", outcomes)
	}
}

// TestStoreExtractedTriples_StaleResurrectionFallsBackToFirstSeen covers
// requirement 11: the superseding edge has no source messages at all
// (CorrectionInput.SourceMessageIDs left unset) — isStaleResurrection
// must fall back to that edge's own FirstSeen for the comparison, and
// log that it did. Set up directly via CorrectMemory (bypassing
// storeExtractedTriples' correction path, and so needing no self entity
// at all) specifically so SourceMessageIDs is genuinely empty, not just
// hard to resolve.
func TestStoreExtractedTriples_StaleResurrectionFallsBackToFirstSeen(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Fallback Test Subject "+uuid.New().String())
	predicate := "fallback_predicate_" + uuid.New().String()

	acmeLit := "Acme"
	acmeEdge := &memory.Edge{SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &acmeLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0, DecayRate: memory.DecayRateStable}
	if err := ts.edges.Create(ctx, acmeEdge); err != nil {
		t.Fatalf("create acme edge: %v", err)
	}

	globexLit := "Globex"
	corrected, err := ts.edges.CorrectMemory(ctx, acmeEdge.ID, memory.CorrectionInput{
		SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &globexLit,
		// SourceMessageIDs deliberately left nil/unset.
	})
	if err != nil {
		t.Fatalf("supersede acme edge: %v", err)
	}
	if len(corrected.SourceMessageIDs) != 0 {
		t.Fatalf("expected the correcting edge to have no source messages, got %v", corrected.SourceMessageIDs)
	}

	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	// A message timestamped before the correcting edge's FirstSeen — the
	// fallback comparison point — reasserting Acme.
	before := corrected.FirstSeen.Add(-1 * time.Hour)
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", before)
	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: predicate, Object: acmeLit, ObjectType: "literal", Confidence: 0.9},
	}
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, before)
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionSkippedStale {
		t.Fatalf("expected a stale skip via the first_seen fallback, got %+v", outcomes)
	}

	found := false
	for _, l := range logLines {
		if strings.Contains(l, "first_seen") && strings.Contains(l, corrected.ID.String()) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a log line naming the first_seen fallback for edge %s, got: %v", corrected.ID, logLines)
	}
}

// TestStoreExtractedTriples_DedupMergedDuplicateDoesNotReinforceDeadEdge
// covers requirement 14: superseded_by is overloaded — the Reflection
// Engine's dedup pass (reflection.MergeDuplicates, §9.2) also sets it, on
// a duplicate edge pointing at its surviving cluster-mate, for a reason
// that has nothing to do with the fact being wrong. A triple whose only
// superseded match is such a dedup-merged duplicate must NOT reinforce
// that dead edge. Per this issue's own scope notes, the accepted
// (documented, not worked around) behavior is that a "fresh" triple in
// this situation creates a brand-new live edge for the next
// MergeDuplicates pass to re-merge, rather than this guard reinforcing
// the dead duplicate the way the old, buggy FindMatchingEdge would have.
func TestStoreExtractedTriples_DedupMergedDuplicateDoesNotReinforceDeadEdge(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Dedup Overlap Test Subject "+uuid.New().String())
	predicate := "dedup_overlap_predicate_" + uuid.New().String()

	t1 := time.Now()
	survivorMsg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", t1)
	survivorLit := "went to the gym today"
	survivorEdge := &memory.Edge{
		SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &survivorLit,
		Confidence: 0.8, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{survivorMsg.ID},
	}
	if err := ts.edges.Create(ctx, survivorEdge); err != nil {
		t.Fatalf("create survivor edge: %v", err)
	}

	duplicateLit := "went to the gym"
	duplicateEdge := &memory.Edge{
		SubjectID: subject.ID, Predicate: predicate, ObjectLiteral: &duplicateLit,
		Confidence: 0.75, Importance: 0.5, SourceType: memory.SourceTypeWhatsAppText, SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{survivorMsg.ID},
	}
	if err := ts.edges.Create(ctx, duplicateEdge); err != nil {
		t.Fatalf("create duplicate edge: %v", err)
	}
	// Simulate reflection.MergeDuplicates' own mergeCluster: the
	// duplicate's superseded_by points at the survivor — NOT at a
	// corrected value, there's no IsCorrection involved at all.
	if _, err := ts.pool.Exec(ctx, `UPDATE edges SET superseded_by = $1 WHERE id = $2`, survivorEdge.ID, duplicateEdge.ID); err != nil {
		t.Fatalf("simulate dedup merge: %v", err)
	}
	originalDuplicateLastReinforced := duplicateEdge.LastReinforced

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	// A LATER message exactly repeating the (dead) duplicate's own
	// phrasing.
	t2 := t1.Add(1 * time.Hour)
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", t2)
	triples := []extraction.ExtractedTriple{
		{Subject: subject.CanonicalName, Predicate: predicate, Object: duplicateLit, ObjectType: "literal", Confidence: 0.9},
	}
	outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, t2)
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected a fresh Create (the accepted dedup-overlap behavior — see this test's own doc comment), got %+v", outcomes)
	}
	newEdge := outcomes[0].Edge
	if newEdge == nil || newEdge.ID == duplicateEdge.ID || newEdge.ID == survivorEdge.ID {
		t.Fatalf("expected a brand-new edge distinct from both the survivor and the dead duplicate, got %+v", newEdge)
	}

	reloadedDuplicate, err := ts.edges.GetByID(ctx, duplicateEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(duplicateEdge): %v", err)
	}
	if !reloadedDuplicate.LastReinforced.Equal(originalDuplicateLastReinforced) {
		t.Errorf("expected the dead duplicate edge NOT to be reinforced, got last_reinforced %v want %v",
			reloadedDuplicate.LastReinforced, originalDuplicateLastReinforced)
	}
}
