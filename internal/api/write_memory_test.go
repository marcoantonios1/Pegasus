package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- The full-row-EdgeStore.Update-clobbers-concurrent-changes fix ---
//
// EdgeStore.Update used to write EVERY column from whatever *Edge
// snapshot a caller handed it. Its one production caller, UpdateMemory,
// got that snapshot via GetByID, changed Importance in Go, then wrote
// the whole row back — a read-modify-write window during which ANY
// other writer to the same row (Reinforce, CorrectMemory, SoftDelete,
// MergeDuplicateEdges) had its own change silently overwritten by the
// stale snapshot's values. EdgeStore.Update is gone; UpdateMemory now
// calls EdgeStore.UpdateImportance, a single-column UPDATE with no such
// window.

// TestUpdateMemory_ConcurrentWithReinforce_NeverClobbersLastReinforced
// covers requirement 6 — the actual regression reproducer. 200
// iterations of two goroutines hammering the SAME edge concurrently:
// one calling UpdateMemory with a changing importance, the other calling
// Reinforce with a strictly increasing timestamp. Run with -race.
//
// This fails against the old GetByID-then-Update(whole row)
// implementation: a goroutine's stale GetByID snapshot, taken before
// several Reinforce calls landed, gets written back by its own
// Update(), silently moving last_reinforced BACKWARDS to that snapshot's
// value — exactly the bug this fix closes. Confirmed by temporarily
// reverting UpdateMemory to that shape and running this test: it fails
// (see this fix's own PR description).
func TestUpdateMemory_ConcurrentWithReinforce_NeverClobbersLastReinforced(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Concurrent UpdateMemory Subject")
	lit := "concurrent update test"
	originalMsgIDs := []uuid.UUID{uuid.New(), uuid.New()}
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.1, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: originalMsgIDs,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	const iterations = 200
	base := time.Now()

	var wg sync.WaitGroup
	wg.Add(2)

	importances := make([]float64, iterations)
	for i := range importances {
		importances[i] = float64(i%100) / 100.0
	}
	maxTimestamp := base.Add(time.Duration(iterations-1) * time.Second)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if _, err := a.UpdateMemory(ctx, edge.ID, UpdateMemoryFields{Importance: &importances[i]}); err != nil {
				t.Errorf("UpdateMemory iteration %d: %v", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			at := base.Add(time.Duration(i) * time.Second)
			if err := ts.edges.Reinforce(ctx, edge.ID, at, at); err != nil {
				t.Errorf("Reinforce iteration %d: %v", i, err)
				return
			}
		}
	}()

	wg.Wait()

	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if !reloaded.LastReinforced.Equal(maxTimestamp) {
		t.Errorf("expected last_reinforced == max issued timestamp %v, got %v (a concurrent UpdateMemory clobbered it with a stale snapshot)", maxTimestamp, reloaded.LastReinforced)
	}

	validImportance := false
	for _, v := range importances {
		if reloaded.Importance == v {
			validImportance = true
			break
		}
	}
	if !validImportance {
		t.Errorf("expected final importance to be one of the written values, got %v", reloaded.Importance)
	}

	if len(reloaded.SourceMessageIDs) != len(originalMsgIDs) {
		t.Errorf("expected source_message_ids intact (len %d), got %v", len(originalMsgIDs), reloaded.SourceMessageIDs)
	}
}

// --- Requirement 7: deterministic stale-snapshot contract tests — each
// performs some OTHER write on the edge first, then calls UpdateMemory,
// and asserts the other write's effect survived. ---

// TestUpdateMemory_PreservesReinforce covers 7(a): Reinforce(T1), then
// UpdateMemory — last_reinforced must still be T1, not reverted.
func TestUpdateMemory_PreservesReinforce(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Preserve Reinforce Subject")
	lit := "preserve reinforce test"
	// Backdated well before t1 below — Reinforce's own GREATEST contract
	// only ever moves last_reinforced FORWARD, so t1 must be later than
	// whatever the edge starts at for this test to actually exercise
	// "Reinforce moved it to t1."
	edge := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.1, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, time.Now().Add(-48*time.Hour))

	t1 := time.Now().Add(-time.Hour)
	if err := ts.edges.Reinforce(ctx, edge.ID, t1, t1); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}

	newImportance := 0.77
	updated, err := a.UpdateMemory(ctx, edge.ID, UpdateMemoryFields{Importance: &newImportance})
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	if !updated.LastReinforced.Equal(t1) {
		t.Errorf("expected last_reinforced still %v, got %v (UpdateMemory reverted a prior Reinforce)", t1, updated.LastReinforced)
	}
	if updated.Importance != newImportance {
		t.Errorf("expected importance %v, got %v", newImportance, updated.Importance)
	}
}

// TestUpdateMemory_DoesNotResurrectCorrectedEdge covers 7(b): CorrectMemory
// supersedes the edge, then UpdateMemory is called on the now-superseded
// old edge — superseded_by must still point at the correcting edge
// (never reverted to nil, which would resurrect the corrected-away
// fact), and confidence/decay_locked must stay exactly as CorrectMemory
// left them (UpdateMemory never touches either, by construction, but
// this is the end-to-end proof, not just a read of the code).
func TestUpdateMemory_DoesNotResurrectCorrectedEdge(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Preserve Correction Subject")
	oldLit := "her birthday is in April"
	oldEdge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: &oldLit,
		Confidence: 0.7, Importance: 0.3, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}
	if err := ts.edges.Create(ctx, oldEdge); err != nil {
		t.Fatalf("create old edge: %v", err)
	}

	newLit := "her birthday is in March"
	corrected, err := ts.edges.CorrectMemory(ctx, oldEdge.ID, memory.CorrectionInput{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: &newLit,
	})
	if err != nil {
		t.Fatalf("CorrectMemory: %v", err)
	}

	newImportance := 0.42
	updated, err := a.UpdateMemory(ctx, oldEdge.ID, UpdateMemoryFields{Importance: &newImportance})
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	if updated.SupersededBy == nil || *updated.SupersededBy != corrected.ID {
		t.Fatalf("expected superseded_by still %v (not resurrected), got %v", corrected.ID, updated.SupersededBy)
	}
	if updated.Confidence != 0.7 {
		t.Errorf("expected confidence untouched (0.7), got %v", updated.Confidence)
	}
	if updated.DecayLocked {
		t.Errorf("expected decay_locked untouched (false — this is the OLD, corrected-away edge, not the new correction), got true")
	}
	if updated.Importance != newImportance {
		t.Errorf("expected importance updated to %v, got %v", newImportance, updated.Importance)
	}
}

// TestUpdateMemory_DoesNotUndoSoftDelete covers 7(c): SoftDelete
// supersedes the edge with a tombstone, then UpdateMemory — superseded_by
// must still point at the tombstone (the edge must not come back from
// deletion).
func TestUpdateMemory_DoesNotUndoSoftDelete(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Preserve SoftDelete Subject")
	lit := "fact to be deleted"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.3, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	tombstone, err := ts.edges.SoftDelete(ctx, edge.ID)
	if err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	newImportance := 0.55
	updated, err := a.UpdateMemory(ctx, edge.ID, UpdateMemoryFields{Importance: &newImportance})
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}
	if updated.SupersededBy == nil || *updated.SupersededBy != tombstone.ID {
		t.Fatalf("expected superseded_by still %v (the edge must stay deleted), got %v", tombstone.ID, updated.SupersededBy)
	}
}

// TestUpdateMemory_PreservesMergedSourceMessageIDs covers 7(d): a
// MergeDuplicateEdges merge appends source_message_ids onto the
// survivor, then UpdateMemory — the merged IDs must survive, not be
// reverted to whatever the survivor held before the merge.
func TestUpdateMemory_PreservesMergedSourceMessageIDs(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Preserve Merge Subject")
	lit := "run a marathon"
	survivorMsgID := uuid.New()
	survivor := &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: &lit,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{survivorMsgID},
	}
	if err := ts.edges.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	duplicateMsgID := uuid.New()
	duplicate := &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{duplicateMsgID},
	}
	if err := ts.edges.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}

	if _, err := ts.edges.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID}); err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}

	newImportance := 0.63
	updated, err := a.UpdateMemory(ctx, survivor.ID, UpdateMemoryFields{Importance: &newImportance})
	if err != nil {
		t.Fatalf("UpdateMemory: %v", err)
	}

	hasSurvivorID, hasDuplicateID := false, false
	for _, id := range updated.SourceMessageIDs {
		if id == survivorMsgID {
			hasSurvivorID = true
		}
		if id == duplicateMsgID {
			hasDuplicateID = true
		}
	}
	if !hasSurvivorID || !hasDuplicateID {
		t.Errorf("expected both the survivor's and duplicate's source_message_ids to survive, got %v", updated.SourceMessageIDs)
	}
}

// --- Requirement 8: UpdateImportance's own contract ---

// TestUpdateImportance_ReturnsFreshRowIncludingConcurrentChanges covers
// requirement 8's first half: the returned edge reflects a change made
// AFTER an earlier read, not a stale snapshot — the whole point of
// reading the row back via RETURNING instead of trusting a caller's
// in-memory copy.
func TestUpdateImportance_ReturnsFreshRowIncludingConcurrentChanges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "UpdateImportance Fresh Row Subject")
	lit := "fresh row test"
	// Backdated well before reinforcedAt below — see
	// TestUpdateMemory_PreservesReinforce's own comment on why (Reinforce's
	// GREATEST contract only ever moves last_reinforced forward).
	edge := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.1, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, time.Now().Add(-48*time.Hour))

	// An earlier read (simulating a caller that looked at this row
	// before the change below landed).
	if _, err := ts.edges.GetByID(ctx, edge.ID); err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	reinforcedAt := time.Now().Add(-5 * time.Minute)
	if err := ts.edges.Reinforce(ctx, edge.ID, reinforcedAt, reinforcedAt); err != nil {
		t.Fatalf("Reinforce (the 'concurrent change'): %v", err)
	}

	newImportance := 0.88
	updated, err := ts.edges.UpdateImportance(ctx, edge.ID, newImportance)
	if err != nil {
		t.Fatalf("UpdateImportance: %v", err)
	}
	if !updated.LastReinforced.Equal(reinforcedAt) {
		t.Errorf("expected the returned row to reflect the Reinforce that happened after the earlier read, got last_reinforced=%v want=%v", updated.LastReinforced, reinforcedAt)
	}
	if updated.Importance != newImportance {
		t.Errorf("expected importance %v, got %v", newImportance, updated.Importance)
	}
}

// TestUpdateImportance_NonexistentIDReturnsErrEdgeNotFound covers
// requirement 8's second half: a nonexistent edge ID.
func TestUpdateImportance_NonexistentIDReturnsErrEdgeNotFound(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	_, err := ts.edges.UpdateImportance(ctx, uuid.New(), 0.5)
	if !errors.Is(err, memory.ErrEdgeNotFound) {
		t.Errorf("expected ErrEdgeNotFound, got %v", err)
	}
}

// TestUpdateImportance_NonNotFoundErrorIsNotLabeledNotFound covers
// requirement 8's third half, same reasoning as the lockEdgeForUpdate
// error-classification fix this method deliberately reuses
// (ErrEdgeNotFound ONLY wraps pgx.ErrNoRows): a real failure — here, an
// already-cancelled context — must come back as its own distinct error,
// never matching ErrEdgeNotFound.
func TestUpdateImportance_NonNotFoundErrorIsNotLabeledNotFound(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "UpdateImportance Real Error Subject")
	lit := "real error test"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.1, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ts.edges.UpdateImportance(cancelledCtx, edge.ID, 0.9)
	if err == nil {
		t.Fatal("expected an error for a cancelled context, got nil")
	}
	if errors.Is(err, memory.ErrEdgeNotFound) {
		t.Errorf("expected a cancelled context NOT to be labeled ErrEdgeNotFound, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled) to hold, got: %v", err)
	}
}
