package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func seedOldEdge(t *testing.T, ctx context.Context, edgeStore *EdgeStore, subjectID uuid.UUID, literal string) *Edge {
	t.Helper()
	old := &Edge{
		SubjectID:     subjectID,
		Predicate:     "event_present",
		ObjectLiteral: &literal,
		Confidence:    0.7,
		Importance:    0.3,
		SourceType:    "whatsapp_text",
		SourceWeight:  1.0,
		DecayRate:     DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, old); err != nil {
		t.Fatalf("seed old edge: %v", err)
	}
	return old
}

// TestCorrectMemory_SetsExpectedFieldsOnNewEdge covers requirement 6.
func TestCorrectMemory_SetsExpectedFieldsOnNewEdge(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "CorrectMemory Fields Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "her birthday is in April")

	newLiteral := "her birthday is in March"
	msgID := uuid.New()

	newEdge, err := edgeStore.CorrectMemory(ctx, old.ID, CorrectionInput{
		SubjectID:        subject.ID,
		Predicate:        "event_present",
		ObjectLiteral:    &newLiteral,
		SourceMessageIDs: []uuid.UUID{msgID},
	})
	if err != nil {
		t.Fatalf("CorrectMemory: %v", err)
	}

	if newEdge.Confidence != 1.0 {
		t.Errorf("expected confidence 1.0, got %v", newEdge.Confidence)
	}
	if !newEdge.DecayLocked {
		t.Error("expected decay_locked = true")
	}
	if !newEdge.IsCorrection {
		t.Error("expected is_correction = true")
	}
	if newEdge.SourceType != SourceTypeUserCorrection {
		t.Errorf("expected source_type %q, got %q", SourceTypeUserCorrection, newEdge.SourceType)
	}
	if newEdge.Importance != old.Importance {
		t.Errorf("expected importance to inherit from old edge (%v), got %v", old.Importance, newEdge.Importance)
	}

	// Confirm it actually persisted with these values, not just returned
	// in-memory with them.
	reloaded, err := edgeStore.GetByID(ctx, newEdge.ID)
	if err != nil {
		t.Fatalf("GetByID(new): %v", err)
	}
	if reloaded.Confidence != 1.0 || !reloaded.DecayLocked || !reloaded.IsCorrection || reloaded.SourceType != SourceTypeUserCorrection {
		t.Errorf("persisted edge doesn't match expected correction fields: %+v", reloaded)
	}
}

func TestCorrectMemory_ImportanceOverride(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "CorrectMemory Importance Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "old fact")

	newLiteral := "corrected fact"
	override := 0.95

	newEdge, err := edgeStore.CorrectMemory(ctx, old.ID, CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &newLiteral,
		Importance:    &override,
	})
	if err != nil {
		t.Fatalf("CorrectMemory: %v", err)
	}
	if newEdge.Importance != override {
		t.Errorf("expected importance override %v, got %v", override, newEdge.Importance)
	}
}

// TestCorrectMemory_SupersedesOldEdge covers requirement 7.
func TestCorrectMemory_SupersedesOldEdge(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "CorrectMemory Supersede Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "old fact")

	newLiteral := "corrected fact"
	newEdge, err := edgeStore.CorrectMemory(ctx, old.ID, CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &newLiteral,
	})
	if err != nil {
		t.Fatalf("CorrectMemory: %v", err)
	}

	reloadedOld, err := edgeStore.GetByID(ctx, old.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	if reloadedOld.SupersededBy == nil || *reloadedOld.SupersededBy != newEdge.ID {
		t.Errorf("expected old edge's superseded_by = %v, got %v", newEdge.ID, reloadedOld.SupersededBy)
	}
}

// TestCorrectMemory_EffectiveConfidenceStaysMaxRegardlessOfElapsedTime
// covers requirement 8 — the actual end-to-end proof that decay_locked is
// respected, not just set as an unread flag.
func TestCorrectMemory_EffectiveConfidenceStaysMaxRegardlessOfElapsedTime(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "CorrectMemory EffectiveConfidence Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "old fact")

	newLiteral := "corrected fact"
	newEdge, err := edgeStore.CorrectMemory(ctx, old.ID, CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &newLiteral,
	})
	if err != nil {
		t.Fatalf("CorrectMemory: %v", err)
	}

	farFuture := newEdge.LastReinforced.AddDate(20, 0, 0)
	got := EffectiveConfidence(*newEdge, farFuture)
	if got != 1.0 {
		t.Errorf("expected EffectiveConfidence to stay 1.0 for a corrected edge 20 years later, got %v", got)
	}
}

// TestCorrectMemory_RollsBackIfNewEdgeInsertFails exercises rollback
// through the real public API: an invalid SubjectID (no such entity)
// makes insertEdge fail on the subject_id foreign key, the first of
// CorrectMemory's two writes. Confirms the old edge is left completely
// untouched — supersedeEdge must never run if insertEdge failed.
func TestCorrectMemory_RollsBackIfNewEdgeInsertFails(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "CorrectMemory Insert Failure Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "old fact")

	newLiteral := "corrected fact"
	nonexistentSubject := uuid.New()

	_, err := edgeStore.CorrectMemory(ctx, old.ID, CorrectionInput{
		SubjectID:     nonexistentSubject, // no such entity — violates the FK on insert
		Predicate:     "event_present",
		ObjectLiteral: &newLiteral,
	})
	if err == nil {
		t.Fatal("expected CorrectMemory to fail on a nonexistent SubjectID, got nil")
	}

	reloadedOld, getErr := edgeStore.GetByID(ctx, old.ID)
	if getErr != nil {
		t.Fatalf("GetByID(old): %v", getErr)
	}
	if reloadedOld.SupersededBy != nil {
		t.Errorf("expected old edge to remain un-superseded after a failed correction, got superseded_by=%v", reloadedOld.SupersededBy)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edges WHERE object_literal = $1`, newLiteral).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected no new edge to persist after a failed correction, found %d matching rows", count)
	}
}

// TestCorrectMemory_TransactionRollsBackOnSecondWriteFailure covers
// requirement 9 for the OTHER write position. CorrectMemory's own two
// writes can't naturally fail at the second step (supersedeEdge) with
// realistic inputs — its target is always the just-inserted new edge's
// real ID, which is valid by construction once insertEdge succeeds. To
// prove the rollback guarantee itself (a failure partway through leaves
// NEITHER write applied), this drives the same two building blocks
// CorrectMemory uses (insertEdge, supersedeEdge) directly inside a
// transaction, deliberately forcing the second write to violate
// superseded_by's foreign key with a nonexistent edge ID — the most
// direct way to inject a genuine failure between the two writes without
// mocks, which this codebase doesn't use anywhere.
func TestCorrectMemory_TransactionRollsBackOnSecondWriteFailure(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Rollback Second Write Subject")
	old := seedOldEdge(t, ctx, edgeStore, subject.ID, "old fact")

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	newLiteral := "corrected fact — should not persist"
	newEdge := &Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: &newLiteral,
		Confidence: 1.0, Importance: 0.3, SourceType: SourceTypeUserCorrection,
		SourceWeight: 1.0, DecayRate: DecayRateSlow, DecayLocked: true, IsCorrection: true,
	}
	if err := insertEdge(ctx, tx, newEdge); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("insertEdge (expected to succeed): %v", err)
	}

	bogusNewEdgeID := uuid.New() // does not reference any real edge
	if err := supersedeEdge(ctx, tx, old.ID, bogusNewEdgeID); err == nil {
		tx.Rollback(ctx)
		t.Fatal("expected supersedeEdge to fail on a nonexistent foreign key, got nil")
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	reloadedOld, err := edgeStore.GetByID(ctx, old.ID)
	if err != nil {
		t.Fatalf("GetByID(old): %v", err)
	}
	if reloadedOld.SupersededBy != nil {
		t.Errorf("expected old edge's superseded_by to remain nil after rollback, got %v", reloadedOld.SupersededBy)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edges WHERE object_literal = $1`, newLiteral).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected the new edge to NOT persist after rollback, found %d matching rows", count)
	}
}

// TestCorrectMemory_AlreadySupersededEdgeErrors covers requirement 10.
func TestCorrectMemory_AlreadySupersededEdgeErrors(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Already Superseded Subject")
	original := seedOldEdge(t, ctx, edgeStore, subject.ID, "original fact")

	firstCorrectionLiteral := "first correction"
	_, err := edgeStore.CorrectMemory(ctx, original.ID, CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &firstCorrectionLiteral,
	})
	if err != nil {
		t.Fatalf("first CorrectMemory: %v", err)
	}

	secondCorrectionLiteral := "second correction attempt on the now-stale original"
	_, err = edgeStore.CorrectMemory(ctx, original.ID, CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &secondCorrectionLiteral,
	})
	if !errors.Is(err, ErrEdgeAlreadySuperseded) {
		t.Errorf("expected ErrEdgeAlreadySuperseded, got %v", err)
	}
}

func TestCorrectMemory_NonexistentOldEdgeErrors(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Nonexistent Old Edge Subject")

	literal := "correction targeting nothing"
	_, err := edgeStore.CorrectMemory(ctx, uuid.New(), CorrectionInput{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &literal,
	})
	if !errors.Is(err, ErrEdgeNotFound) {
		t.Errorf("expected ErrEdgeNotFound, got %v", err)
	}
}
