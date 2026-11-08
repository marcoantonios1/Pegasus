package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// TestSoftDelete_NonexistentOldEdgeErrors covers requirement 8: SoftDelete
// on a nonexistent edge must still return ErrEdgeNotFound (and
// pgx.ErrNoRows) — the same lockEdgeForUpdate error-classification fix
// CorrectMemory's own equivalent test covers, since SoftDelete reuses
// lockEdgeForUpdate directly rather than duplicating its logic.
func TestSoftDelete_NonexistentOldEdgeErrors(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	edgeStore := NewEdgeStore(pool)

	_, err := edgeStore.SoftDelete(ctx, uuid.New())
	if !errors.Is(err, ErrEdgeNotFound) {
		t.Errorf("expected ErrEdgeNotFound, got %v", err)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("expected errors.Is(err, pgx.ErrNoRows) to also hold, got %v", err)
	}
}

// TestSoftDelete_AlreadySupersededEdgeErrors is the SoftDelete analog of
// TestCorrectMemory_AlreadySupersededEdgeErrors — a deletion targeting an
// already-superseded edge is a caller bug, surfaced rather than silently
// redirected.
func TestSoftDelete_AlreadySupersededEdgeErrors(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "SoftDelete Already Superseded Subject")
	original := seedOldEdge(t, ctx, edgeStore, subject.ID, "original fact "+uuid.New().String())

	if _, err := edgeStore.SoftDelete(ctx, original.ID); err != nil {
		t.Fatalf("first SoftDelete: %v", err)
	}

	_, err := edgeStore.SoftDelete(ctx, original.ID)
	if !errors.Is(err, ErrEdgeAlreadySuperseded) {
		t.Errorf("expected ErrEdgeAlreadySuperseded, got %v", err)
	}
}
