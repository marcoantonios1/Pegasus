package memory

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// SourceTypeUserDeletion is the source_type SoftDelete writes on the
// tombstone edge it creates — distinct from SourceTypeUserCorrection
// (correct_memory.go): a deletion isn't asserting a different fact is
// true, it's asserting there is no fact here anymore.
const SourceTypeUserDeletion = "user_deletion"

// SoftDelete implements api.DeleteMemory's storage layer. Edges are never
// hard-deleted (see EdgeStore.Delete's own doc comment, below) — no
// schema column exists for "deleted" any more than one exists for
// "pruned" (see prune_candidate.go). "Delete" here means: write a
// zero-confidence, zero-importance tombstone edge for the same (subject,
// predicate, object) triple, and supersede oldEdgeID with it — the same
// insert-new-edge-then-supersede-old-edge transaction shape CorrectMemory
// uses (correct_memory.go), reusing its unexported lockEdgeForUpdate/
// supersedeEdge/insertEdge helpers directly rather than duplicating that
// transaction logic, WITHOUT modifying CorrectMemory itself. This is a
// deliberate distinct decision from CorrectMemory, not a call to it:
// CorrectMemory hardcodes Confidence=1.0 (a correction is maximally
// trusted) and DecayLocked=true/IsCorrection=true — none of which is what
// a deletion needs, so this is its own small transaction, not a
// CorrectMemory call followed by a patch-up.
//
// Deliberately does NOT set DecayLocked or IsCorrection on the tombstone,
// unlike a real correction. IsPruneCandidate (prune_candidate.go)
// short-circuits to "never prune" when either flag is true — setting
// either here would make the tombstone immune to the very default-
// retrieval exclusion DeleteMemory exists to produce. Confidence=0 and
// Importance=0 already put the tombstone below both of
// IsPruneCandidate's floors through its normal (non-short-circuited)
// path — that's what actually does the excluding.
//
// The tombstone carries no source_message_ids: this is an API-driven
// action (a caller explicitly asking to delete a fact), not something
// extracted from a message, so there is no new source message to
// attribute it to — and copying the OLD edge's provenance onto a
// zeroed-out fact would misrepresent what those messages actually said.
//
// Returns ErrEdgeAlreadySuperseded (same sentinel CorrectMemory uses) if
// oldEdgeID has already been superseded — deleting an already-superseded
// (or already-deleted) edge is a caller bug, surfaced rather than
// silently redirected, same reasoning as CorrectMemory's own doc comment.
func (s *EdgeStore) SoftDelete(ctx context.Context, oldEdgeID uuid.UUID) (*Edge, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin delete transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds

	old, err := lockEdgeForUpdate(ctx, tx, oldEdgeID)
	if err != nil {
		return nil, err
	}
	if old.SupersededBy != nil {
		return nil, ErrEdgeAlreadySuperseded
	}

	tombstone := &Edge{
		SubjectID:     old.SubjectID,
		Predicate:     old.Predicate,
		ObjectID:      old.ObjectID,
		ObjectLiteral: old.ObjectLiteral,
		Confidence:    0,
		Importance:    0,
		SourceType:    SourceTypeUserDeletion,
		SourceWeight:  1.0,
		DecayRate:     DefaultDecayRate(old.Predicate),
	}

	if err := insertEdge(ctx, tx, tombstone); err != nil {
		return nil, fmt.Errorf("insert tombstone edge: %w", err)
	}
	if err := supersedeEdge(ctx, tx, oldEdgeID, tombstone.ID); err != nil {
		return nil, fmt.Errorf("supersede deleted edge: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit delete transaction: %w", err)
	}

	return tombstone, nil
}
