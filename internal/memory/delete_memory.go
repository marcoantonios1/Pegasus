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

// IsTombstone reports whether e is a deletion marker — the ONE explicit
// definition "deleted" has anywhere in this schema (there is no
// is_deleted column; see this file's own doc comment on SoftDelete for
// why source_type is the marker instead). Checks SourceType only, by
// design: confidence=0/importance=0 also happen to be true of a
// tombstone (SoftDelete sets them), but defining "deleted" in terms of
// those would conflate it with "a real, non-deleted fact that simply
// decayed or was scored unimportant" — a genuinely different case
// SearchMemory's own doc comment is explicit must still be returned by a
// keyword lookup. source_type is the only field that distinguishes "this
// row IS the deletion" from "this row just happens to score low."
func IsTombstone(e Edge) bool {
	return e.SourceType == SourceTypeUserDeletion
}

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
// unlike a real correction — semantically a tombstone is neither of
// those things.
//
// Exclusion from default retrieval is now EXPLICIT, not incidental: see
// IsTombstone and memory.searchFiltersWhere, which SearchRelevant and
// SearchByText both build on specifically so they can't diverge. Before
// that fix, the only thing that excluded a tombstone from
// GetRelevantContext was the SIDE EFFECT of Confidence=0/Importance=0
// falling below IsPruneCandidate's floors — which meant SearchByText
// (SearchMemory's own query, which deliberately does NOT apply
// IsPruneCandidate — see that method's own doc comment) had nothing at
// all keeping a tombstone out, and a deleted fact's keyword search
// stayed findable forever. Confidence=0/Importance=0 remain the correct
// values for a tombstone to hold regardless (nothing about this fact is
// true or important anymore) — they're just no longer what actually does
// the excluding.
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
