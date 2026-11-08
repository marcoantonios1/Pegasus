package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ErrMergeConflict is returned by MergeDuplicateEdges when re-validating
// the candidates UNDER THE LOCK finds the caller's view no longer
// matches reality: a row that no longer exists, is already superseded,
// is a correction, is decay_locked, or is a tombstone (memory.IsTombstone)
// — any edge that must never be merged, whether that was already true
// when the caller built its candidate set or became true in the
// meantime. Nothing is written when this is returned — the same
// "surface a stale/invalid caller assumption, don't silently redirect"
// principle as ErrEdgeAlreadySuperseded (correct_memory.go). Distinct
// from a genuine infrastructure error (a failed Begin/Exec/Commit),
// which this method returns unwrapped — a caller (reflection.
// MergeDuplicates) treats the two very differently: this cluster's merge
// didn't happen because the data moved, which is fine to skip and move
// on from; an infra error means something is actually wrong and the
// whole pass should stop.
var ErrMergeConflict = errors.New("memory: duplicate-edge merge candidates no longer valid")

// MergeDuplicateEdges implements reflection.MergeDuplicates' actual
// database write, as ONE transaction, following the CorrectMemory/
// SoftDelete transaction pattern exactly (correct_memory.go/
// delete_memory.go): lock every row FOR UPDATE first, re-validate
// against the FRESH locked rows (never a caller-supplied snapshot),
// write narrow statements only, commit or roll back as one unit.
//
// This is the fix for three real bugs found in review of the previous,
// non-transactional implementation (dedup.go's old mergeCluster):
//
//  1. last_reinforced could move BACKWARDS. The old code computed its
//     "latest confirmation" from FirstSeen only — never LastReinforced —
//     then wrote it unconditionally. An edge first seen 100 days ago but
//     reinforced yesterday, merged with an in-window duplicate also
//     first seen around then, had its last_reinforced overwritten with
//     something close to 100 days ago: EffectiveConfidence silently
//     crashed (observed: confidence 0.90 -> 0.57 in the reproducer).
//     Fixed here by computing from LastReinforced (across the survivor
//     AND every duplicate, read fresh under the lock) and writing via
//     GREATEST, the same order-independence Reinforce already uses.
//  2. No protection for locked edges. The old code's only filter was
//     SupersededBy != nil — an is_correction or decay_locked edge (or,
//     after the delete-exclusion fix, a tombstone) could be silently
//     absorbed as a duplicate, or even become the chosen survivor with
//     an ordinary edge superseded into it. Re-validated here, under the
//     lock, as its own exclusion (requirement 1b) — reflection.
//     MergeDuplicates ALSO excludes these before clustering even starts
//     (its own doc comment has that reasoning), so this is defense in
//     depth against a candidate becoming locked between when the caller
//     built its set and when this transaction actually runs, not the
//     only place this is checked.
//  3. Not transactional, and each write touched the WHOLE row. Three
//     separate statements (Update survivor, Reinforce survivor, Update
//     each duplicate) could each fail independently, leaving a
//     half-merged state with no rollback. Worse, Update() writes every
//     column from the caller's in-memory snapshot — a concurrent
//     reinforcement or UpdateMemory importance change made between
//     candidate-loading and this running would be silently clobbered
//     (lost update), and dedup runs concurrently with live ingestion, so
//     this was a real, not hypothetical, race. Fixed by doing everything
//     in one transaction with FOR UPDATE locks, and writing ONLY
//     source_message_ids/last_reinforced on the survivor and
//     superseded_by on each duplicate — never a full-row Update(), so
//     nothing else on any edge can be clobbered by this method.
//
// Deliberately does NOT touch first_seen, confidence, importance, or any
// other field — the survivor keeps its own object_literal and every
// other field exactly as it was (existing rule, unchanged).
//
// ids are locked in a deterministic order (sorted) specifically so two
// concurrent merges that happen to share an edge can never deadlock by
// locking the same two rows in opposite orders.
//
// Returns the survivor's post-merge state (SourceMessageIDs and
// LastReinforced updated; every other field exactly as it was) on
// success — the AUTHORITATIVE values, computed from the rows this
// method locked and re-validated itself, not something the caller needs
// to (and must not) recompute independently from its own possibly-stale
// view of the same edges.
func (s *EdgeStore) MergeDuplicateEdges(ctx context.Context, survivorID uuid.UUID, duplicateIDs []uuid.UUID) (*Edge, error) {
	if len(duplicateIDs) == 0 {
		return nil, nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin merge transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds

	ids := append([]uuid.UUID{survivorID}, duplicateIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	locked := make(map[uuid.UUID]*Edge, len(ids))
	for _, id := range ids {
		e, lockErr := lockEdgeForUpdate(ctx, tx, id)
		if lockErr != nil {
			// lockEdgeForUpdate now distinguishes a genuinely missing row
			// (ErrEdgeNotFound, wrapping pgx.ErrNoRows) from every other
			// failure — see its own doc comment for the fix (it used to
			// wrap EVERYTHING as ErrEdgeNotFound, which would have made
			// this check silently convert a real infrastructure failure,
			// e.g. a connection error or a deadlock, into ErrMergeConflict
			// below — downgrading a hard failure the caller
			// (reflection.MergeDuplicates) should stop on into a benign
			// skip). A genuinely missing row IS exactly the kind of
			// "caller's view no longer matches reality" this method's own
			// ErrMergeConflict exists for (edges are never hard-deleted,
			// so in practice this means a stale/invalid candidate ID, not
			// data that actually vanished) — anything else is a real
			// error and must propagate as one.
			if errors.Is(lockErr, ErrEdgeNotFound) {
				return nil, ErrMergeConflict
			}
			return nil, fmt.Errorf("lock edge %s: %w", id, lockErr)
		}
		locked[id] = e
	}

	// Re-validate every row UNDER THE LOCK (requirement 1b) — never the
	// caller's snapshot, which may have been loaded before a
	// reinforcement, a correction, a deletion, or a prior merge touched
	// one of these rows.
	for _, id := range ids {
		e := locked[id]
		if e.SupersededBy != nil || e.IsCorrection || e.DecayLocked || IsTombstone(*e) {
			return nil, ErrMergeConflict
		}
	}

	survivor := locked[survivorID]

	// Merged source_message_ids: ordered DISTINCT union, survivor's own
	// IDs first, then each duplicate's in the caller's given order — the
	// old code appended duplicates' IDs blindly, which could introduce
	// repeats when edges shared a source message.
	seen := make(map[uuid.UUID]bool, len(survivor.SourceMessageIDs))
	merged := make([]uuid.UUID, 0, len(survivor.SourceMessageIDs))
	for _, id := range survivor.SourceMessageIDs {
		if !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	for _, dupID := range duplicateIDs {
		for _, id := range locked[dupID].SourceMessageIDs {
			if !seen[id] {
				seen[id] = true
				merged = append(merged, id)
			}
		}
	}
	if merged == nil {
		merged = []uuid.UUID{}
	}

	// Merged last_reinforced: the MAX across the survivor AND every
	// duplicate (requirement 1c) — not FirstSeen, which is what the old
	// code used and is what moved this backwards.
	maxLastReinforced := survivor.LastReinforced
	for _, dupID := range duplicateIDs {
		if lr := locked[dupID].LastReinforced; lr.After(maxLastReinforced) {
			maxLastReinforced = lr
		}
	}

	// Narrow write #1: survivor's source_message_ids and last_reinforced
	// only. GREATEST is belt-and-suspenders given the row is already
	// locked (nothing else can be concurrently modifying it right now),
	// but costs nothing and matches Reinforce's own order-independence
	// contract for anyone reading this column's update rule in isolation.
	if _, err := tx.Exec(ctx, `
		UPDATE edges SET source_message_ids = $1, last_reinforced = GREATEST(last_reinforced, $2)
		WHERE id = $3
	`, merged, maxLastReinforced, survivorID); err != nil {
		return nil, fmt.Errorf("update survivor %s: %w", survivorID, err)
	}

	// Narrow write #2, one per duplicate: superseded_by only.
	for _, dupID := range duplicateIDs {
		if err := supersedeEdge(ctx, tx, dupID, survivorID); err != nil {
			return nil, fmt.Errorf("supersede duplicate %s: %w", dupID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit merge transaction: %w", err)
	}

	survivor.SourceMessageIDs = merged
	survivor.LastReinforced = maxLastReinforced
	return survivor, nil
}
