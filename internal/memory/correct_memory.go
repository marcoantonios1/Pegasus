package memory

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// SourceTypeUserCorrection is the source_type CorrectMemory always writes.
// edges.source_type has no CHECK constraint (plain TEXT NOT NULL, verified
// against migration 000002) — this value was already documented as valid
// on Edge.SourceType's own comment, so no migration is needed to add it.
const SourceTypeUserCorrection = "user_correction"

// ErrEdgeNotFound is returned by CorrectMemory when oldEdgeID doesn't
// exist. Wraps pgx.ErrNoRows, so callers can check either this or
// pgx.ErrNoRows via errors.Is.
var ErrEdgeNotFound = errors.New("memory: edge not found")

// ErrEdgeAlreadySuperseded is returned by CorrectMemory when oldEdgeID has
// already been superseded by something else. Requirement 4's decision:
// error loudly rather than walk the supersession chain to find the
// current edge and correct that instead. A correction naming an
// already-superseded edge is most likely a caller bug (stale edge ID,
// double-submitted correction) — silently redirecting it to "whatever the
// chain currently resolves to" would hide that instead of surfacing it,
// and could correct the wrong thing if the chain moved somewhere the
// caller didn't expect. If Hermes-side code later has a real need to
// "correct whatever is currently true," that's a deliberate
// chain-walking helper to build then, not a fallback to guess at here.
var ErrEdgeAlreadySuperseded = errors.New("memory: cannot correct an edge that has already been superseded")

// CorrectionInput carries what a caller supplies to construct the
// correcting edge. Deliberately does NOT include Confidence, DecayLocked,
// IsCorrection, or SourceType — CorrectMemory hardcodes those itself (see
// its doc comment) specifically so a careless caller can't pass something
// other than the fixed values a correction always has.
type CorrectionInput struct {
	SubjectID        uuid.UUID
	Predicate        string
	ObjectID         *uuid.UUID  // exactly one of ObjectID/ObjectLiteral, same XOR rule as any edge
	ObjectLiteral    *string
	SourceMessageIDs []uuid.UUID // the message(s) where Marco issued the correction

	// Importance, if nil, is inherited from the edge being corrected — the
	// correction changes how CERTAIN the fact is, not how much it matters,
	// so the old edge's importance carries over by default. Pass a
	// non-nil override for the rare case where the correction itself
	// reveals the fact matters more (or less) than originally scored.
	Importance *float64
}

// CorrectMemory implements proposal §7.6's dedicated correction write
// path. A correction from Marco does not compete as a normal extracted
// edge through decay/confidence math — it short-circuits the pipeline:
//
//   - Confidence is hardcoded to 1.0 here, not derived from
//     CorrectionInput and not passed through ApplySourceWeight (the
//     previous issue's extraction-confidence path) — a correction's
//     confidence is set directly because Marco said so, not computed from
//     any model's self-reported score.
//   - DecayLocked = true and IsCorrection = true, both hardcoded.
//   - SourceType = SourceTypeUserCorrection, hardcoded.
//   - SourceWeight = 1.0: a correction IS the source of truth: no trust
//     discount applies the way ApplySourceWeight discounts an inferred
//     extraction. This is a provenance/audit value (feeds
//     WhyDoWeBelieveThis(), §12), not a multiplier applied here — nothing
//     multiplies it against anything since Confidence is already fixed.
//   - The OLD edge is superseded via superseded_by, the same mechanism
//     §7.2 uses for any contradiction, just triggered explicitly instead
//     of by a competing extraction. General inferred-contradiction
//     detection (§7.2's broader "two competing edges, older one decays"
//     flow) is NOT built here — that's a separate mechanism/issue. This
//     function does only the minimal supersession write this specific
//     path needs (see supersedeEdge) — if/when the general contradiction
//     logic is built as reusable code, that write should likely move to
//     share it rather than these two implementations diverging.
//
// Both writes (the new edge, the old edge's supersession) happen in one
// transaction: either both apply or neither does. A correction that
// half-applies — a new maximum-confidence edge written but the old,
// wrong one still reads as current, or vice versa — is worse than no
// correction at all.
//
// No validation that the new edge's subject/predicate matches the old
// edge's — a correction may legitimately change either ("that's not
// Sara's birthday, that's her sister's").
func (s *EdgeStore) CorrectMemory(ctx context.Context, oldEdgeID uuid.UUID, correction CorrectionInput) (*Edge, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin correction transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds

	old, err := lockEdgeForUpdate(ctx, tx, oldEdgeID)
	if err != nil {
		return nil, err
	}
	if old.SupersededBy != nil {
		return nil, ErrEdgeAlreadySuperseded
	}

	importance := old.Importance
	if correction.Importance != nil {
		importance = *correction.Importance
	}

	sourceMessageIDs := correction.SourceMessageIDs
	if sourceMessageIDs == nil {
		sourceMessageIDs = []uuid.UUID{}
	}

	newEdge := &Edge{
		SubjectID:        correction.SubjectID,
		Predicate:        correction.Predicate,
		ObjectID:         correction.ObjectID,
		ObjectLiteral:    correction.ObjectLiteral,
		Confidence:       1.0,
		Importance:       importance,
		SourceType:       SourceTypeUserCorrection,
		SourceWeight:     1.0,
		SourceMessageIDs: sourceMessageIDs,
		DecayRate:        DefaultDecayRate(correction.Predicate),
		DecayLocked:      true,
		IsCorrection:     true,
	}

	if err := insertEdge(ctx, tx, newEdge); err != nil {
		return nil, fmt.Errorf("insert correcting edge: %w", err)
	}

	if err := supersedeEdge(ctx, tx, oldEdgeID, newEdge.ID); err != nil {
		return nil, fmt.Errorf("supersede corrected edge: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit correction transaction: %w", err)
	}

	return newEdge, nil
}

// lockEdgeForUpdate loads an edge inside tx with FOR UPDATE, so two
// concurrent corrections targeting the same edge can't both read
// SupersededBy as nil and both proceed — the second waits for the first
// transaction to commit or roll back, then sees its actual, current
// SupersededBy value.
func lockEdgeForUpdate(ctx context.Context, tx queryer, id uuid.UUID) (*Edge, error) {
	var e Edge

	err := tx.QueryRow(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(
		&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
		&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
		&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
		&e.SupersededBy, &e.IsCorrection,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEdgeNotFound, err)
	}

	return &e, nil
}

// supersedeEdge is the minimal write CorrectMemory needs: point oldEdgeID
// at newEdgeID. This is NOT a general contradiction-handling helper —
// §7.2's broader inferred-contradiction flow (a new extracted edge
// competing with an existing one, the older one's confidence dropping
// sharply) is out of scope for this issue. If that general mechanism gets
// built later as reusable code, this call site is the one to point at it
// so the two supersession paths don't diverge into different behavior.
func supersedeEdge(ctx context.Context, q queryer, oldEdgeID, newEdgeID uuid.UUID) error {
	_, err := q.Exec(ctx, `UPDATE edges SET superseded_by = $1 WHERE id = $2`, newEdgeID, oldEdgeID)
	return err
}
