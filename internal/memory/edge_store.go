package memory

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EdgeStore struct {
	db *pgxpool.Pool
}

func NewEdgeStore(db *pgxpool.Pool) *EdgeStore {
	return &EdgeStore{db: db}
}

func (s *EdgeStore) Create(ctx context.Context, e *Edge) error {
	// source_message_ids is NOT NULL DEFAULT '{}'; a nil Go slice encodes as
	// SQL NULL rather than an empty array, so normalize it.
	if e.SourceMessageIDs == nil {
		e.SourceMessageIDs = []uuid.UUID{}
	}

	return s.db.QueryRow(ctx, `
		INSERT INTO edges (
			subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			decay_rate, decay_locked, superseded_by, is_correction
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
		)
		RETURNING id, first_seen, last_reinforced
	`,
		e.SubjectID,
		e.Predicate,
		e.ObjectID,
		e.ObjectLiteral,
		e.Confidence,
		e.Importance,
		e.SourceType,
		e.SourceWeight,
		e.SourceMessageIDs,
		e.DecayRate,
		e.DecayLocked,
		e.SupersededBy,
		e.IsCorrection,
	).Scan(&e.ID, &e.FirstSeen, &e.LastReinforced)
}

func (s *EdgeStore) GetByID(ctx context.Context, id uuid.UUID) (*Edge, error) {
	var e Edge

	err := s.db.QueryRow(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE id = $1
	`, id).Scan(
		&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
		&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
		&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
		&e.SupersededBy, &e.IsCorrection,
	)
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (s *EdgeStore) Update(ctx context.Context, e *Edge) error {
	if e.SourceMessageIDs == nil {
		e.SourceMessageIDs = []uuid.UUID{}
	}

	_, err := s.db.Exec(ctx, `
		UPDATE edges
		SET
			subject_id = $1, predicate = $2, object_id = $3, object_literal = $4,
			confidence = $5, importance = $6, source_type = $7, source_weight = $8,
			source_message_ids = $9, last_reinforced = $10, decay_rate = $11,
			decay_locked = $12, superseded_by = $13, is_correction = $14
		WHERE id = $15
	`,
		e.SubjectID,
		e.Predicate,
		e.ObjectID,
		e.ObjectLiteral,
		e.Confidence,
		e.Importance,
		e.SourceType,
		e.SourceWeight,
		e.SourceMessageIDs,
		e.LastReinforced,
		e.DecayRate,
		e.DecayLocked,
		e.SupersededBy,
		e.IsCorrection,
		e.ID,
	)

	return err
}

// GetBySubjectAndPredicate uses the (subject_id, predicate) index from
// migration 000002. This is the retrieval pattern GetRelevantContext() will
// build on.
func (s *EdgeStore) GetBySubjectAndPredicate(ctx context.Context, subjectID uuid.UUID, predicate string) ([]*Edge, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE subject_id = $1 AND predicate = $2
	`, subjectID, predicate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var edges []*Edge
	for rows.Next() {
		var e Edge

		if err := rows.Scan(
			&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
			&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
			&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
			&e.SupersededBy, &e.IsCorrection,
		); err != nil {
			return nil, err
		}

		edges = append(edges, &e)
	}

	return edges, rows.Err()
}

// Reinforce updates only last_reinforced — nothing else. Kept separate
// from Update() specifically so reinforcing an edge can never accidentally
// rewrite confidence, predicate, or any other field via a stray
// full-object update; this method's whole contract is "touch the
// timestamp, nothing more."
//
// now is taken as an explicit parameter (matching Update's existing
// pattern of binding e.LastReinforced directly rather than relying on SQL
// now()) rather than letting Postgres own the value — this keeps the
// method deterministically testable without wall-clock sleeps.
//
// decay_locked edges ARE reinforced by this method: last_reinforced still
// updates, since a correction being independently reconfirmed later is
// still real information worth recording. What decay_locked actually
// protects is confidence, which this method never touches regardless of
// the flag — see EffectiveConfidence, which short-circuits on
// DecayLocked before ever consulting LastReinforced. Reinforcing a locked
// edge's timestamp doesn't weaken that protection.
func (s *EdgeStore) Reinforce(ctx context.Context, edgeID uuid.UUID, now time.Time) error {
	_, err := s.db.Exec(ctx, `
		UPDATE edges SET last_reinforced = $1 WHERE id = $2
	`, now, edgeID)
	return err
}

// FindMatchingEdge looks for an existing edge with the same subject_id,
// predicate, and object (object_id or object_literal — exact match only;
// near-duplicate fuzzy merging of similar-but-not-identical objects is the
// Reflection Engine's job, §9.2, not this) as the given values. Returns
// (nil, nil) if no match exists. Used to decide whether a newly extracted
// triple should reinforce an existing edge (Reinforce) or become a new one
// (Create) — that decision itself is the caller's, not this method's.
func (s *EdgeStore) FindMatchingEdge(ctx context.Context, subjectID uuid.UUID, predicate string, objectID *uuid.UUID, objectLiteral *string) (*Edge, error) {
	candidates, err := s.GetBySubjectAndPredicate(ctx, subjectID, predicate)
	if err != nil {
		return nil, err
	}

	for _, e := range candidates {
		if edgeObjectMatches(e, objectID, objectLiteral) {
			return e, nil
		}
	}

	return nil, nil
}

// edgeObjectMatches reports whether e's object (object_id or
// object_literal — the schema's XOR constraint guarantees exactly one of
// e's two is set) exactly matches the given objectID/objectLiteral. Exact
// match only, by design — see FindMatchingEdge.
func edgeObjectMatches(e *Edge, objectID *uuid.UUID, objectLiteral *string) bool {
	if objectID != nil && e.ObjectID != nil {
		return *objectID == *e.ObjectID
	}
	if objectLiteral != nil && e.ObjectLiteral != nil {
		return *objectLiteral == *e.ObjectLiteral
	}
	return false
}

// Delete is intentionally not implemented: edges are never hard-deleted per
// the proposal's data model. Superseding an edge is done via SupersededBy,
// set through Update.
