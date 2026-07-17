package memory

import (
	"context"

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

// Delete is intentionally not implemented: edges are never hard-deleted per
// the proposal's data model. Superseding an edge is done via SupersededBy,
// set through Update.
