package memory

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EntityStore struct {
	db *pgxpool.Pool
}

func NewEntityStore(db *pgxpool.Pool) *EntityStore {
	return &EntityStore{db: db}
}

func (s *EntityStore) Create(ctx context.Context, e *Entity) error {
	// metadata is NOT NULL DEFAULT '{}'::jsonb; a nil Go map encodes as SQL
	// NULL rather than the JSON object the column requires, so normalize it.
	if e.Metadata == nil {
		e.Metadata = map[string]any{}
	}

	return s.db.QueryRow(ctx, `
		INSERT INTO entities (type, canonical_name, is_self, metadata)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at
	`,
		e.Type,
		e.CanonicalName,
		e.IsSelf,
		e.Metadata,
	).Scan(&e.ID, &e.CreatedAt, &e.UpdatedAt)
}

func (s *EntityStore) GetByID(ctx context.Context, id uuid.UUID) (*Entity, error) {
	var e Entity

	err := s.db.QueryRow(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE id = $1
	`, id).Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &e, nil
}

// GetByIDs returns every entity in ids, in no particular order — used by
// WhyDoWeBelieveThis (internal/api, proposal §12) to resolve message
// sender_ids to canonical_name for the "Mentioned by" breakdown. Returns
// an empty slice, not an error, for an empty or nil ids.
func (s *EntityStore) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Entity, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.db.Query(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE id = ANY($1)
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entities []*Entity
	for rows.Next() {
		var e Entity
		if err := rows.Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		entities = append(entities, &e)
	}

	return entities, rows.Err()
}

func (s *EntityStore) Update(ctx context.Context, e *Entity) error {
	if e.Metadata == nil {
		e.Metadata = map[string]any{}
	}

	return s.db.QueryRow(ctx, `
		UPDATE entities
		SET type = $1, canonical_name = $2, is_self = $3, metadata = $4, updated_at = now()
		WHERE id = $5
		RETURNING updated_at
	`,
		e.Type,
		e.CanonicalName,
		e.IsSelf,
		e.Metadata,
		e.ID,
	).Scan(&e.UpdatedAt)
}

func (s *EntityStore) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `DELETE FROM entities WHERE id = $1`, id)
	return err
}
