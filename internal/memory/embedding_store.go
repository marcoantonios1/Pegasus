package memory

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
)

// EmbeddingStore requires the pgvector pgx codec to be registered on the
// pool's connections (e.g. via pgxpool.Config.AfterConnect calling
// pgxvec "github.com/pgvector/pgvector-go/pgx".RegisterTypes), so that the
// vector column round-trips through pgvector.Vector without manual casting.
type EmbeddingStore struct {
	db *pgxpool.Pool
}

func NewEmbeddingStore(db *pgxpool.Pool) *EmbeddingStore {
	return &EmbeddingStore{db: db}
}

func (s *EmbeddingStore) Create(ctx context.Context, e *Embedding) error {
	return s.db.QueryRow(ctx, `
		INSERT INTO embeddings (message_id, vector)
		VALUES ($1, $2)
		RETURNING id
	`,
		e.MessageID,
		pgvector.NewVector(e.Vector),
	).Scan(&e.ID)
}

func (s *EmbeddingStore) GetByID(ctx context.Context, id uuid.UUID) (*Embedding, error) {
	var e Embedding
	var vec pgvector.Vector

	err := s.db.QueryRow(ctx, `
		SELECT id, message_id, vector
		FROM embeddings
		WHERE id = $1
	`, id).Scan(&e.ID, &e.MessageID, &vec)
	if err != nil {
		return nil, err
	}

	e.Vector = vec.Slice()

	return &e, nil
}

func (s *EmbeddingStore) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `DELETE FROM embeddings WHERE id = $1`, id)
	return err
}

// Update is intentionally not implemented: embeddings are immutable once
// written. A changed message gets a new Embedding row, not an in-place update.
