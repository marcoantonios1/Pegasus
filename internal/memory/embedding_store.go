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

// SearchSimilarMessages returns the topN messages whose embedding is
// closest (cosine similarity) to queryVector, ordered nearest first —
// SearchSemantic's underlying query (internal/api, proposal §13): pure
// vector search over raw message content, distinct from
// EdgeStore.SearchRelevant's structured/graph retrieval over edges.
//
// Requires the pgvector pgx codec registered on the pool (see this
// store's own doc comment above) — already a hard requirement for any
// EmbeddingStore use, unlike EdgeStore.SearchRelevant where it's new.
func (s *EmbeddingStore) SearchSimilarMessages(ctx context.Context, queryVector []float32, topN int) ([]*Message, error) {
	rows, err := s.db.Query(ctx, `
		SELECT m.id, m.conversation_id, m.sender_id, m.media_type, m.raw_text, m.transcript,
			m.transcript_confidence, m.media_ref, m.processed, m.timestamp
		FROM embeddings em
		JOIN messages m ON m.id = em.message_id
		ORDER BY em.vector <=> $1
		LIMIT $2
	`, pgvector.NewVector(queryVector), topN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(
			&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp,
		); err != nil {
			return nil, err
		}
		msgs = append(msgs, &m)
	}

	return msgs, rows.Err()
}

func (s *EmbeddingStore) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `DELETE FROM embeddings WHERE id = $1`, id)
	return err
}

// Update is intentionally not implemented: embeddings are immutable once
// written. A changed message gets a new Embedding row, not an in-place update.
