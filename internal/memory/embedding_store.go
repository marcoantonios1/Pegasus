package memory

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// GetByMessageID returns the embedding for messageID, or (nil, nil) if
// none exists. Added for the write pipeline (internal/api), which needs
// to check idempotently whether a message already has an embedding
// before generating a new one — reprocessing or retrying the same
// message (e.g. reinforcement) must not produce duplicate embedding rows.
//
// This is a SELECT-then-INSERT check at the Go layer, not a DB-level
// guarantee: the embeddings table has no UNIQUE constraint on message_id
// (see migration 000004 — deliberately not added there, since embeddings
// are append-only "a changed message gets a new row" per Update's own
// doc comment below, and a unique constraint would conflict with that if
// re-embedding on content change is ever needed). That means this check
// is sufficient for the write pipeline's actual concern — sequential
// reprocessing/retry of the same message — but is not race-proof against
// two truly concurrent calls embedding the same message_id at once;
// nothing in this pipeline does that today.
func (s *EmbeddingStore) GetByMessageID(ctx context.Context, messageID uuid.UUID) (*Embedding, error) {
	var e Embedding
	var vec pgvector.Vector

	err := s.db.QueryRow(ctx, `
		SELECT id, message_id, vector
		FROM embeddings
		WHERE message_id = $1
		LIMIT 1
	`, messageID).Scan(&e.ID, &e.MessageID, &vec)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
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

// ReplaceForMessage atomically replaces every existing embedding row for
// messageID with a single new row holding vector — the delete-and-insert
// cmd/backfill_embeddings' --rebuild mode needs (api.RebuildEmbeddings),
// for re-embedding messages whose existing vector was generated without
// nomic-embed-text's required task prefix (the task-prefix fix: see
// extraction.EmbeddingDocumentPrefix's own doc comment).
//
// Both statements run in ONE transaction specifically so a message is
// NEVER observed with zero embedding rows mid-run: vector must already
// be the newly-computed one (the caller computes it BEFORE calling this
// — see RebuildEmbeddings' own doc comment for why that order matters),
// so the delete and insert here are purely the atomic swap, never the
// place a failed embedding call could leave a message with no rows at
// all. If either statement fails, the transaction rolls back and the
// message's OLD row is exactly as it was before this call — ReplaceForMessage
// never partially applies.
//
// Does not change EmbeddingStore's "embeddings are immutable, a changed
// message gets a new row" rule in general (see Update's own doc comment,
// still true for every other caller) — this is a narrow, deliberate
// exception for the one-time rebuild this fix ships, not a general
// in-place-update method; a normal re-embed (e.g. message content
// somehow changing) still has no supported path here.
func (s *EmbeddingStore) ReplaceForMessage(ctx context.Context, messageID uuid.UUID, vector []float32) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op once Commit succeeds

	if _, err := tx.Exec(ctx, `DELETE FROM embeddings WHERE message_id = $1`, messageID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO embeddings (message_id, vector) VALUES ($1, $2)
	`, messageID, pgvector.NewVector(vector)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Update is intentionally not implemented: embeddings are immutable once
// written. A changed message gets a new Embedding row, not an in-place
// update. ReplaceForMessage, above, is a narrow, named exception for the
// task-prefix fix's one-time rebuild — not a general-purpose reopening
// of that rule.
