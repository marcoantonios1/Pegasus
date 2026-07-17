package memory

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MessageStore struct {
	db *pgxpool.Pool
}

func NewMessageStore(db *pgxpool.Pool) *MessageStore {
	return &MessageStore{db: db}
}

func (s *MessageStore) Create(ctx context.Context, m *Message) error {
	return s.db.QueryRow(ctx, `
		INSERT INTO messages (
			conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
		RETURNING id
	`,
		m.ConversationID,
		m.SenderID,
		m.MediaType,
		m.RawText,
		m.Transcript,
		m.TranscriptConfidence,
		m.MediaRef,
		m.Processed,
		m.Timestamp,
	).Scan(&m.ID)
}

func (s *MessageStore) GetByID(ctx context.Context, id uuid.UUID) (*Message, error) {
	var m Message

	err := s.db.QueryRow(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp
		FROM messages
		WHERE id = $1
	`, id).Scan(
		&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
		&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp,
	)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *MessageStore) Update(ctx context.Context, m *Message) error {
	_, err := s.db.Exec(ctx, `
		UPDATE messages
		SET
			conversation_id = $1, sender_id = $2, media_type = $3, raw_text = $4,
			transcript = $5, transcript_confidence = $6, media_ref = $7,
			processed = $8, timestamp = $9
		WHERE id = $10
	`,
		m.ConversationID,
		m.SenderID,
		m.MediaType,
		m.RawText,
		m.Transcript,
		m.TranscriptConfidence,
		m.MediaRef,
		m.Processed,
		m.Timestamp,
		m.ID,
	)

	return err
}

// Delete is intentionally not implemented: messages are never hard-deleted
// per the proposal's data model.
