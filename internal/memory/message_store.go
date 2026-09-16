package memory

import (
	"context"
	"time"

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

// GetByIDs returns every message in ids, in no particular order — used by
// WhyDoWeBelieveThis (internal/api, proposal §12) to resolve an edge's
// source_message_ids into the actual messages backing it (timestamps for
// "Seen", sender_id for "Mentioned by", media_type for the source-type
// breakdown). Returns an empty slice, not an error, for an empty or nil
// ids.
func (s *MessageStore) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Message, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.db.Query(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp
		FROM messages
		WHERE id = ANY($1)
	`, ids)
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

// ConversationIDsForSender returns every distinct conversation_id senderID
// has ever sent a message in, with no time restriction — used to resolve
// which conversation(s) belong to a given contact regardless of whether
// they've sent anything within whatever window a caller's stat
// computation cares about (see reflection.RecomputeRelationshipStats,
// proposal §10). Uses messages_sender_id_idx.
func (s *MessageStore) ConversationIDsForSender(ctx context.Context, senderID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT conversation_id FROM messages WHERE sender_id = $1
	`, senderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// GetByConversationSince returns every message in conversationID at or
// after since, ordered by timestamp ascending — the full interleaved
// thread, both directions. Uses messages_conversation_id_idx.
func (s *MessageStore) GetByConversationSince(ctx context.Context, conversationID uuid.UUID, since time.Time) ([]*Message, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp
		FROM messages
		WHERE conversation_id = $1 AND timestamp >= $2
		ORDER BY timestamp ASC
	`, conversationID, since)
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

// CountByConversationSince returns the total message count (both
// directions summed) per conversation_id, restricted to messages at or
// after since. Used to compute the population Marco's per-contact message
// frequency is normalized against (see
// reflection.RecomputeRelationshipStats, proposal §10) without having to
// hydrate every contact's full message rows just to count them.
func (s *MessageStore) CountByConversationSince(ctx context.Context, since time.Time) (map[uuid.UUID]int, error) {
	rows, err := s.db.Query(ctx, `
		SELECT conversation_id, count(*) FROM messages
		WHERE timestamp >= $1
		GROUP BY conversation_id
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]int)
	for rows.Next() {
		var id uuid.UUID
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		counts[id] = n
	}

	return counts, rows.Err()
}

// LastMessageTime returns the most recent message timestamp across every
// conversation in conversationIDs, or the zero time if none of them have
// any messages (including when conversationIDs is empty). Deliberately
// unwindowed: recency (see reflection.RecomputeRelationshipStats) needs to
// know how long ago the last real contact was even when that predates the
// trailing window used for frequency/reply-speed — "quiet for the last
// window" and "quiet for the last two years" are very different recency
// signals that a windowed query alone can't distinguish.
func (s *MessageStore) LastMessageTime(ctx context.Context, conversationIDs []uuid.UUID) (time.Time, error) {
	if len(conversationIDs) == 0 {
		return time.Time{}, nil
	}

	var t *time.Time
	if err := s.db.QueryRow(ctx, `
		SELECT max(timestamp) FROM messages WHERE conversation_id = ANY($1)
	`, conversationIDs).Scan(&t); err != nil {
		return time.Time{}, err
	}
	if t == nil {
		return time.Time{}, nil
	}

	return *t, nil
}
