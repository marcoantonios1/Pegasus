package memory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
			transcript_confidence, media_ref, processed, timestamp, external_id
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
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
		m.ExternalID,
	).Scan(&m.ID)
}

// CreateIfAbsent is Create's idempotent counterpart (the message-ingestion
// idempotency fix): if m.ExternalID is set and a row already exists for
// (m.ConversationID, *m.ExternalID) — the partial unique index migration
// 000007 added — nothing new is inserted; instead m is overwritten in
// place with the EXISTING row's full contents (ID, Processed, Transcript/
// TranscriptConfidence in particular — a caller resuming an interrupted
// import needs to see what's actually stored, not what it was about to
// write), and created=false is returned. Otherwise a new row is inserted
// exactly like Create, and created=true.
//
// Atomic by construction, not check-then-insert: a single INSERT ...
// ON CONFLICT ... DO NOTHING, falling back to a SELECT only when nothing
// was actually inserted. This matters because message ingestion is NOT
// single-threaded in practice — Whatsmeow can redeliver a live message
// while a historical re-import of the same conversation is also running,
// and a naive "SELECT, then INSERT if not found" would race: both could
// see "not found" and both insert, defeating the whole point of this
// method.
//
// m.ExternalID == nil (an adapter had no real ID for this message — see
// Message.ExternalID's own doc comment) always inserts: there's no dedup
// key to conflict on, so this degrades to exactly Create's behavior,
// created=true unconditionally. Callers are expected to log this case
// themselves (see internal/api's messageExternalID) — this method has no
// visibility into WHY ExternalID is nil, only that it is.
func (s *MessageStore) CreateIfAbsent(ctx context.Context, m *Message) (created bool, err error) {
	if m.ExternalID == nil {
		if err := s.Create(ctx, m); err != nil {
			return false, err
		}
		return true, nil
	}

	err = s.db.QueryRow(ctx, `
		INSERT INTO messages (
			conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp, external_id
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		ON CONFLICT (conversation_id, external_id) WHERE external_id IS NOT NULL DO NOTHING
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
		m.ExternalID,
	).Scan(&m.ID)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}

	// ON CONFLICT ... DO NOTHING inserted nothing (RETURNING produced no
	// row) — the conflicting row already exists; load it so the caller's
	// m reflects what's actually stored.
	existing, err := s.getByConversationAndExternalID(ctx, m.ConversationID, *m.ExternalID)
	if err != nil {
		return false, fmt.Errorf("load existing message (conversation_id=%s external_id=%s): %w", m.ConversationID, *m.ExternalID, err)
	}
	*m = *existing
	return false, nil
}

// getByConversationAndExternalID is CreateIfAbsent's own lookup for the
// "already exists" branch — not exported, since (conversation_id,
// external_id) is an ingestion-pipeline concept (adapter-provided IDs),
// not a general query any other caller has needed so far.
func (s *MessageStore) getByConversationAndExternalID(ctx context.Context, conversationID uuid.UUID, externalID string) (*Message, error) {
	var m Message

	err := s.db.QueryRow(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp, external_id
		FROM messages
		WHERE conversation_id = $1 AND external_id = $2
	`, conversationID, externalID).Scan(
		&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
		&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
	)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func (s *MessageStore) GetByID(ctx context.Context, id uuid.UUID) (*Message, error) {
	var m Message

	err := s.db.QueryRow(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp, external_id
		FROM messages
		WHERE id = $1
	`, id).Scan(
		&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
		&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
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
			processed = $8, timestamp = $9, external_id = $10
		WHERE id = $11
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
		m.ExternalID,
		m.ID,
	)

	return err
}

// Delete is intentionally not implemented: messages are never hard-deleted
// per the proposal's data model.

// MarkProcessed sets processed = true for id — a narrow, single-field
// update, same reasoning as EdgeStore.Reinforce: a full Update() risks
// accidentally touching an unrelated field (raw_text, sender_id, ...)
// when only this one needs to change. Used by the write pipeline
// (internal/api) once a message's window has been extracted (or once
// it's determined there's nothing further to do with it).
func (s *MessageStore) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `UPDATE messages SET processed = true WHERE id = $1`, id)
	return err
}

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
			transcript_confidence, media_ref, processed, timestamp, external_id
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
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
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
			transcript_confidence, media_ref, processed, timestamp, external_id
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
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
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

// CountByConversationAndSenderSince returns the message count per
// (conversation_id, sender_id) pair, restricted to messages at or after
// since — the group-chat-attribution fix's own per-(conversation,
// sender) grouped query (one query, not one per contact — see
// reflection.RecomputeRelationshipStats' own doc comment on why that
// matters once a group conversation can have more than two senders):
// CountByConversationSince's total-per-conversation count can't tell a
// group's members' activity apart from each other or from Marco's own,
// which group-chat frequency attribution (§10 rule C) needs.
func (s *MessageStore) CountByConversationAndSenderSince(ctx context.Context, since time.Time) (map[uuid.UUID]map[uuid.UUID]int, error) {
	rows, err := s.db.Query(ctx, `
		SELECT conversation_id, sender_id, count(*) FROM messages
		WHERE timestamp >= $1
		GROUP BY conversation_id, sender_id
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]map[uuid.UUID]int)
	for rows.Next() {
		var conversationID, senderID uuid.UUID
		var n int
		if err := rows.Scan(&conversationID, &senderID, &n); err != nil {
			return nil, err
		}
		if counts[conversationID] == nil {
			counts[conversationID] = make(map[uuid.UUID]int)
		}
		counts[conversationID][senderID] = n
	}

	return counts, rows.Err()
}

// DistinctSendersByConversation returns, for each conversation_id in
// conversationIDs, the set of every sender_id that has EVER sent a
// message in it — deliberately unwindowed (see
// reflection.RecomputeRelationshipStats' group-detection rule): whether a
// conversation is a group chat is a property of who has ever
// participated in it, not of who happened to speak in one particular
// trailing window, so a group that's gone quiet doesn't flip back to
// looking like a 1:1 just because this window is sparse. Returns an
// empty map, not an error, for an empty conversationIDs.
func (s *MessageStore) DistinctSendersByConversation(ctx context.Context, conversationIDs []uuid.UUID) (map[uuid.UUID]map[uuid.UUID]bool, error) {
	senders := make(map[uuid.UUID]map[uuid.UUID]bool)
	if len(conversationIDs) == 0 {
		return senders, nil
	}

	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT conversation_id, sender_id FROM messages
		WHERE conversation_id = ANY($1)
	`, conversationIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var conversationID, senderID uuid.UUID
		if err := rows.Scan(&conversationID, &senderID); err != nil {
			return nil, err
		}
		if senders[conversationID] == nil {
			senders[conversationID] = make(map[uuid.UUID]bool)
		}
		senders[conversationID][senderID] = true
	}

	return senders, rows.Err()
}

// ListWithoutEmbeddings returns every message with no matching row in
// embeddings — the query cmd/backfill_embeddings needs to find messages
// written before embedding generation was wired into the write pipeline
// (see internal/api/pipeline.go's embedMessages and storeExtractedTriples'
// own doc comment on the gap it closed). A NOT EXISTS query, not a loop
// calling EmbeddingStore.GetByMessageID per message: that check is right
// for what it was built for (the write pipeline's own single-message
// idempotency check, called once per already-known message ID) but
// doesn't scale to "find every unembedded message across the whole
// table" the way one SQL query does.
//
// Unpaginated: a personal-scale corpus (proposal §6.1) isn't expected to
// need pagination for a one-shot backfill — add it if real message volume
// ever makes that assumption wrong, not speculatively here.
func (s *MessageStore) ListWithoutEmbeddings(ctx context.Context) ([]*Message, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			m.id, m.conversation_id, m.sender_id, m.media_type, m.raw_text, m.transcript,
			m.transcript_confidence, m.media_ref, m.processed, m.timestamp, m.external_id
		FROM messages m
		WHERE NOT EXISTS (
			SELECT 1 FROM embeddings e WHERE e.message_id = m.id
		)
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var m Message

		if err := rows.Scan(
			&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
		); err != nil {
			return nil, err
		}

		msgs = append(msgs, &m)
	}

	return msgs, rows.Err()
}

// ListWithText returns every message with embeddable text (see
// hasUsableTextWhere), regardless of processed or embedded state —
// api.RebuildEmbeddings' own candidate set (the task-prefix fix's
// re-embed-everything pass, cmd/backfill_embeddings' --rebuild mode):
// unlike ListWithoutEmbeddings (which answers "no embeddings row yet")
// or ListUnprocessedWithText (which answers "processed=false AND has
// text"), a rebuild needs EVERY message that has ever been embeddable,
// including ones already embedded (that's the whole point — their
// existing vector was generated without nomic-embed-text's required
// task prefix and needs replacing) and ones not yet processed by
// extraction (their raw_text/transcript already exists regardless of
// whether a window has closed over them yet).
//
// Checked for an existing equivalent first: neither of the two queries
// above answers "has text," unconditionally, for every message — each
// adds its own extra filter (no embedding row / unprocessed) this
// caller explicitly does NOT want.
func (s *MessageStore) ListWithText(ctx context.Context) ([]*Message, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp, external_id
		FROM messages
		WHERE `+hasUsableTextWhere+`
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var m Message

		if err := rows.Scan(
			&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
		); err != nil {
			return nil, err
		}

		msgs = append(msgs, &m)
	}

	return msgs, rows.Err()
}

// hasUsableTextWhere is the "has embeddable text" condition ALONE (no
// processed check — callers AND it with their own) — the same rule
// api.messageText applies when deciding what to embed (raw_text if
// present and non-empty, else transcript), duplicated here at the SQL
// level rather than called from this package: messageText lives in
// package api, which this package must not import (memory is the lower
// layer api composes, per this codebase's established dependency
// direction — see package api's own doc comment). Shared by
// ListUnprocessedWithText and CountUnprocessedWithoutText specifically so
// the two queries' notion of "has text" can't drift apart from each
// other, even though messageText itself (a different package) isn't the
// single source either query can literally call.
const hasUsableTextWhere = `
	(raw_text IS NOT NULL AND raw_text != '')
	OR (transcript IS NOT NULL AND transcript != '')
`

// ListUnprocessedWithText returns every processed=false message that has
// embeddable text (see unprocessedWithTextWhere), ordered by
// (conversation_id, timestamp) — the live-window restart-recovery sweep's
// own query (api.RecoverUnprocessedLive): these are exactly the messages
// whose live window never got a chance to close (extraction.LiveWindower
// never force-closes on its own — see the live-window idle-flush fix's
// own history for why), re-windowable in the same chronological order
// they actually arrived in.
//
// Checked for an existing equivalent first: ListWithoutEmbeddings (above)
// answers a different question — "no embeddings row at all," regardless
// of processed — and would also return plenty of ALREADY-processed
// messages; nothing in this package already answers "unprocessed AND has
// text."
//
// A message with processed=false and NO usable text (e.g. a voice note
// whose transcription failed entirely) deliberately does NOT appear
// here — see CountUnprocessedWithoutText for counting those instead, and
// RecoverUnprocessedLive's own doc comment for why retrying their
// transcription is out of scope.
func (s *MessageStore) ListUnprocessedWithText(ctx context.Context) ([]*Message, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, conversation_id, sender_id, media_type, raw_text, transcript,
			transcript_confidence, media_ref, processed, timestamp, external_id
		FROM messages
		WHERE processed = false AND (`+hasUsableTextWhere+`)
		ORDER BY conversation_id, timestamp
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var m Message

		if err := rows.Scan(
			&m.ID, &m.ConversationID, &m.SenderID, &m.MediaType, &m.RawText, &m.Transcript,
			&m.TranscriptConfidence, &m.MediaRef, &m.Processed, &m.Timestamp, &m.ExternalID,
		); err != nil {
			return nil, err
		}

		msgs = append(msgs, &m)
	}

	return msgs, rows.Err()
}

// CountUnprocessedWithoutText returns how many processed=false messages
// have NO usable text — the complement of ListUnprocessedWithText's own
// filter (hasUsableTextWhere), for api.RecoverUnprocessedLive's
// RecoveryStats to report rather than silently skip.
func (s *MessageStore) CountUnprocessedWithoutText(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, `
		SELECT count(*) FROM messages
		WHERE processed = false AND NOT (`+hasUsableTextWhere+`)
	`).Scan(&count)
	return count, err
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

// LastMessageTimeBySender is LastMessageTime restricted to one sender —
// the group-chat-attribution fix's own recency rule (§10 rule E): in a
// group conversation, a member's recency must come from THEIR OWN last
// message in it, not whoever in the group spoke most recently (that
// would credit a silent member with the group's general activity, the
// same misattribution this whole fix addresses for frequency and
// reply_speed). Returns the zero time, not an error, for an empty
// conversationIDs.
func (s *MessageStore) LastMessageTimeBySender(ctx context.Context, senderID uuid.UUID, conversationIDs []uuid.UUID) (time.Time, error) {
	if len(conversationIDs) == 0 {
		return time.Time{}, nil
	}

	var t *time.Time
	if err := s.db.QueryRow(ctx, `
		SELECT max(timestamp) FROM messages WHERE sender_id = $1 AND conversation_id = ANY($2)
	`, senderID, conversationIDs).Scan(&t); err != nil {
		return time.Time{}, err
	}
	if t == nil {
		return time.Time{}, nil
	}

	return *t, nil
}
