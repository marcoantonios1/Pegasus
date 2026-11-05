-- Message ingestion idempotency: lets CreateIfAbsent (internal/memory)
-- detect "this exact message was already stored" via each adapter's own
-- deterministic RawMessage.ExternalID (a synthetic hash for export
-- adapters, a native platform message ID for the live WhatsApp adapter),
-- which the write pipeline previously computed and then discarded.
--
-- Nullable: external_id is NOT required on every row — a RawMessage with
-- a genuinely empty ExternalID is still stored (with external_id = NULL,
-- no dedup key, logged as a warning by the caller), never dropped.
ALTER TABLE messages ADD COLUMN external_id TEXT;

-- Scope is (conversation_id, external_id), not external_id alone: a
-- native platform message ID (the live WhatsApp adapter) is only
-- guaranteed unique within its own chat, and conversation_id is already
-- derived from (platform, external conversation ID) — see
-- internal/api/pipeline.go's resolveConversationID. Partial (WHERE
-- external_id IS NOT NULL): NULL never participates in a unique index
-- comparison in Postgres anyway, but the explicit WHERE clause is also
-- exactly the conflict target CreateIfAbsent's ON CONFLICT clause must
-- repeat verbatim to target this specific index.
CREATE UNIQUE INDEX messages_conversation_id_external_id_idx
    ON messages (conversation_id, external_id)
    WHERE external_id IS NOT NULL;
