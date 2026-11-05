DROP INDEX IF EXISTS messages_conversation_id_external_id_idx;
ALTER TABLE messages DROP COLUMN IF EXISTS external_id;
