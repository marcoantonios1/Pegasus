CREATE TABLE messages (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id        UUID NOT NULL,
    sender_id              UUID NOT NULL REFERENCES entities(id),
    media_type             TEXT NOT NULL CHECK (media_type IN ('text', 'voice', 'image', 'video')),
    raw_text               TEXT NULL,
    transcript             TEXT NULL,
    transcript_confidence  FLOAT NULL,
    media_ref              TEXT NULL,
    processed              BOOLEAN NOT NULL DEFAULT false,
    timestamp              TIMESTAMPTZ NOT NULL
);

CREATE INDEX messages_conversation_id_idx ON messages (conversation_id);
CREATE INDEX messages_sender_id_idx ON messages (sender_id);
