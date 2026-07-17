CREATE EXTENSION IF NOT EXISTS vector;

-- ON DELETE CASCADE: messages are never hard-deleted per the proposal's data
-- model, so this never fires in practice today. It's set as a safety net
-- against orphaned vectors if that no-hard-delete assumption ever changes.
CREATE TABLE embeddings (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  UUID NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    vector      VECTOR(768)
);

-- HNSW index on `vector` is deferred to a separate migration — tuning
-- parameters (m, ef_construction) aren't decided yet. See roadmap item
-- "pgvector HNSW index setup".
