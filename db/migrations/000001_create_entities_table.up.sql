CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid()

CREATE TABLE entities (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type           TEXT NOT NULL CHECK (type IN ('person', 'topic', 'place', 'project', 'event', 'object')),
    canonical_name TEXT NOT NULL,
    is_self        BOOLEAN NOT NULL DEFAULT false,
    metadata       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "at most one row can have is_self = true" — a unique partial index is the
-- standard Postgres way to express this, since CHECK constraints can't see
-- other rows.
CREATE UNIQUE INDEX entities_single_self_idx ON entities (is_self) WHERE is_self = true;

CREATE INDEX entities_type_idx ON entities (type);
CREATE INDEX entities_canonical_name_idx ON entities (canonical_name);