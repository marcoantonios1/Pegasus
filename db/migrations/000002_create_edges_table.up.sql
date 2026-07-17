CREATE TABLE edges (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id          UUID NOT NULL REFERENCES entities(id),
    predicate           TEXT NOT NULL,
    object_id           UUID NULL REFERENCES entities(id),
    object_literal      TEXT NULL,
    confidence          FLOAT NOT NULL,
    importance          FLOAT NOT NULL,
    source_type         TEXT NOT NULL,
    source_weight       FLOAT NOT NULL,
    source_message_ids  UUID[] NOT NULL DEFAULT '{}',
    first_seen          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_reinforced     TIMESTAMPTZ NOT NULL DEFAULT now(),
    decay_rate          FLOAT NOT NULL,
    decay_locked        BOOLEAN NOT NULL DEFAULT false,
    superseded_by       UUID NULL REFERENCES edges(id),
    is_correction       BOOLEAN NOT NULL DEFAULT false,

    -- exactly one of object_id / object_literal must be set: an edge points
    -- either at another entity or at a literal value, never both, never neither.
    CONSTRAINT edges_object_xor CHECK (num_nonnulls(object_id, object_literal) = 1)
);

CREATE INDEX edges_subject_id_predicate_idx ON edges (subject_id, predicate);
CREATE INDEX edges_superseded_by_idx ON edges (superseded_by);
