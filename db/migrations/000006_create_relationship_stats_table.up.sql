CREATE TABLE relationship_stats (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_id     UUID NOT NULL REFERENCES entities(id),
    frequency      FLOAT NOT NULL,
    reply_speed    FLOAT NOT NULL,
    humor_level    FLOAT NOT NULL,
    closeness      FLOAT NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One current row per contact, overwritten (upserted) on each
-- consolidation pass, rather than an append-only history table — see
-- internal/memory/relationship_stats_store.go's Upsert. §10.1's
-- GetRelationshipHistory() is edge-based (reconstructed from the edges
-- table's own first_seen/last_reinforced history) and does not need this
-- table to retain past rows, so a single-current-row-per-contact design
-- is the simpler, sufficient choice given nothing else in the proposal
-- needs relationship_stats history queried over time.
CREATE UNIQUE INDEX relationship_stats_contact_id_idx ON relationship_stats (contact_id);
