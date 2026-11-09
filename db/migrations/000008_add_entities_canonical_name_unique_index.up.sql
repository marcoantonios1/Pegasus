-- Entity resolution race fix: api.resolveEntity/resolveSender used to do
-- GetByCanonicalName (a lookup) then EntityStore.Create (an unconditional
-- INSERT) with nothing enforcing uniqueness in between — two concurrent
-- callers resolving the same name (two imports running at once, or a
-- live message and a historical import both mentioning the same person)
-- could both miss the lookup and both insert, producing two rows for
-- what should be one entity.
--
-- The unique constraint matches the LOOKUP exactly: lower(canonical_name)
-- alone, NOT (canonical_name, type). Resolution has always been
-- name-only — extraction's own objectEntityTypeForPredicate assigns a
-- NEW object-side entity a default type that often differs from the
-- same name's existing subject-side entity (e.g. "Sara" mentioned as a
-- subject vs. as a plain object can default to different types) — a
-- (name, type) key would split those into two entities that correctly
-- unify into one today. The first creator's type wins when two
-- concurrent creates race for the same name; see
-- EntityStore.GetOrCreateByCanonicalName's own doc comment for that
-- contract, stated in code, not just here.
--
-- Pre-check below, NOT an automatic merge: an entity row is referenced
-- by edges.subject_id/object_id, messages.sender_id, and
-- relationship_stats.contact_id — merging two existing duplicate rows
-- means repointing every one of those foreign keys onto a single
-- survivor, and can itself create duplicate edges (two rows each citing
-- the same subject+predicate+object once repointed) or collide on
-- relationship_stats' own UNIQUE contact_id constraint (two rows
-- repointing to the same contact_id). That is real, separate work — a
-- dedicated merge tool, its own issue — not something to improvise
-- inside this migration. If duplicates already exist, this migration
-- refuses to guess how to resolve them and fails loudly instead; the
-- diagnostic query below also appears in the exception message.
--
-- Diagnostic query (same one the exception message points at):
--   SELECT lower(canonical_name), count(*), array_agg(id)
--   FROM entities
--   GROUP BY lower(canonical_name)
--   HAVING count(*) > 1;
DO $$
DECLARE
    dup_group_count INT;
    sample_names TEXT;
BEGIN
    SELECT count(*) INTO dup_group_count
    FROM (
        SELECT lower(canonical_name)
        FROM entities
        GROUP BY lower(canonical_name)
        HAVING count(*) > 1
    ) dup_groups;

    IF dup_group_count > 0 THEN
        SELECT string_agg(quote_literal(name), ', ') INTO sample_names
        FROM (
            SELECT lower(canonical_name) AS name
            FROM entities
            GROUP BY lower(canonical_name)
            HAVING count(*) > 1
            ORDER BY count(*) DESC
            LIMIT 5
        ) top_offenders;

        RAISE EXCEPTION
            'entities_canonical_name_lower_uidx: % duplicate canonical_name group(s) exist (case-insensitive); cannot create a unique index. First few offending names: %. This migration will NOT auto-merge duplicates (they are referenced by edges/messages/relationship_stats foreign keys — merging requires repointing those, a separate tool/issue). Resolve duplicates by hand, or wipe the database first if this is disposable test data. Diagnostic query: SELECT lower(canonical_name), count(*), array_agg(id) FROM entities GROUP BY lower(canonical_name) HAVING count(*) > 1;',
            dup_group_count, sample_names;
    END IF;
END $$;

CREATE UNIQUE INDEX entities_canonical_name_lower_uidx ON entities (lower(canonical_name));
