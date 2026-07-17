-- vector_cosine_ops: cosine similarity is the standard distance metric for
-- nomic-embed-text embeddings, and GetRelevantContext()'s hybrid retrieval
-- (proposal §11) ranks by similarity, not raw L2 distance.
--
-- m = 16, ef_construction = 64 are pgvector's own documented defaults — a
-- reasonable starting point for a personal-scale corpus (hundreds of
-- thousands of vectors per §6.1), not tuned yet. Revisit if recall or build
-- time becomes a problem at real data volume.
--
-- Not using CREATE INDEX CONCURRENTLY: golang-migrate wraps each migration
-- in a transaction, and CONCURRENTLY cannot run inside one. Acceptable at
-- Phase 0 with an empty/small table; a production-scale backfill later will
-- need a non-transactional migration or a manual CONCURRENTLY build.
CREATE INDEX embeddings_vector_hnsw_idx ON embeddings
    USING hnsw (vector vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
