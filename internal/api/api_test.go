package api

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// testDB connects to a Postgres instance for integration testing,
// mirroring internal/memory's and internal/reflection's own testDB
// helpers (each package owns its own DB-test bootstrap per this repo's
// established convention — see reflection/dedup_test.go's testDB doc
// comment). Skips (does not fail) the test if no database is reachable.
//
// Unlike those two, this one MUST register the pgvector pgx codec (via
// AfterConnect) — every store in internal/memory this package composes
// worked fine without it, but EdgeStore.SearchRelevant and
// EmbeddingStore.SearchSimilarMessages both bind a pgvector.Vector query
// parameter directly, which fails without this registration. No other
// package in this codebase has needed this before, since nothing
// previously exercised pgvector end-to-end against a real database.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("PEGASUS_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://pegasus:pegasus@localhost:5432/pegasus?sslmode=disable"
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("no test database available (%v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("no test database available (ping failed: %v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

// testStores bundles every store this package's tests construct
// repeatedly, plus the raw pool for direct backdating queries (matching
// reflection/dedup_test.go's createTestEdge pattern).
type testStores struct {
	pool     *pgxpool.Pool
	entities *memory.EntityStore
	edges    *memory.EdgeStore
	messages *memory.MessageStore
	embeds   *memory.EmbeddingStore
	relStats *memory.RelationshipStatsStore
}

func newTestStores(t *testing.T) *testStores {
	pool := testDB(t)
	return &testStores{
		pool:     pool,
		entities: memory.NewEntityStore(pool),
		edges:    memory.NewEdgeStore(pool),
		messages: memory.NewMessageStore(pool),
		embeds:   memory.NewEmbeddingStore(pool),
		relStats: memory.NewRelationshipStatsStore(pool),
	}
}

func createTestEntity(t *testing.T, ctx context.Context, ts *testStores, entityType, name string) *memory.Entity {
	t.Helper()
	e := &memory.Entity{Type: entityType, CanonicalName: name}
	if err := ts.entities.Create(ctx, e); err != nil {
		t.Fatalf("create test entity: %v", err)
	}
	return e
}

// createTestEdge creates an edge, then backdates FirstSeen/LastReinforced
// to firstSeen via a direct SQL update — same pattern and rationale as
// reflection/dedup_test.go's createTestEdge (Create() always stamps
// first_seen from the DB's now(), and tests need precise, back-dated
// values to control decay/window behavior deterministically).
func createTestEdge(t *testing.T, ctx context.Context, ts *testStores, e *memory.Edge, firstSeen time.Time) *memory.Edge {
	t.Helper()
	if err := ts.edges.Create(ctx, e); err != nil {
		t.Fatalf("create test edge: %v", err)
	}
	if _, err := ts.pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, firstSeen, e.ID); err != nil {
		t.Fatalf("backdate test edge first_seen: %v", err)
	}
	e.FirstSeen = firstSeen
	e.LastReinforced = firstSeen
	return e
}

func createTestMessage(t *testing.T, ctx context.Context, ts *testStores, conversationID, senderID uuid.UUID, mediaType string, sentAt time.Time) *memory.Message {
	t.Helper()
	m := &memory.Message{
		ConversationID: conversationID,
		SenderID:       senderID,
		MediaType:      mediaType,
		Timestamp:      sentAt,
	}
	if err := ts.messages.Create(ctx, m); err != nil {
		t.Fatalf("create test message: %v", err)
	}
	return m
}

const embeddingDim = 768

// basisVector is a 768-dim vector with value at just one index — a
// simple, hand-reasoned-about way to control cosine similarity between
// test vectors without needing real embedding content: two basisVectors
// at the same index (or a nearVector near that index) are highly similar;
// basisVectors at different indices are orthogonal (similarity 0).
func basisVector(index int, value float32) []float32 {
	v := make([]float32, embeddingDim)
	v[index] = value
	return v
}

// nearVector is close to, but not identical to, basisVector(index, 1) —
// cosine similarity near 1 but not exactly 1, for tests that want "highly
// similar but not a trivial exact match".
func nearVector(index int) []float32 {
	v := make([]float32, embeddingDim)
	v[index] = 0.95
	v[(index+1)%embeddingDim] = 0.05
	return v
}

// randomBasisIndex picks a fresh random index into a basisVector/
// nearVector for this test — NOT a fixed constant like 0 or 1. These
// tests run against a real, persistent, shared local dev database with
// no per-test reset (same convention as every other DB-backed test in
// this codebase), and EmbeddingStore.SearchSimilarMessages searches the
// WHOLE embeddings table with no scoping filter, by design (it's a pure
// vector search — see its own doc comment). A fixed index would mean
// every test run leaves behind an identical vector, so a later run's
// "nearest" query ties exactly against leftover rows from earlier runs,
// and Postgres doesn't guarantee which tied row comes back first — this
// bit a real test failure during development (two runs of the same
// nearVector(0) both at distance 0, LIMIT 1 non-deterministically
// returning either). A random index makes an accidental collision with
// leftover data astronomically unlikely without needing to clean up the
// shared table.
func randomBasisIndex() int {
	return rand.Intn(embeddingDim)
}

func createTestEmbedding(t *testing.T, ctx context.Context, ts *testStores, messageID uuid.UUID, vector []float32) *memory.Embedding {
	t.Helper()
	e := &memory.Embedding{MessageID: messageID, Vector: vector}
	if err := ts.embeds.Create(ctx, e); err != nil {
		t.Fatalf("create test embedding: %v", err)
	}
	return e
}

// fakeEmbedder returns a fixed vector (or a fixed error) regardless of
// input text — lets tests control exactly what "the embedded query"
// looks like without a live Costguard call, same pattern as
// bulkimport's WindowExtractor fake pattern for *extraction.Extractor.
type fakeEmbedder struct {
	vector []float32
	err    error
}

func (f *fakeEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.vector, nil
}
