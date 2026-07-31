package memory

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testDB connects to a Postgres instance for integration testing, using
// PEGASUS_TEST_DATABASE_URL if set, falling back to the local
// docker-compose DSN (see docker-compose.yml / README's "Local
// development" section). Skips — does not fail — the test if no database
// is reachable, so `go test ./...` doesn't break on a machine without
// Postgres running.
//
// This is the first DB-backed test in this codebase; every other store
// method so far (EntityStore, EdgeStore's other methods, MessageStore,
// EmbeddingStore) has only ever been verified manually via `docker
// compose up` + a throwaway CLI, per this repo's established convention.
// Requirement 8 of the decay/reinforcement issue specifically needs to
// assert no duplicate row gets inserted, which can't be checked without a
// real database — hence this pattern, chosen deliberately over continuing
// manual-only verification. Run `docker compose up -d postgres migrate`
// from the repo root before running this test for real; otherwise it
// skips cleanly.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("PEGASUS_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://pegasus:pegasus@localhost:5432/pegasus?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
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

func createTestEntity(t *testing.T, ctx context.Context, store *EntityStore, name string) *Entity {
	t.Helper()
	e := &Entity{Type: "person", CanonicalName: name}
	if err := store.Create(ctx, e); err != nil {
		t.Fatalf("create test entity: %v", err)
	}
	return e
}

// TestEdgeStore_Reinforce covers requirement 8: create an edge, advance
// time via an explicitly injected `now` (no wall-clock sleep), reinforce
// it, and assert last_reinforced updated, confidence unchanged, and — the
// part that specifically needs a real database — that no duplicate row
// was inserted.
func TestEdgeStore_Reinforce(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Reinforce Test Subject")

	literal := "olives"
	edge := &Edge{
		SubjectID:     subject.ID,
		Predicate:     "dislikes",
		ObjectLiteral: &literal,
		Confidence:    0.9,
		Importance:    0.5,
		SourceType:    "whatsapp_text",
		SourceWeight:  1.0,
		DecayRate:     DecayRateStable,
	}
	if err := edgeStore.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	injectedNow := edge.LastReinforced.Add(72 * time.Hour) // simulate 3 days passing

	if err := edgeStore.Reinforce(ctx, edge.ID, injectedNow); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}

	reloaded, err := edgeStore.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if !reloaded.LastReinforced.Equal(injectedNow) {
		t.Errorf("expected last_reinforced = %v, got %v", injectedNow, reloaded.LastReinforced)
	}
	if reloaded.Confidence != edge.Confidence {
		t.Errorf("expected confidence unchanged at %v, got %v", edge.Confidence, reloaded.Confidence)
	}
	if reloaded.Predicate != edge.Predicate {
		t.Errorf("expected predicate unchanged at %q, got %q", edge.Predicate, reloaded.Predicate)
	}

	var rowCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM edges WHERE subject_id = $1 AND predicate = $2`,
		subject.ID, "dislikes",
	).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 row after Reinforce (no duplicate insert), got %d", rowCount)
	}
}

func TestEdgeStore_Reinforce_DecayLockedEdgeTimestampStillUpdates(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Reinforce Locked Edge Subject")

	literal := "her birthday is in March"
	edge := &Edge{
		SubjectID:     subject.ID,
		Predicate:     "event_present",
		ObjectLiteral: &literal,
		Confidence:    1.0,
		Importance:    0.9,
		SourceType:    "user_correction",
		SourceWeight:  1.0,
		DecayRate:     DecayRateStable,
		DecayLocked:   true,
	}
	if err := edgeStore.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	injectedNow := edge.LastReinforced.Add(24 * time.Hour)
	if err := edgeStore.Reinforce(ctx, edge.ID, injectedNow); err != nil {
		t.Fatalf("Reinforce: %v", err)
	}

	reloaded, err := edgeStore.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	// Reinforce still updates the timestamp even on a locked edge — see
	// the Reinforce doc comment for why this is the intended behavior.
	if !reloaded.LastReinforced.Equal(injectedNow) {
		t.Errorf("expected last_reinforced to update even on a decay_locked edge, got %v want %v", reloaded.LastReinforced, injectedNow)
	}
	// But confidence must never move, and EffectiveConfidence must ignore
	// the newly-updated timestamp entirely because DecayLocked short-
	// circuits before ever consulting it.
	if reloaded.Confidence != 1.0 {
		t.Errorf("expected confidence unchanged at 1.0, got %v", reloaded.Confidence)
	}
	farFuture := injectedNow.AddDate(10, 0, 0)
	if got := EffectiveConfidence(*reloaded, farFuture); got != 1.0 {
		t.Errorf("expected EffectiveConfidence to stay 1.0 for a decay_locked edge regardless of elapsed time, got %v", got)
	}
}

func TestEdgeStore_FindMatchingEdge(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "FindMatchingEdge Test Subject")

	olives := "olives"
	cilantro := "cilantro"

	edge1 := &Edge{
		SubjectID: subject.ID, Predicate: "dislikes", ObjectLiteral: &olives,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text",
		SourceWeight: 1.0, DecayRate: DecayRateStable,
	}
	if err := edgeStore.Create(ctx, edge1); err != nil {
		t.Fatalf("create edge1: %v", err)
	}

	match, err := edgeStore.FindMatchingEdge(ctx, subject.ID, "dislikes", nil, &olives)
	if err != nil {
		t.Fatalf("FindMatchingEdge: %v", err)
	}
	if match == nil || match.ID != edge1.ID {
		t.Fatalf("expected to find edge1 by exact object_literal match, got %+v", match)
	}

	noMatch, err := edgeStore.FindMatchingEdge(ctx, subject.ID, "dislikes", nil, &cilantro)
	if err != nil {
		t.Fatalf("FindMatchingEdge: %v", err)
	}
	if noMatch != nil {
		t.Fatalf("expected no match for a different object_literal, got %+v", noMatch)
	}

	noMatchPredicate, err := edgeStore.FindMatchingEdge(ctx, subject.ID, "likes", nil, &olives)
	if err != nil {
		t.Fatalf("FindMatchingEdge: %v", err)
	}
	if noMatchPredicate != nil {
		t.Fatalf("expected no match for a different predicate even with the same object, got %+v", noMatchPredicate)
	}
}
