package memory

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDB connects to a Postgres instance for integration testing, using
// PEGASUS_TEST_DATABASE_URL if set, else building a DSN from the
// POSTGRES_PASSWORD environment variable (see .env.example; export it
// with `set -a; source .env; set +a` before running `go test`) against
// the standard local docker-compose user/db/host. Skips — does not fail —
// the test if no database is configured or reachable at all, so `go test
// ./...` doesn't break on a machine without Postgres running. But an
// authentication failure against a database that IS reachable fails the
// test loudly instead: silently skipping on a wrong password would let a
// Postgres password rotation turn this entire DB-backed suite green by
// skipping everything, rather than surfacing that tests aren't actually
// running.
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
		if pw := os.Getenv("POSTGRES_PASSWORD"); pw != "" {
			dsn = "postgres://pegasus:" + pw + "@localhost:5432/pegasus?sslmode=disable"
		}
	}
	if dsn == "" {
		t.Skip("no test database configured — set PEGASUS_TEST_DATABASE_URL, or POSTGRES_PASSWORD (`set -a; source .env; set +a`), to enable this test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		if isAuthFailure(err) {
			t.Fatalf("authentication failed connecting to the test database (%v) — the Postgres password was likely rotated; set PEGASUS_TEST_DATABASE_URL to the current DSN, or re-export POSTGRES_PASSWORD (`set -a; source .env; set +a`) before running go test", err)
		}
		t.Skipf("no test database available (%v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}

	t.Cleanup(pool.Close)
	return pool
}

// isAuthFailure reports whether err is Postgres's "password
// authentication failed" error (SQLSTATE 28P01) — the one failure mode
// testDB must not silently skip on, since skipping on it would make a
// password rotation invisible to the whole DB-backed test suite.
func isAuthFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "28P01" {
		return true
	}
	return strings.Contains(err.Error(), "password authentication failed")
}

// createTestEntity always suffixes name with a fresh UUID before
// creating it — the entity-resolution race fix (migration 000008) put a
// case-insensitive UNIQUE index on canonical_name, so the many existing
// callers of this helper that passed a fixed, human-readable literal
// would otherwise collide with each other and with themselves across
// repeated runs against this suite's shared, never-reset dev database
// (harmless before that unique index existed, a flat-out Create()
// failure — ErrEntityExists — after it does). Suffixing here, once,
// fixes every existing call site without touching each individually.
func createTestEntity(t *testing.T, ctx context.Context, store *EntityStore, name string) *Entity {
	t.Helper()
	e := &Entity{Type: "person", CanonicalName: name + " " + uuid.New().String()}
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

	if err := edgeStore.Reinforce(ctx, edge.ID, injectedNow, edge.FirstSeen); err != nil {
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
	if err := edgeStore.Reinforce(ctx, edge.ID, injectedNow, edge.FirstSeen); err != nil {
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

// TestEdgeStore_FindMatchingEdge_ExcludesTombstone is the delete-
// exclusion fix's confirm-then-fix test (requirement 6): a LIVE
// tombstone (superseded_by IS NULL, source_type=user_deletion — exactly
// what SoftDelete leaves behind) with the same subject/predicate/object
// as what a normal, non-correction triple would match must NOT be
// returned by FindMatchingEdge. Before the fix, it was: a tombstone is
// live by every other criterion this method checks, so a fact the user
// later reasserts would get "reinforced" into the zero-confidence
// tombstone instead of creating a fresh edge — permanently unable to be
// relearned.
func TestEdgeStore_FindMatchingEdge_ExcludesTombstone(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "FindMatchingEdge Tombstone Subject")

	olives := "olives"
	tombstone := &Edge{
		SubjectID: subject.ID, Predicate: "dislikes", ObjectLiteral: &olives,
		Confidence: 0, Importance: 0, SourceType: SourceTypeUserDeletion,
		SourceWeight: 1.0, DecayRate: DecayRateStable,
	}
	if err := edgeStore.Create(ctx, tombstone); err != nil {
		t.Fatalf("create tombstone: %v", err)
	}

	match, err := edgeStore.FindMatchingEdge(ctx, subject.ID, "dislikes", nil, &olives)
	if err != nil {
		t.Fatalf("FindMatchingEdge: %v", err)
	}
	if match != nil {
		t.Errorf("expected FindMatchingEdge to never match a tombstone, got %+v", match)
	}
}

// TestEdgeStore_FindLiveEdgesBySubjectAndPredicate_ExcludesTombstone
// covers the same requirement for the correction-target candidate
// lookup: a live tombstone must never appear among a correction's
// candidates, or a tombstone could be picked as a correction target
// (superseding a confidence-0/importance-0 dead marker with a
// "correction" makes no sense — there is nothing live there to correct).
func TestEdgeStore_FindLiveEdgesBySubjectAndPredicate_ExcludesTombstone(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "FindLiveEdges Tombstone Subject")

	olives := "olives"
	tombstone := &Edge{
		SubjectID: subject.ID, Predicate: "dislikes", ObjectLiteral: &olives,
		Confidence: 0, Importance: 0, SourceType: SourceTypeUserDeletion,
		SourceWeight: 1.0, DecayRate: DecayRateStable,
	}
	if err := edgeStore.Create(ctx, tombstone); err != nil {
		t.Fatalf("create tombstone: %v", err)
	}

	candidates, err := edgeStore.FindLiveEdgesBySubjectAndPredicate(ctx, subject.ID, "dislikes")
	if err != nil {
		t.Fatalf("FindLiveEdgesBySubjectAndPredicate: %v", err)
	}
	if len(candidates) != 0 {
		t.Errorf("expected zero live candidates (the only live edge is a tombstone), got %+v", candidates)
	}
}

// TestEdgeStore_FindMatchingEdge_NeverReturnsSupersededEdge is the
// stale-resurrection fix's own unit test (requirement 12): once an edge
// is superseded, FindMatchingEdge must never return it again for the
// same exact object, even though the object text is unchanged. This is
// the opposite of this method's old, deliberately-reverted behavior —
// see FindMatchingEdge's own doc comment for why including superseded
// edges turned out to be a real bug (silent resurrection of corrected-
// away facts), not a safe simplification.
func TestEdgeStore_FindMatchingEdge_NeverReturnsSupersededEdge(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "FindMatchingEdge Superseded Test Subject")

	acme := "Acme"
	edge := &Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: &acme,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text",
		SourceWeight: 1.0, DecayRate: DecayRateStable,
	}
	if err := edgeStore.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	globex := "Globex"
	if _, err := edgeStore.CorrectMemory(ctx, edge.ID, CorrectionInput{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: &globex,
	}); err != nil {
		t.Fatalf("supersede edge: %v", err)
	}

	match, err := edgeStore.FindMatchingEdge(ctx, subject.ID, "works_at", nil, &acme)
	if err != nil {
		t.Fatalf("FindMatchingEdge: %v", err)
	}
	if match != nil {
		t.Errorf("expected FindMatchingEdge to never return a superseded edge, got %+v", match)
	}
}

// TestEdgeStore_FindSupersededMatches_OnlyReturnsSupersededEdges is the
// stale-resurrection fix's unit test for FindSupersededMatches' own
// contract: it must return the dead "Acme" edge (exact object match,
// superseded), and must NOT return the live "Globex" edge that
// superseded it, even though both share the same subject+predicate.
func TestEdgeStore_FindSupersededMatches_OnlyReturnsSupersededEdges(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "FindSupersededMatches Test Subject")

	acme := "Acme"
	edge := &Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: &acme,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text",
		SourceWeight: 1.0, DecayRate: DecayRateStable,
	}
	if err := edgeStore.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	globex := "Globex"
	corrected, err := edgeStore.CorrectMemory(ctx, edge.ID, CorrectionInput{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: &globex,
	})
	if err != nil {
		t.Fatalf("supersede edge: %v", err)
	}

	matches, err := edgeStore.FindSupersededMatches(ctx, subject.ID, "works_at", nil, &acme)
	if err != nil {
		t.Fatalf("FindSupersededMatches: %v", err)
	}
	if len(matches) != 1 || matches[0].ID != edge.ID {
		t.Fatalf("expected exactly the superseded Acme edge, got %+v", matches)
	}

	noMatches, err := edgeStore.FindSupersededMatches(ctx, subject.ID, "works_at", nil, &globex)
	if err != nil {
		t.Fatalf("FindSupersededMatches: %v", err)
	}
	for _, m := range noMatches {
		if m.ID == corrected.ID {
			t.Errorf("expected the live Globex edge never to be returned as a superseded match, got %+v", noMatches)
		}
	}
}
