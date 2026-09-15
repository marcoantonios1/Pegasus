package reflection

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// testDB connects to a Postgres instance for integration testing, mirroring
// internal/memory's testDB helper (edge_store_reinforce_test.go) — that
// helper is unexported in a different package, so this is a deliberate,
// small duplication rather than a new shared abstraction, matching this
// repo's existing convention of each package owning its own DB-test
// bootstrap. Skips (does not fail) the test if no database is reachable.
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

func createTestEntity(t *testing.T, ctx context.Context, store *memory.EntityStore, name string) *memory.Entity {
	t.Helper()
	e := &memory.Entity{Type: "person", CanonicalName: name}
	if err := store.Create(ctx, e); err != nil {
		t.Fatalf("create test entity: %v", err)
	}
	return e
}

// createTestEdge creates an edge and then immediately overrides its
// FirstSeen/LastReinforced to firstSeen via a direct SQL update — Create()
// always stamps first_seen from the DB's now(), and this test suite needs
// precise, back-dated FirstSeen values (e.g. "hours apart", "8 days
// apart") to exercise the window boundary deterministically, without
// wall-clock sleeps. first_seen has no dedicated setter on EdgeStore (by
// design — it's meant to be immutable after creation in normal operation),
// so this test helper reaches around it via the raw pool, for test setup
// only.
func createTestEdge(t *testing.T, ctx context.Context, pool *pgxpool.Pool, store *memory.EdgeStore, e *memory.Edge, firstSeen time.Time) *memory.Edge {
	t.Helper()
	if err := store.Create(ctx, e); err != nil {
		t.Fatalf("create test edge: %v", err)
	}

	_, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, firstSeen, e.ID)
	if err != nil {
		t.Fatalf("backdate test edge first_seen: %v", err)
	}
	e.FirstSeen = firstSeen
	e.LastReinforced = firstSeen
	return e
}

func reloadEdge(t *testing.T, ctx context.Context, store *memory.EdgeStore, id uuid.UUID) *memory.Edge {
	t.Helper()
	e, err := store.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("reload edge %s: %v", id, err)
	}
	return e
}

func TestMergeDuplicates(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Test Subject")
	otherEntity := createTestEntity(t, ctx, entityStore, "Some Other Entity")
	sameEntity := createTestEntity(t, ctx, entityStore, "Referenced Entity")

	base := time.Now().Add(-72 * time.Hour) // anchor well in the past, all offsets computed from here

	lit := func(s string) *string { return &s }

	// --- Case 1: exact literal duplicates within the window -> must merge
	exactA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	exactB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(2*time.Hour))

	// --- Case 2: similar-but-not-identical literals within threshold -> must merge
	similarA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: lit("went to the gym"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	similarB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: lit("went to the gym today"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(3*time.Hour))

	// --- Case 3: literals below similarity threshold -> must NOT merge
	distinctA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("hiking in the mountains"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, base)
	distinctB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("cooking italian food"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, base.Add(time.Hour))

	// --- Case 4: same subject/predicate/similar literal but OUTSIDE the window -> must NOT merge (reinforcement territory)
	outsideWindowA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "mood_signal", ObjectLiteral: lit("feeling stressed about work"),
		Confidence: 0.6, Importance: 0.3, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateTransient,
	}, base)
	outsideWindowB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "mood_signal", ObjectLiteral: lit("feeling stressed about work"),
		Confidence: 0.6, Importance: 0.3, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateTransient,
	}, base.Add(8*24*time.Hour)) // 8 days later, well outside the 24h default window

	// --- Case 5a: object_id, identical entity -> must merge
	entIdenticalA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "family_member_of", ObjectID: &sameEntity.ID,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	entIdenticalB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "family_member_of", ObjectID: &sameEntity.ID,
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(time.Hour))

	// --- Case 5b: object_id, different entity -> must NOT merge (contradiction, not duplicate)
	entDifferentA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectID: &sameEntity.ID,
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, base)
	entDifferentB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectID: &otherEntity.ID,
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, base.Add(time.Hour))

	// --- Case 6: a 3-edge near-duplicate cluster -> must all collapse onto one survivor
	clusterA := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "routine_is", ObjectLiteral: lit("wakes up early"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	clusterB := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "routine_is", ObjectLiteral: lit("wakes up early now"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(time.Hour))
	clusterC := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "routine_is", ObjectLiteral: lit("wakes up early now"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(2*time.Hour))

	candidates := []*memory.Edge{
		exactA, exactB,
		similarA, similarB,
		distinctA, distinctB,
		outsideWindowA, outsideWindowB,
		entIdenticalA, entIdenticalB,
		entDifferentA, entDifferentB,
		clusterA, clusterB, clusterC,
	}

	mergedCount, err := MergeDuplicates(ctx, edgeStore, candidates, DefaultDedupConfig())
	if err != nil {
		t.Fatalf("MergeDuplicates: %v", err)
	}

	// 1 (exact) + 1 (similar) + 1 (entity-identical) + 2 (3-way cluster) = 5
	if mergedCount != 5 {
		t.Errorf("expected mergedCount = 5, got %d", mergedCount)
	}

	assertMerged := func(survivorID, dupID uuid.UUID) {
		t.Helper()
		dup := reloadEdge(t, ctx, edgeStore, dupID)
		if dup.SupersededBy == nil || *dup.SupersededBy != survivorID {
			t.Errorf("expected edge %s superseded_by %s, got %v", dupID, survivorID, dup.SupersededBy)
		}
	}
	assertNotMerged := func(id uuid.UUID) {
		t.Helper()
		e := reloadEdge(t, ctx, edgeStore, id)
		if e.SupersededBy != nil {
			t.Errorf("expected edge %s to remain unmerged, got superseded_by %v", id, *e.SupersededBy)
		}
	}

	// Case 1: exact duplicates merge, survivor is the earlier edge (exactA).
	assertMerged(exactA.ID, exactB.ID)
	assertNotMerged(exactA.ID)

	// Case 2: similar duplicates merge, survivor is the earlier edge (similarA).
	assertMerged(similarA.ID, similarB.ID)
	assertNotMerged(similarA.ID)

	// Case 3: below threshold -> neither merges.
	assertNotMerged(distinctA.ID)
	assertNotMerged(distinctB.ID)

	// Case 4: outside window -> neither merges, despite identical literals.
	assertNotMerged(outsideWindowA.ID)
	assertNotMerged(outsideWindowB.ID)

	// Case 5a: identical entity reference -> merges.
	assertMerged(entIdenticalA.ID, entIdenticalB.ID)

	// Case 5b: different entity reference -> neither merges (contradiction, not dedup).
	assertNotMerged(entDifferentA.ID)
	assertNotMerged(entDifferentB.ID)

	// Case 6: all three cluster members collapse onto clusterA (earliest FirstSeen).
	assertMerged(clusterA.ID, clusterB.ID)
	assertMerged(clusterA.ID, clusterC.ID)
	assertNotMerged(clusterA.ID)

	// --- Survivor field assertions (requirements 10-12) ---

	survivor := reloadEdge(t, ctx, edgeStore, exactA.ID)
	// LastReinforced bumped to the latest FirstSeen among the cluster (exactB's).
	if !survivor.LastReinforced.Equal(exactB.FirstSeen) {
		t.Errorf("expected survivor last_reinforced = %v (latest duplicate's first_seen), got %v", exactB.FirstSeen, survivor.LastReinforced)
	}
	// SourceMessageIDs includes both original edges' message IDs.
	wantIDs := map[uuid.UUID]bool{exactA.SourceMessageIDs[0]: true, exactB.SourceMessageIDs[0]: true}
	if len(survivor.SourceMessageIDs) != 2 {
		t.Fatalf("expected survivor to have 2 source_message_ids, got %d: %v", len(survivor.SourceMessageIDs), survivor.SourceMessageIDs)
	}
	for _, id := range survivor.SourceMessageIDs {
		if !wantIDs[id] {
			t.Errorf("unexpected source_message_id %s on survivor", id)
		}
	}

	// Loser's other fields are untouched beyond superseded_by.
	loser := reloadEdge(t, ctx, edgeStore, exactB.ID)
	if loser.Confidence != exactB.Confidence {
		t.Errorf("expected loser confidence unchanged at %v, got %v", exactB.Confidence, loser.Confidence)
	}
	if loser.DecayLocked != exactB.DecayLocked {
		t.Errorf("expected loser decay_locked unchanged at %v, got %v", exactB.DecayLocked, loser.DecayLocked)
	}
	if loser.ObjectLiteral == nil || *loser.ObjectLiteral != *exactB.ObjectLiteral {
		t.Errorf("expected loser object_literal unchanged, got %v", loser.ObjectLiteral)
	}
	if len(loser.SourceMessageIDs) != 1 || loser.SourceMessageIDs[0] != exactB.SourceMessageIDs[0] {
		t.Errorf("expected loser source_message_ids unchanged, got %v", loser.SourceMessageIDs)
	}

	// Survivor keeps its own (earlier) object_literal, not the duplicate's phrasing.
	similarSurvivor := reloadEdge(t, ctx, edgeStore, similarA.ID)
	if similarSurvivor.ObjectLiteral == nil || *similarSurvivor.ObjectLiteral != "went to the gym" {
		t.Errorf("expected survivor to keep its own object_literal %q, got %v", "went to the gym", similarSurvivor.ObjectLiteral)
	}
}

func TestLiteralSimilarity(t *testing.T) {
	cases := []struct {
		name    string
		a, b    string
		wantMin float64
		wantMax float64
	}{
		{"identical", "went to the gym", "went to the gym", 1.0, 1.0},
		{"near-duplicate one extra word", "went to the gym", "went to the gym today", 0.85, 1.0},
		{"unrelated", "hiking in the mountains", "cooking italian food", 0.0, 0.3},
		{"case and punctuation insensitive", "Went to the Gym!", "went to the gym", 1.0, 1.0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := literalSimilarity(c.a, c.b)
			if got < c.wantMin || got > c.wantMax {
				t.Errorf("literalSimilarity(%q, %q) = %v, want in [%v, %v]", c.a, c.b, got, c.wantMin, c.wantMax)
			}
		})
	}
}
