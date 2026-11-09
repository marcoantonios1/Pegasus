package reflection

import (
	"context"
	"errors"
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

// markTestEdgeSuperseded sets superseded_by directly via SQL — test-only
// setup for "this edge is already superseded" scenarios, now that
// EdgeStore.Update (a full-row write-back any test could previously
// (ab)use for this) no longer exists — see internal/memory/edge_store.go's
// own write-contract comment.
func markTestEdgeSuperseded(t *testing.T, ctx context.Context, pool *pgxpool.Pool, edgeID, supersededByID uuid.UUID) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE edges SET superseded_by = $1 WHERE id = $2`, supersededByID, edgeID); err != nil {
		t.Fatalf("mark test edge superseded: %v", err)
	}
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
	// exactA reinforced AFTER exactB's FirstSeen, deliberately — this is
	// requirement 9's seed fix: createTestEdge leaves LastReinforced ==
	// FirstSeen for every edge, so without this, the old (buggy)
	// "latest FirstSeen among the cluster" computation and the fixed
	// "latest LastReinforced among the cluster" computation happen to
	// agree, and this test would pass either way — proving nothing about
	// which one is actually implemented. Bumping exactA's LastReinforced
	// past exactB's FirstSeen makes the two computations disagree: the
	// old code would have moved it BACKWARD to exactB's FirstSeen; the
	// fix must leave it at exactAReinforcedAt (already the max).
	exactAReinforcedAt := base.Add(10 * time.Hour)
	if err := edgeStore.Reinforce(ctx, exactA.ID, exactAReinforcedAt, exactA.FirstSeen); err != nil {
		t.Fatalf("reinforce exactA: %v", err)
	}
	exactA.LastReinforced = exactAReinforcedAt

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

	mergedCount, err := MergeDuplicates(ctx, edgeStore, candidates, DefaultDedupConfig(), nil)
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
	// LastReinforced is the MAX LastReinforced across the cluster — NOT
	// the latest FirstSeen (the old, buggy behavior this fix replaces;
	// see exactAReinforcedAt's own setup comment above for why the seed
	// is deliberately built so these two would disagree if the old
	// computation were still in place).
	if !survivor.LastReinforced.Equal(exactAReinforcedAt) {
		t.Errorf("expected survivor last_reinforced = %v (max last_reinforced across the cluster), got %v", exactAReinforcedAt, survivor.LastReinforced)
	}
	if survivor.LastReinforced.Before(exactAReinforcedAt) {
		t.Error("expected last_reinforced to never move backward")
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

// TestMergeDuplicates_ExcludesTombstone covers the delete-exclusion
// audit's dedup finding (requirement 9): MergeDuplicates' own `live`
// filter only checks SupersededBy == nil, with no tombstone check — a
// soft-deleted edge's LIVE tombstone, sharing the same subject/predicate
// and (by construction) the exact same object the deleted edge had,
// could otherwise cluster with a genuinely live, merely-similar edge and
// — since the survivor is picked by EARLIEST FirstSeen, and a tombstone
// is typically older than whatever reasserts the fact later — become the
// merge SURVIVOR: a confidence-0/importance-0 tombstone absorbing a real
// live edge's source_message_ids, corrupting it.
func TestMergeDuplicates_ExcludesTombstone(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Tombstone Subject")
	base := time.Now().Add(-72 * time.Hour)
	lit := func(s string) *string { return &s }

	// Same known-good similar-literal pair as TestMergeDuplicates' own
	// Case 2 ("went to the gym" / "went to the gym today",
	// Dice coefficient 8/9 ≈ 0.889, above the 0.85 default threshold).
	tombstone := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: lit("went to the gym"),
		Confidence: 0, Importance: 0, SourceType: memory.SourceTypeUserDeletion, SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, base)
	live := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: lit("went to the gym today"),
		Confidence: 0.7, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, base.Add(1*time.Hour))

	mergedCount, err := MergeDuplicates(ctx, edgeStore, []*memory.Edge{tombstone, live}, DefaultDedupConfig(), nil)
	if err != nil {
		t.Fatalf("MergeDuplicates: %v", err)
	}
	if mergedCount != 0 {
		t.Errorf("expected zero merges (the tombstone must never be clustered at all), got %d", mergedCount)
	}

	reloadedLive := reloadEdge(t, ctx, edgeStore, live.ID)
	if reloadedLive.SupersededBy != nil {
		t.Errorf("expected the live edge to remain live (not superseded by the tombstone), got superseded_by=%v", reloadedLive.SupersededBy)
	}
	reloadedTombstone := reloadEdge(t, ctx, edgeStore, tombstone.ID)
	if reloadedTombstone.SupersededBy != nil {
		t.Errorf("expected the tombstone to remain untouched, got superseded_by=%v", reloadedTombstone.SupersededBy)
	}
	if len(reloadedTombstone.SourceMessageIDs) != 0 {
		t.Errorf("expected the tombstone to never absorb the live edge's source_message_ids, got %v", reloadedTombstone.SourceMessageIDs)
	}
}

// TestMergeDuplicates_ExcludesLockedEdgesBeforeClustering covers
// requirement 10: a cluster containing an is_correction edge (and,
// separately, a decay_locked-only edge) plus an ordinary near-duplicate
// must merge NOTHING — the locked edge is excluded before clustering
// even starts (MergeDuplicates' own candidate filter), so it never
// becomes a survivor OR a duplicate. The ordinary edge, left with no one
// to cluster with, simply stays live and unmerged.
func TestMergeDuplicates_ExcludesLockedEdgesBeforeClustering(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(e *memory.Edge)
	}{
		{"is_correction", func(e *memory.Edge) { e.IsCorrection = true; e.DecayLocked = true }},
		{"decay_locked_only", func(e *memory.Edge) { e.DecayLocked = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testDB(t)
			ctx := context.Background()
			entityStore := memory.NewEntityStore(pool)
			edgeStore := memory.NewEdgeStore(pool)

			subject := createTestEntity(t, ctx, entityStore, "Dedup Locked Candidate Subject "+tc.name)
			base := time.Now().Add(-72 * time.Hour)
			litFn := func(s string) *string { return &s }

			locked := &memory.Edge{
				SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
				Confidence: 1.0, Importance: 0.9, SourceType: memory.SourceTypeUserCorrection, SourceWeight: 1.0,
				DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
			}
			tc.mod(locked)
			locked = createTestEdge(t, ctx, pool, edgeStore, locked, base)

			ordinary := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
				SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
				Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
				DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
			}, base.Add(1*time.Hour))

			lockedOriginalSourceIDs := append([]uuid.UUID{}, locked.SourceMessageIDs...)

			mergedCount, err := MergeDuplicates(ctx, edgeStore, []*memory.Edge{locked, ordinary}, DefaultDedupConfig(), nil)
			if err != nil {
				t.Fatalf("MergeDuplicates: %v", err)
			}
			if mergedCount != 0 {
				t.Errorf("expected zero merges (the locked edge must never be a merge candidate), got %d", mergedCount)
			}

			reloadedLocked := reloadEdge(t, ctx, edgeStore, locked.ID)
			if reloadedLocked.SupersededBy != nil {
				t.Errorf("expected the locked edge's superseded_by to stay nil (never a duplicate), got %v", reloadedLocked.SupersededBy)
			}
			if reloadedLocked.Confidence != locked.Confidence {
				t.Errorf("expected the locked edge's confidence untouched, got %v", reloadedLocked.Confidence)
			}
			if len(reloadedLocked.SourceMessageIDs) != len(lockedOriginalSourceIDs) {
				t.Errorf("expected the locked edge's source_message_ids untouched, got %v", reloadedLocked.SourceMessageIDs)
			}

			// The real proof the locked edge was never chosen as
			// survivor: if it had been, ordinary.SupersededBy would now
			// point at it.
			reloadedOrdinary := reloadEdge(t, ctx, edgeStore, ordinary.ID)
			if reloadedOrdinary.SupersededBy != nil {
				t.Errorf("expected the ordinary edge to remain live (not superseded by the locked edge), got superseded_by=%v", reloadedOrdinary.SupersededBy)
			}
		})
	}
}

// TestMergeDuplicates_FailingClusterDoesNotUndoCommittedClusters covers
// requirement 13: two independent clusters, one that merges
// successfully and one engineered to hit memory.ErrMergeConflict (a
// race — one of its edges becomes is_correction=true, directly in the
// DB, AFTER the in-memory candidates slice was already built, so
// MergeDuplicates' own pre-clustering filter — which only sees the
// STALE Go struct — can't catch it; MergeDuplicateEdges' re-validation
// under the lock does). The failing cluster must be skipped without
// undoing the other cluster's already-committed merge, and mergedCount
// must reflect only the successful one.
func TestMergeDuplicates_FailingClusterDoesNotUndoCommittedClusters(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	base := time.Now().Add(-72 * time.Hour)
	litFn := func(s string) *string { return &s }

	subjectA := createTestEntity(t, ctx, entityStore, "Dedup Partial Failure Subject A")
	edgeA1 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subjectA.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	edgeA2 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subjectA.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(1*time.Hour))

	subjectB := createTestEntity(t, ctx, entityStore, "Dedup Partial Failure Subject B")
	edgeB1 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subjectB.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	edgeB2 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subjectB.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(1*time.Hour))

	// Candidates slice built BEFORE the race — matches a real
	// orchestrator loading a snapshot, then another process (live
	// ingestion issuing a correction) mutating the DB before the dedup
	// pass's own transaction runs.
	candidates := []*memory.Edge{edgeA1, edgeA2, edgeB1, edgeB2}

	if _, err := pool.Exec(ctx, `UPDATE edges SET is_correction = true, decay_locked = true WHERE id = $1`, edgeB2.ID); err != nil {
		t.Fatalf("simulate concurrent correction on edgeB2: %v", err)
	}

	mergedCount, err := MergeDuplicates(ctx, edgeStore, candidates, DefaultDedupConfig(), nil)
	if err != nil {
		t.Fatalf("MergeDuplicates: %v", err)
	}
	if mergedCount != 1 {
		t.Errorf("expected mergedCount = 1 (only cluster A), got %d", mergedCount)
	}

	reloadedA2 := reloadEdge(t, ctx, edgeStore, edgeA2.ID)
	if reloadedA2.SupersededBy == nil || *reloadedA2.SupersededBy != edgeA1.ID {
		t.Errorf("expected cluster A's merge committed (edgeA2 superseded by edgeA1), got superseded_by=%v", reloadedA2.SupersededBy)
	}

	reloadedB1 := reloadEdge(t, ctx, edgeStore, edgeB1.ID)
	if reloadedB1.SupersededBy != nil {
		t.Errorf("expected cluster B's survivor untouched, got superseded_by=%v", reloadedB1.SupersededBy)
	}
	reloadedB2 := reloadEdge(t, ctx, edgeStore, edgeB2.ID)
	if reloadedB2.SupersededBy != nil {
		t.Errorf("expected cluster B's (now-correction) edge untouched beyond what was set directly, got superseded_by=%v", reloadedB2.SupersededBy)
	}
	if !reloadedB2.IsCorrection {
		t.Error("expected edgeB2 to still be is_correction (set directly, never reverted)")
	}
}

// TestMergeDuplicates_CancelledContextStopsThePassRatherThanSkipping
// covers requirement 9 at the dedup pass's own level (one layer above
// memory.TestMergeDuplicateEdges_CancelledContextIsNotMisclassifiedAsConflict,
// which covers the same fix one level down): before the
// lockEdgeForUpdate-error-classification fix, a cancelled/failed
// database call during a merge could come back from
// EdgeStore.MergeDuplicateEdges as memory.ErrMergeConflict — which
// MergeDuplicates' own branch (errors.Is(mergeErr, memory.
// ErrMergeConflict) -> log and continue, see its own doc comment)
// treats as a completely benign "this cluster's data moved, skip it and
// keep going" outcome. A real infrastructure failure must stop the
// whole pass instead, loudly, not be absorbed as if nothing of note
// happened.
func TestMergeDuplicates_CancelledContextStopsThePassRatherThanSkipping(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	base := time.Now().Add(-72 * time.Hour)
	litFn := func(s string) *string { return &s }

	subject := createTestEntity(t, ctx, entityStore, "Dedup Cancelled Context Pass Subject")
	edge1 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base)
	edge2 := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: litFn("run a marathon"),
		Confidence: 0.8, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}, base.Add(1*time.Hour))
	candidates := []*memory.Edge{edge1, edge2}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	mergedCount, err := MergeDuplicates(cancelledCtx, edgeStore, candidates, DefaultDedupConfig(), nil)
	if err == nil {
		t.Fatal("expected a cancelled context to stop the pass with an error, got nil")
	}
	if errors.Is(err, memory.ErrMergeConflict) {
		t.Errorf("expected a cancelled context NOT to be absorbed as memory.ErrMergeConflict (a silent skip), got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled) to hold, got: %v", err)
	}
	if mergedCount != 0 {
		t.Errorf("expected mergedCount 0 (nothing committed before the failure), got %d", mergedCount)
	}

	reloaded2 := reloadEdge(t, ctx, edgeStore, edge2.ID)
	if reloaded2.SupersededBy != nil {
		t.Errorf("expected the cluster untouched after a cancelled-context failure, got superseded_by=%v", reloaded2.SupersededBy)
	}
}
