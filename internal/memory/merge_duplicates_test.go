package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Dedup transaction fix: EdgeStore.MergeDuplicateEdges ---
//
// These exercise the actual database write directly (not through
// reflection.MergeDuplicates' clustering/filtering), against real
// Postgres.

func lit(s string) *string { return &s }

// TestMergeDuplicateEdges_NeverMovesLastReinforcedBackward is the
// issue's own reproducer (requirement 8): an edge first seen 100 days
// ago but reinforced yesterday, merged with a duplicate from around the
// same time it was first seen. The OLD code computed its "latest
// confirmation" from FirstSeen only, so last_reinforced would be
// overwritten to ~99 days ago and EffectiveConfidence would crash from
// ~0.90 to ~0.57 (the exact numbers in the issue's own reproducer,
// confirmed independently below). The fix must leave last_reinforced
// (and therefore EffectiveConfidence) completely unchanged by the
// merge.
func TestMergeDuplicateEdges_NeverMovesLastReinforcedBackward(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Timestamp Regression Subject")
	now := time.Now()
	firstSeen100DaysAgo := now.Add(-100 * 24 * time.Hour)
	reinforcedYesterday := now.Add(-1 * 24 * time.Hour)

	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	// Backdate BOTH columns to the 100-days-ago baseline first — Create()
	// stamps last_reinforced = now() by default, and Reinforce's
	// GREATEST can only move last_reinforced FORWARD, so reinforcing
	// straight to "yesterday" from a last_reinforced of "now" would be a
	// silent no-op.
	if _, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, firstSeen100DaysAgo, survivor.ID); err != nil {
		t.Fatalf("backdate survivor: %v", err)
	}
	if err := edgeStore.Reinforce(ctx, survivor.ID, reinforcedYesterday, firstSeen100DaysAgo); err != nil {
		t.Fatalf("reinforce survivor: %v", err)
	}

	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{uuid.New()},
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}
	// Within the dedup window of survivor's FirstSeen (doesn't matter to
	// MergeDuplicateEdges itself, which does no windowing — this is just
	// realistic setup matching what clusterDuplicates would have grouped).
	duplicateFirstSeen := firstSeen100DaysAgo.Add(2 * time.Hour)
	if _, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, duplicateFirstSeen, duplicate.ID); err != nil {
		t.Fatalf("backdate duplicate: %v", err)
	}

	beforeMerge, err := edgeStore.GetByID(ctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetByID(survivor, before): %v", err)
	}
	confidenceBefore := EffectiveConfidence(*beforeMerge, now)
	// Sanity-check the issue's own reproducer numbers independently: ~1
	// day elapsed should read close to 0.9 still (not anywhere near the
	// ~0.57 the bug produced from ~99 days elapsed).
	if confidenceBefore < 0.85 {
		t.Fatalf("test setup itself is wrong: expected EffectiveConfidence before merge to be close to 0.9 (1 day elapsed), got %v", confidenceBefore)
	}

	updatedSurvivor, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}
	if !updatedSurvivor.LastReinforced.Equal(reinforcedYesterday) {
		t.Errorf("expected returned survivor.LastReinforced = %v, got %v", reinforcedYesterday, updatedSurvivor.LastReinforced)
	}

	afterMerge, err := edgeStore.GetByID(ctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetByID(survivor, after): %v", err)
	}
	if !afterMerge.LastReinforced.Equal(reinforcedYesterday) {
		t.Errorf("expected last_reinforced unchanged at %v (never moved backward), got %v", reinforcedYesterday, afterMerge.LastReinforced)
	}
	if afterMerge.LastReinforced.Before(beforeMerge.LastReinforced) {
		t.Error("last_reinforced moved backward")
	}

	confidenceAfter := EffectiveConfidence(*afterMerge, now)
	if confidenceBefore != confidenceAfter {
		t.Errorf("expected EffectiveConfidence unchanged by the merge: before=%v after=%v (bug would produce ~0.57)", confidenceBefore, confidenceAfter)
	}
}

// TestMergeDuplicateEdges_DuplicateReinforcedMoreRecentlyWins covers the
// other half of requirement 8: when a DUPLICATE (not the survivor) was
// reinforced more recently, the survivor must take THAT later value —
// confirming the merge genuinely computes a MAX across the whole
// cluster, not just "keep the survivor's own existing value."
func TestMergeDuplicateEdges_DuplicateReinforcedMoreRecentlyWins(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Duplicate Wins Subject")
	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)

	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, old, survivor.ID); err != nil {
		t.Fatalf("backdate survivor: %v", err)
	}

	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}
	duplicateFirstSeen := old.Add(2 * time.Hour)
	duplicateReinforcedYesterday := now.Add(-1 * 24 * time.Hour)
	// Backdate both columns first — same reasoning as the survivor's own
	// setup above and TestMergeDuplicateEdges_NeverMovesLastReinforcedBackward's
	// comment: Reinforce's GREATEST can't move last_reinforced backward
	// from Create()'s now() default.
	if _, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, duplicateFirstSeen, duplicate.ID); err != nil {
		t.Fatalf("backdate duplicate: %v", err)
	}
	if err := edgeStore.Reinforce(ctx, duplicate.ID, duplicateReinforcedYesterday, duplicateFirstSeen); err != nil {
		t.Fatalf("reinforce duplicate: %v", err)
	}

	updatedSurvivor, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}
	if !updatedSurvivor.LastReinforced.Equal(duplicateReinforcedYesterday) {
		t.Errorf("expected survivor to take the duplicate's more recent last_reinforced %v, got %v", duplicateReinforcedYesterday, updatedSurvivor.LastReinforced)
	}
}

// TestMergeDuplicateEdges_ThreeMemberCluster covers requirement 8's
// "include a 3-member cluster": the max LastReinforced must be computed
// across ALL members, not just the survivor and one duplicate.
func TestMergeDuplicateEdges_ThreeMemberCluster(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Three Member Subject")
	now := time.Now()
	old := now.Add(-100 * 24 * time.Hour)

	mkEdge := func(firstSeenOffset time.Duration, msgID uuid.UUID) *Edge {
		e := &Edge{
			SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
			Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
			DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{msgID},
		}
		if err := edgeStore.Create(ctx, e); err != nil {
			t.Fatalf("create edge: %v", err)
		}
		fs := old.Add(firstSeenOffset)
		if _, err := pool.Exec(ctx, `UPDATE edges SET first_seen = $1, last_reinforced = $1 WHERE id = $2`, fs, e.ID); err != nil {
			t.Fatalf("backdate edge: %v", err)
		}
		e.FirstSeen, e.LastReinforced = fs, fs
		return e
	}

	msgA, msgB, msgC := uuid.New(), uuid.New(), uuid.New()
	survivor := mkEdge(0, msgA)
	dup1 := mkEdge(1*time.Hour, msgB)
	dup2 := mkEdge(2*time.Hour, msgC)

	// dup2 (the LAST-first-seen member, not the survivor) is the one
	// reinforced most recently — the max must still be found even though
	// it's neither the survivor nor the first duplicate checked.
	mostRecentReinforcement := now.Add(-6 * time.Hour)
	if err := edgeStore.Reinforce(ctx, dup2.ID, mostRecentReinforcement, dup2.FirstSeen); err != nil {
		t.Fatalf("reinforce dup2: %v", err)
	}

	updatedSurvivor, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{dup1.ID, dup2.ID})
	if err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}
	if !updatedSurvivor.LastReinforced.Equal(mostRecentReinforcement) {
		t.Errorf("expected max last_reinforced across all 3 members = %v, got %v", mostRecentReinforcement, updatedSurvivor.LastReinforced)
	}

	wantIDs := map[uuid.UUID]bool{msgA: true, msgB: true, msgC: true}
	if len(updatedSurvivor.SourceMessageIDs) != 3 {
		t.Fatalf("expected 3 distinct source_message_ids, got %v", updatedSurvivor.SourceMessageIDs)
	}
	for _, id := range updatedSurvivor.SourceMessageIDs {
		if !wantIDs[id] {
			t.Errorf("unexpected source_message_id %s", id)
		}
	}

	for _, dupID := range []uuid.UUID{dup1.ID, dup2.ID} {
		dup, err := edgeStore.GetByID(ctx, dupID)
		if err != nil {
			t.Fatalf("GetByID(%s): %v", dupID, err)
		}
		if dup.SupersededBy == nil || *dup.SupersededBy != survivor.ID {
			t.Errorf("expected %s superseded_by %s, got %v", dupID, survivor.ID, dup.SupersededBy)
		}
	}
}

// TestMergeDuplicateEdges_SourceMessageIDsUnionIsDistinct covers
// requirement 14: the merged source_message_ids must not contain
// duplicate entries when the survivor and a duplicate share a source
// message, and must be survivor-first, then each duplicate's own
// remaining IDs.
func TestMergeDuplicateEdges_SourceMessageIDsUnionIsDistinct(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Union Distinct Subject")
	shared, onlyOnDup := uuid.New(), uuid.New()

	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{shared},
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{shared, onlyOnDup},
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}

	updatedSurvivor, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}
	if len(updatedSurvivor.SourceMessageIDs) != 2 {
		t.Fatalf("expected exactly 2 distinct source_message_ids (shared counted once), got %v", updatedSurvivor.SourceMessageIDs)
	}
	if updatedSurvivor.SourceMessageIDs[0] != shared || updatedSurvivor.SourceMessageIDs[1] != onlyOnDup {
		t.Errorf("expected order [shared, onlyOnDup], got %v", updatedSurvivor.SourceMessageIDs)
	}
}

// TestMergeDuplicateEdges_LockedEdgeAborts covers requirement 11's
// second half (combined with 10's direct-layer check): an edge that is
// is_correction, decay_locked, a tombstone, or already superseded must
// make MergeDuplicateEdges itself refuse with ErrMergeConflict and write
// nothing — defense in depth below reflection.MergeDuplicates' own
// candidate filtering, exercised directly here.
func TestMergeDuplicateEdges_LockedEdgeAborts(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	for _, tc := range []struct {
		name string
		mod  func(e *Edge)
	}{
		{"is_correction", func(e *Edge) { e.IsCorrection = true }},
		{"decay_locked", func(e *Edge) { e.DecayLocked = true }},
		{"tombstone", func(e *Edge) { e.SourceType = SourceTypeUserDeletion; e.Confidence, e.Importance = 0, 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := createTestEntity(t, ctx, entityStore, "Dedup Locked Abort Subject "+tc.name)

			survivor := &Edge{
				SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
				Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
			}
			if err := edgeStore.Create(ctx, survivor); err != nil {
				t.Fatalf("create survivor: %v", err)
			}
			duplicate := &Edge{
				SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
				Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
			}
			tc.mod(duplicate)
			if err := edgeStore.Create(ctx, duplicate); err != nil {
				t.Fatalf("create duplicate: %v", err)
			}

			originalSourceIDs := append([]uuid.UUID{}, survivor.SourceMessageIDs...)

			_, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
			if !errors.Is(err, ErrMergeConflict) {
				t.Fatalf("expected ErrMergeConflict, got %v", err)
			}

			reloadedSurvivor, err := edgeStore.GetByID(ctx, survivor.ID)
			if err != nil {
				t.Fatalf("GetByID(survivor): %v", err)
			}
			if len(reloadedSurvivor.SourceMessageIDs) != len(originalSourceIDs) {
				t.Errorf("expected survivor's source_message_ids unchanged, got %v", reloadedSurvivor.SourceMessageIDs)
			}

			reloadedDup, err := edgeStore.GetByID(ctx, duplicate.ID)
			if err != nil {
				t.Fatalf("GetByID(duplicate): %v", err)
			}
			if reloadedDup.SupersededBy != nil {
				t.Errorf("expected the locked duplicate to remain unsuperseded, got %v", reloadedDup.SupersededBy)
			}
		})
	}
}

// TestMergeDuplicateEdges_StaleSnapshotDoesNotClobberFreshChanges covers
// requirement 11's first half: MergeDuplicateEdges is called with only
// IDs (never a caller-held *Edge snapshot), so a direct, concurrent
// change to the survivor's importance (something MergeDuplicateEdges
// never touches at all) and last_reinforced (something it DOES touch,
// via GREATEST) must both survive the merge intact.
func TestMergeDuplicateEdges_StaleSnapshotDoesNotClobberFreshChanges(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Stale Snapshot Subject")

	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.9, Importance: 0.3, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}

	// Simulates a concurrent process (UpdateMemory, a live reinforcement)
	// acting on the survivor AFTER a caller elsewhere already loaded its
	// own in-memory candidate snapshot but BEFORE this merge runs.
	freshImportance := 0.95
	// Later than whatever Create() just stamped survivor.LastReinforced
	// with (now(), a few ms ago at most) — Reinforce's GREATEST can only
	// move last_reinforced forward, so this must be AFTER creation, not
	// an earlier "recent" time.
	veryRecent := time.Now().Add(1 * time.Minute)
	if _, err := pool.Exec(ctx, `UPDATE edges SET importance = $1 WHERE id = $2`, freshImportance, survivor.ID); err != nil {
		t.Fatalf("simulate concurrent importance change: %v", err)
	}
	if err := edgeStore.Reinforce(ctx, survivor.ID, veryRecent, survivor.FirstSeen); err != nil {
		t.Fatalf("simulate concurrent reinforcement: %v", err)
	}

	// Merge using only the IDs — exactly what reflection.MergeDuplicates
	// does; it never passes a snapshot struct into this method.
	updatedSurvivor, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err != nil {
		t.Fatalf("MergeDuplicateEdges: %v", err)
	}
	if !updatedSurvivor.LastReinforced.Equal(veryRecent) {
		t.Errorf("expected the later, concurrently-set last_reinforced %v to survive, got %v", veryRecent, updatedSurvivor.LastReinforced)
	}

	reloaded, err := edgeStore.GetByID(ctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if reloaded.Importance != freshImportance {
		t.Errorf("expected the concurrently-set importance %v to survive (MergeDuplicateEdges must never touch it), got %v", freshImportance, reloaded.Importance)
	}
}

// TestMergeDuplicateEdges_MidMergeFailureRollsBack covers requirement
// 12: a real DB error between the survivor UPDATE and the duplicate
// UPDATE must roll back BOTH — the transaction must leave the database
// exactly in its pre-merge state. Simulated with a temporary trigger
// (not a production test seam) that raises whenever superseded_by is
// set on an edge — i.e. exactly the duplicate-UPDATE step, which always
// runs after the survivor UPDATE in MergeDuplicateEdges' own write
// order.
func TestMergeDuplicateEdges_MidMergeFailureRollsBack(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION pg_temp_test_raise_on_supersede() RETURNS TRIGGER AS $$
		BEGIN
			IF NEW.superseded_by IS NOT NULL AND OLD.superseded_by IS NULL THEN
				RAISE EXCEPTION 'simulated mid-merge failure for test';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		t.Fatalf("create trigger function: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TRIGGER test_raise_on_supersede
		BEFORE UPDATE ON edges
		FOR EACH ROW EXECUTE FUNCTION pg_temp_test_raise_on_supersede();
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS test_raise_on_supersede ON edges`); err != nil {
			t.Errorf("cleanup: drop trigger: %v", err)
		}
		if _, err := pool.Exec(ctx, `DROP FUNCTION IF EXISTS pg_temp_test_raise_on_supersede()`); err != nil {
			t.Errorf("cleanup: drop trigger function: %v", err)
		}
	})

	subject := createTestEntity(t, ctx, entityStore, "Dedup Rollback Subject")
	originalMsgID := uuid.New()
	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: DecayRateSlow, SourceMessageIDs: []uuid.UUID{originalMsgID},
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	originalLastReinforced := survivor.LastReinforced

	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}

	_, err := edgeStore.MergeDuplicateEdges(ctx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err == nil {
		t.Fatal("expected the simulated trigger failure to surface as an error")
	}

	reloadedSurvivor, err := edgeStore.GetByID(ctx, survivor.ID)
	if err != nil {
		t.Fatalf("GetByID(survivor): %v", err)
	}
	if len(reloadedSurvivor.SourceMessageIDs) != 1 || reloadedSurvivor.SourceMessageIDs[0] != originalMsgID {
		t.Errorf("expected survivor's source_message_ids rolled back to %v, got %v", []uuid.UUID{originalMsgID}, reloadedSurvivor.SourceMessageIDs)
	}
	if !reloadedSurvivor.LastReinforced.Equal(originalLastReinforced) {
		t.Errorf("expected survivor's last_reinforced rolled back to %v, got %v", originalLastReinforced, reloadedSurvivor.LastReinforced)
	}

	reloadedDup, err := edgeStore.GetByID(ctx, duplicate.ID)
	if err != nil {
		t.Fatalf("GetByID(duplicate): %v", err)
	}
	if reloadedDup.SupersededBy != nil {
		t.Errorf("expected the duplicate's superseded_by rolled back to nil, got %v", reloadedDup.SupersededBy)
	}
}

// TestMergeDuplicateEdges_CancelledContextIsNotMisclassifiedAsConflict
// covers requirement 9 at the lockEdgeForUpdate-error-classification
// fix's own caller: before that fix, lockEdgeForUpdate wrapped EVERY
// query failure as ErrEdgeNotFound, which this method's own check
// (errors.Is(lockErr, ErrEdgeNotFound) -> return ErrMergeConflict, see
// its own doc comment) would have silently turned a real infrastructure
// failure — here, a cancelled ctx — into a benign "stale candidate, skip
// it" outcome. reflection.MergeDuplicates (the dedup pass) treats
// ErrMergeConflict as skippable but anything else as a hard stop, so
// this one misclassification would have made a cancelled/failed dedup
// pass look like it just found nothing to merge. Confirmed fixed: a
// cancelled ctx against a REAL candidate pair (both rows genuinely
// exist and would otherwise merge cleanly) must come back as its own
// distinct error, never ErrMergeConflict.
func TestMergeDuplicateEdges_CancelledContextIsNotMisclassifiedAsConflict(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	edgeStore := NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Dedup Cancelled Context Subject")

	survivor := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, survivor); err != nil {
		t.Fatalf("create survivor: %v", err)
	}
	duplicate := &Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0, DecayRate: DecayRateSlow,
	}
	if err := edgeStore.Create(ctx, duplicate); err != nil {
		t.Fatalf("create duplicate: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := edgeStore.MergeDuplicateEdges(cancelledCtx, survivor.ID, []uuid.UUID{duplicate.ID})
	if err == nil {
		t.Fatal("expected an error for a cancelled context, got nil")
	}
	if errors.Is(err, ErrMergeConflict) {
		t.Errorf("expected a cancelled context NOT to be misclassified as ErrMergeConflict, got: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled) to hold, got: %v", err)
	}

	// Neither row touched — the transaction never got far enough to
	// write anything (or rolled back if it did).
	reloadedDup, err := edgeStore.GetByID(ctx, duplicate.ID)
	if err != nil {
		t.Fatalf("GetByID(duplicate): %v", err)
	}
	if reloadedDup.SupersededBy != nil {
		t.Errorf("expected the duplicate to remain unsuperseded after a cancelled-context failure, got %v", reloadedDup.SupersededBy)
	}
}
