package reflection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// TestApplyBatchDecay covers requirement 7: a mix of normal, decay_locked,
// and superseded edges — assert correct effective confidence per normal
// edge, decay_locked edges unchanged regardless of elapsed time, and
// superseded edges excluded from the result set entirely.
func TestApplyBatchDecay(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Batch Decay Test Subject")
	lit := func(s string) *string { return &s }

	base := time.Now().Add(-30 * 24 * time.Hour)
	now := base.Add(30 * 24 * time.Hour) // 30 days after first_seen/last_reinforced

	// Normal edge: decays per the DecaySlow rate, no lock.
	normal := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: lit("Acme Corp"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, base)

	// decay_locked edge: same age, same decay rate, but must not decay at all.
	locked := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "event_present", ObjectLiteral: lit("her birthday is in March"),
		Confidence: 1.0, Importance: 0.9, SourceType: "user_correction", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow, DecayLocked: true,
	}, base)

	// Superseded edge: must not appear in the result set at all.
	// superseded_by has a DB foreign-key constraint back into edges, so it
	// has to point at a real row — create a second edge to play that role.
	supersededBy := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("cilantro"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, base)
	superseded := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("olives"),
		Confidence: 0.8, Importance: 0.4, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, base)
	superseded.SupersededBy = &supersededBy.ID
	if err := edgeStore.Update(ctx, superseded); err != nil {
		t.Fatalf("mark edge superseded: %v", err)
	}

	edges := []*memory.Edge{normal, locked, superseded}

	results, err := ApplyBatchDecay(ctx, edgeStore, edges, now)
	if err != nil {
		t.Fatalf("ApplyBatchDecay: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results (superseded edge excluded), got %d: %+v", len(results), results)
	}

	byID := make(map[uuid.UUID]float64, len(results))
	for _, r := range results {
		byID[r.EdgeID] = r.EffectiveConfidence
	}

	if _, ok := byID[superseded.ID]; ok {
		t.Errorf("expected superseded edge %s to be excluded from results, but it was present", superseded.ID)
	}

	wantNormal := memory.EffectiveConfidence(*normal, now)
	gotNormal, ok := byID[normal.ID]
	if !ok {
		t.Fatalf("expected a result for normal edge %s", normal.ID)
	}
	if gotNormal != wantNormal {
		t.Errorf("normal edge: expected EffectiveConfidence %v, got %v", wantNormal, gotNormal)
	}
	if gotNormal >= normal.Confidence {
		t.Errorf("expected normal edge's effective confidence to have decayed below its base %v after 30 days, got %v", normal.Confidence, gotNormal)
	}

	gotLocked, ok := byID[locked.ID]
	if !ok {
		t.Fatalf("expected a result for decay_locked edge %s", locked.ID)
	}
	if gotLocked != locked.Confidence {
		t.Errorf("decay_locked edge: expected effective confidence unchanged at %v regardless of elapsed time, got %v", locked.Confidence, gotLocked)
	}
}

// TestApplyBatchDecay_NeverWritesConfidenceColumn is the regression test
// for this issue's CRITICAL constraint: ApplyBatchDecay must never write a
// decayed value back into edges.confidence. It re-fetches the same edges
// from the store after the batch run and asserts their stored Confidence
// is exactly unchanged from before — this is the test that would catch
// the exact bug this issue is most at risk of introducing (compounding
// decay by writing the derived value back into the immutable base).
func TestApplyBatchDecay_NeverWritesConfidenceColumn(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	subject := createTestEntity(t, ctx, entityStore, "Confidence Immutability Test Subject")
	lit := func(s string) *string { return &s }

	// Far enough in the past that EffectiveConfidence's returned value is
	// guaranteed to differ meaningfully from the stored base — if this
	// batch pass ever did write it back, this gap makes the corruption
	// detectable rather than accidentally passing because decay was ~0.
	base := time.Now().Add(-180 * 24 * time.Hour)
	now := time.Now()

	edges := make([]*memory.Edge, 0, 3)
	preRunConfidence := make(map[uuid.UUID]float64, 3)

	specs := []struct {
		predicate   string
		literal     string
		confidence  float64
		decayRate   float64
		decayLocked bool
	}{
		{"goal_is", "learn Portuguese", 0.85, memory.DecayRateSlow, false},
		{"event_present", "recovering from surgery", 1.0, memory.DecayRateSlow, true},
		{"mood_signal", "stressed", 0.6, memory.DecayRateTransient, false},
	}

	for _, s := range specs {
		e := createTestEdge(t, ctx, pool, edgeStore, &memory.Edge{
			SubjectID: subject.ID, Predicate: s.predicate, ObjectLiteral: lit(s.literal),
			Confidence: s.confidence, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
			DecayRate: s.decayRate, DecayLocked: s.decayLocked,
		}, base)
		edges = append(edges, e)
		preRunConfidence[e.ID] = e.Confidence
	}

	results, err := ApplyBatchDecay(ctx, edgeStore, edges, now)
	if err != nil {
		t.Fatalf("ApplyBatchDecay: %v", err)
	}
	if len(results) != len(edges) {
		t.Fatalf("expected %d results, got %d", len(edges), len(results))
	}

	// Sanity: at least one result must actually reflect real decay
	// (differ from the stored base), otherwise this test could pass
	// vacuously even if something were quietly persisting decayed values
	// that happened to equal the base.
	sawDecay := false
	for _, r := range results {
		if r.EffectiveConfidence != preRunConfidence[r.EdgeID] {
			sawDecay = true
		}
	}
	if !sawDecay {
		t.Fatalf("expected at least one edge's EffectiveConfidence to differ from its stored base confidence over a 180-day gap; got no decay at all, this test's premise doesn't hold")
	}

	for _, e := range edges {
		reloaded := reloadEdge(t, ctx, edgeStore, e.ID)
		want := preRunConfidence[e.ID]
		if reloaded.Confidence != want {
			t.Errorf("edge %s (%s): stored confidence changed by ApplyBatchDecay — want %v (unchanged), got %v", e.ID, e.Predicate, want, reloaded.Confidence)
		}
	}
}
