package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// TestRankAndFilter_PrunedExcludedUnlessIncluded is a pure, DB-free unit
// test of the ranking helper's prune gate — fast, exact, and independent
// of any SQL wiring.
func TestRankAndFilter_PrunedExcludedUnlessIncluded(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pruneCfg := memory.DefaultPruneConfig()

	normal := &memory.Edge{Confidence: 0.9, Importance: 0.8, DecayRate: memory.DecayRateStable, LastReinforced: now}
	lowConfidence := &memory.Edge{Confidence: 0.01, Importance: 0.8, DecayRate: memory.DecayRateStable, LastReinforced: now}

	candidates := []memory.EdgeCandidate{{Edge: normal, Similarity: 0.5}, {Edge: lowConfidence, Similarity: 0.5}}

	excluded := rankAndFilter(candidates, now, DefaultRankingWeights(), pruneCfg, false, 10)
	if len(excluded) != 1 || excluded[0] != normal {
		t.Fatalf("expected only the normal edge with includePruned=false, got %+v", excluded)
	}

	included := rankAndFilter(candidates, now, DefaultRankingWeights(), pruneCfg, true, 10)
	if len(included) != 2 {
		t.Fatalf("expected both edges with includePruned=true, got %d", len(included))
	}
}

// TestRankAndFilter_RankReflectsWeightedFormula is a pure unit test
// proving the weighted formula, not just similarity, actually drives
// order: with importance weighted heavily, a lower-similarity/higher-
// importance edge outranks a higher-similarity/trivial one.
func TestRankAndFilter_RankReflectsWeightedFormula(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pruneCfg := memory.DefaultPruneConfig()

	highSimLowImportance := &memory.Edge{Confidence: 0.5, Importance: 0.15, DecayRate: memory.DecayRateStable, LastReinforced: now}
	lowSimHighImportance := &memory.Edge{Confidence: 0.5, Importance: 0.95, DecayRate: memory.DecayRateStable, LastReinforced: now}

	candidates := []memory.EdgeCandidate{
		{Edge: highSimLowImportance, Similarity: 1.0},
		{Edge: lowSimHighImportance, Similarity: 0.0},
	}

	// Importance-dominant weights: proves the formula is actually applied
	// (not a hardcoded similarity sort) by deliberately inverting what a
	// pure-similarity ranking would produce.
	weights := RankingWeights{Similarity: 0.1, Confidence: 0.1, Importance: 0.8}

	ranked := rankAndFilter(candidates, now, weights, pruneCfg, false, 10)
	if len(ranked) != 2 {
		t.Fatalf("expected both edges ranked, got %d", len(ranked))
	}
	if ranked[0] != lowSimHighImportance {
		t.Errorf("expected the lower-similarity/higher-importance edge to rank first under importance-dominant weights, got %+v first", ranked[0])
	}
	if ranked[1] != highSimLowImportance {
		t.Errorf("expected the higher-similarity/trivial edge to rank second, got %+v second", ranked[1])
	}
}

// TestGetRelevantContext_PrunedExcludedByDefault_IncludedWhenRequested
// covers requirement 10's first half end-to-end against a real database:
// a genuinely low-confidence/low-importance edge is excluded from
// GetRelevantContext's default results and included when the caller
// explicitly asks for it via Filters.IncludePruned.
func TestGetRelevantContext_PrunedExcludedByDefault_IncludedWhenRequested(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Prune Test Subject")
	lit := func(s string) *string { return &s }

	now := time.Now()
	normal := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("hiking"),
		Confidence: 0.9, Importance: 0.8, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, now)
	pruned := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("trivial detail"),
		Confidence: 0.02, Importance: 0.01, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, now)

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	byDefault, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &subject.ID}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext (default): %v", err)
	}
	assertContainsEdge(t, byDefault, normal.ID, true)
	assertContainsEdge(t, byDefault, pruned.ID, false)

	withPruned, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &subject.ID, IncludePruned: true}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext (include_pruned): %v", err)
	}
	assertContainsEdge(t, withPruned, normal.ID, true)
	assertContainsEdge(t, withPruned, pruned.ID, true)
}

// TestGetRelevantContext_RankingReflectsWeightedFormula covers
// requirement 10's second half end-to-end: a real vector query, real
// embeddings, and custom weights that make a lower-similarity/higher-
// importance edge outrank a higher-similarity/trivial one, through the
// full GetRelevantContext -> SearchRelevant -> rankAndFilter path.
func TestGetRelevantContext_RankingReflectsWeightedFormula(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Ranking Test Subject")
	sender := createTestEntity(t, ctx, ts, "person", "Ranking Test Sender")
	lit := func(s string) *string { return &s }

	now := time.Now()
	msg1 := createTestMessage(t, ctx, ts, uuid.New(), sender.ID, "text", now)
	createTestEmbedding(t, ctx, ts, msg1.ID, nearVector(0)) // near-identical to the query vector below

	msg2 := createTestMessage(t, ctx, ts, uuid.New(), sender.ID, "text", now)
	createTestEmbedding(t, ctx, ts, msg2.ID, basisVector(1, 1)) // orthogonal to the query vector

	highSim := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("trivial"),
		Confidence: 0.5, Importance: 0.15, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{msg1.ID},
	}, now)
	lowSim := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("important"),
		Confidence: 0.5, Importance: 0.95, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: []uuid.UUID{msg2.ID},
	}, now)

	embedder := &fakeEmbedder{vector: basisVector(0, 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	results, err := a.GetRelevantContext(ctx, "some query", Filters{
		SubjectID: &subject.ID,
		Weights:   RankingWeights{Similarity: 0.1, Confidence: 0.1, Importance: 0.8},
	}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected both edges returned, got %d: %+v", len(results), results)
	}
	if results[0].ID != lowSim.ID {
		t.Errorf("expected the lower-similarity/higher-importance edge (%s) to rank first under importance-dominant weights, got %s first", lowSim.ID, results[0].ID)
	}
	if results[1].ID != highSim.ID {
		t.Errorf("expected the higher-similarity/trivial edge (%s) to rank second, got %s", highSim.ID, results[1].ID)
	}
}

func assertContainsEdge(t *testing.T, edges []*memory.Edge, id uuid.UUID, want bool) {
	t.Helper()
	got := false
	for _, e := range edges {
		if e.ID == id {
			got = true
			break
		}
	}
	if got != want {
		t.Errorf("expected edge %s present=%v, got present=%v (result set: %d edges)", id, want, got, len(edges))
	}
}
