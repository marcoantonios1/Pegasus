package reflection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Sizes per §6.1's "hundreds of thousands of edges" personal-scale
// estimate: benchEdgeCount stands in for the relevant-edge slice a single
// consolidation pass (or a single retrieval query's candidate set) would
// touch, and benchQueryCount is a rough stand-in for a day's worth of
// Hermes retrieval calls hitting that same edge population between
// consolidation passes.
//
// These two constants back the "batch decay is confirmed cheaper than
// per-query computation at realistic data volume" acceptance criterion.
// Measured on Apple M2 Max at these sizes (`go test
// ./internal/reflection/... -run '^$' -bench BenchmarkConfidence
// -benchtime=2s -benchmem`):
//
//	BenchmarkConfidence_InlinePerQuery-12                   157   15,601,552 ns/op        0 B/op    0 allocs/op
//	BenchmarkConfidence_BatchOncePerConsolidationPass-12    430    5,501,134 ns/op  682,624 B/op   34 allocs/op
//
// The batch-once approach is ~2.8x faster in wall time at this N/M, despite
// paying allocation cost the inline approach doesn't (building the
// uuid.UUID -> float64 cache). That allocation cost is fixed per
// consolidation pass (proportional to N), while the savings scale with M —
// so the batch approach's advantage grows with query volume between
// consolidation passes and shrinks (or could theoretically invert, for very
// small M) as query volume drops. At M=50 and realistic idle-triggered
// consolidation frequency (proposal §9.1: every 30-45min of idle, or at
// most every 18-24h), M relative to consolidation-pass frequency is
// comfortably in the range where batching wins — which is what this
// benchmark exists to confirm numerically rather than assume.
const (
	benchEdgeCount  = 10_000
	benchQueryCount = 50
)

// benchSink defeats dead-code elimination of the computed values — without
// something observable happening to the result, the compiler is free to
// notice EffectiveConfidence's output is never used and optimize the call
// away entirely, which would make both benchmarks measure ~nothing.
var benchSink float64

// benchEdges builds a synthetic population of edges spread across ages
// (0-399 days since last_reinforced) and decay rates, so EffectiveConfidence
// is doing real, varied exponent work rather than being trivially
// cacheable/constant-folded by the compiler.
func benchEdges(n int) []*memory.Edge {
	edges := make([]*memory.Edge, n)
	now := time.Now()
	rates := []float64{memory.DecayRateStable, memory.DecayRateSlow, memory.DecayRateTimeBound, memory.DecayRateTransient}

	for i := 0; i < n; i++ {
		edges[i] = &memory.Edge{
			ID:             uuid.New(),
			Confidence:     0.5 + 0.5*float64(i%2), // vary base confidence a bit
			DecayRate:      rates[i%len(rates)],
			LastReinforced: now.Add(-time.Duration(i%400) * 24 * time.Hour),
		}
	}
	return edges
}

// BenchmarkConfidence_InlinePerQuery simulates the naive approach this
// issue is comparing against: every one of benchQueryCount retrieval
// queries recomputes memory.EffectiveConfidence for every candidate edge
// inline, with no caching across queries within the same consolidation-pass
// window.
func BenchmarkConfidence_InlinePerQuery(b *testing.B) {
	edges := benchEdges(benchEdgeCount)
	now := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var sink float64
		for q := 0; q < benchQueryCount; q++ {
			for _, e := range edges {
				sink += memory.EffectiveConfidence(*e, now)
			}
		}
		benchSink = sink
	}
}

// BenchmarkConfidence_BatchOncePerConsolidationPass simulates this issue's
// approach: ApplyBatchDecay computes EffectiveConfidence exactly once per
// edge per consolidation pass, the results are cached, and all
// benchQueryCount retrieval queries within that pass's window reuse the
// cached value instead of recomputing it.
func BenchmarkConfidence_BatchOncePerConsolidationPass(b *testing.B) {
	edges := benchEdges(benchEdgeCount)
	now := time.Now()
	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		results, err := ApplyBatchDecay(ctx, nil, edges, now)
		if err != nil {
			b.Fatal(err)
		}

		cache := make(map[uuid.UUID]float64, len(results))
		for _, r := range results {
			cache[r.EdgeID] = r.EffectiveConfidence
		}

		var sink float64
		for q := 0; q < benchQueryCount; q++ {
			for _, e := range edges {
				sink += cache[e.ID]
			}
		}
		benchSink = sink
	}
}
