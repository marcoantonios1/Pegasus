package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// §11's own "5-8" default topN, and the hard cap regardless of what a
// caller requests — a caller bug (or a deliberately huge request) can
// never pull the whole table through this method.
const (
	DefaultTopN = 6
	MaxTopN     = 50

	// candidatePoolMultiplier/maxCandidatePool size the SQL-level fetch
	// (EdgeStore.SearchRelevant) beyond topN: prune-gating and re-ranking
	// happen in Go against this pool (see RankingWeights' doc comment for
	// why), so the pool needs enough slack that a topN-sized *final*
	// result is still likely even after some candidates are pruned out or
	// reordered. Both are first-guess tuning values, not validated against
	// real query patterns.
	candidatePoolMultiplier = 5
	maxCandidatePool        = 200
)

// RankingWeights are §11's "configurable weights... expected to need real
// tuning" for GetRelevantContext's final ranking:
//
//	score = Similarity*similarity + Confidence*effectiveConfidence + Importance*importance
//
// using EffectiveConfidence (decayed, memory.EffectiveConfidence), not the
// raw stored Edge.Confidence — same reasoning as IsPruneCandidate's own
// choice (prune_candidate.go): a decayed-into-irrelevance fact should
// rank low even if it was written with high confidence.
//
// DefaultRankingWeights below is a first guess, not validated against
// real data — the same caveat as every other first-guess weight/threshold
// in this build (dedup similarity, decay rates, closeness weights, prune
// floors).
type RankingWeights struct {
	Similarity float64
	Confidence float64
	Importance float64
}

// DefaultRankingWeights returns a first-guess weighting that favors
// semantic relevance somewhat over confidence and importance, on the
// reasoning that a query's whole point is finding relevant content —
// confidence/importance then break ties and demote weak matches, rather
// than dominating the ranking outright. Unvalidated; see RankingWeights'
// doc comment.
func DefaultRankingWeights() RankingWeights {
	return RankingWeights{Similarity: 0.5, Confidence: 0.3, Importance: 0.2}
}

func (w RankingWeights) withDefaults() RankingWeights {
	if w.Similarity == 0 && w.Confidence == 0 && w.Importance == 0 {
		return DefaultRankingWeights()
	}
	return w
}

// Filters are GetRelevantContext's (and SearchMemory's) structured
// filters, layered on top of memory.SearchFilters with two additional,
// API-level concerns that don't belong at the SQL layer: IncludePruned
// and Weights, both of which depend on Go-side decay computation (see
// memory.SearchFilters' own doc comment for why that split exists).
type Filters struct {
	SubjectID   *uuid.UUID
	SourceTypes []string
	Since       *time.Time

	// IncludePruned bypasses the default confidence/importance prune gate
	// (memory.IsPruneCandidate) — the escape hatch §11's acceptance
	// criteria require: GetRelevantContext must still have SOME way to
	// return everything when a caller explicitly wants it, not just the
	// curated default. false (the zero value) is the default-filtered
	// behavior; a caller has to opt in to seeing pruned edges, matching
	// "confidence-gated by default" being the point of this filter
	// existing at all.
	IncludePruned bool

	// Weights, if the zero value, falls back to DefaultRankingWeights via
	// withDefaults() — same zero-value-fallback pattern as
	// reflection.DedupConfig/TriggerConfig and memory.PruneConfig.
	Weights RankingWeights

	// PruneConfig, if the zero value, falls back to
	// memory.DefaultPruneConfig() via memory.PruneConfig's own
	// withDefaults(). Exposed here rather than hardcoded so a caller can
	// use a different confidence/importance floor than the package
	// default without needing a second entry point.
	PruneConfig memory.PruneConfig
}

func (f Filters) searchFilters() memory.SearchFilters {
	return memory.SearchFilters{
		SubjectID:   f.SubjectID,
		SourceTypes: f.SourceTypes,
		Since:       f.Since,
	}
}

// GetRelevantContext implements proposal §11: hybrid vector + structured
// retrieval over edges, confidence-gated and importance-weighted, capped
// at a sane topN. query may be empty — GetContact (contact.go) calls this
// internally with an empty query and a SubjectID filter to get "top edges
// about this person" with no free-text search involved; an empty query
// skips embedding entirely and EdgeStore.SearchRelevant falls back to a
// recency-only candidate order (see that method's doc comment), so
// ranking then depends only on the Confidence/Importance weight terms.
//
// topN is clamped to [1, MaxTopN]; DefaultTopN is used for topN <= 0.
func (a *API) GetRelevantContext(ctx context.Context, query string, filters Filters, topN int) ([]*memory.Edge, error) {
	topN = clampTopN(topN)

	var queryVector []float32
	if query != "" {
		if a.embedder == nil {
			return nil, fmt.Errorf("GetRelevantContext: query %q requires an embedder, but this API was constructed with none", query)
		}
		var err error
		queryVector, err = a.embedder.Embed(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("GetRelevantContext: embed query: %w", err)
		}
	}

	poolSize := topN * candidatePoolMultiplier
	if poolSize > maxCandidatePool {
		poolSize = maxCandidatePool
	}

	candidates, err := a.edges.SearchRelevant(ctx, queryVector, filters.searchFilters(), poolSize)
	if err != nil {
		return nil, fmt.Errorf("GetRelevantContext: search relevant edges: %w", err)
	}

	return rankAndFilter(candidates, time.Now(), filters.Weights.withDefaults(), filters.PruneConfig, filters.IncludePruned, topN), nil
}

func clampTopN(topN int) int {
	if topN <= 0 {
		return DefaultTopN
	}
	if topN > MaxTopN {
		return MaxTopN
	}
	return topN
}

// rankAndFilter applies the confidence/importance prune gate (unless
// includePruned) and the weighted ranking formula to candidates, then
// returns the top topN edges. Kept as a standalone function (rather than
// inlined into GetRelevantContext) specifically so tests can supply now
// explicitly and verify decay-sensitive ranking/pruning deterministically,
// without depending on wall-clock time.Now() — GetRelevantContext itself
// always calls this with the real time.Now().
func rankAndFilter(candidates []memory.EdgeCandidate, now time.Time, weights RankingWeights, pruneCfg memory.PruneConfig, includePruned bool, topN int) []*memory.Edge {
	type scored struct {
		edge  *memory.Edge
		score float64
	}

	results := make([]scored, 0, len(candidates))
	for _, c := range candidates {
		if !includePruned && memory.IsPruneCandidate(*c.Edge, now, pruneCfg) {
			continue
		}

		effectiveConfidence := memory.EffectiveConfidence(*c.Edge, now)
		score := weights.Similarity*c.Similarity + weights.Confidence*effectiveConfidence + weights.Importance*c.Edge.Importance
		results = append(results, scored{edge: c.Edge, score: score})
	}

	sort.SliceStable(results, func(i, j int) bool {
		return results[i].score > results[j].score
	})

	if len(results) > topN {
		results = results[:topN]
	}

	edges := make([]*memory.Edge, len(results))
	for i, r := range results {
		edges[i] = r.edge
	}
	return edges
}
