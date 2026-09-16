package api

import (
	"context"
	"fmt"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// searchMemoryLimit caps SearchMemory's result count. Proposal §13 gives
// SearchMemory no topN parameter (unlike GetRelevantContext/
// SearchSemantic, which both take one explicitly) — this exists so a
// broad keyword (matching a common substring across many edges) still
// can't pull an unbounded result set. Reuses MaxTopN rather than
// inventing a third distinct cap value with no stated reason to differ.
const searchMemoryLimit = MaxTopN

// SearchMemory implements proposal §13's structured/keyword search: edges
// whose subject canonical_name, predicate, or object_literal matches
// query text (see EdgeStore.SearchByText for the exact match rule).
//
// Distinct from SearchSemantic: SearchMemory is closer to a structured/
// exact query — find edges matching specific subject/predicate/object
// terms — while SearchSemantic is meaning-based, finding content related
// to a concept even without any keyword overlap at all. The two are not
// the same operation under different names; SearchMemory also returns
// edges (graph facts), SearchSemantic returns messages (raw content).
//
// Excludes superseded edges by default, same as GetRelevantContext, and
// for the same reason (see memory.SearchByText's doc comment). Unlike
// GetRelevantContext, this does NOT apply IsPruneCandidate's confidence/
// importance gate — a keyword match is an explicit, targeted lookup for
// specific facts (closer in spirit to GetByID/GetBySubjectAndPredicate
// than to GetRelevantContext's curated default-retrieval ranking), so
// pruning a match the caller asked for by name would be surprising rather
// than helpful.
func (a *API) SearchMemory(ctx context.Context, query string, filters Filters) ([]*memory.Edge, error) {
	edges, err := a.edges.SearchByText(ctx, query, filters.searchFilters(), searchMemoryLimit)
	if err != nil {
		return nil, fmt.Errorf("SearchMemory: %w", err)
	}
	return edges, nil
}

// SearchSemantic implements proposal §13's pure vector similarity search:
// embeds queryText and returns the topN nearest messages by cosine
// similarity (EmbeddingStore.SearchSimilarMessages) — raw content, not
// edges. See SearchMemory's doc comment for how the two search methods
// differ in kind, not just in name.
//
// topN is clamped the same way as GetRelevantContext's (see clampTopN).
func (a *API) SearchSemantic(ctx context.Context, queryText string, topN int) ([]*memory.Message, error) {
	if a.embedder == nil {
		return nil, fmt.Errorf("SearchSemantic: this API was constructed with no embedder")
	}

	vec, err := a.embedder.Embed(ctx, queryText)
	if err != nil {
		return nil, fmt.Errorf("SearchSemantic: embed query: %w", err)
	}

	msgs, err := a.embeddings.SearchSimilarMessages(ctx, vec, clampTopN(topN))
	if err != nil {
		return nil, fmt.Errorf("SearchSemantic: %w", err)
	}
	return msgs, nil
}
