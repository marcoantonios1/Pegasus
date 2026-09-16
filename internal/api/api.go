// Package api implements Pegasus's Memory API read methods (proposal
// §13) — the surface Hermes actually calls to retrieve context, not an
// internal consolidation mechanism like internal/reflection. This issue
// covers only the read methods (GetRelevantContext, SearchMemory,
// SearchSemantic, GetContact, GetRelationship, GetRelationshipHistory,
// WhyDoWeBelieveThis, GetStyleProfile) — every write method
// (StoreMemory, UpdateMemory, DeleteMemory, ImportWhatsApp,
// ProcessLiveMessage) and CorrectMemory (already built,
// internal/memory/correct_memory.go) are out of scope here.
//
// No package under internal/ was previously the "public API surface" —
// internal/memory holds per-table stores, internal/reflection holds the
// consolidation pipeline's individual stages. This package composes both
// (plus internal/extraction's Costguard client, for embedding query
// text) into the actual methods §13 specifies; nothing here duplicates
// SQL that already lives in a store — every method is built by calling
// into internal/memory, per this codebase's established convention (see
// internal/reflection's own files, none of which contain raw SQL either).
package api

import (
	"context"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Embedder is the subset of *extraction.CostguardClient this package
// needs, narrowed to an interface so tests can inject a fake instead of
// requiring a live Costguard/model call — same pattern as
// internal/bulkimport.WindowExtractor narrowing *extraction.Extractor.
// *extraction.CostguardClient satisfies this via its Embed method.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// API holds every store this package's read methods need. Construct via
// New; all fields are read-only after construction.
type API struct {
	edges      *memory.EdgeStore
	messages   *memory.MessageStore
	entities   *memory.EntityStore
	embeddings *memory.EmbeddingStore
	relStats   *memory.RelationshipStatsStore
	embedder   Embedder
}

// New builds an API from its component stores plus an Embedder for
// query-text embedding (GetRelevantContext, SearchSemantic). embedder may
// be nil if a caller never intends to use either of those two methods —
// every other method here has no embedding dependency and works fine
// with a nil embedder; the two that do will return an error explaining
// why rather than panicking on a nil call.
func New(
	edges *memory.EdgeStore,
	messages *memory.MessageStore,
	entities *memory.EntityStore,
	embeddings *memory.EmbeddingStore,
	relStats *memory.RelationshipStatsStore,
	embedder Embedder,
) *API {
	return &API{
		edges:      edges,
		messages:   messages,
		entities:   entities,
		embeddings: embeddings,
		relStats:   relStats,
		embedder:   embedder,
	}
}
