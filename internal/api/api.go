// Package api implements Pegasus's Memory API (proposal §13) — the
// surface Hermes actually calls, not an internal consolidation mechanism
// like internal/reflection. The read methods (GetRelevantContext,
// SearchMemory, SearchSemantic, GetContact, GetRelationship,
// GetRelationshipHistory, WhyDoWeBelieveThis, GetStyleProfile) were built
// first; this file's write/pipeline methods (ProcessLiveMessage,
// ImportWhatsApp, StoreMemory, UpdateMemory, DeleteMemory) are this
// issue's addition — the actual pipeline glue wiring adapter -> triage ->
// extraction -> storage together across pieces built in earlier issues.
// CorrectMemory (internal/memory/correct_memory.go) is untouched by
// either issue; ProcessLiveMessage/ImportWhatsApp call it where a
// correction is detected (see storeExtractedTriples).
//
// No package under internal/ was previously the "public API surface" —
// internal/memory holds per-table stores, internal/reflection holds the
// consolidation pipeline's individual stages. This package composes both
// (plus internal/extraction for the Costguard client, extraction/triage
// pipeline, and live windowing) into the actual methods §13 specifies;
// nothing here duplicates SQL that already lives in a store — every
// method is built by calling into internal/memory, per this codebase's
// established convention (see internal/reflection's own files, none of
// which contain raw SQL either).
package api

import (
	"context"
	"log"
	"sync"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
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

// Extractor is the subset of *extraction.Extractor the write pipeline
// needs (proposal §5's Pass 2). Deliberately its own declaration here
// rather than importing internal/bulkimport for its equivalent
// WindowExtractor interface — bulkimport is a leaf consumer of
// extraction, not a shared library this package should depend on just
// for a one-method interface type; Go interfaces satisfy structurally, so
// *extraction.Extractor already implements both without either package
// knowing about the other.
type Extractor interface {
	ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error)
}

// Triager is the subset of *extraction.Triager the write pipeline needs
// (proposal §5's Pass 1). Two methods, not one: IsCandidate triages a
// single message (ProcessLiveMessage's live path — a verdict is needed
// before it's even known which window a message ends up in);
// IsWindowCandidate triages a whole window in one call (ImportWhatsApp's
// batched historical path — see extraction.Triager.IsWindowCandidate's
// own doc comment for why bulk import needs this different call pattern,
// not a loop calling IsCandidate per message).
type Triager interface {
	IsCandidate(ctx context.Context, text string) (bool, error)
	IsWindowCandidate(ctx context.Context, w extraction.Window) (bool, error)
}

// ActivityRecorder is the subset of *reflection.Trigger the write
// pipeline needs. Narrowed to an interface — rather than importing
// internal/reflection.Trigger directly — so tests can inject a fake
// without constructing a real Trigger (which starts a polling goroutine),
// and so this package's dependency on "something to notify of live
// activity" stays a one-method contract, not a concrete type.
type ActivityRecorder interface {
	RecordActivity()
}

// API holds every store this package's read methods need, plus the
// write-pipeline dependencies attached via WithPipeline. Construct via
// New; all fields are unexported and set only at construction/via
// WithPipeline.
type API struct {
	edges      *memory.EdgeStore
	messages   *memory.MessageStore
	entities   *memory.EntityStore
	embeddings *memory.EmbeddingStore
	relStats   *memory.RelationshipStatsStore
	embedder   Embedder

	triager          Triager
	extractor        Extractor
	activityRecorder ActivityRecorder
	selfExternalIDs  map[string]bool
	logger           func(format string, args ...any)

	transcriber         Transcriber
	audioFetcher        AudioFetcher
	transcriptionConfig TranscriptionConfig

	liveWindowsMu sync.Mutex
	liveWindows   map[uuid.UUID]*liveConversationState
}

// New builds an API from its component stores plus an Embedder for
// query-text embedding (GetRelevantContext, SearchSemantic). embedder may
// be nil if a caller never intends to use either of those two methods —
// every other read method has no embedding dependency and works fine
// with a nil embedder; the two that do will return an error explaining
// why rather than panicking on a nil call.
//
// New's signature is deliberately unchanged from the read-methods issue
// that first defined it — this issue's write-pipeline dependencies
// (Triager, Extractor, ActivityRecorder, self-identification) are
// attached separately via WithPipeline instead of being added here as
// more required constructor parameters, specifically so existing callers
// (and the read methods' own tests) built against this exact signature
// keep working unchanged.
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

// PipelineDeps carries ProcessLiveMessage/ImportWhatsApp's dependencies —
// see WithPipeline.
type PipelineDeps struct {
	Triager          Triager
	Extractor        Extractor
	ActivityRecorder ActivityRecorder

	// SelfExternalIDs are the platform sender IDs (e.g. WhatsApp JIDs)
	// that identify Marco's own outgoing messages — RawMessage carries no
	// separate "is this me" flag (see resolveSender's doc comment for
	// why), so the pipeline has to be told explicitly which external IDs
	// are Marco rather than inferring it from content.
	SelfExternalIDs []string

	// Logger receives one line of progress per completed window during
	// ImportWhatsApp — the minimum progress-tracking bar its own doc
	// comment settles on (log lines with counts, not a checkpoint file/
	// table; see that comment for why full resumability is out of this
	// issue's scope). Defaults to log.Printf if nil.
	Logger func(format string, args ...any)

	// Transcriber, AudioFetcher, and TranscriptionConfig configure the
	// voice pipeline (proposal §5/§8.4) — see voice_transcription.go for
	// the full design. Transcriber is required for ImportWhatsApp/
	// ProcessLiveMessage to transcribe voice messages at all; AudioFetcher
	// is required in addition for the fetch to actually succeed (see its
	// own doc comment on ImportWhatsApp always having one vs.
	// ProcessLiveMessage not, by default). TranscriptionConfig's zero
	// value falls back to its own documented defaults via withDefaults().
	Transcriber         Transcriber
	AudioFetcher        AudioFetcher
	TranscriptionConfig TranscriptionConfig
}

// WithPipeline attaches write-pipeline dependencies to an already-
// constructed API and returns the same *API (for chaining onto New).
// ProcessLiveMessage and ImportWhatsApp return a clear error rather than
// a nil-pointer panic if called before this.
func (a *API) WithPipeline(deps PipelineDeps) *API {
	a.triager = deps.Triager
	a.extractor = deps.Extractor
	a.activityRecorder = deps.ActivityRecorder
	a.logger = deps.Logger
	a.transcriber = deps.Transcriber
	a.audioFetcher = deps.AudioFetcher
	a.transcriptionConfig = deps.TranscriptionConfig

	a.selfExternalIDs = make(map[string]bool, len(deps.SelfExternalIDs))
	for _, id := range deps.SelfExternalIDs {
		a.selfExternalIDs[id] = true
	}

	return a
}

func (a *API) logf(format string, args ...any) {
	if a.logger != nil {
		a.logger(format, args...)
		return
	}
	log.Printf(format, args...)
}
