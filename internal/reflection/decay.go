package reflection

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// DecayResult is one edge's precomputed read-time confidence from a single
// ApplyBatchDecay pass — EdgeID plus the memory.EffectiveConfidence value
// computed for it at that pass's `now`. Deliberately does not carry the
// full *memory.Edge: callers already hold the edges they passed in, and
// keeping this to just the (ID, value) pair makes it obvious at a glance
// that nothing here is a write-back candidate for the Confidence column
// (see ApplyBatchDecay's doc comment for why that distinction matters).
type DecayResult struct {
	EdgeID              uuid.UUID
	EffectiveConfidence float64
}

// ApplyBatchDecay implements the "decay" stage of proposal §9.2's
// consolidation pipeline (merge → summaries → stats → decay → pruning →
// embeddings): precompute memory.EffectiveConfidence once per eligible
// edge for this consolidation pass, so the pruning-flag stage that follows
// it doesn't need to recompute the same exponential per edge per read.
//
// CRITICAL: this function never writes to the database, and specifically
// never writes to the edges.confidence column. Edge.Confidence is
// extracted_confidence * source_weight (§7.5) — an immutable base value.
// EffectiveConfidence derives the *current*, decayed value from it at read
// time without mutating it (see EffectiveConfidence's own doc comment).
// If a decayed number were ever written back into Confidence, the next
// decay computation — this function's next pass, or any read-time
// EffectiveConfidence() call elsewhere — would decay from an
// already-decayed number, silently compounding the exponential incorrectly
// on every cycle with no way to detect it after the fact. That is why this
// function's return type is a plain slice of results, not something that
// touches *memory.Edge in place or calls store.Update — there is no write
// path here for a future change to accidentally wire up. The store
// parameter is accepted (matching the shape of MergeDuplicates, the other
// §9.2 pipeline stage in this package) but unused by this pure computation
// step; it is not queried or written.
//
// decay_locked edges (per memory.EffectiveConfidence) still get a
// DecayResult — they need to appear in the result set so the downstream
// pruning stage sees a stable, non-decayed value for them rather than
// silently missing entries — but EffectiveConfidence already returns
// e.Confidence unmodified for those, so no second decay_locked guard is
// added here: duplicating that check would risk drifting from the
// read-time behavior it must match exactly.
//
// Edges already superseded (SupersededBy != nil) are skipped entirely: a
// superseded edge is no longer current, its confidence is no longer
// relevant to retrieval or to the pruning-flag decision, so computing
// (and downstream consumers having to handle) a decay value for it would
// be wasted work with no reader.
//
// This is a pure computation step, not a scheduled job — it has no
// ticker or trigger of its own. It's meant to be called from wherever the
// consolidation orchestrator sequences §9's pipeline stages, the same
// callback Trigger.onConsolidate (trigger.go) invokes on idle/max-interval
// firing. That orchestrator does not exist yet as reusable code (same
// status as MergeDuplicates, dedup.go) — when it's built, this stage
// slots in right after MergeDuplicates and before the pruning-flag step,
// operating over the same candidate edge set. Until then, this function is
// standalone and directly testable/benchmarkable without that
// orchestration existing.
func ApplyBatchDecay(ctx context.Context, store *memory.EdgeStore, edges []*memory.Edge, now time.Time) ([]DecayResult, error) {
	results := make([]DecayResult, 0, len(edges))

	for i, e := range edges {
		// Cheap, infrequent cancellation check rather than one per edge —
		// matches Trigger's doc'd expectation that a long-running
		// consolidation stage checks ctx at "reasonable checkpoints", not
		// on every single unit of work, since a real candidate set can be
		// on the order of hundreds of thousands of edges (§6.1).
		if i%4096 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		if e.SupersededBy != nil {
			continue
		}

		results = append(results, DecayResult{
			EdgeID:              e.ID,
			EffectiveConfidence: memory.EffectiveConfidence(*e, now),
		})
	}

	return results, nil
}
