package memory

import "time"

// PruneConfig carries the two floors IsPruneCandidate checks against.
// Configurable rather than hardcoded, matching the bar this codebase
// already applies to source_weight (source_weights.go) and every other
// first-guess threshold built so far.
//
// ConfidenceFloor and ImportanceFloor's defaults below are first-guess
// starting values, not validated against real data — the same caveat as
// this build's other first-guess thresholds (dedup.go's
// SimilarityThreshold/Window, relationship_stats.go's closeness weights
// and half-lives). They need real tuning once pruning actually affects
// what GetRelevantContext() (not yet built) surfaces and its output can
// be judged against what should and shouldn't have been excluded.
type PruneConfig struct {
	ConfidenceFloor float64
	ImportanceFloor float64
}

// Default floors for PruneConfig. First guesses — see PruneConfig's doc
// comment.
const (
	// DefaultConfidenceFloor: an edge whose EffectiveConfidence has decayed
	// below 0.15 is treated as no longer reliable enough for default
	// retrieval. Picked low deliberately — this is meant to catch facts
	// that have decayed into irrelevance, not facts that are merely no
	// longer at full strength.
	DefaultConfidenceFloor = 0.15

	// DefaultImportanceFloor: an edge whose Importance is below 0.1 is
	// treated as too trivial for default retrieval regardless of how
	// confident Pegasus is that it's true — importance and confidence are
	// answering different questions (§7.4 vs. §7.1), and either one being
	// too low is independently sufficient to prune.
	DefaultImportanceFloor = 0.1
)

// DefaultPruneConfig returns a PruneConfig populated with the package
// defaults.
func DefaultPruneConfig() PruneConfig {
	return PruneConfig{
		ConfidenceFloor: DefaultConfidenceFloor,
		ImportanceFloor: DefaultImportanceFloor,
	}
}

// IsPruneCandidate implements proposal §9.2's "flag low-importance/
// low-confidence edges for exclusion" pipeline step as a read-time
// computed check, not a stored column — the same design already used for
// confidence decay (EffectiveConfidence) and the event-tense rule
// (extraction.EffectivePredicate). §9.2's pipeline diagram lists this as a
// consolidation-pass step, which could read as "a batch job writes a
// stored flag"; that is deliberately not what this does. There is no
// schema column for it, and a boolean that depends on decaying confidence
// would go stale between consolidation passes — an edge can cross below
// ConfidenceFloor hours after the last batch run, and a stored flag
// wouldn't know that until the next pass. Since confidence already decays
// at read time, pruning eligibility should too: it's a derived property
// of two numbers Pegasus already has (EffectiveConfidence and Importance),
// computed fresh on every call, not a fact that needs writing down. If
// real query load later makes recomputing this per-call expensive, that's
// a caching decision for GetRelevantContext() to make — not a reason to
// bake a stale flag into the schema now.
//
// An edge is a prune candidate if EffectiveConfidence(edge, now) is below
// cfg.ConfidenceFloor, OR edge.Importance is below cfg.ImportanceFloor —
// note EffectiveConfidence (the decayed value), not edge.Confidence (the
// raw stored write-time value): a fact that decayed into irrelevance must
// be excluded even if it was written with high confidence originally,
// since that's the entire point of decay existing.
//
// decay_locked and is_correction edges are never a prune candidate,
// regardless of their importance/confidence values — checked as a
// short-circuit before the threshold logic. A user correction (§7.6) is
// by definition high-value supervision signal; it must not silently
// vanish from default retrieval just because its Importance field happens
// to be low.
//
// Superseded edges (SupersededBy != nil) get no special case here — a
// deliberate decision, not an oversight. A superseded edge is already
// excluded from meaningful default retrieval by the supersession logic
// itself (whatever consumes SupersededBy already knows to prefer the
// current edge over a superseded one); this function only adds an
// additional confidence/importance-based exclusion on top of that, so it
// has nothing extra to do for a superseded edge specifically.
//
// Call this only from GetRelevantContext()'s default retrieval path (not
// yet built — a separate, larger issue). A direct/explicit lookup for a
// specific edge (EdgeStore.GetByID, GetBySubjectAndPredicate) must NOT
// apply this filter — nothing calls IsPruneCandidate from those paths,
// so a pruned edge stays fully queryable by direct lookup; pruning only
// affects default retrieval ranking, never direct lookup, by construction.
func IsPruneCandidate(edge Edge, now time.Time, cfg PruneConfig) bool {
	if edge.DecayLocked || edge.IsCorrection {
		return false
	}

	if EffectiveConfidence(edge, now) < cfg.ConfidenceFloor {
		return true
	}
	if edge.Importance < cfg.ImportanceFloor {
		return true
	}

	return false
}
