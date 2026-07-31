package memory

import (
	"math"
	"time"
)

// EffectiveConfidence implements proposal §7.1's decay formula, computed
// at READ time:
//
//	current_confidence = base_confidence * exp(-decay_rate * days_since_last_reinforced)
//
// The stored Edge.Confidence column is never rewritten by this function —
// it stays whatever it was at last write (extracted_confidence *
// source_weight, per §7.5, or the value CorrectMemory() set directly,
// §7.6). Only the *returned* value reflects decay. This mirrors
// extraction.EffectivePredicate's read-time principle from the
// event-tense issue: derive the current-truth value on read, don't mutate
// storage on a schedule. Reinforcement (EdgeStore.Reinforce) resets
// LastReinforced, which changes what this function returns on the next
// read — it does not touch Confidence either.
//
// If e.DecayLocked is true, e.Confidence is returned completely
// unmodified regardless of how much time has elapsed. decay_locked is set
// by CorrectMemory() (§7.6, a separate issue) — corrections are Pegasus's
// highest-quality supervision signal and must not quietly erode the way
// an inferred fact would.
func EffectiveConfidence(e Edge, now time.Time) float64 {
	if e.DecayLocked {
		return e.Confidence
	}

	daysSinceReinforced := now.Sub(e.LastReinforced).Hours() / 24
	if daysSinceReinforced < 0 {
		// LastReinforced in the future relative to now shouldn't happen in
		// practice, but exp() of a positive exponent would return a value
		// greater than base_confidence, which is nonsensical for a decay
		// function — clamp to zero elapsed days (no decay) instead of
		// letting confidence increase.
		daysSinceReinforced = 0
	}

	return e.Confidence * math.Exp(-e.DecayRate*daysSinceReinforced)
}
