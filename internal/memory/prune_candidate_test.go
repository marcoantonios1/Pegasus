package memory

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

var testPruneConfig = PruneConfig{ConfidenceFloor: 0.15, ImportanceFloor: 0.1}

// TestIsPruneCandidate_ZeroConfigFallsBackToDefaults guards against a
// caller passing the zero-value PruneConfig{} (e.g. forgetting
// DefaultPruneConfig()) and silently getting 0/0 floors, under which
// IsPruneCandidate would essentially never fire. Matches the zero-value-
// fallback pattern already used for TriggerConfig and DedupConfig.
func TestIsPruneCandidate_ZeroConfigFallsBackToDefaults(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{
		Confidence:     0.05, // below DefaultConfidenceFloor (0.15)
		Importance:     0.9,
		DecayRate:      DecayRateStable,
		LastReinforced: now,
	}

	if got := IsPruneCandidate(e, now, PruneConfig{}); !got {
		t.Errorf("expected PruneConfig{} to fall back to working default floors (so a low-confidence edge is still flagged), got false")
	}
}

// TestIsPruneCandidate_ThresholdCombinations covers requirement 7:
// confidence/importance combinations above and below each floor
// individually, and both below.
func TestIsPruneCandidate_ThresholdCombinations(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		confidence float64
		importance float64
		want       bool
	}{
		{"both above floor", 0.9, 0.5, false},
		{"confidence exactly at floor is not below it", 0.15, 0.5, false},
		{"importance exactly at floor is not below it", 0.9, 0.1, false},
		{"confidence below floor only", 0.05, 0.5, true},
		{"importance below floor only", 0.9, 0.05, true},
		{"both below floor", 0.05, 0.05, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// LastReinforced == now: zero elapsed time, so EffectiveConfidence
			// equals the raw stored confidence for this table — decay's own
			// effect on the result is covered separately, by
			// TestIsPruneCandidate_UsesDecayedConfidenceNotRaw below.
			e := Edge{
				Confidence:     c.confidence,
				Importance:     c.importance,
				DecayRate:      DecayRateStable,
				LastReinforced: now,
			}

			got := IsPruneCandidate(e, now, testPruneConfig)
			if got != c.want {
				t.Errorf("IsPruneCandidate(confidence=%v, importance=%v) = %v, want %v", c.confidence, c.importance, got, c.want)
			}
		})
	}
}

// TestIsPruneCandidate_UsesDecayedConfidenceNotRaw covers requirement 8:
// a high raw stored confidence that has decayed (via elapsed time) below
// the floor must return true — proving this function reads
// EffectiveConfidence, not edge.Confidence directly. This is the entire
// reason IsPruneCandidate is read-time rather than a stale batch flag.
func TestIsPruneCandidate_UsesDecayedConfidenceNotRaw(t *testing.T) {
	lastReinforced := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{
		Confidence:     0.95, // high raw confidence at write time
		Importance:     0.9,  // comfortably above the importance floor
		DecayRate:      DecayRateTransient,
		LastReinforced: lastReinforced,
	}

	// Sanity: at write time (no elapsed days), this edge is nowhere near a
	// prune candidate.
	if got := IsPruneCandidate(e, lastReinforced, testPruneConfig); got {
		t.Fatalf("test setup is wrong: edge should not be a prune candidate at zero elapsed time, IsPruneCandidate = %v", got)
	}

	// Far enough later that DecayRateTransient's fast half-life has
	// decayed EffectiveConfidence well below the floor, even though
	// e.Confidence itself never changed.
	muchLater := lastReinforced.AddDate(0, 0, 30)
	effective := EffectiveConfidence(e, muchLater)
	if effective >= testPruneConfig.ConfidenceFloor {
		t.Fatalf("test setup is wrong: expected decayed EffectiveConfidence below the floor %v, got %v", testPruneConfig.ConfidenceFloor, effective)
	}

	if got := IsPruneCandidate(e, muchLater, testPruneConfig); !got {
		t.Errorf("expected a decayed-below-floor edge (raw confidence %v, effective %v) to be a prune candidate, got false", e.Confidence, effective)
	}
	if e.Confidence != 0.95 {
		t.Errorf("IsPruneCandidate must never mutate the edge's raw stored confidence, got %v", e.Confidence)
	}
}

// TestIsPruneCandidate_DecayLockedNeverPruned and
// TestIsPruneCandidate_IsCorrectionNeverPruned cover requirement 9 as two
// separate cases, not conflated: decay_locked and is_correction are set
// independently by CorrectMemory() (both true for a correction, per its
// doc comment), but nothing in principle stops an edge from having only
// one of the two, so the OR/exclusion logic is tested for each flag on
// its own.
func TestIsPruneCandidate_DecayLockedNeverPruned(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{
		Confidence:     0.01, // deliberately far below the floor
		Importance:     0.01, // deliberately far below the floor
		DecayRate:      DecayRateStable,
		LastReinforced: now,
		DecayLocked:    true,
		IsCorrection:   false,
	}

	if got := IsPruneCandidate(e, now, testPruneConfig); got {
		t.Errorf("expected a decay_locked edge to never be a prune candidate regardless of importance/confidence, got true")
	}
}

func TestIsPruneCandidate_IsCorrectionNeverPruned(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{
		Confidence:     0.01, // deliberately far below the floor
		Importance:     0.01, // deliberately far below the floor
		DecayRate:      DecayRateStable,
		LastReinforced: now,
		DecayLocked:    false,
		IsCorrection:   true,
	}

	if got := IsPruneCandidate(e, now, testPruneConfig); got {
		t.Errorf("expected an is_correction edge to never be a prune candidate regardless of importance/confidence, got true")
	}
}

// TestIsPruneCandidate_SupersededEdgeGetsNoSpecialCase covers requirement
// 10: superseded edges get no dedicated exclusion in IsPruneCandidate
// itself — a deliberate decision (see the function's doc comment), not an
// oversight. Supersession is handled entirely by whatever consumes
// SupersededBy elsewhere; this function only ever answers the
// confidence/importance question, so a superseded edge is evaluated by
// exactly the same threshold/lock logic as any other edge here.
func TestIsPruneCandidate_SupersededEdgeGetsNoSpecialCase(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	supersededByID := uuid.New()

	belowFloor := Edge{
		Confidence: 0.9, Importance: 0.01, // importance below floor
		DecayRate: DecayRateStable, LastReinforced: now,
		SupersededBy: &supersededByID,
	}
	if got := IsPruneCandidate(belowFloor, now, testPruneConfig); !got {
		t.Errorf("expected a superseded edge below the importance floor to still be flagged true (no special-case exemption for supersession), got false")
	}

	aboveFloor := Edge{
		Confidence: 0.9, Importance: 0.9,
		DecayRate: DecayRateStable, LastReinforced: now,
		SupersededBy: &supersededByID,
	}
	if got := IsPruneCandidate(aboveFloor, now, testPruneConfig); got {
		t.Errorf("expected a superseded edge above both floors to evaluate false same as any other edge, got true")
	}
}
