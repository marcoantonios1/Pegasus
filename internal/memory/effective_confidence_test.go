package memory

import (
	"math"
	"testing"
	"time"
)

const floatTolerance = 1e-9

func TestEffectiveConfidence_HandComputedValue(t *testing.T) {
	// base_confidence=0.9, decay_rate=0.01, 30 days elapsed.
	// expected = 0.9 * exp(-0.01*30) = 0.9 * exp(-0.3)
	lastReinforced := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := lastReinforced.AddDate(0, 0, 30)

	e := Edge{Confidence: 0.9, DecayRate: 0.01, LastReinforced: lastReinforced}

	want := 0.9 * math.Exp(-0.01*30)
	got := EffectiveConfidence(e, now)

	if math.Abs(got-want) > floatTolerance {
		t.Errorf("EffectiveConfidence() = %v, want %v (diff %v)", got, want, math.Abs(got-want))
	}
	// Sanity-check the hand-derived constant independently of math.Exp, so
	// this test would still catch a wrong formula even if math.Exp and the
	// implementation somehow shared a bug: exp(-0.3) ≈ 0.740818.
	if math.Abs(want-0.9*0.7408182206817179) > floatTolerance {
		t.Fatalf("test setup itself is wrong: exp(-0.3) sanity check failed")
	}
}

func TestEffectiveConfidence_ZeroElapsedTimeReturnsBaseConfidence(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{Confidence: 0.85, DecayRate: 0.5, LastReinforced: now}

	got := EffectiveConfidence(e, now)
	if math.Abs(got-0.85) > floatTolerance {
		t.Errorf("expected zero elapsed time to leave confidence unchanged, got %v", got)
	}
}

func TestEffectiveConfidence_FutureLastReinforcedClampedToZeroDecay(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	future := now.AddDate(0, 0, 10)
	e := Edge{Confidence: 0.7, DecayRate: 0.5, LastReinforced: future}

	got := EffectiveConfidence(e, now)
	if math.Abs(got-0.7) > floatTolerance {
		t.Errorf("expected a future LastReinforced to clamp to zero decay (no confidence increase), got %v", got)
	}
}

// TestEffectiveConfidence_RealDecayRatesAfterElapsedTime is the
// edge-timestamp fix's requirement 12/9 check, using the REAL production
// decay-rate constants (not arbitrary 0.01/0.5 values like the other
// tests in this file) — confirming EffectiveConfidence, unchanged by this
// fix, keeps doing the right thing once fed a genuinely old
// LastReinforced.
//
// Two separate elapsed-time scales are used deliberately, not one shared
// span: DecayRateStable's own doc comment (decay_rates.go) gives ~3.6%
// decay as its illustrative "barely moves" figure specifically at the
// ONE-YEAR mark — its ~19-year implied half-life means that "barely"
// claim stops holding at a multi-year span (over the full 2019->2026 gap
// used for the decaying-predicate check below, DecayRateStable itself
// drops to ~0.68, a real difference, just much smaller than a half-life-
// scale decay). So: the DECAYING check uses the full 2019->2026 gap
// (strictly below, emphatically so), and the STABLE check uses a
// one-year gap (barely moves), matching each constant's own documented
// claim rather than asserting something neither constant was designed to
// promise at an arbitrary shared timescale.
func TestEffectiveConfidence_RealDecayRatesAfterElapsedTime(t *testing.T) {
	lastReinforced := time.Date(2019, 3, 14, 9, 0, 0, 0, time.UTC)
	now := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	daysElapsed := now.Sub(lastReinforced).Hours() / 24

	decaying := Edge{Confidence: 0.9, DecayRate: DecayRateSlow, LastReinforced: lastReinforced}
	gotDecaying := EffectiveConfidence(decaying, now)
	wantDecaying := 0.9 * math.Exp(-DecayRateSlow*daysElapsed)

	if math.Abs(gotDecaying-wantDecaying) > floatTolerance {
		t.Errorf("EffectiveConfidence(decaying) = %v, want hand-computed %v", gotDecaying, wantDecaying)
	}
	if !(gotDecaying < decaying.Confidence) {
		t.Errorf("expected a decaying predicate's effective confidence (%v) to be strictly below stored confidence (%v) after %v days",
			gotDecaying, decaying.Confidence, daysElapsed)
	}

	oneYearLater := lastReinforced.AddDate(1, 0, 0)
	stable := Edge{Confidence: 0.9, DecayRate: DecayRateStable, LastReinforced: lastReinforced}
	gotStable := EffectiveConfidence(stable, oneYearLater)
	if diff := stable.Confidence - gotStable; diff < 0 || diff > 0.05 {
		t.Errorf("expected a stable-rate predicate to barely move over 1 year, got %v (diff from stored confidence %v)", gotStable, diff)
	}
}

func TestEffectiveConfidence_DecayLockedNeverChanges(t *testing.T) {
	lastReinforced := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	e := Edge{
		Confidence:     0.95,
		DecayRate:      DecayRateSlow,
		LastReinforced: lastReinforced,
		DecayLocked:    true,
	}

	cases := []time.Time{
		lastReinforced,
		lastReinforced.AddDate(0, 0, 1),
		lastReinforced.AddDate(1, 0, 0),
		lastReinforced.AddDate(50, 0, 0), // decades later
	}

	for _, now := range cases {
		got := EffectiveConfidence(e, now)
		if got != 0.95 {
			t.Errorf("decay_locked edge changed: EffectiveConfidence(now=%v) = %v, want unchanged 0.95", now, got)
		}
	}
}
