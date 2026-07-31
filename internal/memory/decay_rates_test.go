package memory

import (
	"math"
	"testing"
	"time"
)

// halfLifeTolerance is looser than floatTolerance: the decay_rate
// constants are rounded to 4 significant figures for readability (see
// decay_rates.go's doc comments), so their actual half-life is close to,
// not bit-exact with, the target used to derive them.
const halfLifeTolerance = 0.001

func effectiveAt(t *testing.T, decayRate float64, days float64) float64 {
	t.Helper()
	lastReinforced := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := lastReinforced.Add(time.Duration(days * float64(24*time.Hour)))
	e := Edge{Confidence: 1.0, DecayRate: decayRate, LastReinforced: lastReinforced}
	return EffectiveConfidence(e, now)
}

func TestDecayRateSlow_HalfLifeIs150Days(t *testing.T) {
	got := effectiveAt(t, DecayRateSlow, 150)
	if math.Abs(got-0.5) > halfLifeTolerance {
		t.Errorf("DecayRateSlow at 150 days: got confidence %v, want ~0.5 (its designed half-life)", got)
	}
}

func TestDecayRateTransient_HalfLifeIs6Hours(t *testing.T) {
	sixHoursInDays := 6.0 / 24.0
	got := effectiveAt(t, DecayRateTransient, sixHoursInDays)
	if math.Abs(got-0.5) > halfLifeTolerance {
		t.Errorf("DecayRateTransient at 6 hours: got confidence %v, want ~0.5 (its designed half-life)", got)
	}
}

func TestDecayRateStable_HalfLifeIsOnTheOrderOfDecades(t *testing.T) {
	// Stable's own justification (decay_rates.go) is that its implied
	// half-life (ln(2)/0.0001 ≈ 6931 days ≈ 19 years) is what makes
	// "near-zero" an honest description, not just an assertion. Verify
	// that arithmetic here rather than only in a comment.
	impliedHalfLifeDays := math.Log(2) / DecayRateStable
	if impliedHalfLifeDays < 365*15 {
		t.Errorf("expected Stable's implied half-life to be at least 15 years, got %.0f days (~%.1f years)", impliedHalfLifeDays, impliedHalfLifeDays/365)
	}

	got := effectiveAt(t, DecayRateStable, impliedHalfLifeDays)
	if math.Abs(got-0.5) > halfLifeTolerance {
		t.Errorf("DecayRateStable at its own implied half-life: got confidence %v, want ~0.5", got)
	}

	// And the practically-relevant check: after 1 year, decay should be
	// small (single-digit percent), consistent with "near-zero" on the
	// timescales this system actually operates over.
	afterOneYear := effectiveAt(t, DecayRateStable, 365)
	if afterOneYear < 0.95 {
		t.Errorf("expected less than 5%% decay after 1 year at DecayRateStable, got confidence %v", afterOneYear)
	}
}

func TestDecayRateTimeBound_EqualsStableByDesign(t *testing.T) {
	// Locks in the deliberate choice documented in decay_rates.go: decay
	// contributes ~nothing for event_future because the read-time date
	// rule is authoritative, not because someone forgot to pick a "real"
	// number. If this ever needs to diverge, it should be a conscious
	// change to this test, not a silent drift.
	if DecayRateTimeBound != DecayRateStable {
		t.Errorf("expected DecayRateTimeBound == DecayRateStable (deliberate no-op), got %v vs %v", DecayRateTimeBound, DecayRateStable)
	}
}

func TestDefaultDecayRate_KnownPredicates(t *testing.T) {
	cases := []struct {
		predicate string
		want      float64
	}{
		{"event_past", DecayRateStable},
		{"likes", DecayRateStable},
		{"dislikes", DecayRateStable},
		{"event_present", DecayRateSlow},
		{"works_at", DecayRateSlow},
		{"event_future", DecayRateTimeBound},
		{"mood_signal", DecayRateTransient},
	}

	for _, c := range cases {
		if got := DefaultDecayRate(c.predicate); got != c.want {
			t.Errorf("DefaultDecayRate(%q) = %v, want %v", c.predicate, got, c.want)
		}
	}
}

func TestDefaultDecayRate_UnknownPredicateFallsBackToSlow(t *testing.T) {
	got := DefaultDecayRate("some_future_predicate_not_yet_in_the_table")
	if got != DecayRateSlow {
		t.Errorf("expected unknown predicate to fall back to DecayRateSlow, got %v", got)
	}
}
