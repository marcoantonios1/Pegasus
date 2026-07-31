package memory

import "github.com/marcoantonios1/Pegasus/internal/extraction"

// Default decay rates per proposal §7.1's four buckets. decay_rate is
// per-day, consumed by EffectiveConfidence's fixed formula:
//
//	current_confidence = base_confidence * exp(-decay_rate * days_since_last_reinforced)
//
// §7.1 names buckets and gives category examples but no concrete numbers —
// decay_rate is a FLOAT column that needs an actual value at write time,
// so picking these is this issue's job. Each is derived by solving
// exp(-r*t) = 0.5 for the target half-life t (or, for Stable, by picking
// an epsilon small enough that its own implied half-life is on the order
// of decades — shown below rather than asserted).
const (
	// Stable traits (§7.1: dislikes_food, birthday) — "near-zero" decay,
	// not literally zero: exp(-0*t) is mathematically fine, but a decay
	// rate of exactly 0 would make the formula non-invertible if anything
	// later needs to solve it the other way (e.g. "how many days until
	// confidence drops below X" divides by decay_rate). 0.0001 keeps it
	// strictly positive.
	//
	// Its implied half-life is ln(2)/0.0001 ≈ 6931 days ≈ 19 years — which
	// is itself the actual justification for calling this "near-zero" on
	// the timescales a personal system like this operates over (years of
	// conversation history, not multiple decades). At 1 year (365 days):
	// exp(-0.0001*365) = exp(-0.0365) ≈ 0.964 — about 3.6% decay after a
	// full year with no reinforcement.
	DecayRateStable = 0.0001

	// Slow-changing facts (§7.1: lives_in, works_at; §8.5's own example
	// for event_present is "lives in Beirut") — half-life of 150 days, the
	// midpoint of §7.1's "several months" / 120-180 day guidance:
	//
	//	exp(-r*150) = 0.5  =>  r = ln(2)/150 ≈ 0.004621
	//
	// A contradicting edge still drops the OLD edge's confidence sharply
	// and immediately per §7.2 — that's a separate mechanism (not built in
	// this issue), not something this decay rate needs to account for.
	DecayRateSlow = 0.004621

	// Time-bound facts (§7.1: has_event/plans_to, i.e. event_future post-
	// §8.5 split) — proposal calls this "hard expiry once the date
	// passes," but that expiry is already implemented as a READ-TIME rule
	// (extraction.EffectivePredicate, from the event-tense issue): it
	// compares the event's actual target date against now, not a smooth
	// decay curve. Giving event_future its own fast decay_rate on top of
	// that would be a second, independently-tuned expiry mechanism
	// answering the same question differently — the two would fight each
	// other rather than agree. The date-based rule is authoritative;
	// decay is a deliberate near-zero no-op here, so it's set equal to
	// DecayRateStable rather than some arbitrary "fast" number that isn't
	// actually meant to do anything. If this constant's value ever needs
	// to diverge from Stable's, that would be a sign the two mechanisms
	// need to be reconciled, not a sign to just pick a bigger number.
	DecayRateTimeBound = DecayRateStable

	// Transient state (§7.1: mood_signal) — half-life of 6 hours (0.25
	// days), the "hours not days" scale §7.1 asks for:
	//
	//	exp(-r*0.25) = 0.5  =>  r = ln(2)/0.25 ≈ 2.7726
	//
	// mood_signal is proposal §7.1's own named example for this bucket,
	// but it is NOT currently part of extraction's closed vocabulary
	// (internal/extraction.Predicates, §8.1) — per §7.3, per-message
	// sentiment is computed by a separate, more conservative model, not
	// written as a structured extraction triple. Included here for
	// forward compatibility with that future pipeline, not because
	// extraction can produce this predicate today.
	DecayRateTransient = 2.7726
)

// predicateDecayRates maps each closed-vocabulary predicate to its default
// decay_rate. extraction.Predicates (internal/extraction/predicates.go) is
// the single source of truth for predicate name strings — referenced here
// via its named constants where they exist, rather than redefining a
// second, possibly-drifting list of predicate name literals.
//
// §7.1 only names a handful of predicates explicitly per bucket
// (dislikes_food, birthday, lives_in, works_at, has_event/plans_to,
// mood_signal). Everything else below is this issue's own reasoned
// mapping by analogy — a first pass (v1), same spirit as the predicate
// vocabulary itself being flagged as v1 and expected to be refined:
//   - Stable: event_past (a completed historical fact stays true forever),
//     likes/dislikes (directly matches §7.1's own "dislikes_food"
//     example), family_member_of/pet_of (relationships that don't
//     meaningfully change), nickname_is, inside_joke_ref (a joke
//     reference doesn't become "false" over time the way a residence
//     does — its retrieval relevance fading is an importance/pruning
//     concern for the Reflection Engine, §9, not a confidence-decay one).
//   - Slow: event_present (§8.5's own "lives in Beirut" example),
//     works_at, studied_at, role_is, routine_is, goal_is, building — all
//     ongoing-state facts that plausibly shift over months, the same
//     character as §7.1's lives_in/works_at examples.
//   - TimeBound: event_future only.
var predicateDecayRates = map[string]float64{
	extraction.PredicateEventPast: DecayRateStable,
	"likes":                       DecayRateStable,
	"dislikes":                    DecayRateStable,
	"family_member_of":            DecayRateStable,
	"pet_of":                      DecayRateStable,
	"nickname_is":                 DecayRateStable,
	"inside_joke_ref":             DecayRateStable,

	extraction.PredicateEventPresent: DecayRateSlow,
	"works_at":                       DecayRateSlow,
	"studied_at":                     DecayRateSlow,
	"role_is":                        DecayRateSlow,
	"routine_is":                     DecayRateSlow,
	"goal_is":                        DecayRateSlow,
	"building":                       DecayRateSlow,

	extraction.PredicateEventFuture: DecayRateTimeBound,

	"mood_signal": DecayRateTransient,
}

// DefaultDecayRate returns predicate's default decay_rate, or
// DecayRateSlow if predicate isn't in the table. Slow is the fallback
// rather than Stable (which would silently mean "practically never
// decays") or a panic (which would break the very first predicate someone
// adds to the vocabulary without also updating this table) — a
// conservative middle ground for an unrecognized predicate.
func DefaultDecayRate(predicate string) float64 {
	if r, ok := predicateDecayRates[predicate]; ok {
		return r
	}
	return DecayRateSlow
}
