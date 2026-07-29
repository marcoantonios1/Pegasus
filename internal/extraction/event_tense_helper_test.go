package extraction

import (
	"testing"
	"time"
)

func TestEffectivePredicate_ElapsedEventFutureBecomesPast(t *testing.T) {
	eventDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) // 2 months after eventDate

	got := EffectivePredicate(PredicateEventFuture, eventDate, now)
	if got != PredicateEventPast {
		t.Errorf("expected an elapsed event_future to become event_past, got %q", got)
	}
}

func TestEffectivePredicate_FutureEventFutureUnchanged(t *testing.T) {
	eventDate := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) // 3 months before eventDate

	got := EffectivePredicate(PredicateEventFuture, eventDate, now)
	if got != PredicateEventFuture {
		t.Errorf("expected a not-yet-elapsed event_future to stay event_future, got %q", got)
	}
}

func TestEffectivePredicate_EventDateExactlyNowStaysFuture(t *testing.T) {
	// Boundary case: "now.After(eventDate)" is false when they're equal, so
	// an event dated for exactly this instant has not yet "passed" per
	// this function's documented interpretation.
	instant := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	got := EffectivePredicate(PredicateEventFuture, instant, instant)
	if got != PredicateEventFuture {
		t.Errorf("expected eventDate == now to stay event_future (not yet passed), got %q", got)
	}
}

func TestEffectivePredicate_NonEventPredicatesPassThroughUnchanged(t *testing.T) {
	eventDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // far in the past
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	cases := []string{
		PredicateEventPast,
		PredicateEventPresent,
		"likes",
		"dislikes",
		"works_at",
		"nickname_is",
	}

	for _, predicate := range cases {
		got := EffectivePredicate(predicate, eventDate, now)
		if got != predicate {
			t.Errorf("EffectivePredicate(%q, ...) = %q, expected passthrough unchanged", predicate, got)
		}
	}
}
