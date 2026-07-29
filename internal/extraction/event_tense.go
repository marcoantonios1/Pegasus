package extraction

import "time"

// EffectivePredicate implements proposal §8.5's event_future → event_past
// rule: once an event_future edge's target date has passed, whoever reads
// it should treat it as event_past. This is explicitly a READ-TIME rule —
// nothing in this package (or anywhere else in this issue) mutates a
// stored edge's predicate. GetRelevantContext() (not built yet) is the
// intended caller once it exists; this function exists now so that call
// site has something correct to call.
//
// eventDate is the event's own target date — e.g. the calendar date
// "next week" resolves to, relative to the message that mentioned it —
// which is NOT the same as an edge's first_seen or last_reinforced. Those
// describe when the edge was written, not when the planned event actually
// occurs, and using them as a stand-in would be wrong: it would make
// "going to Italy next week" silently flip to event_past the moment
// enough real time passes since the message was sent, regardless of the
// trip's actual date.
//
// The schema currently has no column for an event's target date (flagged,
// not silently worked around — see the decision recorded in this issue).
// EffectivePredicate therefore takes eventDate as an explicit parameter
// rather than deriving it from a memory.Edge: resolving and storing that
// date is a separate schema/migration decision for a follow-up issue: this
// function is correct and ready to use the moment that value exists
// somewhere a caller can pass in.
//
// A date "has passed" when now is strictly after eventDate — an event
// dated for earlier today is not yet treated as past by this function.
func EffectivePredicate(predicate string, eventDate time.Time, now time.Time) string {
	if predicate != PredicateEventFuture {
		return predicate
	}
	if now.After(eventDate) {
		return PredicateEventPast
	}
	return predicate
}
