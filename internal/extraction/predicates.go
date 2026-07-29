// Package extraction implements Pegasus's structured extraction pipeline
// (proposal §8): the closed predicate vocabulary, the prompt that turns a
// conversation window into a request for qwen3-coder:30b via Costguard, and
// the windowing/parsing/validation logic around that call.
//
// This package produces (subject, predicate, object) triples only. It does
// not resolve subject/object strings to entity IDs, does not write to the
// edges table, and does not implement confidence decay, CorrectMemory(), or
// anything in the Reflection Engine (§9) — those are separate issues.
package extraction

// VocabularyVersion identifies this predicate list. Bump it whenever a
// predicate is added, renamed, or removed, so extraction output — and any
// edges eventually derived from it — can be traced back to the vocabulary
// version that produced it. A predicate rename should never silently
// orphan an old edge's semantics; the version is what makes that
// traceable, alongside git history for the actual diff.
const VocabularyVersion = 1

// Predicate is one entry in the closed vocabulary. Description is injected
// into the extraction prompt (so the model knows what each predicate
// means, not just its name) and doubles as documentation here.
type Predicate struct {
	Name        string
	Category    string
	Description string
}

// Predicates is vocabulary v1 — a first pass derived from proposal §3.3's
// prose categories and §8.5's already-final event temporal split, not an
// attempt at an exhaustive list. Per §8.1: "the vocabulary is expanded
// deliberately as new predicate needs are identified," not left open for
// the model to invent, and not maximized on day one — a too-broad
// vocabulary up front defeats the point of closing it. Expect this list to
// grow; see the manual review findings (extraction_review.md) for
// candidate gaps found while validating against a real message sample.
var Predicates = []Predicate{
	// Preferences
	{Name: "likes", Category: "preferences", Description: "subject has a positive preference for object"},
	{Name: "dislikes", Category: "preferences", Description: "subject has a negative preference for object"},

	// Events — the past/present/future split is a proposal §8.5 decision,
	// not a design choice made in this package. Different default decay
	// behavior per variant is handled elsewhere (confidence decay, a
	// separate issue) — extraction only needs to pick the right variant.
	{Name: "event_past", Category: "events", Description: "subject did or experienced object in the past (e.g. \"went to Rome\")"},
	{Name: "event_present", Category: "events", Description: "subject is currently in/doing object, an ongoing state (e.g. \"lives in Beirut\")"},
	{Name: "event_future", Category: "events", Description: "subject plans to do or experience object (e.g. \"going to Italy next week\")"},

	// Work / education
	{Name: "works_at", Category: "work_education", Description: "subject is employed at object (an organization)"},
	{Name: "studied_at", Category: "work_education", Description: "subject studied or studies at object (a school/university)"},
	{Name: "role_is", Category: "work_education", Description: "subject's job title or role is object"},

	// Routines
	{Name: "routine_is", Category: "routines", Description: "subject regularly or habitually does object"},

	// Goals / projects
	{Name: "goal_is", Category: "goals_projects", Description: "subject's stated goal is object"},
	{Name: "building", Category: "goals_projects", Description: "subject is building or working on object, a project"},

	// People
	{Name: "family_member_of", Category: "people", Description: "subject is a family member of object (subject is the relative, object is who they're related to)"},
	{Name: "pet_of", Category: "people", Description: "subject is a pet belonging to object (subject is the pet, object is the owner)"},
	{Name: "nickname_is", Category: "people", Description: "subject's nickname is object, a literal string"},

	// Shared context
	{Name: "inside_joke_ref", Category: "shared_context", Description: "subject and object share an inside joke or recurring reference"},
}

var predicateNames = func() map[string]bool {
	m := make(map[string]bool, len(Predicates))
	for _, p := range Predicates {
		m[p.Name] = true
	}
	return m
}()

// IsValidPredicate reports whether name is in the closed vocabulary. This
// is the actual enforcement mechanism for "closed" — a prompt instruction
// alone is a request the model can violate; this check is what makes
// violations rejectable rather than silently stored.
func IsValidPredicate(name string) bool {
	return predicateNames[name]
}
