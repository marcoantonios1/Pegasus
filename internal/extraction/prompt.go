package extraction

import (
	"fmt"
	"strings"
	"text/template"
)

// promptTemplateText implements the exact call shape from proposal §8.2:
// JSON array only, no prose, { subject, predicate, object, object_type,
// confidence } per item, [] if nothing extractable. The closed predicate
// list (§8.1) is injected so the model has no room to invent predicates —
// ValidateTriples (parse.go) is the actual enforcement, this is the
// request.
//
// An explicit instruction against extracting "subject said 'X'" tautologies
// on unparseable slang was tried and reverted after validating against a
// real message sample: it didn't fix the target failure (the model still
// produced "said 'X'" triples on Arabizi content it couldn't interpret) and
// it regressed a different window from a correct [] to hallucinated noise.
// Net effect was unclear-to-negative, so it's not here — see
// extraction_review.md for the full before/after comparison. Worth
// revisiting with a different approach, not with this same instruction.
//
// Event tense classification (proposal §8.5, event_past/present/future):
//   - Classified relative to the mentioning MESSAGE's own timestamp, not
//     to whenever extraction happens to run. This matters specifically for
//     historical backlog import: a message from two years ago saying "next
//     week" must come out event_future (relative to when it was said), not
//     event_past just because two years have since elapsed in the real
//     world. Getting this wrong at extraction time can't be fixed later by
//     the read-time event_future→event_past rule (EffectivePredicate) —
//     that rule only ever moves event_future forward into event_past based
//     on elapsed time, it doesn't correct a tense that was misclassified
//     backward in the first place.
//   - Ambiguous/unclear tense defaults to event_present, never a guessed
//     event_past or event_future — event_present carries no hard-expiry
//     read-time behavior, so a wrong guess here is the least harmful of
//     the three.
const promptTemplateText = `Given this conversation snippet (with speaker labels and timestamps), extract factual, preference, and event triples about the people in it.

Allowed predicates — use ONLY these exact values for "predicate", never invent others:
{{range .Predicates}}- {{.Name}} ({{.Category}}): {{.Description}}
{{end}}
Conversation:
{{range .Messages}}[{{.Timestamp.Format "2006-01-02 15:04:05"}}] {{.Speaker}}: {{.Text}}
{{end}}
Output ONLY a JSON array, no prose, no markdown code fences, no explanation before or after it. Each item must have exactly these fields:
{"subject": string, "predicate": string, "object": string, "object_type": "entity" | "literal", "confidence": number between 0 and 1, "is_correction": true | false}

"predicate" must be exactly one of the allowed predicate names listed above.
"object_type" is "entity" if object refers to a person/place/thing that could itself be a subject elsewhere, "literal" for a plain value (a food, a city name used only as a value, a nickname string, etc.).
"is_correction" is true only when the speaker is explicitly correcting or retracting something they or someone else previously stated as fact (e.g. "actually, that's wrong, I moved to Beirut last month, not Dubai" or "no, my birthday is in March, not April") — not for a new fact, an update to an ongoing situation stated as new information, or a simple change of mind about a future plan. When in doubt, use false: a missed correction still gets written as a new fact and can be corrected later, but a false positive here would route an ordinary fact through the wrong storage path.

For event_past / event_present / event_future specifically: classify the tense relative to the TIMESTAMP OF THE MESSAGE that mentions the event, not relative to today's date. A message timestamped two years ago saying "next week" describes an event_future relative to that message's own timestamp — it happened in the past from today's perspective, but it was a future plan at the moment it was said, and that is what determines the predicate. Do not use today's date to decide event tense.
If an event's tense is genuinely unclear or ambiguous from the phrasing, use event_present rather than guessing event_past or event_future — event_present is the safest default since it does not trigger a hard-expiry rule the way a wrongly-guessed event_future would.

If nothing is extractable from this conversation snippet, output exactly: []
`

var promptTemplate = template.Must(template.New("extraction").Parse(promptTemplateText))

type promptData struct {
	Predicates []Predicate
	Messages   []WindowMessage
}

// BuildPrompt renders the extraction prompt for one window against the
// given predicate vocabulary (normally the package-level Predicates var —
// exposed as a parameter so a test can exercise BuildPrompt with a small
// fixed vocabulary instead of the full list).
func BuildPrompt(w Window, vocab []Predicate) (string, error) {
	var buf strings.Builder
	data := promptData{Predicates: vocab, Messages: w.Messages}
	if err := promptTemplate.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("build extraction prompt: %w", err)
	}
	return buf.String(), nil
}
