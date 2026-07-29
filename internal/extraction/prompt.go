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
const promptTemplateText = `Given this conversation snippet (with speaker labels and timestamps), extract factual, preference, and event triples about the people in it.

Allowed predicates — use ONLY these exact values for "predicate", never invent others:
{{range .Predicates}}- {{.Name}} ({{.Category}}): {{.Description}}
{{end}}
Conversation:
{{range .Messages}}[{{.Timestamp.Format "2006-01-02 15:04:05"}}] {{.Speaker}}: {{.Text}}
{{end}}
Output ONLY a JSON array, no prose, no markdown code fences, no explanation before or after it. Each item must have exactly these fields:
{"subject": string, "predicate": string, "object": string, "object_type": "entity" | "literal", "confidence": number between 0 and 1}

"predicate" must be exactly one of the allowed predicate names listed above.
"object_type" is "entity" if object refers to a person/place/thing that could itself be a subject elsewhere, "literal" for a plain value (a food, a city name used only as a value, a nickname string, etc.).
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
