package extraction

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// ExtractedTriple is one (subject, predicate, object) fact as extraction
// produces it — subject/object are the raw strings the model saw in the
// conversation (e.g. "Sara", "olives"), not resolved entity IDs. Resolving
// those strings to entities.id rows and writing edges is downstream work
// (entity resolution / the Relationship Graph stage in the architecture
// diagram), out of scope for this package.
type ExtractedTriple struct {
	Subject    string
	Predicate  string
	Object     string
	ObjectType string // "entity" | "literal"
	Confidence float64
}

type rawTriple struct {
	Subject    string  `json:"subject"`
	Predicate  string  `json:"predicate"`
	Object     string  `json:"object"`
	ObjectType string  `json:"object_type"`
	Confidence float64 `json:"confidence"`
}

// ParseModelOutput parses the model's raw text response into triples. §8.2
// instructs the model to output ONLY a JSON array, but this tolerates a
// markdown code fence wrapped around it anyway — a cheap defensive strip,
// since "wrap the answer in ```json anyway" is a common enough deviation
// from a "no prose" instruction to be worth handling rather than failing
// outright on. Anything else malformed is a real parse error, returned as
// one — not silently coerced into an empty result.
func ParseModelOutput(raw string) ([]ExtractedTriple, error) {
	cleaned := stripCodeFence(raw)

	var rawTriples []rawTriple
	if err := json.Unmarshal([]byte(cleaned), &rawTriples); err != nil {
		return nil, fmt.Errorf("parse model output as JSON array: %w", err)
	}

	triples := make([]ExtractedTriple, len(rawTriples))
	for i, rt := range rawTriples {
		triples[i] = ExtractedTriple{
			Subject:    rt.Subject,
			Predicate:  rt.Predicate,
			Object:     rt.Object,
			ObjectType: rt.ObjectType,
			Confidence: rt.Confidence,
		}
	}
	return triples, nil
}

func stripCodeFence(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}

// Logger receives one line per rejected triple during validation.
type Logger func(format string, args ...any)

// ValidateTriples filters parsed triples down to ones satisfying the
// closed vocabulary and basic shape constraints. This — not the prompt
// instruction — is the actual enforcement mechanism for "closed
// vocabulary": every rejection is logged rather than silently dropped, so
// vocabulary violations from the model are visible instead of swallowed.
// A coder-tuned model with a hard-constrained list should rarely trigger
// this, but "rarely" isn't "never," which is the whole reason this step
// exists rather than trusting the prompt alone.
func ValidateTriples(triples []ExtractedTriple, logf Logger) []ExtractedTriple {
	if logf == nil {
		logf = func(format string, args ...any) { log.Printf(format, args...) }
	}

	var valid []ExtractedTriple
	for _, t := range triples {
		if !IsValidPredicate(t.Predicate) {
			logf("extraction: rejecting triple, predicate %q is not in the closed vocabulary (subject=%q object=%q)", t.Predicate, t.Subject, t.Object)
			continue
		}
		if t.ObjectType != "entity" && t.ObjectType != "literal" {
			logf("extraction: rejecting triple, object_type %q must be \"entity\" or \"literal\" (subject=%q predicate=%q)", t.ObjectType, t.Subject, t.Predicate)
			continue
		}
		if t.Subject == "" || t.Object == "" {
			logf("extraction: rejecting triple with empty subject or object (predicate=%q)", t.Predicate)
			continue
		}
		if t.Confidence < 0 || t.Confidence > 1 {
			logf("extraction: rejecting triple, confidence %v out of range [0,1] (subject=%q predicate=%q)", t.Confidence, t.Subject, t.Predicate)
			continue
		}
		valid = append(valid, t)
	}
	return valid
}
