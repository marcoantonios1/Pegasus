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
//
// IsCorrection did not exist before the write-pipeline issue (proposal
// §13) needed a way to detect "Marco is explicitly correcting a
// previously stated fact" so ProcessLiveMessage/ImportWhatsApp can route
// it through memory.CorrectMemory instead of a normal edge write. That
// issue's own instructions were explicit that if extraction had no way to
// flag this, the minimal fix was extending extraction's own output shape
// with an is_correction field, rather than standing up a separate
// classifier — this is that minimal extension, not a second model call.
type ExtractedTriple struct {
	Subject      string
	Predicate    string
	Object       string
	ObjectType   string // "entity" | "literal"
	Confidence   float64
	IsCorrection bool

	// CorrectionSourceIndex is the "i" value (prompt.go's conversationLine)
	// of the window-local conversation line the model says this
	// correction came from — present only when IsCorrection is true, nil
	// otherwise (ValidateTriples does not require it to be set even when
	// IsCorrection is true; that gate lives in internal/api's
	// storeExtractedTriples, not here — see its own doc comment).
	//
	// This is NOT a verified identity and must never be trusted as one:
	// it is the model's own self-report, parsed out of model output the
	// same as every other field here, extracted from a prompt built out
	// of untrusted conversation content. Prompt-injection/forged-
	// attribution fix (storeExtractedTriples' own doc comment has the
	// full reasoning): whether the message at this index was ACTUALLY
	// sent by the self entity is checked against STORED, trusted data
	// (the message's own sender_id, resolved to an entity, checked for
	// IsSelf) before is_correction is ever honored — never against
	// anything the model reported, including a speaker name it might
	// also echo back.
	CorrectionSourceIndex *int
}

type rawTriple struct {
	Subject               string  `json:"subject"`
	Predicate             string  `json:"predicate"`
	Object                string  `json:"object"`
	ObjectType            string  `json:"object_type"`
	Confidence            float64 `json:"confidence"`
	IsCorrection          bool    `json:"is_correction"`
	CorrectionSourceIndex *int    `json:"correction_source_index,omitempty"`
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
			Subject:               rt.Subject,
			Predicate:             rt.Predicate,
			Object:                rt.Object,
			ObjectType:            rt.ObjectType,
			Confidence:            rt.Confidence,
			IsCorrection:          rt.IsCorrection,
			CorrectionSourceIndex: rt.CorrectionSourceIndex,
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
//
// CorrectionSourceIndex is deliberately NOT validated here beyond the
// shape-level acceptance *int already gives it: ParseModelOutput's
// json.Unmarshal into an *int destination already rejects a non-integer
// value (a JSON number with a fractional/exponent part, a string, an
// object — anything that isn't a bare integer) by failing the WHOLE
// response's decode, the same hard-parse-error behavior any other
// malformed field already gets — no extra per-field code needed for
// that. Whether an is_correction=true triple's index is present, in
// range, and actually resolves to the self entity is a TRUST decision
// against stored data, not a shape/vocabulary one — that check lives in
// internal/api's storeExtractedTriples (the self-sender gate), which also
// owns the fail-safe "downgrade to a normal claim" behavior for a
// missing/out-of-range/non-self index. This function only ever decides
// whether a triple's SHAPE is acceptable, never who's trusted.
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
