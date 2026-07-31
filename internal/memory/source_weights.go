package memory

import "fmt"

// Source types with a defined weight (§7.5). Named constants avoid
// stringly-typed drift between this table and whatever ingestion/
// extraction code eventually sets Edge.SourceType — same pattern as
// extraction.PredicateEventPast etc. for predicate names.
const (
	SourceTypeWhatsAppText     = "whatsapp_text"
	SourceTypeVoiceTranscript  = "voice_transcript"
	SourceTypeInstagramDM      = "instagram_dm"
	SourceTypeInstagramCaption = "instagram_caption"
	SourceTypeInstagramStory   = "instagram_story"
)

// SourceWeights is proposal §7.5's starting trust-weight table, applied at
// WRITE time by ApplySourceWeight — not stored as a second, competing
// confidence field. Mirrors decay_rates.go's map-literal pattern for the
// same shape of concern (a per-key lookup with a documented default/error
// behavior), for consistency between the two.
//
// Exported, unlike predicateDecayRates in decay_rates.go, specifically so
// its keys are independently inspectable from a test (see
// TestSourceWeights_HasExactlyFiveKeys) — that's what makes "configurable,
// not hardcoded inline" a checkable property instead of just a claim in
// this comment.
//
// Deciding weights for source types not yet listed here (Telegram,
// live-image-derived edges, etc.) is an explicitly later decision — see
// ApplySourceWeight's error behavior for why an unlisted source_type
// fails loudly instead of guessing.
var SourceWeights = map[string]float64{
	SourceTypeWhatsAppText:     1.0,
	SourceTypeVoiceTranscript:  0.95,
	SourceTypeInstagramDM:      1.0,
	SourceTypeInstagramCaption: 0.4,
	SourceTypeInstagramStory:   0.2,
}

// ApplySourceWeight implements §7.5's write-time formula:
//
//	effective_confidence = extracted_confidence * source_weight
//
// Named distinctly from the read-time EffectiveConfidence(edge, now) decay
// helper (effective_confidence.go, previous issue) to avoid a collision —
// the two are deliberately different mechanisms applied at different
// times, not overloads of the same idea. This one runs once, at edge
// creation; decay is recomputed on every read thereafter.
//
// This is a WRITE-time computation, not read-time like decay. §7.5 is
// explicit about why the two differ: "retrieval logic only ever reasons
// about one confidence number" — source_weight collapses into the single
// stored Edge.Confidence value up front, so nothing downstream needs to
// treat source trust as a second axis every query has to know about.
// source_type itself is still stored on the edge separately (schema
// column, migration 000002) for provenance/audit (feeds
// WhyDoWeBelieveThis(), §12) — only Confidence is pre-multiplied; this
// function does not touch or return source_type.
//
// An unmapped sourceType returns an error rather than silently defaulting
// to 1.0 (would overstate trust for an unvetted source) or 0 (would zero
// out the edge entirely) — a new source type appearing in real ingested
// data with no explicit weight decision is exactly the kind of silent
// assumption that should surface immediately as a caught error, not
// disappear into a guessed number.
func ApplySourceWeight(extractedConfidence float64, sourceType string) (float64, error) {
	weight, ok := SourceWeights[sourceType]
	if !ok {
		return 0, fmt.Errorf("no source_weight configured for source_type %q — add it to SourceWeights before writing edges from this source", sourceType)
	}
	return extractedConfidence * weight, nil
}
