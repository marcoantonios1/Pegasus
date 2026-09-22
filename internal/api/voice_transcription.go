package api

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Transcriber is the subset of *extraction.CostguardClient this package
// needs (proposal §5/§8.4's voice pipeline), narrowed to an interface so
// tests can inject a fake instead of requiring a live Costguard/Speaches
// call — same pattern as Embedder/Extractor/Triager.
type Transcriber interface {
	Transcribe(ctx context.Context, audio []byte, filename string, opts extraction.TranscribeOptions) (*extraction.TranscriptionResponse, error)
}

// AudioFetcher resolves a voice message's media_ref to raw audio bytes.
//
// ImportWhatsApp always has one: it constructs one internally, scoped to
// the export file's own directory, where a WhatsApp "with media" export
// places sibling audio files (media_ref is just the filename WhatsApp's
// export text references, e.g. "PTT-20260702-WA0011.opus" — see
// internal/ingestion/whatsapp/export.go's attachedFileRe).
//
// ProcessLiveMessage does NOT get one automatically. A live WhatsApp
// voice note's MediaURL is a whatsmeow-hosted, authenticated/encrypted
// media reference (internal/ingestion/whatsapp/live.go), and nothing in
// this codebase has a mechanism to download and decrypt that today — no
// whatsmeow client reference reaches this package at all. If
// WithPipeline's AudioFetcher is nil (the default), voice messages in
// ProcessLiveMessage fall back to being stored with no transcription
// attempted (see resolveTextForTriage) — a live AudioFetcher (calling
// into whatsmeow's own Download method) is a real, separate follow-up
// issue, not built here; flagged, not silently pretended solved.
type AudioFetcher func(ctx context.Context, mediaRef string) ([]byte, error)

// TranscriptionConfig configures the voice pipeline's escalation
// decision (shouldEscalateTranscription) and where escalated calls route.
type TranscriptionConfig struct {
	// ConfidenceThreshold: a local transcription with
	// MeanSegmentAvgLogprob below this escalates to OpenAI — proposal
	// §5's base mechanism. See DefaultTranscriptionConfidenceThreshold's
	// own doc comment for why this alone is known-insufficient for
	// Arabic content specifically, and shouldEscalateTranscription for
	// the additional override that exists because of it.
	ConfidenceThreshold float64

	// OpenAIProviderHint is the Costguard-registered provider name sent
	// as X-Costguard-Provider on an escalation call (e.g.
	// "openai_primary" — see costguard/config.json's "providers" section
	// for this deployment's actual registered name). See
	// extraction.TranscribeOptions.ProviderHint's own doc comment for the
	// important caveat that this deployment's current
	// AUDIO_TRANSCRIPTION_PROVIDER=local config ignores this header
	// entirely — the hint is built correctly for when that's
	// reconfigured, it can't force routing Costguard itself disables.
	OpenAIProviderHint string
}

const (
	// DefaultTranscriptionConfidenceThreshold: first-guess starting
	// value, not validated — same caveat as every other first-guess
	// threshold in this build (dedup similarity, decay rates, prune
	// floors). Picked from cmd/whisper_spotcheck's real review.md data:
	// every English-dominant file (rated 100% by Marco, by ear) had
	// MeanSegmentAvgLogprob no worse than -0.41 (997737946584480.mp4);
	// -0.7 sits comfortably below that entire observed-good range, so it
	// shouldn't over-escalate ordinary English content while still
	// catching a transcription confident enough range to be clearly
	// worse than anything observed as fine. This threshold's real
	// significance is now narrower than §5 originally imagined, though:
	// per review.md's own correlation analysis (r=0.547 between rating
	// and avg_logprob — "moderate positive, not strong") and its worst
	// finding (the single catastrophic failure, total language
	// misidentification, had avg_logprob that was only the 3rd-worst in
	// the set — confidence completely missed it), this threshold mostly
	// matters for non-Arabic content now; Arabic escalates unconditionally
	// regardless of this value — see shouldEscalateTranscription.
	DefaultTranscriptionConfidenceThreshold = -0.7

	// DefaultOpenAIProviderHint matches this deployment's actual
	// Costguard config (costguard/config.json's providers.openai_primary)
	// — see OpenAIProviderHint's own doc comment for the routing caveat.
	DefaultOpenAIProviderHint = "openai_primary"
)

func (c TranscriptionConfig) withDefaults() TranscriptionConfig {
	if c.ConfidenceThreshold == 0 {
		c.ConfidenceThreshold = DefaultTranscriptionConfidenceThreshold
	}
	if c.OpenAIProviderHint == "" {
		c.OpenAIProviderHint = DefaultOpenAIProviderHint
	}
	return c
}

// isArabicLanguage reports whether lang (a TranscriptionResponse.Language
// value) is Arabic. Handles both shapes actually observed in real data
// (cmd/whisper_spotcheck/review.md): Speaches (local) returns the short
// ISO-639-1 code "ar"; real OpenAI returns the full word "arabic". Case-
// insensitive defensively, though local's own output is consistently
// lowercase in every sample seen.
//
// Scoped to Arabic specifically, not "any non-English" — proposal §5's
// spot-check evidence is specifically about Arabic (all six files rated
// below 100% in review.md involved Arabic, alone or code-switched; no
// French-only or other-language failure was observed in that sample).
// Broadening this to "any non-English" would be extrapolating past what
// was actually tested, presented as if validated when it isn't.
func isArabicLanguage(lang string) bool {
	l := strings.ToLower(strings.TrimSpace(lang))
	return l == "ar" || l == "arabic"
}

// shouldEscalateTranscription decides whether a local transcription
// result needs escalation to OpenAI. Escalates on EITHER:
//
//   - MeanSegmentAvgLogprob below cfg.ConfidenceThreshold — proposal §5's
//     stated base mechanism ("escalate on low per-segment transcription
//     confidence"), or
//   - the detected language is Arabic, REGARDLESS of confidence.
//
// The second condition exists because of a specific, real finding in
// cmd/whisper_spotcheck/review.md's analysis, not a hypothetical: local
// confidence did NOT reliably predict accuracy for Arabic/code-switched
// content (rating-vs-avg_logprob correlation r=0.547, "moderate... not
// strong"), and completely missed the single worst failure in the
// sample — a case where local mis-identified the language entirely and
// produced garbage, at a confidence score that wasn't even the worst one
// in the set. A pure "escalate if avg_logprob below threshold"
// implementation would miss exactly that failure mode, which review.md's
// own recommendation addresses directly: "escalate unconditionally...
// skip the confidence gate entirely for non-English-detected segments,
// since confidence didn't reliably separate good from catastrophic
// here." DO NOT simplify this back to confidence-only — that reintroduces
// the exact problem the spot-check already found and documented.
func shouldEscalateTranscription(resp *extraction.TranscriptionResponse, cfg TranscriptionConfig) bool {
	if isArabicLanguage(resp.Language) {
		return true
	}
	return resp.MeanSegmentAvgLogprob() < cfg.ConfidenceThreshold
}

// transcribeVoiceMessage runs the full local -> maybe-escalate sequence
// for one voice message's audio and returns the resulting transcript
// text and confidence, or ok=false if transcription failed entirely.
//
// Escalation, when triggered (shouldEscalateTranscription), passes
// Language="ar" on the escalation call specifically when the local pass
// detected Arabic — the documented-but-per-NOTES.md-UNVERIFIED mitigation
// for Whisper's translate-instead-of-transcribe tendency on Arabic audio.
// This is passed through whenever Costguard's endpoint accepts it (it's
// a standard Whisper API field); there is no separate capability check,
// since an unsupported field would simply be ignored server-side rather
// than error.
//
// Failure behavior, decided explicitly (same bar as the embeddings
// issue's log-and-continue requirement): if local transcription itself
// errors (not a low-confidence success — an actual failure), escalation
// is still attempted, giving OpenAI a chance before giving up entirely.
// If escalation then ALSO fails: total failure, ok=false, logged with the
// message ID — the caller must leave the message unprocessed, not marked
// done with no transcript (see resolveTextForTriage). If local SUCCEEDED
// (just low-confidence or Arabic) but escalation fails, this falls back
// to the local result rather than discarding a real, already-obtained
// transcript — a real transcript at lower confidence is still more
// useful than losing the message from the graph entirely, and this is
// NOT the "both attempts fail" case requirement 6 describes.
func (a *API) transcribeVoiceMessage(ctx context.Context, msgID uuid.UUID, audio []byte, filename string) (text string, confidence float64, ok bool) {
	cfg := a.transcriptionConfig.withDefaults()

	localResp, localErr := a.transcriber.Transcribe(ctx, audio, filename, extraction.TranscribeOptions{})

	var needsEscalation bool
	var detectedLang string
	if localErr != nil {
		a.logf("transcribeVoiceMessage: local transcription failed for message %s: %v", msgID, localErr)
		needsEscalation = true
	} else {
		detectedLang = localResp.Language
		needsEscalation = shouldEscalateTranscription(localResp, cfg)
	}

	if !needsEscalation {
		return localResp.Text, localResp.MeanSegmentAvgLogprob(), true
	}

	escalateOpts := extraction.TranscribeOptions{ProviderHint: cfg.OpenAIProviderHint}
	if isArabicLanguage(detectedLang) {
		escalateOpts.Language = "ar"
	}

	escalatedResp, escalatedErr := a.transcriber.Transcribe(ctx, audio, filename, escalateOpts)
	if escalatedErr != nil {
		if localErr == nil {
			a.logf("transcribeVoiceMessage: escalation failed for message %s (%v) — falling back to local result", msgID, escalatedErr)
			return localResp.Text, localResp.MeanSegmentAvgLogprob(), true
		}
		a.logf("transcribeVoiceMessage: both local and escalated transcription failed for message %s: local=%v escalated=%v", msgID, localErr, escalatedErr)
		return "", 0, false
	}

	return escalatedResp.Text, escalatedResp.MeanSegmentAvgLogprob(), true
}

// transcribeAndStore fetches msg's audio (via fetcher, keyed by mediaRef)
// and transcribes it (transcribeVoiceMessage), persisting the result onto
// msg (transcript + transcript_confidence) before returning the
// transcript text for the caller to feed into the SAME triage/extraction
// path a text message would use (requirement 5 — no divergent extraction
// logic for voice-derived text).
//
// fetcher is passed explicitly rather than always reading a.audioFetcher
// — ImportWhatsApp always has one (a locally-scoped fetcher over the
// export's own directory, built fresh per call since it depends on that
// specific export's path) where ProcessLiveMessage may not (a.audioFetcher,
// possibly nil — see AudioFetcher's own doc comment for why a live one
// isn't built here). A shared *API struct field would force either
// mutating it per ImportWhatsApp call (unsafe if ever called concurrently
// on the same *API) or a second field just for this — passing it as a
// parameter avoids both.
//
// Returns ok=false — logged via a.logf with the message ID, never
// silent — if there's no way to get audio bytes at all (fetcher nil or
// mediaRef nil) or if transcription failed entirely. This function never
// marks msg processed either way — msg.Processed is the caller's decision
// once it knows whether there's anything further to attempt (see
// resolveTextForTriage / ProcessLiveMessage / ImportWhatsApp).
func (a *API) transcribeAndStore(ctx context.Context, msg *memory.Message, mediaRef *string, fetcher AudioFetcher) (string, bool) {
	if fetcher == nil {
		a.logf("transcribeAndStore: no AudioFetcher configured, message %s stays untranscribed", msg.ID)
		return "", false
	}
	if mediaRef == nil {
		a.logf("transcribeAndStore: message %s has media_type=voice but no media_ref, nothing to fetch", msg.ID)
		return "", false
	}

	audio, err := fetcher(ctx, *mediaRef)
	if err != nil {
		a.logf("transcribeAndStore: fetch audio for message %s (%s): %v", msg.ID, *mediaRef, err)
		return "", false
	}

	text, confidence, ok := a.transcribeVoiceMessage(ctx, msg.ID, audio, *mediaRef)
	if !ok {
		return "", false
	}

	msg.Transcript = &text
	msg.TranscriptConfidence = &confidence
	if err := a.messages.Update(ctx, msg); err != nil {
		a.logf("transcribeAndStore: store transcript for message %s: %v", msg.ID, err)
		return "", false
	}

	return text, true
}
