package extraction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

// TranscriptionSegment mirrors the OpenAI-compatible verbose_json segment
// shape — confirmed present, with real (non-placeholder) values, in both
// Speaches (local) and real OpenAI Whisper responses (see
// cmd/whisper_spotcheck/NOTES.md: "does Speaches expose segment-level
// confidence? Yes"). AvgLogprob/NoSpeechProb are proposal §5's
// escalation-trigger signal.
type TranscriptionSegment struct {
	ID               int     `json:"id"`
	Start            float64 `json:"start"`
	End              float64 `json:"end"`
	Text             string  `json:"text"`
	AvgLogprob       float64 `json:"avg_logprob"`
	NoSpeechProb     float64 `json:"no_speech_prob"`
	CompressionRatio float64 `json:"compression_ratio"`
}

// TranscriptionResponse mirrors Costguard's /v1/audio/transcriptions
// verbose_json response. Deliberately has no per-word field (unlike
// cmd/whisper_spotcheck's own transcriptionResponse, which has Words for
// manual comparison purposes): requesting
// timestamp_granularities[]=word was tried and dropped by the spot-check
// (NOTES.md) — it suppresses segment data entirely on real OpenAI and
// returns only placeholder (always-0) word probabilities, so this
// production path never requests it and never needs to parse it.
type TranscriptionResponse struct {
	Task     string                 `json:"task"`
	Language string                 `json:"language"`
	Duration float64                `json:"duration"`
	Text     string                 `json:"text"`
	Segments []TranscriptionSegment `json:"segments"`
}

// MeanSegmentAvgLogprob is the single-number confidence summary §5's
// escalation trigger and messages.transcript_confidence are both derived
// from — the same derivation cmd/whisper_spotcheck's computeMetrics
// already uses for its report (mean of each segment's avg_logprob), not
// a new statistic invented for production. Returns 0 for a response with
// no segments (e.g. silence/empty audio) — deliberately not treated as
// maximum confidence, since there's nothing to be confident about.
func (r *TranscriptionResponse) MeanSegmentAvgLogprob() float64 {
	if len(r.Segments) == 0 {
		return 0
	}
	var sum float64
	for _, s := range r.Segments {
		sum += s.AvgLogprob
	}
	return sum / float64(len(r.Segments))
}

// TranscriptionModel is whisper-1 — cmd/whisper_spotcheck's own
// transcribe.go default, the model field Costguard's
// /v1/audio/transcriptions expects regardless of whether it actually
// routes to local Speaches or real OpenAI (that routing is a
// Costguard-side decision — see TranscribeOptions.ProviderHint and
// Transcribe's own doc comment).
const TranscriptionModel = "whisper-1"

// TranscribeOptions configures one Transcribe call.
type TranscribeOptions struct {
	// Language, if non-empty, is passed through as the request's
	// "language" field (ISO-639-1, e.g. "ar") — a documented, but per
	// cmd/whisper_spotcheck/NOTES.md explicitly UNVERIFIED, mitigation
	// for Whisper's tendency to translate Arabic instead of transcribing
	// it. Left empty lets Whisper auto-detect (the normal default).
	Language string

	// ProviderHint, if non-empty, is sent as X-Costguard-Provider —
	// Costguard's per-request provider-selection header (see
	// costguard/internal/gateway/provider_hint.go) — to force this one
	// call to a specific registered provider (e.g. "openai_primary" for
	// §5's OpenAI escalation) rather than Costguard's normal model-based
	// routing. Left empty uses Costguard's default routing for
	// TranscriptionModel.
	//
	// Deployment note this method cannot fix: this Costguard instance's
	// current config has AUDIO_TRANSCRIPTION_PROVIDER=local, which (per
	// costguard/internal/gateway/audio.go) routes every
	// /v1/audio/transcriptions request straight to local Speaches BEFORE
	// ever checking this header — an escalation call passing
	// ProviderHint="openai_primary" against THIS deployment would
	// silently still hit local Speaches, not actually reach OpenAI,
	// until that setting is reconfigured. Built correctly for when it
	// is; cannot force routing Costguard itself is configured not to
	// offer.
	ProviderHint string
}

// TranscriptionStatusError is returned by Transcribe when Costguard
// responds with a non-200 status, carrying the actual status code so
// callers — specifically Transcribe's own retry-on-502 logic — can
// distinguish a transient failure from a genuine one without parsing
// error text.
type TranscriptionStatusError struct {
	StatusCode int
	Body       string
}

func (e *TranscriptionStatusError) Error() string {
	return fmt.Sprintf("costguard returned status %d: %s", e.StatusCode, e.Body)
}

// transcriptionRetryBackoff is how long Transcribe waits before its one
// retry on a 502 — see Transcribe's own doc comment for what this works
// around.
const transcriptionRetryBackoff = 3 * time.Second

// transcriptionTimeout is deliberately longer than CostguardClient's
// other calls (its HTTPClient defaults to 60s, fine for chat/embeddings)
// — cmd/whisper_spotcheck's own harness used a 5-minute client
// specifically for transcription, since the local Speaches idle-restart
// case (see Transcribe's own doc comment) can leave a request pending
// noticeably longer than a normal ~25s transcription while the container
// comes back up. A dedicated *http.Client with this timeout (sharing
// CostguardClient.HTTPClient's Transport for connection reuse) is used
// only for this call, so it doesn't loosen the timeout for Complete/
// CompleteWithModel/Embed.
const transcriptionTimeout = 5 * time.Minute

// Transcribe calls Costguard's OpenAI-compatible audio transcription
// endpoint (/v1/audio/transcriptions, response_format=verbose_json) —
// the exact call cmd/whisper_spotcheck's transcribeFile already validated
// against both local Speaches and real OpenAI, extracted here into
// reusable production infrastructure rather than re-derived. filename is
// used only for the multipart part name (and thus the file-extension
// hint providers use); audio bytes are supplied once by the caller
// (already read into memory), not reopened from disk here, so the exact
// same bytes can be resent on the 502 retry or an escalation call
// without needing a re-readable file handle.
//
// Two real bugs the spot-check already found, carried forward here
// rather than rediscovered (cmd/whisper_spotcheck/NOTES.md):
//
//   - WhatsApp voice notes are raw .opus files, and OpenAI's real API
//     rejects the .opus extension outright (confirmed live against
//     api.openai.com) while accepting the identical bytes renamed to
//     .ogg. This method renames at the multipart filename level — bytes
//     untouched — for EVERY call, not just ones routed to OpenAI: local
//     Speaches doesn't need it, but applying it unconditionally means
//     one code path instead of two, and it's provably harmless for
//     Speaches (same bytes, a filename it doesn't care about).
//   - Retry-on-502, below: the local Speaches instance unloads its model
//     after 300s idle, and the first request after idle-unload doesn't
//     just reload the model — the whole container process restarts, and
//     that request (observed twice in the spot-check) fails with a 502.
//     A single retry a few seconds later succeeded both times. This is
//     NOT escalation-worthy — it's an operational retry, distinguished
//     from a genuine failure purely by status code (502, exactly once),
//     not by guessing at error text.
//
// Escalation (§5's confidence trigger, plus the spot-check's own
// Arabic-specific finding — see internal/api's shouldEscalateTranscription)
// is NOT decided here: Transcribe makes exactly one logical call (plus
// its own internal 502 retry) with whatever options it's given. Deciding
// whether a SECOND, escalated call is warranted, and making it, is
// internal/api's job — this function is deliberately just the HTTP
// mechanics, reusable for either leg.
func (c *CostguardClient) Transcribe(ctx context.Context, audio []byte, filename string, opts TranscribeOptions) (*TranscriptionResponse, error) {
	resp, err := c.transcribeOnce(ctx, audio, filename, opts)
	if err == nil {
		return resp, nil
	}

	var statusErr *TranscriptionStatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusBadGateway {
		time.Sleep(transcriptionRetryBackoff)
		resp, retryErr := c.transcribeOnce(ctx, audio, filename, opts)
		if retryErr != nil {
			return nil, fmt.Errorf("transcribe (after 502 retry): %w", retryErr)
		}
		return resp, nil
	}

	return nil, err
}

func (c *CostguardClient) transcribeOnce(ctx context.Context, audio []byte, filename string, opts TranscribeOptions) (*TranscriptionResponse, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	partFilename := filename
	if strings.EqualFold(filepath.Ext(partFilename), ".opus") {
		partFilename = strings.TrimSuffix(partFilename, filepath.Ext(partFilename)) + ".ogg"
	}

	part, err := mw.CreateFormFile("file", partFilename)
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, bytes.NewReader(audio)); err != nil {
		return nil, fmt.Errorf("copy audio into request: %w", err)
	}
	_ = mw.WriteField("model", TranscriptionModel)
	_ = mw.WriteField("response_format", "verbose_json")
	if opts.Language != "" {
		_ = mw.WriteField("language", opts.Language)
	}
	// Deliberately NOT requesting timestamp_granularities[]=word — see
	// this method's own doc comment for why.
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/audio/transcriptions", &buf)
	if err != nil {
		return nil, fmt.Errorf("build transcription request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if c.Agent != "" {
		req.Header.Set("X-Costguard-Agent", c.Agent)
	}
	if opts.ProviderHint != "" {
		req.Header.Set("X-Costguard-Provider", opts.ProviderHint)
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	transcriptionClient := &http.Client{Transport: httpClient.Transport, Timeout: transcriptionTimeout}

	resp, err := transcriptionClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call costguard: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read costguard response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &TranscriptionStatusError{StatusCode: resp.StatusCode, Body: truncateForError(string(respBody), 500)}
	}

	var parsed TranscriptionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse transcription response: %w (body: %s)", err, truncateForError(string(respBody), 500))
	}

	return &parsed, nil
}

func truncateForError(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
