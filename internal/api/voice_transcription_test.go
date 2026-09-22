package api

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// fakeTranscriber returns queued responses in call order — one entry per
// expected Transcribe call (local leg, then escalation leg if triggered).
// Records every call's opts so tests can assert what was actually passed
// (e.g. the Arabic-escalation Language="ar" override, or the
// OpenAIProviderHint).
type fakeTranscriber struct {
	responses []fakeTranscribeResult
	calls     []extraction.TranscribeOptions
}

type fakeTranscribeResult struct {
	resp *extraction.TranscriptionResponse
	err  error
}

func (f *fakeTranscriber) Transcribe(_ context.Context, _ []byte, _ string, opts extraction.TranscribeOptions) (*extraction.TranscriptionResponse, error) {
	i := len(f.calls)
	f.calls = append(f.calls, opts)
	if i >= len(f.responses) {
		return nil, fmt.Errorf("fakeTranscriber: no queued response for call %d", i)
	}
	r := f.responses[i]
	return r.resp, r.err
}

func fixedAudioFetcher(data []byte) AudioFetcher {
	return func(_ context.Context, _ string) ([]byte, error) {
		return data, nil
	}
}

func erroringAudioFetcher(err error) AudioFetcher {
	return func(_ context.Context, _ string) ([]byte, error) {
		return nil, err
	}
}

func segResp(language string, avgLogprob float64) *extraction.TranscriptionResponse {
	return &extraction.TranscriptionResponse{
		Language: language,
		Text:     "transcribed text",
		Segments: []extraction.TranscriptionSegment{{AvgLogprob: avgLogprob}},
	}
}

// --- Requirement 9: escalation decision logic in isolation ---

func TestShouldEscalateTranscription(t *testing.T) {
	cfg := TranscriptionConfig{}.withDefaults()

	tests := []struct {
		name string
		resp *extraction.TranscriptionResponse
		want bool
	}{
		{"high confidence English does not escalate", segResp("en", -0.1), false},
		{"low confidence English escalates", segResp("en", -0.9), true},
		{"high confidence Arabic (short code) still escalates", segResp("ar", -0.05), true},
		{"high confidence Arabic (full word, real OpenAI shape) still escalates", segResp("Arabic", -0.05), true},
		{"low confidence non-Arabic non-English escalates via confidence alone", segResp("fr", -0.9), true},
		{"high confidence non-Arabic non-English does not escalate", segResp("fr", -0.1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldEscalateTranscription(tt.resp, cfg); got != tt.want {
				t.Errorf("shouldEscalateTranscription(language=%q, avgLogprob=%v) = %v, want %v",
					tt.resp.Language, tt.resp.Segments[0].AvgLogprob, got, tt.want)
			}
		})
	}
}

// TestTranscribeVoiceMessage_ArabicEscalationPassesLanguageOverride covers
// the unverified-but-documented Arabic mitigation (extraction.TranscribeOptions.Language):
// when the local pass detects Arabic (triggering escalation regardless of
// confidence), the escalation call must carry Language="ar" and the
// configured OpenAIProviderHint.
func TestTranscribeVoiceMessage_ArabicEscalationPassesLanguageOverride(t *testing.T) {
	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{resp: segResp("ar", -0.05)},     // local: high confidence but Arabic
		{resp: segResp("arabic", -0.02)}, // escalated (real OpenAI shape)
	}}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{
		Transcriber:         transcriber,
		TranscriptionConfig: TranscriptionConfig{OpenAIProviderHint: "openai_primary"},
	})

	_, _, ok := a.transcribeVoiceMessage(context.Background(), uuid.New(), []byte("audio"), "note.opus")
	if !ok {
		t.Fatal("expected transcribeVoiceMessage to succeed")
	}
	if len(transcriber.calls) != 2 {
		t.Fatalf("expected exactly 2 Transcribe calls (local + escalation), got %d", len(transcriber.calls))
	}
	escalateOpts := transcriber.calls[1]
	if escalateOpts.Language != "ar" {
		t.Errorf("expected escalation call to pass Language=\"ar\", got %q", escalateOpts.Language)
	}
	if escalateOpts.ProviderHint != "openai_primary" {
		t.Errorf("expected escalation call to pass ProviderHint=\"openai_primary\", got %q", escalateOpts.ProviderHint)
	}
}

// TestTranscribeVoiceMessage_HighConfidenceNonArabicDoesNotEscalate is the
// negative case: a single successful local call, no second Transcribe call.
func TestTranscribeVoiceMessage_HighConfidenceNonArabicDoesNotEscalate(t *testing.T) {
	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{resp: segResp("en", -0.1)},
	}}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{Transcriber: transcriber})

	text, confidence, ok := a.transcribeVoiceMessage(context.Background(), uuid.New(), []byte("audio"), "note.ogg")
	if !ok {
		t.Fatal("expected transcribeVoiceMessage to succeed")
	}
	if text != "transcribed text" {
		t.Errorf("expected local transcript text, got %q", text)
	}
	if confidence != -0.1 {
		t.Errorf("expected local confidence -0.1, got %v", confidence)
	}
	if len(transcriber.calls) != 1 {
		t.Fatalf("expected exactly 1 Transcribe call (no escalation), got %d", len(transcriber.calls))
	}
}

// TestTranscribeVoiceMessage_EscalationFailureFallsBackToLocal covers the
// documented fallback: local succeeded (just low-confidence), escalation
// then failed — the real local transcript must still be returned rather
// than discarded, since this is NOT the "both attempts fail" case.
func TestTranscribeVoiceMessage_EscalationFailureFallsBackToLocal(t *testing.T) {
	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{resp: segResp("en", -0.9)}, // low confidence, triggers escalation
		{err: fmt.Errorf("openai unreachable")},
	}}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{Transcriber: transcriber})

	text, confidence, ok := a.transcribeVoiceMessage(context.Background(), uuid.New(), []byte("audio"), "note.ogg")
	if !ok {
		t.Fatal("expected fallback to local result to succeed (ok=true)")
	}
	if text != "transcribed text" || confidence != -0.9 {
		t.Errorf("expected local fallback result, got text=%q confidence=%v", text, confidence)
	}
}

// TestTranscribeVoiceMessage_BothLegsFail covers requirement 6/12's
// explicit failure behavior: local errors outright, escalation is still
// attempted (giving OpenAI a chance) and also fails — total failure,
// ok=false.
func TestTranscribeVoiceMessage_BothLegsFail(t *testing.T) {
	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{err: fmt.Errorf("local speaches unreachable")},
		{err: fmt.Errorf("openai unreachable")},
	}}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{Transcriber: transcriber})

	text, confidence, ok := a.transcribeVoiceMessage(context.Background(), uuid.New(), []byte("audio"), "note.ogg")
	if ok {
		t.Fatal("expected ok=false when both local and escalated transcription fail")
	}
	if text != "" || confidence != 0 {
		t.Errorf("expected zero-value text/confidence on total failure, got text=%q confidence=%v", text, confidence)
	}
	if len(transcriber.calls) != 2 {
		t.Errorf("expected local transcription failure to still attempt escalation, got %d calls", len(transcriber.calls))
	}
}

// --- transcribeAndStore: AudioFetcher / persistence wiring ---

func TestTranscribeAndStore_NoFetcherConfigured(t *testing.T) {
	a := New(nil, nil, nil, nil, nil, nil)
	msg := &memory.Message{ID: uuid.New()}
	mediaRef := "note.ogg"

	_, ok := a.transcribeAndStore(context.Background(), msg, &mediaRef, nil)
	if ok {
		t.Fatal("expected ok=false with no AudioFetcher configured")
	}
}

func TestTranscribeAndStore_NilMediaRef(t *testing.T) {
	transcriber := &fakeTranscriber{}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{Transcriber: transcriber})
	msg := &memory.Message{ID: uuid.New()}

	_, ok := a.transcribeAndStore(context.Background(), msg, nil, fixedAudioFetcher([]byte("audio")))
	if ok {
		t.Fatal("expected ok=false with a nil media_ref")
	}
	if len(transcriber.calls) != 0 {
		t.Error("expected Transcribe never called when there's no media_ref to fetch")
	}
}

func TestTranscribeAndStore_FetchFailure(t *testing.T) {
	transcriber := &fakeTranscriber{}
	a := New(nil, nil, nil, nil, nil, nil).WithPipeline(PipelineDeps{Transcriber: transcriber})
	msg := &memory.Message{ID: uuid.New()}
	mediaRef := "missing.ogg"

	_, ok := a.transcribeAndStore(context.Background(), msg, &mediaRef, erroringAudioFetcher(fmt.Errorf("file not found")))
	if ok {
		t.Fatal("expected ok=false when the AudioFetcher itself fails")
	}
	if len(transcriber.calls) != 0 {
		t.Error("expected Transcribe never called when audio fetch fails")
	}
}
