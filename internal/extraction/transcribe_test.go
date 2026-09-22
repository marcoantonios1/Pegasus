package extraction

import (
	"context"
	"encoding/json"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testTranscriptionResponse() TranscriptionResponse {
	return TranscriptionResponse{
		Task: "transcribe", Language: "en", Duration: 3.2, Text: "hello there",
		Segments: []TranscriptionSegment{{ID: 0, Start: 0, End: 3.2, Text: "hello there", AvgLogprob: -0.1, NoSpeechProb: 0.01, CompressionRatio: 1.1}},
	}
}

// multipartFilename extracts the "file" part's filename from a
// transcription request, the same way Costguard/OpenAI would read it off
// the multipart form.
func multipartFilename(t *testing.T, r *http.Request) string {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		t.Fatalf("expected a multipart Content-Type, got %q (err=%v)", r.Header.Get("Content-Type"), err)
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if err != nil {
			t.Fatalf("read multipart part: %v", err)
		}
		if part.FormName() == "file" {
			return part.FileName()
		}
	}
}

// --- Requirement 11: .opus -> .ogg rename ---

func TestTranscribe_RenamesOpusToOgg(t *testing.T) {
	var gotFilename string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFilename = multipartFilename(t, r)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(testTranscriptionResponse())
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Transcribe(context.Background(), []byte("fake opus bytes"), "PTT-20260702-WA0011.opus", TranscribeOptions{})
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if gotFilename != "PTT-20260702-WA0011.ogg" {
		t.Errorf("expected multipart filename renamed to .ogg, got %q", gotFilename)
	}
}

// TestTranscribe_LeavesOtherExtensionsAlone confirms the rename is scoped
// to .opus specifically — .ogg and .mp4 (both natively OpenAI-accepted,
// per NOTES.md's confirmed supported-format list) must pass through
// unchanged, not get double-renamed or mangled.
func TestTranscribe_LeavesOtherExtensionsAlone(t *testing.T) {
	for _, filename := range []string{"received-note.ogg", "instagram-voice-note.mp4", "some-file.m4a"} {
		t.Run(filename, func(t *testing.T) {
			var gotFilename string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotFilename = multipartFilename(t, r)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(testTranscriptionResponse())
			}))
			defer server.Close()

			client := NewCostguardClient(server.URL)
			_, err := client.Transcribe(context.Background(), []byte("fake audio bytes"), filename, TranscribeOptions{})
			if err != nil {
				t.Fatalf("Transcribe: %v", err)
			}
			if gotFilename != filename {
				t.Errorf("expected filename unchanged (%q), got %q", filename, gotFilename)
			}
		})
	}
}

// TestTranscribe_OpusRenameAppliesOnEscalationToo confirms the rename is
// unconditional — applied regardless of ProviderHint — since the
// escalation leg (routed to real OpenAI) is exactly the case that fails
// outright without it.
func TestTranscribe_OpusRenameAppliesOnEscalationToo(t *testing.T) {
	var gotFilename, gotProviderHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProviderHeader = r.Header.Get("X-Costguard-Provider")
		gotFilename = multipartFilename(t, r)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(testTranscriptionResponse())
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Transcribe(context.Background(), []byte("fake opus bytes"), "voice-note.opus", TranscribeOptions{ProviderHint: "openai_primary"})
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}
	if gotFilename != "voice-note.ogg" {
		t.Errorf("expected .opus renamed to .ogg on escalation call too, got %q", gotFilename)
	}
	if gotProviderHeader != "openai_primary" {
		t.Errorf("expected X-Costguard-Provider: openai_primary, got %q", gotProviderHeader)
	}
}

// --- Requirement 10: 502 retry-once ---

func TestTranscribe_RetriesOnceOn502ThenSucceeds(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway)
			w.Write([]byte("bad gateway — speaches container restarting"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(testTranscriptionResponse())
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	// Overridden to keep this test fast — DefaultTranscriptionRetryBackoff
	// is now 30s (see its own doc comment for the real-world evidence
	// behind that value), which TranscriptionRetryBackoff exists
	// specifically so a test doesn't have to sit through.
	const testBackoff = 50 * time.Millisecond
	client.TranscriptionRetryBackoff = testBackoff

	start := time.Now()
	resp, err := client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected Transcribe to succeed after one 502 retry, got: %v", err)
	}
	if resp.Text != "hello there" {
		t.Errorf("expected the successful retry's response, got %+v", resp)
	}
	if atomic.LoadInt32(&requestCount) != 2 {
		t.Fatalf("expected exactly 2 requests (1 failed + 1 retry), got %d", requestCount)
	}
	if elapsed < testBackoff {
		t.Errorf("expected Transcribe to wait at least %s before retrying, only took %s", testBackoff, elapsed)
	}
}

// TestTranscribe_DefaultRetryBackoffIsUsedWhenUnset confirms
// transcriptionRetryBackoff() falls back to DefaultTranscriptionRetryBackoff
// (30s, per real evidence — see its own doc comment) when
// CostguardClient.TranscriptionRetryBackoff is left at its zero value, so
// production callers get the real default without having to set it
// explicitly.
func TestTranscribe_DefaultRetryBackoffIsUsedWhenUnset(t *testing.T) {
	client := NewCostguardClient("http://unused.invalid")
	if got := client.transcriptionRetryBackoff(); got != DefaultTranscriptionRetryBackoff {
		t.Errorf("expected default backoff %s when unset, got %s", DefaultTranscriptionRetryBackoff, got)
	}
}

// TestTranscribe_DoesNotRetryOnNon502Status confirms the retry is scoped
// to exactly 502 — a genuine failure (e.g. 400 invalid file format) must
// not be retried and wasted time on, and must surface as an error.
func TestTranscribe_DoesNotRetryOnNon502Status(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error": "Invalid file format"}`))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Transcribe(context.Background(), []byte("audio"), "note.opus", TranscribeOptions{})
	if err == nil {
		t.Fatal("expected an error for a 400 response, got nil")
	}
	if atomic.LoadInt32(&requestCount) != 1 {
		t.Errorf("expected exactly 1 request (no retry on non-502), got %d", requestCount)
	}
}

// TestTranscribe_FailsAfterRetryStillReturns502 confirms a SECOND 502 (the
// retry itself failing) surfaces as a genuine error rather than retrying
// forever or silently swallowing it.
func TestTranscribe_FailsAfterRetryStillReturns502(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("still restarting"))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.TranscriptionRetryBackoff = 50 * time.Millisecond // keep this test fast — see the other backoff override's doc comment
	_, err := client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
	if err == nil {
		t.Fatal("expected an error when the retry also returns 502, got nil")
	}
	if atomic.LoadInt32(&requestCount) != 2 {
		t.Errorf("expected exactly 2 requests (1 initial + 1 retry, no further retries), got %d", requestCount)
	}
}
