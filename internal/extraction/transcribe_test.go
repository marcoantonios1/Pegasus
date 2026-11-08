package extraction

import (
	"context"
	"encoding/json"
	"errors"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// --- Retry backoff respects ctx cancellation (the fix this issue is
// about: a cancelled/timed-out ctx used to still block for the full
// backoff and then issue the retry anyway) ---

// always502Server returns a handler that always responds 502, signaling
// firstRequestArrived once the first request has actually arrived at
// the server (before responding) — lets a test cancel/time out the
// caller's ctx WHILE Transcribe is in its post-502 backoff wait, not
// before the first request was even sent.
func always502Server(t *testing.T, requestCount *int32, firstRequestArrived chan struct{}) *httptest.Server {
	t.Helper()
	var signaled sync.Once
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(requestCount, 1)
		signaled.Do(func() { close(firstRequestArrived) })
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("bad gateway — speaches container restarting"))
	}))
}

// TestTranscribe_CancelledContextAbortsRetryWaitWithoutRetrying covers
// requirement 6: a long backoff (1 hour — long enough that nothing
// could possibly fire it for real within this test) and a ctx cancelled
// ~50ms after the first request arrives must make Transcribe return
// almost immediately with a wrapped context.Canceled, having sent
// exactly ONE request — the retry must never go out once the caller has
// given up.
func TestTranscribe_CancelledContextAbortsRetryWaitWithoutRetrying(t *testing.T) {
	var requestCount int32
	firstRequestArrived := make(chan struct{})
	server := always502Server(t, &requestCount, firstRequestArrived)
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.TranscriptionRetryBackoff = time.Hour // long enough that only ctx cancellation could end the wait

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-firstRequestArrived
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := client.Transcribe(ctx, []byte("audio"), "note.ogg", TranscribeOptions{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when ctx is cancelled during the retry backoff, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled) through Transcribe's wrapping, got: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("expected Transcribe to return almost immediately after cancellation, took %s", elapsed)
	}
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected exactly 1 request (the retry must not be sent after cancellation), got %d", got)
	}
}

// TestTranscribe_DeadlineExceededAbortsRetryWaitWithoutRetrying is the
// context.DeadlineExceeded counterpart of the cancellation test above —
// requirement 7.
func TestTranscribe_DeadlineExceededAbortsRetryWaitWithoutRetrying(t *testing.T) {
	var requestCount int32
	firstRequestArrived := make(chan struct{})
	server := always502Server(t, &requestCount, firstRequestArrived)
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.TranscriptionRetryBackoff = time.Hour

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.Transcribe(ctx, []byte("audio"), "note.ogg", TranscribeOptions{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when ctx's deadline is exceeded during the retry backoff, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected errors.Is(err, context.DeadlineExceeded) through Transcribe's wrapping, got: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("expected Transcribe to return almost immediately after the deadline, took %s", elapsed)
	}
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected exactly 1 request (the retry must not be sent after the deadline), got %d", got)
	}
}

// TestTranscribe_AlreadyCancelledContextSkipsRetryEntirely covers
// requirement 8: ctx cancelled BEFORE the backoff wait even begins
// (synchronously, inside the handler, before the 502 response is even
// returned to the client) must still skip the retry — the select's
// ctx.Done() branch firing on an already-closed channel, not just an
// eventually-closed one.
func TestTranscribe_AlreadyCancelledContextSkipsRetryEntirely(t *testing.T) {
	var requestCount int32
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		cancel() // ctx is already done by the time Transcribe reaches its backoff select
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("bad gateway"))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.TranscriptionRetryBackoff = time.Hour

	start := time.Now()
	_, err := client.Transcribe(ctx, []byte("audio"), "note.ogg", TranscribeOptions{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("expected Transcribe to return almost immediately, took %s", elapsed)
	}
	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Errorf("expected exactly 1 request (no retry sent), got %d", got)
	}
}

// --- Response-content-leak fix: Transcribe (requirements 12, 13, 14) ---

// TestTranscribe_NonOKStatusDoesNotLeakBody covers requirement 12/13 for
// Transcribe: a non-200 body (plain text, a JSON error envelope, and a
// gzip-compressed variant of each) must never put its content — in
// particular error.message, which can echo request detail — into
// TranscriptionStatusError's Error() text, while error.type/error.code
// must still surface. Also confirms errors.As(err, *TranscriptionStatusError)
// still works through this (requirement 14) — Transcribe's own 502-retry
// logic and the voice pipeline's caller both depend on it.
func TestTranscribe_NonOKStatusDoesNotLeakBody(t *testing.T) {
	t.Run("plain text body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(leakSecret))
		}))
		defer server.Close()

		client := NewCostguardClient(server.URL)
		_, err := client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		var statusErr *TranscriptionStatusError
		if !errors.As(err, &statusErr) {
			t.Fatalf("expected errors.As(err, *TranscriptionStatusError), got: %v", err)
		}
		if statusErr.StatusCode != http.StatusBadRequest {
			t.Errorf("expected StatusCode %d, got %d", http.StatusBadRequest, statusErr.StatusCode)
		}
		if strings.Contains(err.Error(), leakSecret) {
			t.Errorf("expected the plain-text body never to appear in the error, got: %v", err)
		}
	})

	t.Run("JSON error envelope: message never leaks, type/code do", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": leakSecret,
					"type":    "invalid_request_error",
					"code":    "bad_audio",
				},
			})
		}))
		defer server.Close()

		client := NewCostguardClient(server.URL)
		_, err := client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if strings.Contains(err.Error(), leakSecret) {
			t.Errorf("expected error.message never to appear in the error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "invalid_request_error") {
			t.Errorf("expected error.type to appear in the error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "bad_audio") {
			t.Errorf("expected error.code to appear in the error, got: %v", err)
		}
	})

	t.Run("gzip-compressed JSON error envelope", func(t *testing.T) {
		body, err := json.Marshal(map[string]any{
			"error": map[string]any{"message": leakSecret, "type": "invalid_request_error"},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		compressed := gzipCompress(t, body)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(compressed)
		}))
		defer server.Close()

		client := NewCostguardClient(server.URL)
		client.HTTPClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}

		_, err = client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if strings.Contains(err.Error(), leakSecret) {
			t.Errorf("expected the decompressed error.message never to appear in the error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "invalid_request_error") {
			t.Errorf("expected error.type to appear in the error, got: %v", err)
		}
	})
}

// TestTranscribe_InvalidJSONOn200DoesNotLeakBody covers the exact bug
// this fix closes in Transcribe specifically: a 200 response whose body
// isn't valid JSON (e.g. Costguard/Speaches itself glitching and
// returning something unparseable) used to append a 500-char raw excerpt
// of that body — which, for a 200 response specifically, IS the
// transcript text — straight into the parse-error message via
// "(body: %s)". It must not anymore.
func TestTranscribe_InvalidJSONOn200DoesNotLeakBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(leakSecret)) // not valid JSON
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Transcribe(context.Background(), []byte("audio"), "note.ogg", TranscribeOptions{})
	if err == nil {
		t.Fatal("expected a parse error, got nil")
	}
	if strings.Contains(err.Error(), leakSecret) {
		t.Errorf("expected the unparseable 200 body never to appear in the error, got: %v", err)
	}
}
