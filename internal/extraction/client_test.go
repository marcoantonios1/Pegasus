package extraction

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCostguardClient_Complete(t *testing.T) {
	var gotReq chatCompletionRequest
	var gotAgentHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		gotAgentHeader = r.Header.Get("X-Costguard-Agent")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: `[{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98}]`}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	out, err := client.Complete(context.Background(), "extract from this conversation")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if gotReq.Model != Model {
		t.Errorf("expected model %q, got %q", Model, gotReq.Model)
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" || gotReq.Messages[0].Content != "extract from this conversation" {
		t.Errorf("unexpected request messages: %+v", gotReq.Messages)
	}
	if gotAgentHeader != "pegasus" {
		t.Errorf("expected X-Costguard-Agent: pegasus, got %q", gotAgentHeader)
	}
	if out != `[{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98}]` {
		t.Errorf("unexpected output: %q", out)
	}
}

func TestCostguardClient_Complete_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("upstream provider unavailable"))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected an error for a non-200 response, got nil")
	}
}

func TestCostguardClient_Complete_NoChoices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(chatCompletionResponse{})
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected an error when the response has no choices, got nil")
	}
}

// --- Gzip response handling (fix-gzip-response-handling) ---
//
// See maybeDecompressGzip's own doc comment for the investigation these
// tests are grounded in: could not reproduce a real gzip-Content-Encoding
// response against a live Costguard instance, but Costguard's own code
// shows awareness of Content-Encoding/body mismatches as a real hazard
// elsewhere (its cache-entry cloning explicitly strips it). These tests
// cover both signals the fix checks — the header, and the body's own
// magic bytes when the header is silent — plus the uncompressed case,
// since a decompression path that isn't also verified NOT to break plain
// responses is a regression risk of its own (see requirement 7's own
// framing for why both cases are required, not just the compressed one).

func gzipCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// TestCostguardClient_CompleteWithModel_GzipContentEncoding covers
// requirement 7's compressed case via the standard HTTP signal: a
// response with Content-Encoding: gzip and a real gzip-compressed body.
// The test client disables Go's OWN transparent gzip handling
// (Transport.DisableCompression: true) specifically so this test
// exercises maybeDecompressGzip itself, not stdlib's automatic
// decompression doing the work invisibly before our code ever runs —
// that automatic path is exactly what this investigation confirmed
// already works for the textbook case (Content-Encoding present, request
// didn't set its own Accept-Encoding), and isn't what needed fixing.
func TestCostguardClient_CompleteWithModel_GzipContentEncoding(t *testing.T) {
	plain := mustMarshalChatResponse(t, "hello from gzip")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(gzipCompress(t, plain))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.HTTPClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}

	out, err := client.CompleteWithModel(context.Background(), "some-model", "test")
	if err != nil {
		t.Fatalf("CompleteWithModel: %v", err)
	}
	if out != "hello from gzip" {
		t.Errorf("expected decompressed content, got %q", out)
	}
}

// TestCostguardClient_CompleteWithModel_GzipMagicBytesNoContentEncodingHeader
// covers the scenario this investigation actually judged most likely to
// explain the real failure: the response body is genuinely gzip
// (real magic bytes, real compressed content) but Content-Encoding is
// absent — nothing announces it. A header-only check would silently miss
// this exact case (Go's own transport wouldn't decompress it either,
// since it never saw a Content-Encoding: gzip to react to). No special
// transport needed here: Go never attempts decompression without a
// Content-Encoding header to react to in the first place, compression
// setting or not.
func TestCostguardClient_CompleteWithModel_GzipMagicBytesNoContentEncodingHeader(t *testing.T) {
	plain := mustMarshalChatResponse(t, "hello from unlabeled gzip")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Deliberately NOT setting Content-Encoding — the case that
		// matters here.
		w.Write(gzipCompress(t, plain))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	out, err := client.CompleteWithModel(context.Background(), "some-model", "test")
	if err != nil {
		t.Fatalf("CompleteWithModel: %v", err)
	}
	if out != "hello from unlabeled gzip" {
		t.Errorf("expected decompressed content via magic-byte sniff, got %q", out)
	}
}

// TestCostguardClient_CompleteWithModel_UncompressedResponseStillWorks is
// requirement 7's required parallel case: an ordinary, uncompressed
// response (no Content-Encoding, no gzip magic bytes) must decode exactly
// as before — a decompression path that only gets exercised for the
// compressed case but silently mishandles the normal one would be its
// own regression.
func TestCostguardClient_CompleteWithModel_UncompressedResponseStillWorks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(mustMarshalChatResponse(t, "plain response"))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	out, err := client.CompleteWithModel(context.Background(), "some-model", "test")
	if err != nil {
		t.Fatalf("CompleteWithModel: %v", err)
	}
	if out != "plain response" {
		t.Errorf("expected plain content unchanged, got %q", out)
	}
}

// TestCostguardClient_CompleteWithModel_GzipErrorBodyStillReadable covers
// maybeDecompressGzip running BEFORE the status-code check: a
// gzip-compressed non-200 error body must still surface as a readable
// error message, not raw compressed bytes dumped into the error string.
func TestCostguardClient_CompleteWithModel_GzipErrorBodyStillReadable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusBadGateway)
		w.Write(gzipCompress(t, []byte("upstream provider unavailable")))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.HTTPClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}

	_, err := client.CompleteWithModel(context.Background(), "some-model", "test")
	if err == nil {
		t.Fatal("expected an error for a non-200 response, got nil")
	}
	if !strings.Contains(err.Error(), "upstream provider unavailable") {
		t.Errorf("expected the decompressed error body in the error message, got: %v", err)
	}
}

// TestMaybeDecompressGzip covers the helper directly, including the
// negative case (Content-Encoding claims gzip but the body isn't valid
// gzip — must error, not silently return garbage).
func TestMaybeDecompressGzip(t *testing.T) {
	plain := []byte(`{"hello":"world"}`)
	compressed := gzipCompress(t, plain)

	t.Run("header says gzip, valid gzip body", func(t *testing.T) {
		got, err := maybeDecompressGzip(compressed, "gzip")
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q", got, plain)
		}
	})

	t.Run("no header, magic bytes present", func(t *testing.T) {
		got, err := maybeDecompressGzip(compressed, "")
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q", got, plain)
		}
	})

	t.Run("no header, no magic bytes: passed through unchanged", func(t *testing.T) {
		got, err := maybeDecompressGzip(plain, "")
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q (should be unchanged)", got, plain)
		}
	})

	t.Run("header claims gzip but body is not valid gzip: errors", func(t *testing.T) {
		_, err := maybeDecompressGzip(plain, "gzip")
		if err == nil {
			t.Fatal("expected an error for a Content-Encoding: gzip response whose body isn't actually gzip")
		}
	})

	t.Run("Content-Encoding header value is case-insensitive", func(t *testing.T) {
		got, err := maybeDecompressGzip(compressed, "GZIP")
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q", got, plain)
		}
	})
}

func mustMarshalChatResponse(t *testing.T, content string) []byte {
	t.Helper()
	resp := chatCompletionResponse{
		Choices: []struct {
			Message chatMessage `json:"message"`
		}{
			{Message: chatMessage{Role: "assistant", Content: content}},
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal chat response: %v", err)
	}
	return b
}
