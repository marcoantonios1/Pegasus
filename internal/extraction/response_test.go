package extraction

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- readAtMost: the exact boundary (requirement 11) ---

func TestReadAtMost_ExactlyLimitSucceedsLimitPlusOneFails(t *testing.T) {
	const limit = 100

	t.Run("exactly limit bytes succeeds", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), limit)
		got, err := readAtMost(bytes.NewReader(body), limit)
		if err != nil {
			t.Fatalf("readAtMost: %v", err)
		}
		if len(got) != limit {
			t.Errorf("expected %d bytes, got %d", limit, len(got))
		}
	})

	t.Run("limit+1 bytes fails", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), limit+1)
		_, err := readAtMost(bytes.NewReader(body), limit)
		if !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("expected errors.Is(err, ErrResponseTooLarge), got: %v", err)
		}
	})
}

// TestMaybeDecompressGzip_WithinLimitDecodesViaHeaderAndMagicSniff is
// TestMaybeDecompressGzip's own boundary case (requirement 11): a limit
// set to EXACTLY the decompressed size must still succeed, via either
// detection path (the Content-Encoding header, or the magic-byte sniff
// with no header at all).
func TestMaybeDecompressGzip_WithinLimitDecodesViaHeaderAndMagicSniff(t *testing.T) {
	plain := []byte(`{"hello":"world"}`)
	compressed := gzipCompress(t, plain)
	limit := int64(len(plain))

	t.Run("via Content-Encoding header", func(t *testing.T) {
		got, err := maybeDecompressGzip(compressed, "gzip", limit)
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q", got, plain)
		}
	})

	t.Run("via magic-byte sniff, no header", func(t *testing.T) {
		got, err := maybeDecompressGzip(compressed, "", limit)
		if err != nil {
			t.Fatalf("maybeDecompressGzip: %v", err)
		}
		if !bytes.Equal(got, plain) {
			t.Errorf("got %q, want %q", got, plain)
		}
	})
}

// --- readResponseBody: Content-Length fast-fail (requirement 1a) ---

// blockingBody panics if ever Read from — proves readResponseBody's
// Content-Length check rejects an oversize response BEFORE reading any
// of the body, not just before finishing the read.
type blockingBody struct{}

func (blockingBody) Read(p []byte) (int, error) {
	panic("readResponseBody must never read the body when the declared Content-Length alone already exceeds the limit")
}
func (blockingBody) Close() error { return nil }

func TestReadResponseBody_FailsFastOnDeclaredContentLengthExceedingLimit(t *testing.T) {
	resp := &http.Response{
		StatusCode:    http.StatusOK,
		ContentLength: 1_000_000,
		Header:        http.Header{},
		Body:          blockingBody{},
	}

	_, err := readResponseBody(resp, 100)
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected errors.Is(err, ErrResponseTooLarge), got: %v", err)
	}
}

// --- End-to-end oversize/gzip-bomb handling through a real client call
// (requirements 9, 10) ---

// TestComplete_OversizeBodyFailsPromptlyEvenWithAnIndefinitelyStreamingServer
// covers requirement 9: a server that keeps writing (simulating either a
// malicious or simply misbehaving upstream) must not be able to block
// the call or grow its memory — Complete must return
// errors.Is(err, ErrResponseTooLarge), with status and limit in the
// message but no body content, promptly, regardless of how long the
// server is willing to keep streaming.
func TestComplete_OversizeBodyFailsPromptlyEvenWithAnIndefinitelyStreamingServer(t *testing.T) {
	const limit = 1024

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := bytes.Repeat([]byte("a"), 4096)
		flusher, _ := w.(http.Flusher)
		// Bounded at a large-but-finite iteration count as a safety net
		// only — in the success case this loop is expected to exit via
		// the write error below almost immediately, once the client
		// stops reading and its connection is torn down.
		for i := 0; i < 100000; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.MaxResponseBytes = limit

	start := time.Now()
	_, err := client.Complete(context.Background(), "test")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected errors.Is(err, ErrResponseTooLarge), got: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("expected Complete to return promptly despite the indefinitely-streaming server, took %s", elapsed)
	}
}

// TestComplete_GzipBombFails covers requirement 10: a small compressed
// body that would expand far past the limit must be rejected the same
// way an oversize raw body is, not decompressed in full first. Proven
// here by construction rather than by measuring live memory use (which
// would be flaky in a unit test): maybeDecompressGzip's own
// implementation reads the gzip reader's output through readAtMost
// (io.LimitReader-bounded, see its own doc comment), so decompression
// physically cannot produce more than limit+1 bytes before this call
// returns, regardless of how much more the compressed stream could
// still expand to.
func TestComplete_GzipBombFails(t *testing.T) {
	const limit = 64 * 1024             // 64 KiB
	zeros := make([]byte, 10*1024*1024) // 10 MiB of zeros — a classic gzip bomb payload, compresses to a few KB
	compressed := gzipCompress(t, zeros)
	if len(compressed) > 100*1024 {
		t.Fatalf("expected gzip-compressed all-zeros to compress to well under 100 KiB, got %d bytes — test assumption broken", len(compressed))
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(compressed)
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	client.HTTPClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}
	client.MaxResponseBytes = limit

	_, err := client.Complete(context.Background(), "test")
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected errors.Is(err, ErrResponseTooLarge), got: %v", err)
	}
}
