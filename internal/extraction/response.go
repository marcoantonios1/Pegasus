package extraction

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DefaultMaxResponseBytes bounds CompleteWithModel/Embed response
// bodies (raw wire bytes, and separately the decompressed output if the
// body is gzip — see readResponseBody). 4 MiB is a first guess, chosen
// to be far above any legitimate chat-completion or embedding response
// (Costguard's JSON envelopes around a model's text output or a single
// embedding vector) and far below memory-exhaustion territory — not
// tuned against real response sizes.
const DefaultMaxResponseBytes = 4 << 20 // 4 MiB

// DefaultMaxTranscriptionResponseBytes is Transcribe's own, larger
// limit: a verbose_json transcription response carries a segments array
// (one entry per few seconds of audio — see TranscriptionSegment) that
// grows with recording length, so a long voice note's response is
// legitimately bigger than a chat/embedding response. 16 MiB is a first
// guess for the same reason DefaultMaxResponseBytes is — comfortably
// above any real transcription response, comfortably below
// memory-exhaustion territory.
const DefaultMaxTranscriptionResponseBytes = 16 << 20 // 16 MiB

// ErrResponseTooLarge is returned (always wrapped, via %w, with the
// status code and limit that triggered it — never any response content)
// when a Costguard response exceeds the caller's configured size limit,
// either as raw wire bytes or, for a gzip-compressed body, as
// decompressed output. Exists as a sentinel so a caller can tell "this
// response was simply too big" apart from a genuine transport/parse
// failure via errors.Is, without parsing error text.
var ErrResponseTooLarge = errors.New("costguard response exceeds the configured size limit")

func (c *CostguardClient) maxResponseBytes() int64 {
	if c.MaxResponseBytes > 0 {
		return c.MaxResponseBytes
	}
	return DefaultMaxResponseBytes
}

func (c *CostguardClient) maxTranscriptionResponseBytes() int64 {
	if c.MaxTranscriptionResponseBytes > 0 {
		return c.MaxTranscriptionResponseBytes
	}
	return DefaultMaxTranscriptionResponseBytes
}

// readResponseBody is the one shared bounded-read path CompleteWithModel,
// Embed, and Transcribe all route through (previously each called
// io.ReadAll directly, with no limit at all, and only CompleteWithModel
// additionally ran the result through gzip decompression). It:
//
//   - fails fast, before reading anything, if resp declares a
//     Content-Length greater than limit — resp.ContentLength is -1 when
//     unknown/chunked, which is never greater than any non-negative
//     limit, so this only ever fires on a genuine, truthful declaration;
//   - otherwise reads at most limit+1 raw bytes (never more, regardless
//     of how much the server is still sending — see readAtMost's own
//     doc comment), returning ErrResponseTooLarge if that's not enough
//     room to hold the whole body;
//   - decompresses through maybeDecompressGzip (header OR magic-byte
//     sniff, exactly as before this fix), applying the SAME limit to
//     the decompressed output — a small compressed body that expands
//     past limit once decompressed (a "gzip bomb") is rejected the same
//     way, not silently allocated in full first.
//
// Every error this returns wraps ErrResponseTooLarge, a plain read
// error, or a gzip error — NEVER any response content; see
// summarizeResponse for the separate, deliberately content-free
// description used when a non-200 status or an unparseable 200 needs to
// go into an error message.
func readResponseBody(resp *http.Response, limit int64) ([]byte, error) {
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("%w (status %d, declared content-length %d bytes, limit %d bytes)", ErrResponseTooLarge, resp.StatusCode, resp.ContentLength, limit)
	}

	raw, err := readAtMost(resp.Body, limit)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, fmt.Errorf("%w (status %d, limit %d bytes)", ErrResponseTooLarge, resp.StatusCode, limit)
		}
		return nil, fmt.Errorf("read costguard response: %w", err)
	}

	decoded, err := maybeDecompressGzip(raw, resp.Header.Get("Content-Encoding"), limit)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, fmt.Errorf("%w (status %d, limit %d bytes, decompressed)", ErrResponseTooLarge, resp.StatusCode, limit)
		}
		return nil, fmt.Errorf("decompress costguard response: %w", err)
	}

	return decoded, nil
}

// readAtMost reads at most limit+1 bytes from r, returning the bare
// ErrResponseTooLarge sentinel (no status/limit text — callers with
// that context wrap it themselves) if more than limit bytes turned out
// to be available.
//
// The +1 (rather than reading exactly limit bytes) is what makes
// "limit bytes succeeds, limit+1 fails" exact rather than fuzzy: reading
// only `limit` bytes could never distinguish a body of EXACTLY limit
// bytes (should succeed) from one of limit+1 or more (should fail) — both
// would just fill the buffer identically. Reading limit+1 bytes answers
// that question directly: if the (limit+1)-th byte is never produced
// (io.LimitReader reports EOF at exactly limit bytes), the body was
// limit bytes or fewer; if it IS produced, there was more than limit.
//
// Reads via io.LimitReader specifically so a server that keeps streaming
// indefinitely (a misbehaving or malicious one) can't block this call or
// grow its memory past limit+1: io.ReadAll stops the moment the limited
// reader reports EOF, which happens as soon as limit+1 bytes have been
// read, regardless of how much more the underlying connection has
// buffered or is still sending — this never blocks waiting for the
// server to finish, and never allocates toward whatever the server
// claims or actually sends beyond that point.
func readAtMost(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, ErrResponseTooLarge
	}
	return body, nil
}

// gzipMagic is gzip's two-byte magic number (RFC 1952 §2.3.1). Used as a
// fallback signal alongside the Content-Encoding header — see
// maybeDecompressGzip's own doc comment for why a header-only check isn't
// enough here.
var gzipMagic = []byte{0x1f, 0x8b}

// maybeDecompressGzip returns body decompressed if it's gzip, or body
// unchanged otherwise. "Is it gzip" is decided two ways, not just one:
//
//   - contentEncoding == "gzip" (case-insensitive) — the standard,
//     correct HTTP signal, and the one a response SHOULD carry.
//   - body's own first two bytes are gzip's magic number (0x1f 0x8b),
//     regardless of what Content-Encoding said (including empty/absent).
//
// The second check exists because of what the original gzip-handling fix
// found, not as speculative defense: a real failure (internal/extraction/
// client.go's CompleteWithModel choking on a gzip-magic-prefixed byte via
// encoding/json's "invalid character '\x1f'") could NOT be reproduced
// live against a real Costguard instance across several direct curl
// tests (both models, small and large responses, with and without an
// explicit Accept-Encoding: gzip) — every one came back with NO
// Content-Encoding header and plain JSON. Costguard's own
// internal/providers/openaicompat package already relies on Go's
// standard transparent gzip decompression when ITS OWN client talks to
// an upstream provider (see its TestDo_GzippedUpstreamBody_MeteredCorrectly),
// and its cache-entry cloning (internal/gateway/response.go's
// cloneHeader) explicitly, deliberately excludes Content-Encoding when
// copying headers — "cached bodies are always stored decoded... replaying
// a Content-Encoding header would be wrong" — which is Costguard's own
// maintainers already treating this exact header/body mismatch as a real
// hazard elsewhere in that codebase, not a hypothetical one here. Given
// the failure DID happen once against real traffic with the unmistakable
// gzip magic byte, could not be reproduced deterministically, and
// Costguard's own code shows awareness of body/header decode mismatches
// as a live concern (caching/retry/streaming paths not audited here — out
// of scope for a Pegasus-side fix) — the most defensible read is an
// intermittent condition where compressed bytes reach this client
// without a Content-Encoding header to announce them. A header-only check
// (as a literal, minimal fix would do) would silently NOT fix that exact
// case. Sniffing the actual bytes closes it regardless of which
// Costguard-side path produced it.
//
// limit bounds the DECOMPRESSED output (via readAtMost) — the
// unbounded-reads fix's own closing of the "gzip bomb" risk a tiny
// compressed body expanding to an enormous decompressed one: this
// package used to run gzip.NewReader through a bare io.ReadAll with no
// limit at all. Unaffected for a plain (non-gzip) body, which this
// function returns unchanged without ever touching limit.
func maybeDecompressGzip(body []byte, contentEncoding string, limit int64) ([]byte, error) {
	isGzipEncoding := strings.EqualFold(strings.TrimSpace(contentEncoding), "gzip")
	isGzipMagic := len(body) >= 2 && body[0] == gzipMagic[0] && body[1] == gzipMagic[1]
	if !isGzipEncoding && !isGzipMagic {
		return body, nil
	}

	r, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("open gzip reader: %w", err)
	}
	defer r.Close()

	decoded, err := readAtMost(r, limit)
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, ErrResponseTooLarge
		}
		return nil, fmt.Errorf("decompress gzip body: %w", err)
	}
	return decoded, nil
}

// errorEnvelope is the standard OpenAI-compatible error shape
// ({"error": {"message": ..., "type": ..., "code": ...}}) Costguard's
// own gateway and every upstream provider it fronts use. Only Type and
// Code are ever read out of it anywhere in this package — see
// summarizeResponse's own doc comment for why Message never is.
type errorEnvelope struct {
	Error struct {
		Type string `json:"type"`
		Code string `json:"code"`
	} `json:"error"`
}

// responseSummary is a deliberately content-free description of a
// Costguard HTTP response, safe to put in an error message or a log
// line — the fix for this package's own response bodies (which can echo
// real prompt/message/transcript text) leaking into exactly those
// places. See String's own doc comment for the exact rendered shape.
type responseSummary struct {
	StatusCode      int
	ContentType     string
	ContentEncoding string
	BodyLength      int
	// BodyHashPrefix is the first 8 hex characters of sha256(body) —
	// included ONLY so repeated failures with the identical body can be
	// correlated across log lines/error reports without needing the
	// body itself. NOT a security boundary: a low-entropy or
	// previously-seen body's hash is trivially guessable/matchable by
	// anyone who also has that body, the same as any other hash of
	// knowable content — this is a correlation aid, not a proof of
	// anything secret.
	BodyHashPrefix string
	// ErrorType/ErrorCode are error.type/error.code from the standard
	// {"error": {...}} envelope, when the body parses as one — each
	// capped at 64 runes with control characters stripped
	// (sanitizeErrorField). error.message is NEVER surfaced here or
	// anywhere else in this summary: providers routinely put
	// human-readable detail in it that echoes back request content
	// (e.g. "invalid value for 'language': <excerpt>"), which is exactly
	// the leak this type exists to prevent — type/code are short,
	// provider-defined machine tokens (e.g. "invalid_request_error",
	// "rate_limit_exceeded"), not free text, and don't carry that risk.
	ErrorType string
	ErrorCode string
}

// summarizeResponse builds a responseSummary for resp/body — resp for
// its status and headers, body for its length, hash, and (if parseable)
// error.type/error.code. body is assumed already decompressed (i.e.
// what readResponseBody returned), so BodyLength and BodyHashPrefix
// describe the actual content, not the wire-compressed bytes.
func summarizeResponse(resp *http.Response, body []byte) responseSummary {
	sum := sha256.Sum256(body)

	s := responseSummary{
		StatusCode:      resp.StatusCode,
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		BodyLength:      len(body),
		BodyHashPrefix:  hex.EncodeToString(sum[:4]),
	}

	var env errorEnvelope
	if json.Unmarshal(body, &env) == nil {
		s.ErrorType = sanitizeErrorField(env.Error.Type)
		s.ErrorCode = sanitizeErrorField(env.Error.Code)
	}

	return s
}

// sanitizeErrorField strips ASCII control characters (including DEL)
// and caps s at 64 runes — defense for a field this package otherwise
// trusts Costguard/the upstream provider to keep short and plain, in
// case either is ever compromised, buggy, or fronting a provider that
// doesn't.
func sanitizeErrorField(s string) string {
	var filtered []rune
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			continue
		}
		filtered = append(filtered, r)
	}
	if len(filtered) > 64 {
		filtered = filtered[:64]
	}
	return string(filtered)
}

// String renders the summary as the one line every non-200/unparseable-
// response error in this package is built from: status, Content-Type,
// Content-Encoding, body length, and the hash prefix always; error_type/
// error_code only when the body parsed as the standard envelope and had
// them. Deliberately excludes the thing every one of those sites used to
// include — the body itself.
func (s responseSummary) String() string {
	msg := fmt.Sprintf("status=%d content-type=%q content-encoding=%q length=%d sha256=%s",
		s.StatusCode, s.ContentType, s.ContentEncoding, s.BodyLength, s.BodyHashPrefix)
	if s.ErrorType != "" {
		msg += fmt.Sprintf(" error_type=%q", s.ErrorType)
	}
	if s.ErrorCode != "" {
		msg += fmt.Sprintf(" error_code=%q", s.ErrorCode)
	}
	return msg
}
