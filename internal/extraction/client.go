package extraction

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Model is qwen3-coder:30b per proposal §5 — coder-tuned models hold
// strict JSON schema adherence better than general chat models, which
// matters more here than raw reasoning quality.
const Model = "qwen3-coder:30b"

// CostguardClient calls Costguard's OpenAI-compatible gateway
// (/v1/chat/completions). Per proposal §1/§2, Pegasus never talks to a
// model provider directly — every model call is routed through Costguard,
// which owns retries, fallback, budget, and logging.
type CostguardClient struct {
	BaseURL string // e.g. "http://localhost:8080"

	// Agent is sent as X-Costguard-Agent, Costguard's existing per-caller
	// attribution header (see its budget.agents config) — lets Costguard's
	// usage/budget tracking distinguish Pegasus's extraction calls from
	// other agents calling through the same gateway.
	Agent string

	HTTPClient *http.Client

	// TranscriptionRetryBackoff overrides Transcribe's one-retry-on-502
	// backoff (see DefaultTranscriptionRetryBackoff's own doc comment for
	// why the real default is 30s, not a short guess). Zero uses the
	// default — a field rather than a package const specifically so tests
	// can inject a short backoff instead of a real test run needing to
	// wait out 30s per retry case.
	TranscriptionRetryBackoff time.Duration
}

func NewCostguardClient(baseURL string) *CostguardClient {
	return &CostguardClient{
		BaseURL:    baseURL,
		Agent:      "pegasus",
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

type chatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

// EmbeddingModel is nomic-embed-text — the model memory.Embedding.Vector's
// own doc comment already names as producing its 768-dim vectors. Query
// text must be embedded with this same model for cosine similarity
// against existing embeddings rows to mean anything.
const EmbeddingModel = "nomic-embed-text"

type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed calls Costguard's OpenAI-compatible embeddings endpoint
// (/v1/embeddings) for EmbeddingModel and returns the resulting vector.
//
// This method did not exist before internal/api's read methods (proposal
// §11/§13) needed it. Despite that issue's expectation of an existing
// embedding call site to reuse, there wasn't one: nothing in this
// codebase — not extraction, not bulkimport, not ingestion — has ever
// called an embeddings endpoint; EmbeddingStore.Create (internal/memory)
// has existed with no production call site at all. This is genuinely new,
// necessary infrastructure (GetRelevantContext and SearchSemantic both
// need to embed a query string before the embeddings table's HNSW index
// is usable for anything), not a second, competing implementation of
// something that already worked — mirrors the same "check first, then
// build the real thing since none existed" situation the relationship-
// stats issue hit with MessageStore's missing query methods.
//
// Separately, and out of scope for this to fix: nothing in this codebase
// calls EmbeddingStore.Create in production either, so the embeddings
// table is likely empty in any real deployment today — the same
// "message-persistence isn't wired up yet" gap bulkimport.go's own doc
// comment already flags for messages/edges. GetRelevantContext and
// SearchSemantic are built against the real schema and will work
// correctly once that ingestion-side gap is closed; they can't produce
// meaningful results before it is.
func (c *CostguardClient) Embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embeddingRequest{Model: EmbeddingModel, Input: text}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Agent != "" {
		req.Header.Set("X-Costguard-Agent", c.Agent)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call costguard: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read costguard response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("costguard returned status %d: %s", resp.StatusCode, respBody)
	}

	var parsed embeddingResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, fmt.Errorf("parse costguard embedding response: %w", err)
	}
	if len(parsed.Data) == 0 {
		return nil, fmt.Errorf("costguard embedding response had no data")
	}

	return parsed.Data[0].Embedding, nil
}

// Complete sends prompt as a single user message to Model via Costguard's
// gateway and returns the model's raw text response. Temperature is low
// but non-zero: this is structured extraction, not creative generation, so
// deterministic-leaning output is what strict JSON adherence needs.
//
// A thin wrapper over CompleteWithModel pinned to Model (qwen3-coder:30b)
// — every existing call site (Extractor.ExtractWindow) keeps working
// unchanged. CompleteWithModel itself exists for Triager (triage.go),
// which needs a second, distinct model (llama3.2:3b, §5's Pass 1) through
// this same Costguard gateway.
func (c *CostguardClient) Complete(ctx context.Context, prompt string) (string, error) {
	return c.CompleteWithModel(ctx, Model, prompt)
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
// The second check exists because of what this fix's own investigation
// found, not as speculative defense: this bug (internal/extraction/
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
func maybeDecompressGzip(body []byte, contentEncoding string) ([]byte, error) {
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

	decoded, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("decompress gzip body: %w", err)
	}
	return decoded, nil
}

// CompleteWithModel is Complete generalized to a caller-chosen model. See
// Complete's doc comment for why this exists as a separate method rather
// than adding a model parameter to Complete itself (signature stability
// for existing callers).
//
// Response decompression (maybeDecompressGzip) happens here, the one
// place both Complete (a thin wrapper) and Triager's CompleteWithModel
// calls converge on — not duplicated into a second call site. Applied
// BEFORE the status-code check so a gzip-compressed ERROR body (not just
// a 200) still produces a readable message rather than raw bytes in the
// error string. Embed (/v1/embeddings) and Transcribe (/v1/audio/
// transcriptions) are deliberately NOT touched here — this fix is scoped
// to the endpoint and failure this issue actually investigated
// (/v1/chat/completions); Embed's own quick check during this
// investigation was inconclusive (a connection failure unrelated to
// gzip, not a confirmed clean non-gzip response), so extending this fix
// there would be an unverified assumption, not a confirmed finding.
func (c *CostguardClient) CompleteWithModel(ctx context.Context, model, prompt string) (string, error) {
	reqBody := chatCompletionRequest{
		Model:       model,
		Messages:    []chatMessage{{Role: "user", Content: prompt}},
		Temperature: 0.1,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal chat completion request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build chat completion request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Agent != "" {
		req.Header.Set("X-Costguard-Agent", c.Agent)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("call costguard: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read costguard response: %w", err)
	}
	respBody, err = maybeDecompressGzip(respBody, resp.Header.Get("Content-Encoding"))
	if err != nil {
		return "", fmt.Errorf("decompress costguard response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("costguard returned status %d: %s", resp.StatusCode, respBody)
	}

	var parsed chatCompletionResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("parse costguard response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("costguard response had no choices")
	}

	return parsed.Choices[0].Message.Content, nil
}
