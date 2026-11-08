package extraction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

	// MaxResponseBytes overrides the response-size limit CompleteWithModel
	// and Embed enforce via readResponseBody (see DefaultMaxResponseBytes'
	// own doc comment for why 4 MiB). Zero uses the default — a field
	// rather than a package const specifically so tests can inject a tiny
	// limit instead of a real test needing to generate a multi-megabyte
	// body to exercise the oversize path.
	MaxResponseBytes int64

	// MaxTranscriptionResponseBytes is MaxResponseBytes' own counterpart
	// for Transcribe, which needs a larger limit (see
	// DefaultMaxTranscriptionResponseBytes' own doc comment for why).
	// Zero uses the default.
	MaxTranscriptionResponseBytes int64
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

	respBody, err := readResponseBody(resp, c.maxResponseBytes())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("costguard returned an error (%s)", summarizeResponse(resp, respBody))
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

// CompleteWithModel is Complete generalized to a caller-chosen model. See
// Complete's doc comment for why this exists as a separate method rather
// than adding a model parameter to Complete itself (signature stability
// for existing callers).
//
// Response reading (readResponseBody, which also runs maybeDecompressGzip)
// happens here, the one place both Complete (a thin wrapper) and
// Triager's CompleteWithModel calls converge on — not duplicated into a
// second call site. Decompression is applied BEFORE the status-code
// check so a gzip-compressed ERROR body (not just a 200) still produces
// a readable summary rather than raw/compressed bytes feeding the error.
//
// Embed and Transcribe route through this same readResponseBody/
// summarizeResponse infrastructure now too (the unbounded-reads and
// response-content-leak fix) — Embed and Transcribe therefore also gain
// gzip handling as a small, deliberate side effect: the original gzip
// fix had scoped them out as unverified (Embed's own quick check back
// then was inconclusive — a connection failure unrelated to gzip, not a
// confirmed clean non-gzip response), but the magic-byte sniff this
// shares with CompleteWithModel can't misfire on their JSON responses
// either, so there's no reason to keep them without it now that the
// bounded-read path is shared code anyway.
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

	respBody, err := readResponseBody(resp, c.maxResponseBytes())
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("costguard returned an error (%s)", summarizeResponse(resp, respBody))
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
