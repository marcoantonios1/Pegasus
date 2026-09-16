package extraction

import (
	"context"
	"fmt"
	"strings"
)

// TriageModel is llama3.2:3b per proposal §5's Pass 1 — a small, fast
// model run against EVERY message, distinct from Model (qwen3-coder:30b),
// which only runs against the ~10-20% of volume Pass 1 flags as
// candidates. Using a much cheaper model at full volume, and reserving
// the expensive one for a filtered subset, is the entire point of the
// two-pass design — Triager must not substitute Model here.
const TriageModel = "llama3.2:3b"

// Triager implements §5's Pass 1: a coarse, fast noise-vs-candidate
// classification over a single message — NOT full extraction, and
// deliberately not window-based like Extractor.ExtractWindow. This
// package's own prompt-building (BuildPrompt) is Window-shaped and
// embeds the full closed predicate vocabulary — appropriate for Pass 2's
// actual extraction, wrong for Pass 1, which needs to stay minimal and
// fast at full message volume rather than paying the vocabulary's prompt
// size on every single message. Triager therefore builds its own,
// separate, minimal prompt rather than reusing BuildPrompt.
type Triager struct {
	Client *CostguardClient
}

func NewTriager(client *CostguardClient) *Triager {
	return &Triager{Client: client}
}

const triagePromptTemplate = `You are a fast, coarse filter, not a careful reader. Decide whether the message below might contain a factual, event, or emotional signal worth extracting later, or whether it's just noise (greetings, acknowledgements, small talk with no content, emoji-only reactions, etc.).

Respond with exactly one word: CANDIDATE or NOISE. No punctuation, no explanation.

Message: %s`

// IsCandidate reports whether text is plausibly worth running through
// full extraction (Pass 2) — vs. pure noise. A response this package
// can't confidently parse as CANDIDATE defaults to false (NOT a
// candidate): triage exists to cut Pass 2's call volume down to the
// ~10-20% §5 describes, so failing toward the cheaper path on an
// ambiguous response preserves that cost discipline. A message wrongly
// skipped here isn't lost forever either — if the same fact matters, it
// typically gets restated and re-triaged, or Reinforce()'s existing
// mechanism (decay issue) picks it up on a later mention; failing the
// other way (defaulting every unparseable response to "candidate") would
// undermine triage's entire purpose.
func (t *Triager) IsCandidate(ctx context.Context, text string) (bool, error) {
	prompt := fmt.Sprintf(triagePromptTemplate, text)

	resp, err := t.Client.CompleteWithModel(ctx, TriageModel, prompt)
	if err != nil {
		return false, fmt.Errorf("triage: %w", err)
	}

	return parseTriageResponse(resp), nil
}

func parseTriageResponse(resp string) bool {
	trimmed := strings.ToUpper(strings.TrimSpace(resp))
	return trimmed == "CANDIDATE" || strings.HasPrefix(trimmed, "CANDIDATE")
}

const triageWindowPromptTemplate = `You are a fast, coarse filter, not a careful reader. Below is a short block of conversation messages. Decide whether ANY of them might contain a factual, event, or emotional signal worth extracting later, or whether the whole block is just noise (greetings, acknowledgements, small talk with no content, emoji-only reactions, etc.).

Respond with exactly one word: CANDIDATE or NOISE. No punctuation, no explanation.

Messages:
%s`

// IsWindowCandidate is IsCandidate generalized to a whole window (proposal
// §5's "cost discipline for historical import" note): ONE triage call
// covers every message in w, rather than one call per message. This is
// the batched call pattern historical/bulk import needs — llama3.2:3b
// triage at years-of-backlog volume needs real throughput, and triaging
// message-by-message the same way the live path does would multiply call
// count by the average window size for no benefit bulk import can use
// (unlike live traffic, a historical window's messages are all already
// available at once, so there's no reason to triage them one at a time).
// The live path (ProcessLiveMessage) uses IsCandidate instead, since a
// live message needs a verdict before it's even known which window it
// will end up in.
func (t *Triager) IsWindowCandidate(ctx context.Context, w Window) (bool, error) {
	var b strings.Builder
	for _, m := range w.Messages {
		fmt.Fprintf(&b, "%s: %s\n", m.Speaker, m.Text)
	}

	prompt := fmt.Sprintf(triageWindowPromptTemplate, b.String())

	resp, err := t.Client.CompleteWithModel(ctx, TriageModel, prompt)
	if err != nil {
		return false, fmt.Errorf("triage window: %w", err)
	}

	return parseTriageResponse(resp), nil
}
