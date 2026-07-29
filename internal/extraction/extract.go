package extraction

import (
	"context"
	"fmt"
)

// Extractor runs the extraction pipeline (§8) over a single window: build
// the prompt from the closed vocabulary, call Model via Costguard, parse
// and validate the response. It does not decide when a window is "ready"
// (that's WindowByCount / LiveWindower's job) and does not resolve triples
// into edges (that's downstream, out of scope here).
type Extractor struct {
	Client *CostguardClient

	// Logger receives one line per rejected triple (see ValidateTriples).
	// Defaults to log.Printf via ValidateTriples if nil.
	Logger Logger
}

func NewExtractor(client *CostguardClient) *Extractor {
	return &Extractor{Client: client}
}

// ExtractWindow runs the full pipeline for one window and returns the
// triples that survived validation against the closed predicate
// vocabulary.
func (e *Extractor) ExtractWindow(ctx context.Context, w Window) ([]ExtractedTriple, error) {
	prompt, err := BuildPrompt(w, Predicates)
	if err != nil {
		return nil, err
	}

	raw, err := e.Client.Complete(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("extraction call: %w", err)
	}

	parsed, err := ParseModelOutput(raw)
	if err != nil {
		return nil, fmt.Errorf("extraction output: %w", err)
	}

	return ValidateTriples(parsed, e.Logger), nil
}
