package main

import (
	"time"

	"github.com/marcoantonios1/Pegasus/internal/api"
)

// RunResult is the full raw output of one `run` execution — everything
// the `report` subcommand needs, all in one JSON file, so the expensive
// phase (real Costguard calls, real DB writes) and the formatting phase
// can be iterated on independently. Same split as cmd/whisper_spotcheck's
// transcribe/report.
type RunResult struct {
	ExportPath string
	StartedAt  time.Time
	FinishedAt time.Time

	// Import is *api.ImportResult exactly as ImportWhatsApp returned it —
	// not a re-shaped or summarized version. See NOTES.md for why
	// ImportWhatsApp's return type was extended to carry this (a real,
	// non-dry-run-specific enhancement, not a special-cased path).
	Import *api.ImportResult

	// RejectedTripleLogLines are the exact lines
	// extraction.ValidateTriples logged for triples that failed the
	// closed-vocabulary check, captured via *extraction.Extractor's own
	// Logger hook — not re-parsed or re-derived, the real rejection
	// messages verbatim.
	RejectedTripleLogLines []string

	// Usage is Costguard's own per-model/per-endpoint token and cost
	// data for this run's time window and agent tag, queried directly
	// from Costguard's usage_records table. See NOTES.md for why this
	// isn't fetched through Costguard's HTTP admin API.
	Usage []UsageRow

	// UsageWindowStart/End are Usage's actual query bounds — pulled back
	// slightly from StartedAt/FinishedAt to account for clock skew
	// between this tool's process and Costguard's own timestamps; see
	// run.go's usage query call for the exact margin.
	UsageWindowStart time.Time
	UsageWindowEnd   time.Time
}

// UsageRow is one (model, path) group's aggregated usage from Costguard's
// usage_records table — see queryCostguardUsage in run.go.
type UsageRow struct {
	Model            string
	Path             string
	RequestCount     int
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	EstimatedCostUSD float64
	// PriceFoundCount is how many of RequestCount had Costguard's own
	// price_found=true — i.e. it actually had a configured price to
	// compute EstimatedCostUSD from, vs. defaulting to 0 because it had
	// no price data at all for that model. PriceFoundCount < RequestCount
	// means EstimatedCostUSD is an UNDERCOUNT, not a confirmed number —
	// see queryCostguardUsage's own doc comment. Real, not hypothetical:
	// this dry-run's own local Ollama models (llama3.2:3b, qwen3-coder:30b)
	// came back price_found=false on every row.
	PriceFoundCount int
}
