// Package bulkimport orchestrates running many exported conversations
// through ingest → window → extract in one call. It adds no new
// capability of its own — every adapter (WhatsApp, Instagram) and the
// extraction pipeline were already built to handle exactly one
// conversation per call, which is correct: a window must never mix
// messages from two different chats. What was missing was the loop that
// discovers multiple conversations and runs each through that existing,
// per-conversation pipeline. That loop is all this package is.
package bulkimport

import (
	"context"
	"fmt"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// WindowExtractor is the subset of *extraction.Extractor this package
// depends on, narrowed to an interface so tests can inject a fake instead
// of requiring a live Costguard/model call. *extraction.Extractor already
// satisfies this.
type WindowExtractor interface {
	ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error)
}

// TripleSink receives extracted triples one window at a time, so a caller
// can stream results (write to a queue, log, or — in a later issue,
// resolve triples to edges) without this package accumulating years of
// history in memory. Mirrors ingestion.Sink's shape. May be nil if the
// caller only cares about the returned Result's counts.
//
// Integration point (not yet wired, no call site exists for it today):
// whatever eventually turns an ExtractedTriple into a memory.Edge and
// calls EdgeStore.Create() MUST compute that edge's Confidence via
// memory.ApplySourceWeight(triple.Confidence, sourceType) first — per
// proposal §7.5, the stored Confidence column has to already be
// extracted_confidence * source_weight, not the raw model-reported value.
// This package doesn't do that resolution itself (entity resolution and
// edge writing are out of scope here, same as the earlier bulk-import and
// extraction issues), so there's deliberately no call to
// ApplySourceWeight in this file yet — this comment is that TODO.
type TripleSink func(conversationID string, window extraction.Window, triples []extraction.ExtractedTriple) error

// ConversationResult summarizes processing for one conversation.
type ConversationResult struct {
	ConversationID   string
	MessagesIngested int
	WindowsProcessed int
	TriplesExtracted int
}

// Result summarizes a bulk-import run across many conversations. A failure
// processing one conversation does not abort the rest of the batch — it's
// recorded in Errors, keyed by whatever identifies that conversation to
// the caller (a filename for WhatsApp, a file path for Instagram).
type Result struct {
	Conversations []ConversationResult
	Errors        map[string]error
}

func (r *Result) TotalMessages() int {
	n := 0
	for _, c := range r.Conversations {
		n += c.MessagesIngested
	}
	return n
}

func (r *Result) TotalTriples() int {
	n := 0
	for _, c := range r.Conversations {
		n += c.TriplesExtracted
	}
	return n
}

// processConversation runs one conversation's already-ingested RawMessage
// values through windowing and extraction. Shared by both platform entry
// points — everything from here down is platform-agnostic once RawMessage
// values exist, the same convergence point the ingestion adapters were
// already designed around.
//
// No triage (proposal §5's Pass 1, llama3.2:3b) runs here — triage hasn't
// been built in any issue yet. Without it, every window gets a full
// qwen3-coder:30b extraction call, not just the ~10-20% of volume triage
// would normally flag as candidates. Bulk-importing years of history
// through this as-is is significantly more expensive than the proposal's
// intended design — a known gap, not a silent omission.
func processConversation(ctx context.Context, conversationID string, raws []ingestion.RawMessage, extractor WindowExtractor, sink TripleSink) (ConversationResult, error) {
	var windowMsgs []extraction.WindowMessage
	for _, r := range raws {
		if r.MediaType != ingestion.MediaTypeText || r.Text == nil {
			// Voice/image entries carry no text at the RawMessage stage —
			// they only gain extractable text after transcription/vision
			// description (§8.4), a separate pipeline stage not run here.
			continue
		}
		windowMsgs = append(windowMsgs, extraction.WindowMessage{
			Speaker:   r.SenderExternalID,
			Timestamp: r.Timestamp,
			Text:      *r.Text,
		})
	}

	windows := extraction.WindowByCount(windowMsgs, extraction.DefaultHistoricalWindowSize)

	result := ConversationResult{
		ConversationID:   conversationID,
		MessagesIngested: len(windowMsgs),
		WindowsProcessed: len(windows),
	}

	for _, w := range windows {
		triples, err := extractor.ExtractWindow(ctx, w)
		if err != nil {
			// A window-level extraction failure is treated as an
			// infrastructure problem (Costguard/network), not bad data —
			// continuing to call a broken endpoint for every remaining
			// window in this conversation would be pointless. Other
			// conversations in the batch still get their own attempt; see
			// ImportWhatsAppDir/ImportInstagramDir.
			return result, fmt.Errorf("extract window: %w", err)
		}

		if sink != nil {
			if err := sink(conversationID, w, triples); err != nil {
				return result, fmt.Errorf("sink: %w", err)
			}
		}

		result.TriplesExtracted += len(triples)
	}

	return result, nil
}
