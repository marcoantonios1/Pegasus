package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// ImportWhatsApp implements proposal §13's historical bulk pipeline for
// one WhatsApp export file: parse -> persist messages -> batched triage
// -> extraction -> storage, ending with the same shape of edges the live
// path (ProcessLiveMessage) would produce for equivalent content.
//
// Scope note: this takes a single export file (one conversation), not a
// directory of many — matching whatsapp.ExportParser.Parse's own shape
// (an io.Reader plus one conversationID), the adapter this method reuses
// per requirement (a). internal/bulkimport.ImportWhatsAppDir already
// solves directory-of-many-conversations orchestration (finding files,
// deriving each conversationID from its filename, continuing past a
// single conversation's failure); re-solving that here would duplicate
// it for no benefit — a caller importing a whole export directory can
// call ImportWhatsApp once per file, the same way ImportWhatsAppDir
// itself internally processes one file at a time.
//
// Why this does NOT call bulkimport.ImportWhatsAppDir directly, despite
// that being the obvious first instinct: its TripleSink callback receives
// only (conversationID string, extraction.Window, triples) — Window's
// WindowMessage values carry no message ID by design (see
// extraction.WindowMessage's own doc comment: extraction stays decoupled
// from storage), and the sink signature exposes no other way to recover
// one either. That was fine for bulkimport's original scope (stream
// triples to whatever a caller wants to do with them), but
// storeExtractedTriples needs each window's actual message UUIDs for
// edge.SourceMessageIDs — bulkimport's existing sink shape can't supply
// that without a fragile match-by-timestamp hack (real WhatsApp export
// timestamps aren't always unique to the second, so this could
// misattribute provenance). Rather than force that hack or change
// WindowMessage's shape (which would ripple into windowing, prompt-
// building, and live-windowing — all already built and tested), this
// method reuses the same LOWER-level pieces bulkimport itself calls
// (whatsapp.ExportParser, extraction.WindowByCount, and now Triager/
// Extractor) directly, keeping its own message-ID bookkeeping in exact
// lockstep with WindowByCount's windowing via windowWithMessageIDs.
//
// Batched triage (§5's "cost discipline for historical import"): ONE
// Triager.IsWindowCandidate call per window, not one IsCandidate call per
// message — see that method's own doc comment. This is what makes
// ImportWhatsApp a genuinely separate, throughput-appropriate path rather
// than ProcessLiveMessage looped per historical message, which would
// triage one message at a time and miss this optimization entirely.
//
// Progress tracking, not full resumability: one log line per completed
// window (via WithPipeline's Logger, defaulting to log.Printf) — the
// minimum progress bar for a "years of backlog" job this issue's own
// scope settles for. Full resumability (checkpointing so an interrupted
// multi-year import can pick back up rather than restart) is flagged as
// a nice-to-have beyond this issue's strict scope, not built here —
// worth doing explicitly if Marco wants it, not assumed.
//
// Does NOT call RecordActivity() on the idle trigger — a historical bulk
// import running in the background is explicitly not "live activity"
// (see ActivityRecorder's own doc comment in process_live_message.go);
// letting it suppress the idle-based consolidation trigger, or skew the
// max-interval fallback's timing, would be misleading, not helpful.
func (a *API) ImportWhatsApp(ctx context.Context, exportPath string) error {
	if a.triager == nil || a.extractor == nil {
		return fmt.Errorf("ImportWhatsApp: API has no Triager/Extractor configured — call WithPipeline first")
	}

	f, err := os.Open(exportPath)
	if err != nil {
		return fmt.Errorf("ImportWhatsApp: open %s: %w", exportPath, err)
	}
	defer f.Close()

	// Matches internal/bulkimport.ImportWhatsAppDir's own convention for
	// deriving a conversation identifier from an export file — filename
	// minus extension — for consistency rather than inventing a second
	// rule.
	externalConversationID := strings.TrimSuffix(filepath.Base(exportPath), filepath.Ext(exportPath))
	conversationID := resolveConversationID("whatsapp", externalConversationID)

	raws, err := whatsapp.NewExportParser().Parse(f, externalConversationID)
	if err != nil {
		return fmt.Errorf("ImportWhatsApp: parse %s: %w", exportPath, err)
	}

	var windowMsgs []extraction.WindowMessage
	var messageIDs []uuid.UUID
	for _, r := range raws {
		if r.MediaType != ingestion.MediaTypeText || r.Text == nil {
			// Same limitation as internal/bulkimport.processConversation:
			// voice/image entries carry no text at this stage (no
			// transcription/vision pipeline wired in anywhere yet) — not
			// extractable, so not windowed. Still stored and marked
			// processed, since there is nothing further to do with them.
			sender, err := a.resolveSender(ctx, r.SenderExternalID)
			if err != nil {
				return fmt.Errorf("ImportWhatsApp: resolve sender: %w", err)
			}
			msg := &memory.Message{
				ConversationID: conversationID, SenderID: sender.ID, MediaType: r.MediaType,
				RawText: r.Text, MediaRef: r.MediaURL, Processed: true, Timestamp: r.Timestamp,
			}
			if err := a.messages.Create(ctx, msg); err != nil {
				return fmt.Errorf("ImportWhatsApp: store message: %w", err)
			}
			continue
		}

		sender, err := a.resolveSender(ctx, r.SenderExternalID)
		if err != nil {
			return fmt.Errorf("ImportWhatsApp: resolve sender: %w", err)
		}
		msg := &memory.Message{
			ConversationID: conversationID, SenderID: sender.ID, MediaType: r.MediaType,
			RawText: r.Text, MediaRef: r.MediaURL, Processed: false, Timestamp: r.Timestamp,
		}
		if err := a.messages.Create(ctx, msg); err != nil {
			return fmt.Errorf("ImportWhatsApp: store message: %w", err)
		}

		windowMsgs = append(windowMsgs, extraction.WindowMessage{Speaker: r.SenderExternalID, Timestamp: r.Timestamp, Text: *r.Text})
		messageIDs = append(messageIDs, msg.ID)
	}

	windows, idGroups := windowWithMessageIDs(windowMsgs, messageIDs, extraction.DefaultHistoricalWindowSize)

	var windowsProcessed, triplesExtracted int
	for i, w := range windows {
		ids := idGroups[i]

		isCandidate, err := a.triager.IsWindowCandidate(ctx, w)
		if err != nil {
			return fmt.Errorf("ImportWhatsApp: triage window %d/%d: %w", i+1, len(windows), err)
		}
		if !isCandidate {
			for _, id := range ids {
				if err := a.messages.MarkProcessed(ctx, id); err != nil {
					return fmt.Errorf("ImportWhatsApp: mark message %s processed: %w", id, err)
				}
			}
			windowsProcessed++
			continue
		}

		triples, err := a.extractor.ExtractWindow(ctx, w)
		if err != nil {
			return fmt.Errorf("ImportWhatsApp: extract window %d/%d: %w", i+1, len(windows), err)
		}

		if _, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, ids, time.Now()); err != nil {
			return fmt.Errorf("ImportWhatsApp: store extracted triples for window %d/%d: %w", i+1, len(windows), err)
		}

		for _, id := range ids {
			if err := a.messages.MarkProcessed(ctx, id); err != nil {
				return fmt.Errorf("ImportWhatsApp: mark message %s processed: %w", id, err)
			}
		}

		windowsProcessed++
		triplesExtracted += len(triples)
		a.logf("ImportWhatsApp %s: window %d/%d done (%d triples so far)", externalConversationID, windowsProcessed, len(windows), triplesExtracted)
	}

	a.logf("ImportWhatsApp %s: done — %d messages, %d windows, %d triples", externalConversationID, len(raws), windowsProcessed, triplesExtracted)
	return nil
}

// windowWithMessageIDs partitions windowMsgs into fixed-size windows via
// extraction.WindowByCount (called exactly as-is, not reimplemented), and
// returns the corresponding message ID slice for each window using the
// identical contiguous, size-based partitioning WindowByCount itself
// uses (a plain messages[i:end] slice — verified by reading its
// implementation, not assumed). Mirroring that same index arithmetic
// once here — rather than modifying WindowByCount to also carry IDs, or
// calling it twice — keeps windows and their message IDs in exact
// lockstep without touching already-tested windowing code.
func windowWithMessageIDs(windowMsgs []extraction.WindowMessage, messageIDs []uuid.UUID, size int) ([]extraction.Window, [][]uuid.UUID) {
	windows := extraction.WindowByCount(windowMsgs, size)

	idGroups := make([][]uuid.UUID, len(windows))
	offset := 0
	for i, w := range windows {
		n := len(w.Messages)
		idGroups[i] = messageIDs[offset : offset+n]
		offset += n
	}

	return windows, idGroups
}
