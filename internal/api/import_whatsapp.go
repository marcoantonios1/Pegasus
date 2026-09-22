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

// WindowResult is one window's outcome within an ImportWhatsApp run:
// whether triage flagged it, and (if so) what extraction/storage did with
// each triple it produced. Messages is carried alongside for reporting
// tools (the Phase 0 dry-run, cmd/phase0_dryrun) that need to show a
// human the actual conversation text behind an extracted edge — that
// context isn't recoverable after the fact from just the resulting edges
// (see EdgeStore.SearchRelevant's doc comment on why edges join back to
// messages via source_message_ids, not the other way around; this avoids
// needing that join for a report that already has the window in hand).
type WindowResult struct {
	Index       int
	Messages    []extraction.WindowMessage
	IsCandidate bool
	Outcomes    []TripleOutcome
}

// ImportResult is ImportWhatsApp's per-conversation summary — added
// specifically because the Phase 0 dry-run tooling (cmd/phase0_dryrun)
// has no other way to observe triage-candidate rate, reinforcement-vs-
// new-edge counts, or per-window provenance without either a special-
// cased dry-run code path (which that issue's own instructions forbid)
// or fragile after-the-fact DB diffing. Any real caller of ImportWhatsApp
// benefits from this same progress/stats data — it is not a dry-run-only
// concern, which is why it lives on the real return type rather than a
// wrapper.
type ImportResult struct {
	ExternalConversationID string

	// MessagesParsed is every message the export file parser (adapter)
	// produced, including image/video entries (and voice entries that
	// failed transcription entirely) that never reach windowing/extraction
	// (see the "not extractable" branch in ImportWhatsApp).
	MessagesParsed int
	// MessagesTextExtractable is the subset that actually got windowed:
	// MediaType == text with non-nil Text, plus MediaType == voice that
	// transcribed successfully (transcript text feeds the same window
	// text/extractable count — requirement 5, no separate accounting for
	// voice-derived text).
	MessagesTextExtractable int

	Windows []WindowResult

	Duration time.Duration
}

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
func (a *API) ImportWhatsApp(ctx context.Context, exportPath string) (*ImportResult, error) {
	if a.triager == nil || a.extractor == nil {
		return nil, fmt.Errorf("ImportWhatsApp: API has no Triager/Extractor configured — call WithPipeline first")
	}

	start := time.Now()

	f, err := os.Open(exportPath)
	if err != nil {
		return nil, fmt.Errorf("ImportWhatsApp: open %s: %w", exportPath, err)
	}
	defer f.Close()

	// Matches internal/bulkimport.ImportWhatsAppDir's own convention for
	// deriving a conversation identifier from an export file — filename
	// minus extension — for consistency rather than inventing a second
	// rule.
	externalConversationID := strings.TrimSuffix(filepath.Base(exportPath), filepath.Ext(exportPath))
	conversationID := resolveConversationID("whatsapp", externalConversationID)

	// A "with media" WhatsApp export's voice entries carry MediaURL as a
	// bare filename (see internal/ingestion/whatsapp/export.go's
	// attachedFileRe) relative to the export file's own directory, where
	// the sibling audio files live — always resolvable here, unlike
	// ProcessLiveMessage's live path (see AudioFetcher's own doc comment
	// for why that one has no fetcher by default).
	audioFetcher := localExportAudioFetcher(filepath.Dir(exportPath))

	raws, err := whatsapp.NewExportParser().Parse(f, externalConversationID)
	if err != nil {
		return nil, fmt.Errorf("ImportWhatsApp: parse %s: %w", exportPath, err)
	}

	result := &ImportResult{ExternalConversationID: externalConversationID, MessagesParsed: len(raws)}
	// Deferred rather than set before each return: an earlier version set
	// this manually before every error return below, and missed the three
	// error paths in the per-message loop just below (a real bug caught in
	// review — a run failing during message storage reported Duration=0
	// instead of the actual elapsed time). A single defer covers every
	// return uniformly, including ones added later, without relying on
	// each one remembering to set it.
	defer func() { result.Duration = time.Since(start) }()

	var windowMsgs []extraction.WindowMessage
	var messageIDs []uuid.UUID
	var mediaTypes []string
	for _, r := range raws {
		sender, err := a.resolveSender(ctx, r.SenderExternalID)
		if err != nil {
			return result, fmt.Errorf("ImportWhatsApp: resolve sender: %w", err)
		}

		// image/video (and a MediaTypeText entry with a nil Text, which the
		// adapter shouldn't produce but isn't guaranteed not to) have no
		// extraction path at all — stored and marked processed immediately,
		// same as before this issue.
		if r.MediaType != ingestion.MediaTypeText && r.MediaType != ingestion.MediaTypeVoice {
			msg := &memory.Message{
				ConversationID: conversationID, SenderID: sender.ID, MediaType: r.MediaType,
				RawText: r.Text, MediaRef: r.MediaURL, Processed: true, Timestamp: r.Timestamp,
			}
			if err := a.messages.Create(ctx, msg); err != nil {
				return result, fmt.Errorf("ImportWhatsApp: store message: %w", err)
			}
			continue
		}
		if r.MediaType == ingestion.MediaTypeText && r.Text == nil {
			msg := &memory.Message{
				ConversationID: conversationID, SenderID: sender.ID, MediaType: r.MediaType,
				RawText: r.Text, MediaRef: r.MediaURL, Processed: true, Timestamp: r.Timestamp,
			}
			if err := a.messages.Create(ctx, msg); err != nil {
				return result, fmt.Errorf("ImportWhatsApp: store message: %w", err)
			}
			continue
		}

		msg := &memory.Message{
			ConversationID: conversationID, SenderID: sender.ID, MediaType: r.MediaType,
			RawText: r.Text, MediaRef: r.MediaURL, Processed: false, Timestamp: r.Timestamp,
		}
		if err := a.messages.Create(ctx, msg); err != nil {
			return result, fmt.Errorf("ImportWhatsApp: store message: %w", err)
		}

		text := ""
		if r.MediaType == ingestion.MediaTypeText {
			text = *r.Text
		} else {
			// Voice: transcription must complete before this message can
			// enter windowing/triage (requirement 5 — no text to triage
			// until transcribed). A total transcription failure leaves the
			// message stored with Processed: false (set above) and simply
			// skips windowing this pass — discoverable/re-attemptable, not
			// silently dropped or a batch-aborting error (requirement 6;
			// transcribeAndStore/transcribeVoiceMessage already logged the
			// failure with the message ID).
			var ok bool
			text, ok = a.transcribeAndStore(ctx, msg, r.MediaURL, audioFetcher)
			if !ok {
				continue
			}
		}

		windowMsgs = append(windowMsgs, extraction.WindowMessage{Speaker: r.SenderExternalID, Timestamp: r.Timestamp, Text: text})
		messageIDs = append(messageIDs, msg.ID)
		mediaTypes = append(mediaTypes, r.MediaType)
	}
	result.MessagesTextExtractable = len(windowMsgs)

	windows, idGroups, mediaTypeGroups := windowWithMessageIDs(windowMsgs, messageIDs, mediaTypes, extraction.DefaultHistoricalWindowSize)

	var triplesExtracted int
	for i, w := range windows {
		ids := idGroups[i]
		wr := WindowResult{Index: i, Messages: w.Messages}

		isCandidate, err := a.triager.IsWindowCandidate(ctx, w)
		if err != nil {
			return result, fmt.Errorf("ImportWhatsApp: triage window %d/%d: %w", i+1, len(windows), err)
		}
		wr.IsCandidate = isCandidate

		if !isCandidate {
			for _, id := range ids {
				if err := a.messages.MarkProcessed(ctx, id); err != nil {
					return result, fmt.Errorf("ImportWhatsApp: mark message %s processed: %w", id, err)
				}
			}
			result.Windows = append(result.Windows, wr)
			continue
		}

		triples, err := a.extractor.ExtractWindow(ctx, w)
		if err != nil {
			return result, fmt.Errorf("ImportWhatsApp: extract window %d/%d: %w", i+1, len(windows), err)
		}

		// A window can legitimately mix text and voice-transcribed
		// messages — see windowSourceType's own doc comment (this is the
		// same bug class the live path had, now fixed there too).
		sourceType, err := windowSourceType(mediaTypeGroups[i])
		if err != nil {
			return result, fmt.Errorf("ImportWhatsApp: window %d/%d: %w", i+1, len(windows), err)
		}

		outcomes, err := a.storeExtractedTriples(ctx, triples, sourceType, ids, time.Now())
		if err != nil {
			return result, fmt.Errorf("ImportWhatsApp: store extracted triples for window %d/%d: %w", i+1, len(windows), err)
		}
		wr.Outcomes = outcomes

		for _, id := range ids {
			if err := a.messages.MarkProcessed(ctx, id); err != nil {
				return result, fmt.Errorf("ImportWhatsApp: mark message %s processed: %w", id, err)
			}
		}

		result.Windows = append(result.Windows, wr)
		triplesExtracted += len(triples)
		a.logf("ImportWhatsApp %s: window %d/%d done (%d triples so far)", externalConversationID, i+1, len(windows), triplesExtracted)
	}

	a.logf("ImportWhatsApp %s: done — %d messages, %d windows, %d triples", externalConversationID, len(raws), len(windows), triplesExtracted)
	return result, nil
}

// localExportAudioFetcher builds an AudioFetcher that reads mediaRef as a
// filename relative to dir — ImportWhatsApp's own AudioFetcher, scoped to
// one export file's directory (see AudioFetcher's own doc comment on why
// ImportWhatsApp always has one, unlike ProcessLiveMessage). mediaRef
// values come from whatsapp.ExportParser (attachedFileRe) as bare
// filenames, e.g. "PTT-20260702-WA0011.opus", with the actual bytes
// sitting alongside the export .txt file in a "with media" export.
func localExportAudioFetcher(dir string) AudioFetcher {
	return func(_ context.Context, mediaRef string) ([]byte, error) {
		path := filepath.Join(dir, mediaRef)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read audio file %s: %w", path, err)
		}
		return data, nil
	}
}

// windowWithMessageIDs partitions windowMsgs into fixed-size windows via
// extraction.WindowByCount (called exactly as-is, not reimplemented), and
// returns the corresponding message ID and media-type slices for each
// window using the identical contiguous, size-based partitioning
// WindowByCount itself uses (a plain messages[i:end] slice — verified by
// reading its implementation, not assumed). Mirroring that same index
// arithmetic once here — rather than modifying WindowByCount to also
// carry IDs, or calling it twice — keeps windows, their message IDs, and
// their media types in exact lockstep without touching already-tested
// windowing code. mediaTypeGroups feeds windowSourceType, since a window
// can mix text and voice-transcribed messages.
func windowWithMessageIDs(windowMsgs []extraction.WindowMessage, messageIDs []uuid.UUID, mediaTypes []string, size int) ([]extraction.Window, [][]uuid.UUID, [][]string) {
	windows := extraction.WindowByCount(windowMsgs, size)

	idGroups := make([][]uuid.UUID, len(windows))
	mediaTypeGroups := make([][]string, len(windows))
	offset := 0
	for i, w := range windows {
		n := len(w.Messages)
		idGroups[i] = messageIDs[offset : offset+n]
		mediaTypeGroups[i] = mediaTypes[offset : offset+n]
		offset += n
	}

	return windows, idGroups, mediaTypeGroups
}
