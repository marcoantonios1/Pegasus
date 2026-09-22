package api

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// liveConversationState is one conversation's open live window plus the
// message IDs backing it, kept in lockstep with the window's own pending
// messages. extraction.LiveWindower carries extraction.WindowMessage
// values only (Speaker/Timestamp/Text, deliberately no ID field — see its
// own doc comment on why extraction stays decoupled from storage), so
// this parallel slice is how ProcessLiveMessage later knows which stored
// message rows to mark processed and attribute SourceMessageIDs to once
// a window completes.
type liveConversationState struct {
	windower   *extraction.LiveWindower
	pendingIDs []uuid.UUID
}

// sourceTypeForRawMessage maps a live RawMessage to the source_type its
// resulting edges should carry (a memory.SourceWeights key). whatsapp/
// text and whatsapp/voice are mapped — voice via
// memory.SourceTypeVoiceTranscript once transcribed, since from that
// point on it's feeding the same triage/extraction path text does (see
// resolveTextForTriage) and needs its own source_weight (§7.5) rather
// than inheriting text's. Extend this switch when a new live adapter is
// added, rather than defaulting silently to a guessed source_type for a
// platform this pipeline has never seen, matching ApplySourceWeight's own
// "fail loud on unmapped" precedent.
func sourceTypeForRawMessage(rawMsg ingestion.RawMessage) (string, error) {
	if rawMsg.Platform == "whatsapp" {
		switch rawMsg.MediaType {
		case ingestion.MediaTypeText:
			return memory.SourceTypeWhatsAppText, nil
		case ingestion.MediaTypeVoice:
			return memory.SourceTypeVoiceTranscript, nil
		}
	}
	return "", fmt.Errorf("no source_type mapping for platform=%q media_type=%q", rawMsg.Platform, rawMsg.MediaType)
}

// resolveTextForTriage returns the text a raw message should be triaged/
// windowed with, and whether there is any (a message with neither text
// nor a successful transcription has nothing to feed the rest of the
// pipeline). For MediaTypeVoice, this is the stage that must complete
// BEFORE triage — a voice message has no text to triage until it's
// transcribed (requirement 5) — via transcribeAndStore, which also
// persists the transcript/confidence onto msg. Image/video have no
// extraction path (no captioning/vision pipeline wired in anywhere) and
// always return ok=false, unchanged from before this issue.
func (a *API) resolveTextForTriage(ctx context.Context, msg *memory.Message, rawMsg ingestion.RawMessage) (string, bool) {
	switch rawMsg.MediaType {
	case ingestion.MediaTypeText:
		if rawMsg.Text == nil {
			return "", false
		}
		return *rawMsg.Text, true
	case ingestion.MediaTypeVoice:
		return a.transcribeAndStore(ctx, msg, rawMsg.MediaURL, a.audioFetcher)
	default:
		return "", false
	}
}

// ProcessLiveMessage implements proposal §13/§5's live pipeline for one
// message: store immediately, triage (Pass 1, llama3.2:3b), and — for
// candidates — accumulate into a per-conversation rolling window that
// triggers extraction (Pass 2, qwen3-coder:30b) once it closes.
//
// Live windowing question, resolved explicitly here: a single incoming
// live message does NOT necessarily trigger extraction on its own — it
// may only close out a PREVIOUS window (see extraction.LiveWindower.Add:
// the message that crosses the time/count bound starts the NEXT window;
// the just-closed one is everything before it, not including that
// message). This reuses extraction.LiveWindower exactly as already
// built — an earlier issue already answered "how live windowing should
// differ from historical batch windowing" (time/count-bounded rolling
// windows, not fixed historical-style batches); this method does not
// re-decide that design, only wires into it.
//
// Requires WithPipeline to have set both Triager and Extractor — returns
// an error rather than a nil-pointer panic if either is missing.
// ActivityRecorder is optional: if configured, RecordActivity() is
// called once the raw message is safely stored, regardless of what
// happens afterward in this call — ProcessLiveMessage being invoked at
// all IS the live-activity signal the idle-trigger issue needs a real
// caller for (see reflection.Trigger.RecordActivity's own doc comment);
// a downstream triage/extraction failure doesn't retroactively make the
// activity not have happened.
//
// Media types: text is triaged directly. Voice is transcribed first
// (resolveTextForTriage -> transcribeAndStore), THEN triaged/windowed
// exactly like text — requirement 5, no divergent extraction logic for
// voice-derived text. Image/video have no extraction path (no
// captioning/vision pipeline wired in anywhere) and are stored but not
// triaged/extracted, marked processed=true immediately since there is
// nothing further this pipeline can do for them today.
//
// A voice message that fails transcription entirely (resolveTextForTriage
// returns ok=false — logged inside transcribeAndStore/
// transcribeVoiceMessage with the message ID either way, never silently)
// is NOT marked processed: it's already stored (msg.Processed defaults to
// false at creation, below), so it stays discoverable and re-attemptable
// rather than either disappearing from the graph with no trace or
// aborting this whole call over one bad audio file. This is the fix for
// the bug this issue's own instructions named explicitly: voice messages
// used to be marked processed=true immediately, with no transcription
// attempt at all — see the old media-type branch this replaced.
func (a *API) ProcessLiveMessage(ctx context.Context, rawMsg ingestion.RawMessage) error {
	if a.triager == nil || a.extractor == nil {
		return fmt.Errorf("ProcessLiveMessage: API has no Triager/Extractor configured — call WithPipeline first")
	}

	conversationID := resolveConversationID(rawMsg.Platform, rawMsg.ConversationID)

	sender, err := a.resolveSender(ctx, rawMsg.SenderExternalID)
	if err != nil {
		return fmt.Errorf("ProcessLiveMessage: resolve sender: %w", err)
	}

	msg := &memory.Message{
		ConversationID: conversationID,
		SenderID:       sender.ID,
		MediaType:      rawMsg.MediaType,
		RawText:        rawMsg.Text,
		MediaRef:       rawMsg.MediaURL,
		Processed:      false,
		Timestamp:      rawMsg.Timestamp,
	}
	if err := a.messages.Create(ctx, msg); err != nil {
		return fmt.Errorf("ProcessLiveMessage: store message: %w", err)
	}

	// Storage succeeded: live activity genuinely happened, regardless of
	// what transcription/triage/extraction does next.
	if a.activityRecorder != nil {
		a.activityRecorder.RecordActivity()
	}

	if rawMsg.MediaType != ingestion.MediaTypeText && rawMsg.MediaType != ingestion.MediaTypeVoice {
		return a.messages.MarkProcessed(ctx, msg.ID)
	}

	text, ok := a.resolveTextForTriage(ctx, msg, rawMsg)
	if !ok {
		// Text: genuinely empty (rawMsg.Text == nil) — nothing to do,
		// same as before. Voice: transcription failed entirely — leave
		// unprocessed/discoverable, see this function's own doc comment.
		if rawMsg.MediaType == ingestion.MediaTypeVoice {
			return nil
		}
		return a.messages.MarkProcessed(ctx, msg.ID)
	}

	isCandidate, err := a.triager.IsCandidate(ctx, text)
	if err != nil {
		return fmt.Errorf("ProcessLiveMessage: triage: %w", err)
	}
	if !isCandidate {
		return a.messages.MarkProcessed(ctx, msg.ID)
	}

	sourceType, err := sourceTypeForRawMessage(rawMsg)
	if err != nil {
		return fmt.Errorf("ProcessLiveMessage: %w", err)
	}

	// Locked section is deliberately narrow: only the in-memory window
	// state mutation, not the extraction call below. Extraction is a
	// network round trip to Costguard — holding the lock through it would
	// serialize every OTHER conversation's live messages behind whichever
	// one happens to be extracting at the time, for no correctness
	// reason (their window state is independent). Only the map/slice
	// mutation itself needs mutual exclusion.
	a.liveWindowsMu.Lock()
	if a.liveWindows == nil {
		a.liveWindows = make(map[uuid.UUID]*liveConversationState)
	}
	state, ok := a.liveWindows[conversationID]
	if !ok {
		state = &liveConversationState{windower: extraction.NewLiveWindower()}
		a.liveWindows[conversationID] = state
	}

	wm := extraction.WindowMessage{Speaker: rawMsg.SenderExternalID, Timestamp: rawMsg.Timestamp, Text: text}
	completed, ready := state.windower.Add(wm)
	state.pendingIDs = append(state.pendingIDs, msg.ID)

	var completedIDs []uuid.UUID
	if ready {
		// The message that just triggered closing the window starts the
		// next one (extraction.LiveWindower.Add's contract), so it's the
		// LAST element of pendingIDs here, not part of the just-completed
		// window.
		completedIDs = state.pendingIDs[:len(state.pendingIDs)-1]
		state.pendingIDs = state.pendingIDs[len(state.pendingIDs)-1:]
	}
	a.liveWindowsMu.Unlock()

	if !ready {
		return nil // message stored; will be marked processed once its window completes
	}

	return a.processCompletedWindow(ctx, *completed, sourceType, completedIDs)
}

// processCompletedWindow runs Pass 2 extraction over a completed window
// and stores the result via storeExtractedTriples — the same function
// ImportWhatsApp's batched path calls — then marks every source message
// processed=true regardless of whether extraction produced any triples:
// an empty result is a legitimate outcome ("[] if nothing extractable"
// per the extraction prompt), not a failure, and those messages are
// still fully processed either way.
func (a *API) processCompletedWindow(ctx context.Context, window extraction.Window, sourceType string, messageIDs []uuid.UUID) error {
	triples, err := a.extractor.ExtractWindow(ctx, window)
	if err != nil {
		return fmt.Errorf("extract window: %w", err)
	}

	if _, err := a.storeExtractedTriples(ctx, triples, sourceType, messageIDs, time.Now()); err != nil {
		return fmt.Errorf("store extracted triples: %w", err)
	}

	for _, id := range messageIDs {
		if err := a.messages.MarkProcessed(ctx, id); err != nil {
			return fmt.Errorf("mark message %s processed: %w", id, err)
		}
	}

	return nil
}
