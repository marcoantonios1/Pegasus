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
// these two parallel slices are how ProcessLiveMessage later knows which
// stored message rows to mark processed/attribute SourceMessageIDs to,
// and what source_type the completed window should carry, once it closes.
type liveConversationState struct {
	windower          *extraction.LiveWindower
	pendingIDs        []uuid.UUID
	pendingMediaTypes []string
}

// windowSourceType derives the single source_type a whole window's worth
// of messages should be stored under — storeExtractedTriples takes one
// sourceType per call, not one per message, but a window (live or
// historical) can legitimately mix text and voice-transcribed messages
// (e.g. a text reply to a voice note landing in the same window). Rather
// than guess or silently pick one arbitrarily, this takes the LOWER-trust
// type whenever ANY message in the window came through transcription:
// SourceWeights scores voice_transcript at 0.95 vs. whatsapp_text's 1.0
// (source_weights.go), so "any voice in the window -> voice_transcript"
// is the conservative choice, not an arbitrary one — it never overstates
// trust for a window part of whose content passed through a transcription
// step. An unrecognized media_type fails loudly rather than defaulting,
// matching ApplySourceWeight's own "fail loud on unmapped" precedent.
//
// Replaces an earlier, buggier design (sourceTypeForRawMessage, computed
// once from whichever message happened to be CURRENTLY arriving) that
// this issue's own review caught: per extraction.LiveWindower.Add's
// contract, the message that triggers a window's closure starts the NEXT
// window and is NOT a member of the one that just closed — so the old
// code was tagging a completed window's source_type using a message that
// wasn't even in it. That was invisible while every message was text
// (the answer was always whatsapp_text either way), but would have
// silently mis-tagged real voice-containing windows the moment voice
// started flowing through this same path.
func windowSourceType(mediaTypes []string) (string, error) {
	sawVoice := false
	for _, mt := range mediaTypes {
		switch mt {
		case ingestion.MediaTypeVoice:
			sawVoice = true
		case ingestion.MediaTypeText:
			// no-op — whatsapp_text unless a voice message elsewhere in
			// the window overrides it.
		default:
			return "", fmt.Errorf("windowSourceType: unexpected media_type %q in a windowed message", mt)
		}
	}
	if sawVoice {
		return memory.SourceTypeVoiceTranscript, nil
	}
	return memory.SourceTypeWhatsAppText, nil
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
	state.pendingMediaTypes = append(state.pendingMediaTypes, rawMsg.MediaType)

	var completedIDs []uuid.UUID
	var completedMediaTypes []string
	if ready {
		// The message that just triggered closing the window starts the
		// next one (extraction.LiveWindower.Add's contract), so it's the
		// LAST element of pendingIDs/pendingMediaTypes here, not part of
		// the just-completed window — completedMediaTypes must reflect
		// the CLOSED window's own messages, not this one (see
		// windowSourceType's own doc comment for the bug this fixes).
		completedIDs = state.pendingIDs[:len(state.pendingIDs)-1]
		state.pendingIDs = state.pendingIDs[len(state.pendingIDs)-1:]
		completedMediaTypes = state.pendingMediaTypes[:len(state.pendingMediaTypes)-1]
		state.pendingMediaTypes = state.pendingMediaTypes[len(state.pendingMediaTypes)-1:]
	}
	a.liveWindowsMu.Unlock()

	if !ready {
		return nil // message stored; will be marked processed once its window completes
	}

	sourceType, err := windowSourceType(completedMediaTypes)
	if err != nil {
		return fmt.Errorf("ProcessLiveMessage: %w", err)
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
