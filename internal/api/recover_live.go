package api

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Restart/failure recovery sweep (Part 2 of the live-window idle-flush
// fix). Design decision, stated explicitly per this issue's own
// instructions: recovery works by RE-DERIVING windows from persisted
// unprocessed rows, NOT by serializing a.liveWindows' own in-memory
// state to disk. Chosen over "document this as an accepted data-loss
// risk" because nothing here is actually unrecoverable: ProcessLiveMessage
// stores every message (processed=false) BEFORE it's ever added to a
// window, so a window that was only ever in memory — never flushed,
// lost on a crash or restart — has every one of its messages still
// sitting in the table exactly as they arrived. Recovery is re-running
// the same windowing+extraction logic a live process would eventually
// have run anyway, over data that was never actually gone.

// RecoveryStats summarizes one RecoverUnprocessedLive (or
// ProcessRecoveredMessages) run.
type RecoveryStats struct {
	// MessagesRecovered is how many messages were fed through a
	// (re-derived) window and reached processCompletedWindow
	// successfully.
	MessagesRecovered int
	// WindowsProcessed is how many windows (closed mid-conversation, or
	// the final unconditional tail flush) were successfully extracted.
	WindowsProcessed int
	// SkippedNoText is how many processed=false messages have no usable
	// text (e.g. a voice note whose transcription failed entirely) —
	// left completely untouched, per requirement 10: retrying failed
	// transcription is out of scope here, and these messages stay
	// exactly as discoverable/re-attemptable as they already were.
	SkippedNoText int
	// Errors is how many windows failed during processCompletedWindow
	// (logged, never retried in-process — same "log loudly, no retry
	// loop" rule the idle flusher itself follows) or had a per-message
	// resolution failure (e.g. a sender entity lookup error) during
	// re-windowing.
	Errors int
}

// SnapshotUnprocessedLive takes the unprocessed-message snapshot
// RecoverUnprocessedLive processes — split into its own method
// specifically so a production caller can sequence recovery correctly
// (requirement 9): take this snapshot SYNCHRONOUSLY, BEFORE live
// ingestion starts accepting events, then start ingestion, THEN run
// ProcessRecoveredMessages on this snapshot in a background goroutine.
// That ordering is what guarantees a live message arriving during
// recovery is simply never IN the snapshot — no double-processing to
// guard against, and no blocking of service startup on however long
// extraction over the whole recovered backlog takes. A caller that
// doesn't need that split (a one-shot recovery tool, a test, anything
// else with nothing concurrently touching the database) can just call
// RecoverUnprocessedLive instead, which does both steps back-to-back.
func (a *API) SnapshotUnprocessedLive(ctx context.Context) ([]*memory.Message, error) {
	return a.messages.ListUnprocessedWithText(ctx)
}

// ProcessRecoveredMessages re-derives live windows from msgs (a snapshot
// from SnapshotUnprocessedLive) and runs each through the exact same
// extraction path (processCompletedWindow) a live window closing
// normally would.
//
// Per conversation (messages already grouped and ordered by timestamp —
// ListUnprocessedWithText's own ORDER BY): fed through a FRESH
// extraction.LiveWindower (the SAME time/count bounds live ingestion
// uses — this issue's own scope note: changing window bounds is out of
// scope), then Flush()ed unconditionally for the final tail. Unlike
// FlushIdleWindows, there is no "wait for idle" step here at all — these
// are messages from a window that already stopped growing (by
// definition: nothing else is going to arrive and extend a SNAPSHOT), so
// the tail is flushed immediately rather than waiting out
// LiveWindowFlushAfter a second time.
//
// No re-triage: every message ListUnprocessedWithText returns already
// passed Pass-1 triage once, when it first arrived — ProcessLiveMessage
// only ever leaves a message processed=false AFTER triage already
// approved it (a triage rejection calls MarkProcessed immediately, see
// its own code) — so these rows are, by construction, already
// triage-approved and simply never reached a window close. Re-triaging
// here would be redundant at best and could disagree with the original
// live verdict at worst.
//
// Speaker label: resolves each message's sender_id to its entity's
// CanonicalName — the SAME label historical windows (ImportWhatsApp) use
// for the extraction prompt, NOT live ProcessLiveMessage's own
// SenderExternalID (a raw platform ID, e.g. a phone number/JID). Stated
// explicitly because it's a real, visible difference: the exact same
// real message would show the extraction prompt "Sara" if its window
// closed live, but "+9611234567" if recovery has to re-derive it,
// purely because of WHEN windowing happened to run — a known,
// documented consequence of recovery being historical-shaped by the
// time it runs, not a bug to silently paper over.
func (a *API) ProcessRecoveredMessages(ctx context.Context, msgs []*memory.Message) (RecoveryStats, error) {
	var stats RecoveryStats

	skipped, err := a.messages.CountUnprocessedWithoutText(ctx)
	if err != nil {
		return stats, fmt.Errorf("ProcessRecoveredMessages: count unprocessed messages without text: %w", err)
	}
	stats.SkippedNoText = skipped
	if skipped > 0 {
		a.logf("ProcessRecoveredMessages: %d unprocessed message(s) have no usable text (e.g. failed voice transcription) — left untouched, not retried", skipped)
	}

	// Group while preserving each conversation's first-appearance order
	// (the snapshot is already ORDER BY conversation_id, timestamp, so
	// this is stable) — not load-bearing for correctness (each
	// conversation's own windowing is independent), just deterministic
	// output for tests/logs.
	byConversation := make(map[uuid.UUID][]*memory.Message)
	var order []uuid.UUID
	for _, m := range msgs {
		if _, seen := byConversation[m.ConversationID]; !seen {
			order = append(order, m.ConversationID)
		}
		byConversation[m.ConversationID] = append(byConversation[m.ConversationID], m)
	}

	for _, conversationID := range order {
		a.recoverOneConversation(ctx, conversationID, byConversation[conversationID], &stats)
	}

	return stats, nil
}

// recoverOneConversation re-windows one conversation's messages (already
// in timestamp order) and dispatches each completed window — see
// ProcessRecoveredMessages' own doc comment for the full design.
func (a *API) recoverOneConversation(ctx context.Context, conversationID uuid.UUID, msgs []*memory.Message, stats *RecoveryStats) {
	windower := extraction.NewLiveWindower()
	var pendingIDs []uuid.UUID
	var pendingMediaTypes []string

	flush := func(w *extraction.Window, ids []uuid.UUID, mediaTypes []string) {
		if w == nil {
			return
		}
		sourceType, err := windowSourceType(mediaTypes)
		if err != nil {
			a.logf("ProcessRecoveredMessages: conversation %s: %v", conversationID, err)
			stats.Errors++
			return
		}
		if err := a.processCompletedWindow(ctx, *w, sourceType, ids); err != nil {
			a.logf("ProcessRecoveredMessages: process window (conversation=%s messages=%v): %v", conversationID, ids, err)
			stats.Errors++
			return
		}
		stats.MessagesRecovered += len(ids)
		stats.WindowsProcessed++
	}

	for _, m := range msgs {
		text, ok := messageText(m)
		if !ok {
			// Shouldn't happen — ListUnprocessedWithText already filtered
			// to messages with usable text — but fail safe (skip, count
			// as an error rather than panic/silently fabricate text) if
			// the snapshot and this check ever disagree.
			a.logf("ProcessRecoveredMessages: message %s has no usable text despite the snapshot filter — skipping", m.ID)
			stats.Errors++
			continue
		}

		sender, err := a.entities.GetByID(ctx, m.SenderID)
		if err != nil {
			a.logf("ProcessRecoveredMessages: resolve sender %s for message %s: %v", m.SenderID, m.ID, err)
			stats.Errors++
			continue
		}

		wm := extraction.WindowMessage{Speaker: sender.CanonicalName, Timestamp: m.Timestamp, Text: text}
		completed, ready := windower.Add(wm)
		pendingIDs = append(pendingIDs, m.ID)
		pendingMediaTypes = append(pendingMediaTypes, m.MediaType)

		if ready {
			// Same lockstep discipline as the live path: the message
			// that triggers closure starts the NEXT window, so it's the
			// LAST element of pendingIDs/pendingMediaTypes, not part of
			// the window that just closed.
			closedIDs := pendingIDs[:len(pendingIDs)-1]
			closedMediaTypes := pendingMediaTypes[:len(pendingMediaTypes)-1]
			pendingIDs = pendingIDs[len(pendingIDs)-1:]
			pendingMediaTypes = pendingMediaTypes[len(pendingMediaTypes)-1:]
			flush(completed, closedIDs, closedMediaTypes)
		}
	}

	// The final tail: flushed unconditionally, no idle wait — see
	// ProcessRecoveredMessages' own doc comment for why.
	flush(windower.Flush(), pendingIDs, pendingMediaTypes)
}

// RecoverUnprocessedLive is SnapshotUnprocessedLive + ProcessRecoveredMessages
// run back-to-back — the convenient one-call form for a caller that
// doesn't need the synchronous-snapshot/asynchronous-processing split
// requirement 9 asks for kept apart (a one-shot recovery tool run before
// anything else is touching the database, or tests). A production
// service wiring live ingestion should call SnapshotUnprocessedLive
// itself, start accepting live events, THEN call ProcessRecoveredMessages
// with that snapshot in a background goroutine — see
// SnapshotUnprocessedLive's own doc comment.
//
// Idempotent by construction, not by separate tracking: a second call
// finds nothing, because the first call's processCompletedWindow already
// marked every one of its messages processed=true (same reasoning as
// ImportWhatsApp/BackfillEmbeddings' own idempotency — see
// storeExtractedTriples' and embedMessages' doc comments).
//
// Known limits, deliberately not worked around:
//   - A crash between storeExtractedTriples and MarkProcessed re-extracts
//     that ONE window on the next recovery run — the same at-least-once
//     limit ImportWhatsApp already has (see its own doc comment); this
//     sweep inherits it rather than solving it differently.
//   - messages has no created_at/source column distinguishing WHEN or
//     HOW a row was written, so this sweep cannot tell a genuinely
//     abandoned live window's rows apart from an interrupted bulk
//     import's own not-yet-processed rows. Run this once at service
//     startup; do NOT run it while a bulk import (ImportWhatsApp) is
//     mid-flight against the same database, or it will race to
//     (correctly, if oddly early) extract that import's still-open
//     windows out from under it.
func (a *API) RecoverUnprocessedLive(ctx context.Context) (RecoveryStats, error) {
	msgs, err := a.SnapshotUnprocessedLive(ctx)
	if err != nil {
		return RecoveryStats{}, fmt.Errorf("RecoverUnprocessedLive: snapshot unprocessed messages: %w", err)
	}
	return a.ProcessRecoveredMessages(ctx, msgs)
}
