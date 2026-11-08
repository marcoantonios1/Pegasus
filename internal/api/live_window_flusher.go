package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
)

// Live-window idle flush. Root cause this fixes: extraction.LiveWindower
// only closes a window from INSIDE Add — when the SAME conversation's
// NEXT message breaks the time/count bound (window.go's own doc
// comment). A chat's trailing messages — anything short of exactly
// hitting that bound — sit in a.liveWindows forever otherwise, and are
// gone from memory on restart. The messages themselves are already
// persisted (ProcessLiveMessage stores every message, processed=false,
// before windowing even runs), so this is a loss of TIMELY EXTRACTION,
// not of raw data — and Part 2 (recover_live.go) closes even that gap
// completely by re-deriving windows from the persisted rows rather than
// requiring this flusher to have run first.
const (
	// DefaultLiveWindowFlushAfter: a window flushes once this long has
	// passed with no new message ARRIVING (wall-clock, never message
	// Timestamp — see liveConversationState.lastArrival's own doc
	// comment). Deliberately equal to extraction.DefaultLiveWindowDuration
	// (the span Add itself uses to decide a window's bound) rather than
	// an independently-chosen value: both describe "how long is this
	// conversation's burst of activity," just from two different angles
	// (content span vs. idle gap), and starting them equal is the
	// natural first guess — they're configured independently
	// (PipelineDeps.LiveWindowFlushAfter), so nothing stops them
	// diverging later if real usage shows they should.
	DefaultLiveWindowFlushAfter = extraction.DefaultLiveWindowDuration

	// DefaultLiveWindowFlushPollInterval: how often FlushIdleWindows is
	// checked by StartLiveWindowFlusher's ticker. Not specified by any
	// proposal section; chosen the same way reflection.DefaultPollInterval
	// was — well under DefaultLiveWindowFlushAfter so an idle window
	// doesn't sit noticeably longer than the threshold before it's
	// actually flushed, without polling so often it's wasted work. A
	// first guess, not validated against real data.
	DefaultLiveWindowFlushPollInterval = 30 * time.Second

	// shutdownFlushTimeout bounds StartLiveWindowFlusher's final,
	// unconditional flush-everything pass on shutdown — a fresh,
	// short-lived context, deliberately NOT the caller's own (already
	// cancelled, by construction — that's WHY this flush is happening)
	// ctx, which couldn't make the Costguard network calls
	// processCompletedWindow needs at all. A first guess generous enough
	// for a final extraction call per still-open conversation to
	// complete, not validated against real data.
	shutdownFlushTimeout = 30 * time.Second
)

// pendingFlush is one window FlushIdleWindows/StartLiveWindowFlusher's
// shutdown path has decided to close, carrying everything
// processCompletedWindow needs — the shared shape collectWindowsToFlush
// returns regardless of WHY a given window qualified.
type pendingFlush struct {
	window     extraction.Window
	sourceType string
	messageIDs []uuid.UUID
}

// collectWindowsToFlush is the one piece of drain-and-dispatch logic
// FlushIdleWindows (idle check) and the shutdown flush (always true)
// both need, kept in exactly one place so the two paths can't diverge on
// HOW a window is drained, only on WHICH windows qualify (the shouldFlush
// predicate, evaluated per conversation under the SAME lock the map
// mutation itself needs).
//
// Deletes every qualifying entry from a.liveWindows as it goes — this is
// what actually fixes the map "currently never shrinks" (this issue's
// own finding): a flushed conversation's state is gone from memory the
// moment this returns, exactly as if its window had closed normally via
// Add.
//
// Returns before any Costguard call is made — the caller (FlushIdleWindows
// or the shutdown path) runs processCompletedWindow for each pendingFlush
// AFTER this returns and the lock is released, same discipline
// ProcessLiveMessage's own Add path already follows for the same reason
// (its own comment: holding the lock through a network round trip would
// serialize every OTHER conversation's live messages behind it for no
// correctness reason).
func (a *API) collectWindowsToFlush(shouldFlush func(state *liveConversationState) bool) []pendingFlush {
	var toProcess []pendingFlush

	a.liveWindowsMu.Lock()
	defer a.liveWindowsMu.Unlock()

	for conversationID, state := range a.liveWindows {
		if len(state.pendingIDs) == 0 || !shouldFlush(state) {
			continue
		}

		completed := state.windower.Flush()
		delete(a.liveWindows, conversationID)
		if completed == nil {
			// Shouldn't happen alongside len(pendingIDs) > 0 — Flush only
			// returns nil when nothing's pending, and pendingIDs is kept
			// in lockstep with the windower's own pending slice — but
			// fail safe (just drop the now-empty state) rather than
			// panic on an invariant this function doesn't itself enforce
			// elsewhere.
			continue
		}

		sourceType, err := windowSourceType(state.pendingMediaTypes)
		if err != nil {
			// Requirement 6: log loudly, no retry loop here. The window
			// is already gone from a.liveWindows (deleted above) — its
			// messages remain processed=false in the database and will
			// be picked up by RecoverUnprocessedLive (recover_live.go).
			a.logf("collectWindowsToFlush: conversation %s: %v — window dropped from memory, messages remain unprocessed for recovery", conversationID, err)
			continue
		}

		toProcess = append(toProcess, pendingFlush{
			window:     *completed,
			sourceType: sourceType,
			messageIDs: append([]uuid.UUID{}, state.pendingIDs...),
		})
	}

	return toProcess
}

// processFlushedWindows runs processCompletedWindow for each already-
// collected window — shared by FlushIdleWindows and the shutdown path.
// Per-window failures are logged and counted toward the first returned
// error, but never stop later windows in the same batch from being
// attempted (requirement 6: no in-process retry loop, but also no
// reason one bad window should block every other conversation's own
// independent flush).
func (a *API) processFlushedWindows(ctx context.Context, windows []pendingFlush) error {
	var firstErr error
	for _, fw := range windows {
		if err := a.processCompletedWindow(ctx, fw.window, fw.sourceType, fw.messageIDs); err != nil {
			a.logf("process flushed live window (source_type=%q messages=%v): %v", fw.sourceType, fw.messageIDs, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// FlushIdleWindows force-closes every open live window whose conversation
// has had no message ARRIVE for at least a.liveWindowFlushAfter (wall-
// clock, a.now() — see liveConversationState.lastArrival's own doc
// comment for why arrival time, never message Timestamp, is the correct
// basis: a history-sync burst of old messages arriving within seconds
// would look "expired" by timestamp and fragment mid-burst, which
// arrival-time idleness never does).
//
// Does NOT call a.activityRecorder.RecordActivity() — requirement 4: this
// is housekeeping (closing out windows nothing is adding to anymore), not
// live activity, and must never feed the Reflection Engine's idle
// detection or it would never see real idle time at all while this
// flusher keeps ticking.
func (a *API) FlushIdleWindows(ctx context.Context) error {
	flushAfter := a.liveWindowFlushAfter
	if flushAfter <= 0 {
		flushAfter = DefaultLiveWindowFlushAfter
	}
	now := a.now()

	toProcess := a.collectWindowsToFlush(func(state *liveConversationState) bool {
		return now.Sub(state.lastArrival) >= flushAfter
	})

	return a.processFlushedWindows(ctx, toProcess)
}

// StartLiveWindowFlusher starts the idle-flush polling goroutine — the
// same ticker + select-on-ctx.Done shape as reflection.Trigger.Start
// (ticker Stop deferred, select on ctx.Done() vs ticker.C), chosen for
// the same reason: a poll loop, not a per-window timer. A
// time.AfterFunc/timer per conversation would need resetting on every
// single message (timer-reset churn) and explicit cleanup on every path
// a window can close through (Add's own closure, this flusher, shutdown)
// to avoid leaking one goroutine/timer per conversation ever seen; a
// single shared poll loop has neither problem and is trivially testable
// with a fake clock (the same technique reflection.Trigger's own tests
// use — see live_window_flusher_test.go), unlike a real per-window timer.
//
// On ctx cancellation (shutdown): runs ONE final pass that flushes EVERY
// open window regardless of idle time — not just the idle ones — using a
// fresh, short-lived, BOUNDED context (shutdownFlushTimeout), since the
// caller's own ctx is already cancelled and can't make the Costguard
// network calls processCompletedWindow needs. This is the one place a
// window is flushed without having been idle: shutdown means no more
// messages are coming for ANY conversation, not just the currently-idle
// ones, so every open window gets one last chance to extract before this
// process goes away.
//
// Does not block: the polling loop (and the final shutdown flush) run in
// a spawned goroutine; this function returns immediately after starting
// it.
func (a *API) StartLiveWindowFlusher(ctx context.Context) {
	pollInterval := a.liveWindowFlushPollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultLiveWindowFlushPollInterval
	}

	ticker := time.NewTicker(pollInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				a.flushAllWindowsOnShutdown()
				return
			case <-ticker.C:
				if err := a.FlushIdleWindows(ctx); err != nil {
					a.logf("StartLiveWindowFlusher: FlushIdleWindows: %v", err)
				}
			}
		}
	}()
}

// flushAllWindowsOnShutdown is StartLiveWindowFlusher's ctx-cancellation
// path — see its own doc comment for why every open window flushes here
// regardless of idle time, and why this uses a fresh bounded context
// rather than the (already cancelled) ctx the caller's own goroutine
// received.
func (a *API) flushAllWindowsOnShutdown() {
	toProcess := a.collectWindowsToFlush(func(*liveConversationState) bool { return true })
	if len(toProcess) == 0 {
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
	defer cancel()

	if err := a.processFlushedWindows(shutdownCtx, toProcess); err != nil {
		a.logf("StartLiveWindowFlusher: shutdown flush: %v", err)
	}
}
