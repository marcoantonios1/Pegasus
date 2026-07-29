package extraction

import "time"

// WindowMessage is one message as extraction sees it: already resolved to
// a display name and text by the caller. This package deliberately does
// not depend on memory.Message or an entity lookup — memory.Message.SenderID
// is a UUID, and resolving that to a display name is a DB-backed lookup
// this package has no business doing. Keeping windowing/prompt-building
// decoupled from storage keeps it unit-testable without Postgres and keeps
// this package to "translation only," consistent with the ingestion
// adapters' RawMessage boundary.
type WindowMessage struct {
	Speaker   string
	Timestamp time.Time
	Text      string
}

// Window is one block of messages to run through extraction together.
type Window struct {
	Messages []WindowMessage
}

// Historical/bulk import windowing (§8.3's "5-10 message" option).
const (
	MinHistoricalWindowSize     = 5
	MaxHistoricalWindowSize     = 10
	DefaultHistoricalWindowSize = 8 // midpoint of the 5-10 range, a starting point
)

// WindowByCount partitions messages into fixed-size, non-overlapping
// windows for historical/bulk import. Message-count windowing is chosen as
// the primary rule for this path because historical import processes a
// fixed, already-known backlog — a count-based window gives a precise,
// predictable total call count up front, which is exactly the cost lever
// §5's "cost discipline for historical import" note is about. Time-based
// windowing doesn't offer that predictability against a backlog where
// message density varies wildly across years of history.
//
// "Sliding" in §8.3 is read here as "the window advances sequentially
// through the conversation," not as overlapping windows: true overlap
// would mean reprocessing the same messages across multiple calls, which
// directly works against windowing's stated primary goal of cutting call
// volume. The tradeoff of non-overlapping windows is that a multi-message
// signal (an inside joke, say) can still land across a window boundary —
// see the manual review findings for how often that happened in practice
// against a real sample.
//
// The final window may be smaller than size if the message count doesn't
// divide evenly; it's still processed, never dropped.
func WindowByCount(messages []WindowMessage, size int) []Window {
	if size <= 0 {
		size = DefaultHistoricalWindowSize
	}

	var windows []Window
	for i := 0; i < len(messages); i += size {
		end := i + size
		if end > len(messages) {
			end = len(messages)
		}
		windows = append(windows, Window{Messages: messages[i:end]})
	}
	return windows
}

// Live windowing (§8.3's "5-minute" option).
const (
	DefaultLiveWindowDuration = 5 * time.Minute
	MaxLiveWindowMessages     = 10 // safety cap: a rapid-fire burst within 5 minutes still closes at 10 messages
)

// LiveWindower accumulates live messages into a time-bounded window. Time-
// based windowing is chosen for live traffic instead of the historical
// path's message-count rule because live message density is uneven — a
// burst of rapid back-and-forth should be grouped by proximity in time,
// and a long gap between messages shouldn't force a window to stay open
// just to reach a message-count target. This mirrors how proposal §4.3
// already splits live vs. historical handling for a different pipeline
// stage (image processing) on the same live/historical axis — applying it
// here too is consistent with an existing pattern, not a new one.
//
// MaxLiveWindowMessages exists so a genuinely rapid-fire live conversation
// (well under 5 minutes) still closes a window at a bounded size, rather
// than growing unboundedly within the time limit.
type LiveWindower struct {
	duration time.Duration
	maxSize  int
	pending  []WindowMessage
}

func NewLiveWindower() *LiveWindower {
	return &LiveWindower{duration: DefaultLiveWindowDuration, maxSize: MaxLiveWindowMessages}
}

// Add appends msg to the pending window. If msg falls within the current
// window's time/count bounds, it's added and Add returns (nil, false). If
// msg would exceed those bounds, the existing pending window is closed and
// returned (without msg), msg starts the next window, and Add returns
// (completedWindow, true) — a message that arrives after the bound belongs
// to the next window, not force-fit into the one it broke.
func (w *LiveWindower) Add(msg WindowMessage) (*Window, bool) {
	if len(w.pending) == 0 {
		w.pending = append(w.pending, msg)
		return nil, false
	}

	elapsed := msg.Timestamp.Sub(w.pending[0].Timestamp)
	if elapsed > w.duration || len(w.pending) >= w.maxSize {
		completed := Window{Messages: w.pending}
		w.pending = []WindowMessage{msg}
		return &completed, true
	}

	w.pending = append(w.pending, msg)
	return nil, false
}

// Flush force-closes the current pending window (e.g. on idle detection or
// shutdown) even if it hasn't hit its time or count bound yet. Returns nil
// if nothing is pending.
func (w *LiveWindower) Flush() *Window {
	if len(w.pending) == 0 {
		return nil
	}
	completed := Window{Messages: w.pending}
	w.pending = nil
	return &completed
}
