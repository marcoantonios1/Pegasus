// Package reflection implements the Reflection Engine's trigger mechanism
// (proposal §9.1) — the idle-detection/bounded-fallback logic that decides
// WHEN a consolidation pass runs. It does not implement the consolidation
// work itself (dedup, batch decay, stat recompute, pruning flags, §9.2) —
// that's injected as a callback and is the subject of separate, later
// issues. This package's only job is calling that callback at the right
// time, with a context that gets cancelled if live activity resumes
// mid-pass.
package reflection

import (
	"context"
	"sync"
	"time"
)

// Defaults per proposal §9.1. All are overridable via TriggerConfig.
const (
	DefaultIdleThreshold = 35 * time.Minute // "e.g. 30-45 minutes of no new live activity" — picked the midpoint
	DefaultMaxInterval   = 20 * time.Hour   // "e.g. 18-24 hours since the last run" — picked the midpoint
	DefaultPollInterval  = time.Minute      // how often the trigger checks elapsed time; not specified by §9.1, chosen to be well under IdleThreshold without polling excessively
)

// Logger is the structured logging interface this package needs, matching
// Costguard's internal/logging.Log shape (Info(event string, fields
// map[string]any)) per this issue's instruction to match that pattern.
// Pegasus has no logging package of its own yet, so this is a local
// interface rather than a dependency on Costguard's package (a different
// module entirely) — swap in a real implementation via Trigger.Log once
// one exists. A nil Logger disables logging.
type Logger interface {
	Info(event string, fields map[string]any)
}

// TriggerConfig carries the two duration knobs §9.1 names. Zero values
// fall back to the DefaultX constants above, matching the zero-value-
// fallback pattern already used in Costguard's report/scheduler.go
// (`if interval <= 0`).
type TriggerConfig struct {
	IdleThreshold time.Duration
	MaxInterval   time.Duration
	PollInterval  time.Duration
}

// Trigger implements §9.1's idle-detection rules:
//   - Idle detection: RecordActivity() tracks a rolling last-activity
//     timestamp.
//   - Debounced: implemented by polling elapsed-since-last-activity
//     rather than a per-message timer — a lull that ends before
//     IdleThreshold is crossed just means the next poll sees a low idle
//     duration again. No explicit reset-on-activity logic needed; it
//     falls out of checking elapsed time rather than firing a timer.
//   - Interruptible: RecordActivity() cancels the in-flight pass's
//     context if one is running. The injected callback receives that
//     context and is responsible for checking ctx.Done() at reasonable
//     checkpoints — this package only guarantees the cancellation is
//     wired, not that the callback reacts to it (that's the
//     consolidation issue's job).
//   - Bounded fallback: checked in the same poll as idle detection (not
//     a second ticker), so the two conditions can't race and
//     double-trigger.
type Trigger struct {
	// Log receives one line per state transition (pass started/finished/
	// cancelled). Optional; nil disables logging. Public field, not a
	// constructor parameter — matches this codebase's existing pattern
	// for optional loggers (e.g. extraction.Extractor.Logger).
	Log Logger

	cfg           TriggerConfig
	onConsolidate func(ctx context.Context)

	// now is swappable so tests can simulate elapsed time without real
	// sleeps (see trigger_test.go's fakeClock) — not exported, same-
	// package tests set it directly after NewTrigger.
	now func() time.Time

	mu           sync.Mutex
	lastActivity time.Time
	lastRunAt    time.Time
	// idleHandled tracks whether a pass has already fired for the current
	// idle stretch, so staying idle doesn't re-trigger on every poll tick.
	// RecordActivity resets it to false. It's a dedicated flag rather than
	// comparing lastRunAt/lastActivity timestamps (an earlier version did
	// that) because at construction both are set from two back-to-back
	// now() calls, making lastRunAt >= lastActivity from the start — a
	// timestamp comparison would then treat the very first idle period as
	// already "handled" before any pass ever ran.
	idleHandled bool
	running     bool
	cancel      context.CancelFunc
}

// NewTrigger builds a Trigger. onConsolidate is called (with a derived,
// cancellable context — see RecordActivity) whenever the idle threshold
// is crossed or the max-interval fallback fires. It is never called
// concurrently with itself: Start's poll loop only calls it synchronously,
// and checks a running flag first.
func NewTrigger(cfg TriggerConfig, onConsolidate func(ctx context.Context)) *Trigger {
	if cfg.IdleThreshold <= 0 {
		cfg.IdleThreshold = DefaultIdleThreshold
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = DefaultMaxInterval
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}

	now := time.Now
	return &Trigger{
		cfg:           cfg,
		onConsolidate: onConsolidate,
		now:           now,
		lastActivity:  now(),
		lastRunAt:     now(),
	}
}

// Start begins the polling goroutine, matching Costguard's Start/ticker/
// select-on-ctx.Done shape (report/scheduler.go) — idle-based rather than
// fixed-interval, so the internals differ (a poll-and-check loop instead
// of "do the thing on every tick"), but the outer shape (ticker + select
// + goroutine, stopped by ctx cancellation) is the same.
func (t *Trigger) Start(ctx context.Context) {
	ticker := time.NewTicker(t.cfg.PollInterval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				t.logInfo("reflection_trigger_stopped", nil)
				return
			case <-ticker.C:
				t.checkAndMaybeRun(ctx)
			}
		}
	}()

	t.logInfo("reflection_trigger_started", map[string]any{
		"idle_threshold": t.cfg.IdleThreshold.String(),
		"max_interval":   t.cfg.MaxInterval.String(),
		"poll_interval":  t.cfg.PollInterval.String(),
	})
}

// RecordActivity marks that live activity just happened — called from
// live message processing and app interaction (those call sites belong to
// other components; this method just needs to exist and be safe to call
// concurrently, which it is via the mutex). If a consolidation pass is
// currently running, its context is cancelled so it yields immediately
// rather than running to completion — live message handling always takes
// priority over background consolidation, per §9.1.
func (t *Trigger) RecordActivity() {
	t.mu.Lock()
	t.lastActivity = t.now()
	t.idleHandled = false
	wasRunning := t.running
	if wasRunning && t.cancel != nil {
		t.cancel()
	}
	t.mu.Unlock()

	if wasRunning {
		t.logInfo("reflection_pass_cancelled", map[string]any{"reason": "activity_during_pass"})
	}
}

// checkAndMaybeRun is one poll tick: decide whether to start a
// consolidation pass, and if so, run it synchronously (within this
// goroutine, not a spawned one) so the running flag genuinely prevents a
// second pass from starting on the next tick, and so ctx cancellation
// from the parent (Start's ctx) still propagates into a running pass via
// context.WithCancel's normal parent->child propagation even though we're
// not selecting on ctx.Done() again until this call returns.
func (t *Trigger) checkAndMaybeRun(parentCtx context.Context) {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}

	now := t.now()
	idleFor := now.Sub(t.lastActivity)
	sinceLastRun := now.Sub(t.lastRunAt)

	// idleHandled guards against re-triggering a new pass on every single
	// poll tick for as long as an idle period continues — it's set once a
	// pass fires and cleared by RecordActivity, so a given idle stretch
	// only ever fires once. The max-interval condition below doesn't need
	// an equivalent guard: lastRunAt is updated at the end of every pass
	// regardless of which condition fired it, so sinceLastRun resets to
	// ~0 immediately after any pass and a full MaxInterval has to
	// re-elapse before it can fire again.
	var reason string
	switch {
	case idleFor >= t.cfg.IdleThreshold && !t.idleHandled:
		reason = "idle_threshold"
	case sinceLastRun >= t.cfg.MaxInterval:
		reason = "max_interval_fallback"
	default:
		t.mu.Unlock()
		return
	}

	runCtx, cancel := context.WithCancel(parentCtx)
	t.running = true
	t.cancel = cancel
	t.idleHandled = true
	t.mu.Unlock()

	t.logInfo("reflection_pass_started", map[string]any{
		"reason":         reason,
		"idle_for":       idleFor.String(),
		"since_last_run": sinceLastRun.String(),
	})

	t.onConsolidate(runCtx)

	t.mu.Lock()
	t.running = false
	t.cancel = nil
	t.lastRunAt = t.now()
	t.mu.Unlock()

	t.logInfo("reflection_pass_finished", map[string]any{"reason": reason})
}

func (t *Trigger) logInfo(event string, fields map[string]any) {
	if t.Log != nil {
		t.Log.Info(event, fields)
	}
}
