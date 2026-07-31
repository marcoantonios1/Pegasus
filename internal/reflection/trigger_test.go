package reflection

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock lets tests simulate elapsed time instantly instead of sleeping
// for real minutes/hours — the actual poll ticker still runs on real time
// (configured with a short PollInterval in these tests, e.g. 10ms), but
// what the trigger COMPUTES as "idle for" / "since last run" comes from
// this clock, which the test controls directly.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for: %s", msg)
	}
}

func newTestTrigger(clock *fakeClock, onConsolidate func(ctx context.Context)) *Trigger {
	trig := NewTrigger(TriggerConfig{
		IdleThreshold: 35 * time.Minute,
		MaxInterval:   20 * time.Hour,
		PollInterval:  10 * time.Millisecond, // fast real ticker so tests don't wait long
	}, onConsolidate)
	trig.now = clock.Now
	return trig
}

// TestTrigger_IdleThresholdCrossed_CallsConsolidateOnce covers requirement 12.
func TestTrigger_IdleThresholdCrossed_CallsConsolidateOnce(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var calls int32
	called := make(chan struct{}, 10)

	trig := newTestTrigger(clock, func(ctx context.Context) {
		atomic.AddInt32(&calls, 1)
		called <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	clock.Advance(36 * time.Minute)

	waitForSignal(t, called, time.Second, "consolidation callback to fire")
	time.Sleep(50 * time.Millisecond) // give a few more poll cycles to prove it doesn't fire again

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected consolidation to be called exactly once, got %d", got)
	}
}

// TestTrigger_ActivityBeforeThreshold_Debounces covers requirement 13.
func TestTrigger_ActivityBeforeThreshold_Debounces(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var calls int32

	trig := newTestTrigger(clock, func(ctx context.Context) {
		atomic.AddInt32(&calls, 1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	// Advance close to, but under, the threshold, then record activity —
	// this must reset the idle clock so the next stretch of elapsed time
	// is measured from the reset point, not from the original start.
	clock.Advance(30 * time.Minute)
	time.Sleep(30 * time.Millisecond) // let a few poll cycles observe the pre-reset state
	trig.RecordActivity()

	clock.Advance(30 * time.Minute) // 60min from start, but only 30min since the reset — still under threshold
	time.Sleep(30 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("expected debounce (activity reset the idle clock) to prevent triggering, got %d calls", got)
	}
}

// TestTrigger_DoesNotRefireWhileIdlePersists is not one of the explicitly
// enumerated tests but validates a correctness property the implementation
// specifically has to guard against: without it, staying idle would
// re-trigger a new pass on every poll tick for as long as the idle period
// continues, not once per idle period.
func TestTrigger_DoesNotRefireWhileIdlePersists(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var calls int32
	called := make(chan struct{}, 10)

	trig := newTestTrigger(clock, func(ctx context.Context) {
		atomic.AddInt32(&calls, 1)
		called <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	clock.Advance(36 * time.Minute)
	waitForSignal(t, called, time.Second, "first pass to fire")

	clock.Advance(2 * time.Hour) // stay idle well beyond the threshold, still well under MaxInterval
	time.Sleep(100 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected exactly 1 call while idle persists without new activity, got %d", got)
	}
}

// TestTrigger_ActivityDuringPass_CancelsContext covers requirement 14.
func TestTrigger_ActivityDuringPass_CancelsContext(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	started := make(chan struct{})
	finished := make(chan struct{})
	var mu sync.Mutex
	var gotErr error

	trig := newTestTrigger(clock, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		mu.Lock()
		gotErr = ctx.Err()
		mu.Unlock()
		close(finished)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	clock.Advance(36 * time.Minute)
	waitForSignal(t, started, time.Second, "pass to start")

	trig.RecordActivity()

	waitForSignal(t, finished, time.Second, "pass to observe cancellation and finish")

	mu.Lock()
	defer mu.Unlock()
	if gotErr != context.Canceled {
		t.Errorf("expected ctx.Err() == context.Canceled after RecordActivity during a pass, got %v", gotErr)
	}
}

// TestTrigger_MaxIntervalFallback_FiresIndependently covers requirement 15.
func TestTrigger_MaxIntervalFallback_FiresIndependently(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	called := make(chan struct{}, 10)

	trig := newTestTrigger(clock, func(ctx context.Context) {
		called <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	// Advance well past MaxInterval (20h), but record activity right
	// after — idle time resets to ~0 (well under IdleThreshold) while
	// time-since-last-run (measured from constrution, untouched by
	// RecordActivity) still exceeds MaxInterval. Proves the fallback
	// fires on its own condition, not because idle also happened to be
	// crossed.
	clock.Advance(21 * time.Hour)
	trig.RecordActivity()

	waitForSignal(t, called, time.Second, "max-interval fallback to fire despite recent activity (idle not crossed)")
}

// TestTrigger_ConcurrentRecordActivity_NoRace covers requirement 16 — run
// with `go test -race`.
func TestTrigger_ConcurrentRecordActivity_NoRace(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	trig := newTestTrigger(clock, func(ctx context.Context) {
		<-ctx.Done()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			trig.RecordActivity()
		}()
	}
	wg.Wait()
}

// testLogger records events for TestTrigger_LogsStateTransitions.
type testLogger struct {
	mu     sync.Mutex
	events []string
}

func (l *testLogger) Info(event string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *testLogger) has(event string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.events {
		if e == event {
			return true
		}
	}
	return false
}

// TestTrigger_LogsStateTransitions covers requirement 10.
func TestTrigger_LogsStateTransitions(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	started := make(chan struct{})

	trig := newTestTrigger(clock, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	})
	log := &testLogger{}
	trig.Log = log

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	if !waitUntil(t, func() bool { return log.has("reflection_trigger_started") }, time.Second) {
		t.Fatal("expected reflection_trigger_started to be logged")
	}

	clock.Advance(36 * time.Minute)
	waitForSignal(t, started, time.Second, "pass to start")

	if !waitUntil(t, func() bool { return log.has("reflection_pass_started") }, time.Second) {
		t.Error("expected reflection_pass_started to be logged")
	}

	trig.RecordActivity()

	if !waitUntil(t, func() bool { return log.has("reflection_pass_cancelled") }, time.Second) {
		t.Error("expected reflection_pass_cancelled to be logged")
	}
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
