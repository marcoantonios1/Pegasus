package reflection

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// Smoke test using the REAL, unoverridden now field (time.Now) and real
// wall-clock sleeps — everything else in this package's test suite
// overrides `now` with a fake clock, so this is the only test that
// exercises the actual production code path (default now, real ticker,
// real elapsed time), just with small durations instead of 35min/20h.
func TestTrigger_RealClockSmoke(t *testing.T) {
	var calls int32
	trig := NewTrigger(TriggerConfig{
		IdleThreshold: 150 * time.Millisecond,
		MaxInterval:   10 * time.Second,
		PollInterval:  20 * time.Millisecond,
	}, func(ctx context.Context) {
		atomic.AddInt32(&calls, 1)
		fmt.Println("consolidation callback fired (real clock)")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trig.Start(ctx)

	// Debounce check: keep sending real activity for 300ms (well past
	// IdleThreshold if it weren't for the resets) and confirm no fire.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		trig.RecordActivity()
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("expected no fire while real activity keeps arriving, got %d calls", got)
	}

	// Now go quiet and let real idle time actually elapse.
	time.Sleep(400 * time.Millisecond)

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected exactly 1 real-clock fire after going idle, got %d", got)
	}
}
