package wasource

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types/events"
)

var fastSettle = settleTimes{reconnect: 200 * time.Millisecond, min: 40 * time.Millisecond, quiet: 30 * time.Millisecond, max: time.Second}

func TestWaitForInitialSync_SettlesAfterQuietHistorySync(t *testing.T) {
	evts := make(chan any, 10)
	evts <- &events.Connected{}
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(15 * time.Millisecond)
			evts <- &events.HistorySync{}
		}
	}()
	start := time.Now()
	n, err := waitForInitialSync(context.Background(), evts, fastSettle)
	if err != nil || n != 3 {
		t.Fatalf("got %d, %v; want 3, nil", n, err)
	}
	// Must outlast the last history sync (~45ms) by the quiet period.
	if d := time.Since(start); d < 70*time.Millisecond || d > 500*time.Millisecond {
		t.Errorf("settled after %s", d)
	}
}

func TestWaitForInitialSync_StaysAtLeastMin(t *testing.T) {
	evts := make(chan any, 1)
	evts <- &events.Connected{}
	start := time.Now()
	if n, err := waitForInitialSync(context.Background(), evts, fastSettle); err != nil || n != 0 {
		t.Fatalf("got %d, %v", n, err)
	}
	if d := time.Since(start); d < fastSettle.min {
		t.Errorf("disconnected after %s, before the %s minimum", d, fastSettle.min)
	}
}

func TestWaitForInitialSync_Failures(t *testing.T) {
	evts := make(chan any, 2)
	evts <- &events.Connected{}
	evts <- &events.LoggedOut{}
	if _, err := waitForInitialSync(context.Background(), evts, fastSettle); !errors.Is(err, errLoggedOutWhilePairing) {
		t.Errorf("logout during sync: %v", err)
	}

	if _, err := waitForInitialSync(context.Background(), make(chan any), fastSettle); err == nil {
		t.Error("no reconnect: want an error")
	}

	// A continuous stream of history syncs is capped by max.
	evts = make(chan any, 1)
	evts <- &events.Connected{}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case evts <- &events.HistorySync{}:
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	start := time.Now()
	if _, err := waitForInitialSync(context.Background(), evts, fastSettle); err != nil {
		t.Errorf("capped wait: %v", err)
	}
	if d := time.Since(start); d > 2*fastSettle.max {
		t.Errorf("max cap not honored: %s", d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waitForInitialSync(ctx, make(chan any), fastSettle); !errors.Is(err, context.Canceled) {
		t.Errorf("cancel: %v", err)
	}
}
