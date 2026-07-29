package extraction

import (
	"testing"
	"time"
)

func msgAt(t time.Time, text string) WindowMessage {
	return WindowMessage{Speaker: "Marco", Timestamp: t, Text: text}
}

func TestWindowByCount_EvenDivision(t *testing.T) {
	base := time.Now()
	var msgs []WindowMessage
	for i := 0; i < 16; i++ {
		msgs = append(msgs, msgAt(base.Add(time.Duration(i)*time.Minute), "hi"))
	}

	windows := WindowByCount(msgs, 8)
	if len(windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(windows))
	}
	if len(windows[0].Messages) != 8 || len(windows[1].Messages) != 8 {
		t.Errorf("expected 8/8 split, got %d/%d", len(windows[0].Messages), len(windows[1].Messages))
	}
}

func TestWindowByCount_RemainderNotDropped(t *testing.T) {
	base := time.Now()
	var msgs []WindowMessage
	for i := 0; i < 10; i++ {
		msgs = append(msgs, msgAt(base.Add(time.Duration(i)*time.Minute), "hi"))
	}

	windows := WindowByCount(msgs, 8)
	if len(windows) != 2 {
		t.Fatalf("expected 2 windows, got %d", len(windows))
	}
	if len(windows[0].Messages) != 8 {
		t.Errorf("expected first window of 8, got %d", len(windows[0].Messages))
	}
	if len(windows[1].Messages) != 2 {
		t.Errorf("expected trailing remainder window of 2, got %d — remainder must not be dropped", len(windows[1].Messages))
	}
}

func TestWindowByCount_Empty(t *testing.T) {
	windows := WindowByCount(nil, 8)
	if len(windows) != 0 {
		t.Errorf("expected 0 windows for empty input, got %d", len(windows))
	}
}

func TestWindowByCount_DefaultsWhenSizeNotPositive(t *testing.T) {
	base := time.Now()
	var msgs []WindowMessage
	for i := 0; i < DefaultHistoricalWindowSize; i++ {
		msgs = append(msgs, msgAt(base.Add(time.Duration(i)*time.Minute), "hi"))
	}

	windows := WindowByCount(msgs, 0)
	if len(windows) != 1 || len(windows[0].Messages) != DefaultHistoricalWindowSize {
		t.Errorf("expected size<=0 to fall back to DefaultHistoricalWindowSize=%d, got %+v", DefaultHistoricalWindowSize, windows)
	}
}

func TestLiveWindower_AccumulatesWithinTimeBound(t *testing.T) {
	w := NewLiveWindower()
	base := time.Now()

	for i := 0; i < 4; i++ {
		completed, closed := w.Add(msgAt(base.Add(time.Duration(i)*time.Minute), "hi"))
		if closed {
			t.Fatalf("message %d unexpectedly closed the window: %+v", i, completed)
		}
	}

	flushed := w.Flush()
	if flushed == nil || len(flushed.Messages) != 4 {
		t.Fatalf("expected a flushed window of 4 messages, got %+v", flushed)
	}
}

func TestLiveWindower_ClosesOnTimeBound(t *testing.T) {
	w := NewLiveWindower()
	base := time.Now()

	w.Add(msgAt(base, "first"))
	w.Add(msgAt(base.Add(2*time.Minute), "second"))
	completed, closed := w.Add(msgAt(base.Add(6*time.Minute), "outside the 5-minute window"))

	if !closed {
		t.Fatal("expected the window to close once a message arrived past the 5-minute bound")
	}
	if completed == nil || len(completed.Messages) != 2 {
		t.Fatalf("expected the closed window to contain the first 2 messages, got %+v", completed)
	}

	// the message that triggered the close should start the next window,
	// not be dropped or force-included in the old one
	flushed := w.Flush()
	if flushed == nil || len(flushed.Messages) != 1 || flushed.Messages[0].Text != "outside the 5-minute window" {
		t.Fatalf("expected the triggering message to start the next window, got %+v", flushed)
	}
}

func TestLiveWindower_ClosesOnMessageCountBound(t *testing.T) {
	w := NewLiveWindower()
	base := time.Now()

	var lastCompleted *Window
	var lastClosed bool
	for i := 0; i < MaxLiveWindowMessages+1; i++ {
		// all within the same minute, well under the 5-minute time bound
		lastCompleted, lastClosed = w.Add(msgAt(base.Add(time.Duration(i)*time.Second), "burst"))
	}

	if !lastClosed {
		t.Fatal("expected the window to close on hitting MaxLiveWindowMessages even though the time bound wasn't reached")
	}
	if lastCompleted == nil || len(lastCompleted.Messages) != MaxLiveWindowMessages {
		t.Fatalf("expected the closed window to contain exactly %d messages, got %+v", MaxLiveWindowMessages, lastCompleted)
	}
}

func TestLiveWindower_FlushEmptyReturnsNil(t *testing.T) {
	w := NewLiveWindower()
	if got := w.Flush(); got != nil {
		t.Errorf("expected Flush() on an empty windower to return nil, got %+v", got)
	}
}
