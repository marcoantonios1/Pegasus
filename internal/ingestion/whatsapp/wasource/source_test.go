package wasource

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp/wasource/wasourcetest"
)

// --- event builders ---

var (
	ownJID     = types.NewADJID("15550001111", 0, 7) // device-suffixed, like Store.ID
	ownPN      = types.NewJID("15550001111", types.DefaultUserServer)
	contactPN  = types.NewJID("15552223333", types.DefaultUserServer)
	contactDev = types.NewADJID("15552223333", 0, 4)
	contactLID = types.NewJID("88887777", types.HiddenUserServer)
)

func textMsg(s string) *waE2E.Message { return &waE2E.Message{Conversation: proto.String(s)} }

func msgEvent(chat, sender types.JID, fromMe bool, m *waE2E.Message) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: fromMe},
			ID:            types.MessageID(fmt.Sprintf("ID%d", time.Now().UnixNano())),
			Timestamp:     time.Now(),
		},
		Message: m,
	}
}

type sinkRecorder struct {
	mu   sync.Mutex
	msgs []ingestion.RawMessage
	err  func(n int) error
	n    int
}

func (r *sinkRecorder) sink(m ingestion.RawMessage) error {
	r.mu.Lock()
	r.n++
	n := r.n
	errFn := r.err
	r.mu.Unlock()
	if errFn != nil {
		if err := errFn(n); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, m)
	r.mu.Unlock()
	return nil
}

func (r *sinkRecorder) got() []ingestion.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ingestion.RawMessage(nil), r.msgs...)
}

type logBuf struct {
	mu    sync.Mutex
	lines []string
}

func (l *logBuf) logf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *logBuf) all() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func startSource(t *testing.T, fc *wasourcetest.FakeClient, rs *sinkRecorder, cfg Config) (*Source, *logBuf) {
	t.Helper()
	lb := &logBuf{}
	cfg.Logf = lb.logf
	if cfg.ReconnectGrace == 0 {
		cfg.ReconnectGrace = time.Millisecond
	}
	if cfg.ReconnectBackoffBase == 0 {
		cfg.ReconnectBackoffBase = time.Millisecond
		cfg.ReconnectBackoffMax = 5 * time.Millisecond
	}
	if cfg.SinkRetryBackoff == 0 {
		cfg.SinkRetryBackoff = time.Millisecond
	}
	s := NewWithClient(fc, cfg)
	if err := s.Start(context.Background(), rs.sink); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s, lb
}

// --- tests ---

func TestDropsAndForwards(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	rs := &sinkRecorder{}
	s, lb := startSource(t, fc, rs, Config{})

	group := types.NewJID("12036302", types.GroupServer)
	bcast := types.NewJID("1234567", types.BroadcastServer)
	news := types.NewJID("120363", types.NewsletterServer)
	reaction := &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("👍")}}
	voice := &waE2E.Message{AudioMessage: &waE2E.AudioMessage{URL: proto.String("https://mmg/x")}}
	image := &waE2E.Message{ImageMessage: &waE2E.ImageMessage{URL: proto.String("https://mmg/i"), Caption: proto.String("cap")}}
	edit := msgEvent(contactPN, contactPN, false, textMsg("edited"))
	edit.IsEdit = true

	fc.Emit(msgEvent(group, contactPN, false, textMsg("in a group")))
	fc.Emit(msgEvent(types.StatusBroadcastJID, contactPN, false, textMsg("status")))
	fc.Emit(msgEvent(bcast, contactPN, false, textMsg("list")))
	fc.Emit(msgEvent(news, contactPN, false, textMsg("news")))
	fc.Emit(msgEvent(contactPN, contactPN, false, reaction))
	fc.Emit(msgEvent(contactPN, contactPN, false, voice))
	fc.Emit(edit)
	fc.Emit(&events.HistorySync{})
	fc.Emit(&events.Presence{From: contactPN})
	fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("hello")))
	fc.Emit(msgEvent(contactPN, contactPN, false, image))

	snap := s.Stats()
	for reason, want := range map[string]int64{
		DropGroup: 1, DropStatusBroadcast: 1, DropBroadcastList: 1, DropNewsletter: 1,
		DropUnsupportedType: 1, DropVoiceLive: 1, DropEdit: 1, DropHistorySync: 1,
	} {
		if snap.Dropped[reason] != want {
			t.Errorf("dropped[%s] = %d, want %d (all: %v)", reason, snap.Dropped[reason], want, snap.Dropped)
		}
	}
	if snap.OtherEvents["Presence"] != 1 {
		t.Errorf("other_events[Presence] = %d, want 1 (%v)", snap.OtherEvents["Presence"], snap.OtherEvents)
	}
	got := rs.got()
	if len(got) != 2 || snap.Forwarded != 2 {
		t.Fatalf("forwarded %d (stats %d), want 2: %+v", len(got), snap.Forwarded, got)
	}
	if got[0].MediaType != ingestion.MediaTypeText || *got[0].Text != "hello" {
		t.Errorf("text message: %+v", got[0])
	}
	if got[1].MediaType != ingestion.MediaTypeImage || *got[1].Text != "cap" || *got[1].MediaURL != "https://mmg/i" {
		t.Errorf("image message: %+v", got[1])
	}
	for _, secret := range []string{"in a group", "status", "hello", "15552223333"} {
		if strings.Contains(lb.all(), secret) {
			t.Errorf("logs contain %q:\n%s", secret, lb.all())
		}
	}
}

func TestOtherEventKeysCapped(t *testing.T) {
	st := newStats()
	for i := 0; i < maxEventTypeKeys+10; i++ {
		st.otherEvent(fmt.Sprintf("T%d", i))
	}
	snap := st.Snapshot()
	if len(snap.OtherEvents) != maxEventTypeKeys+1 || snap.OtherEvents[otherEventOverflow] != 10 {
		t.Errorf("got %d keys, overflow %d", len(snap.OtherEvents), snap.OtherEvents[otherEventOverflow])
	}
}

func TestCanonicalization(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	mappedLID := types.NewJID("11112222", types.HiddenUserServer)
	fc.LIDs[mappedLID] = contactPN
	rs := &sinkRecorder{}
	s, _ := startSource(t, fc, rs, Config{})

	if got := s.AccountID(); got != ownPN.String() || s.Platform() != "whatsapp" {
		t.Fatalf("AccountID = %q on %q, want the device-free %s", got, s.Platform(), ownPN)
	}

	// 1. Incoming from a device-suffixed sender.
	fc.Emit(msgEvent(contactDev, contactDev, false, textMsg("a")))
	// 2. Incoming in LID form with SenderAlt.
	e := msgEvent(contactLID, contactLID, false, textMsg("b"))
	e.Info.SenderAlt = contactDev
	fc.Emit(e)
	// 3. Incoming LID the store can map.
	fc.Emit(msgEvent(mappedLID, mappedLID, false, textMsg("c")))
	// 4. Own message from another of my devices, to a LID chat with RecipientAlt.
	mine := msgEvent(contactLID, types.NewADJID("15550001111", 0, 9), true, textMsg("d"))
	mine.Info.RecipientAlt = contactPN
	fc.Emit(mine)
	// 5. Own message sent from my LID identity.
	mine2 := msgEvent(contactPN, types.NewJID("55554444", types.HiddenUserServer), true, textMsg("e"))
	fc.Emit(mine2)
	// 6. Unmappable LID: kept and counted.
	unknown := types.NewJID("99990000", types.HiddenUserServer)
	fc.Emit(msgEvent(unknown, unknown, false, textMsg("f")))

	got := rs.got()
	if len(got) != 6 {
		t.Fatalf("forwarded %d, want 6", len(got))
	}
	want := []struct{ sender, chat string }{
		{contactPN.String(), contactPN.String()},
		{contactPN.String(), contactPN.String()},
		{contactPN.String(), contactPN.String()},
		{ownPN.String(), contactPN.String()},
		{ownPN.String(), contactPN.String()},
		{unknown.String(), unknown.String()},
	}
	for i, w := range want {
		if got[i].SenderExternalID != w.sender || got[i].ConversationID != w.chat {
			t.Errorf("msg %d: sender=%s chat=%s, want sender=%s chat=%s", i+1, got[i].SenderExternalID, got[i].ConversationID, w.sender, w.chat)
		}
	}
	if n := s.Stats().LIDUnmapped; n != 2 { // sender + chat of message 6
		t.Errorf("lid_unmapped = %d, want 2", n)
	}
}

func TestReconnect_AfterDisconnect(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	fc.ConnectErr = func(n int) error {
		if n == 2 { // first supervised attempt fails
			return errors.New("dial: network down")
		}
		return nil
	}
	s, lb := startSource(t, fc, &sinkRecorder{}, Config{})
	if st := healthState(s); st != StateConnected {
		t.Fatalf("after start: %s", st)
	}

	fc.Emit(&events.Disconnected{})
	waitFor(t, func() bool { return healthState(s) == StateConnected && fc.ConnectCount() == 3 }, "reconnect")

	ready, _, info := s.Health()
	m := info.(map[string]any)
	if !ready || m["reconnects"].(int64) != 1 || m["connect_attempts"].(int64) != 2 {
		t.Errorf("health after reconnect: ready=%v %v", ready, m)
	}
	if !strings.Contains(lb.all(), "reconnect attempt 1 failed") || !strings.Contains(lb.all(), "reconnected after") {
		t.Errorf("expected attempt and reconnect logs:\n%s", lb.all())
	}
}

func TestReconnect_TransientDisconnectStaysReady(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	fc.ConnectErr = func(n int) error {
		if n > 1 {
			return errors.New("still down")
		}
		return nil
	}
	s, _ := startSource(t, fc, &sinkRecorder{}, Config{ReconnectGrace: time.Hour})
	fc.Emit(&events.Disconnected{})
	if ready, _, _ := s.Health(); !ready {
		t.Error("a transient disconnect must not fail readiness")
	}
}

func TestBackoffWaitInterruptedByCancel(t *testing.T) {
	s := NewWithClient(wasourcetest.New(ownJID), Config{ReconnectBackoffBase: time.Hour, ReconnectBackoffMax: time.Hour, ReconnectGrace: time.Hour, Logf: t.Logf})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.supervise(ctx); close(done) }()
	s.markDisconnected() // supervisor now sits in its (1h) grace wait
	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case <-done:
		if d := time.Since(start); d > 100*time.Millisecond {
			t.Errorf("cancel took %s", d)
		}
	case <-time.After(time.Second):
		t.Fatal("supervisor did not stop on cancel during a wait")
	}
}

func TestBackoffCappedWithJitter(t *testing.T) {
	s := NewWithClient(wasourcetest.New(ownJID), Config{ReconnectBackoffBase: time.Second, ReconnectBackoffMax: 10 * time.Second, Logf: t.Logf})
	s.jitter = func() float64 { return 0.5 } // midpoint: no jitter
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second} {
		if got := s.backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %s, want %s", attempt, got, want)
		}
	}
	s.jitter = func() float64 { return 1 }
	if got := s.backoff(10); got != 12*time.Second {
		t.Errorf("max jitter: %s, want 12s", got)
	}
}

func TestFatalStates(t *testing.T) {
	cases := map[string]any{
		"logged_out":      &events.LoggedOut{Reason: events.ConnectFailureLoggedOut},
		"stream_replaced": &events.StreamReplaced{},
		"client_outdated": &events.ClientOutdated{},
		"temporary_ban":   &events.TemporaryBan{Code: events.TempBanBlockedByUsers},
		"connect_failure": &events.ConnectFailure{Reason: events.ConnectFailureMainDeviceGone},
	}
	for name, evt := range cases {
		t.Run(name, func(t *testing.T) {
			fc := wasourcetest.New(ownJID)
			s, lb := startSource(t, fc, &sinkRecorder{}, Config{})
			fc.Emit(evt)
			fc.Emit(&events.Disconnected{}) // what whatsmeow does next
			time.Sleep(30 * time.Millisecond)

			ready, reason, _ := s.Health()
			if ready || !strings.Contains(reason, name) {
				t.Errorf("health: ready=%v reason=%q", ready, reason)
			}
			if fc.ConnectCount() != 1 {
				t.Errorf("reconnect attempted after fatal state: %d connects", fc.ConnectCount())
			}
			if fc.AutoReconnectEnabled() {
				t.Error("whatsmeow auto-reconnect left enabled")
			}
			if c := strings.Count(lb.all(), "ERROR: fatal state"); c != 1 {
				t.Errorf("want exactly one ERROR line, got %d:\n%s", c, lb.all())
			}
			fc.Emit(&events.LoggedOut{}) // a second fatal event doesn't re-log
			if c := strings.Count(lb.all(), "ERROR: fatal state"); c != 1 {
				t.Errorf("repeat fatal event logged again")
			}
		})
	}

	// Transient connect failures are not fatal.
	fc := wasourcetest.New(ownJID)
	s, _ := startSource(t, fc, &sinkRecorder{}, Config{})
	fc.Emit(&events.ConnectFailure{Reason: events.ConnectFailureServiceUnavailable})
	if ready, _, _ := s.Health(); !ready {
		t.Error("503 connect failure must be transient")
	}
}

func TestSinkRetry(t *testing.T) {
	t.Run("transient error retried", func(t *testing.T) {
		fc := wasourcetest.New(ownJID)
		rs := &sinkRecorder{err: func(n int) error {
			if n == 1 {
				return errors.New("db blip")
			}
			return nil
		}}
		s, _ := startSource(t, fc, rs, Config{})
		fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("hi")))
		snap := s.Stats()
		if len(rs.got()) != 1 || snap.Forwarded != 1 || snap.SinkRetries != 1 || snap.SinkFailed != 0 {
			t.Errorf("got %d stored, stats %+v", len(rs.got()), snap)
		}
	})
	t.Run("persistent error counted and logged without content", func(t *testing.T) {
		fc := wasourcetest.New(ownJID)
		rs := &sinkRecorder{err: func(int) error {
			return fmt.Errorf("resolve sender %q: boom", contactPN.String())
		}}
		s, lb := startSource(t, fc, rs, Config{SinkAttempts: 3})
		e := msgEvent(contactPN, contactPN, false, textMsg("very private text"))
		fc.Emit(e)
		snap := s.Stats()
		if snap.SinkFailed != 1 || snap.SinkRetries != 2 || rs.n != 3 {
			t.Errorf("stats %+v, attempts %d", snap, rs.n)
		}
		logs := lb.all()
		if !strings.Contains(logs, string(e.Info.ID)) || !strings.Contains(logs, "sink_failed") {
			t.Errorf("expected a sink_failed line with the message ID:\n%s", logs)
		}
		if strings.Contains(logs, "very private text") || strings.Contains(logs, "15552223333") {
			t.Errorf("log leaked content or a phone number:\n%s", logs)
		}
	})
	t.Run("closed sink not retried", func(t *testing.T) {
		fc := wasourcetest.New(ownJID)
		rs := &sinkRecorder{err: func(int) error { return ingestion.ErrSinkClosed }}
		s, _ := startSource(t, fc, rs, Config{})
		fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("x")))
		if rs.n != 1 || s.Stats().SinkFailed != 1 {
			t.Errorf("attempts %d, stats %+v", rs.n, s.Stats())
		}
	})
}

func TestStop_DrainsInFlightThenRejects(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	entered, release := make(chan struct{}), make(chan struct{})
	rs := &sinkRecorder{err: func(n int) error {
		if n == 1 {
			close(entered)
			<-release
		}
		return nil
	}}
	s := NewWithClient(fc, Config{Logf: t.Logf, ReconnectGrace: time.Hour})
	if err := s.Start(context.Background(), rs.sink); err != nil {
		t.Fatal(err)
	}
	go fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("slow")))
	<-entered

	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(context.Background()) }()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a handler call was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if len(rs.got()) != 1 {
		t.Errorf("in-flight message lost")
	}
	fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("late")))
	if len(rs.got()) != 1 || s.Stats().Dropped[DropAfterStop] != 1 {
		t.Errorf("event after Stop was not rejected: %+v", s.Stats())
	}
	if fc.DisconnectCount() != 1 {
		t.Errorf("Stop should disconnect once, got %d", fc.DisconnectCount())
	}
}

func healthState(s *Source) string {
	_, _, info := s.Health()
	return info.(map[string]any)["state"].(string)
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestDuplicateCountedNotForwardedOrRetried(t *testing.T) {
	fc := wasourcetest.New(ownJID)
	rs := &sinkRecorder{err: func(int) error { return ingestion.ErrDuplicate }}
	s, lb := startSource(t, fc, rs, Config{})
	fc.Emit(msgEvent(contactPN, contactPN, false, textMsg("again")))
	st := s.Stats()
	if rs.n != 1 || st.Duplicates != 1 || st.Forwarded != 0 || st.SinkFailed != 0 || st.SinkRetries != 0 {
		t.Errorf("attempts %d, stats %+v", rs.n, st)
	}
	if strings.Contains(lb.all(), "sink error") {
		t.Errorf("a duplicate must not be logged as a sink error:\n%s", lb.all())
	}
}

func TestLogoutClearsSession(t *testing.T) {
	for name, tc := range map[string]struct {
		evt   any
		clear bool
	}{
		"logged_out":            {&events.LoggedOut{Reason: events.ConnectFailureLoggedOut}, true},
		"connect_failure_403":   {&events.ConnectFailure{Reason: events.ConnectFailureMainDeviceGone}, true},
		"stream_replaced":       {&events.StreamReplaced{}, false},
		"temporary_ban":         {&events.TemporaryBan{Code: events.TempBanBlockedByUsers}, false},
		"transient_failure_503": {&events.ConnectFailure{Reason: events.ConnectFailureServiceUnavailable}, false},
	} {
		t.Run(name, func(t *testing.T) {
			fc := wasourcetest.New(ownJID)
			startSource(t, fc, &sinkRecorder{}, Config{})
			fc.Emit(tc.evt)
			if got := fc.ClearedCount() > 0; got != tc.clear {
				t.Errorf("session cleared = %v, want %v", got, tc.clear)
			}
		})
	}
}
