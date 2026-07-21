package whatsapp

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

func mustJID(user, server string) types.JID {
	return types.NewJID(user, server)
}

func newTestEvent(id string, chat, sender types.JID, fromMe bool, ts time.Time, msg *waE2E.Message) *events.Message {
	return &events.Message{
		Info: types.MessageInfo{
			ID: id,
			MessageSource: types.MessageSource{
				Chat:     chat,
				Sender:   sender,
				IsFromMe: fromMe,
			},
			Timestamp: ts,
		},
		Message: msg,
	}
}

func TestHandleMessage_Text(t *testing.T) {
	chat := mustJID("120363000000000000", "g.us")
	sender := mustJID("15551234567", "s.whatsapp.net")
	ts := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

	var got []ingestion.RawMessage
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		got = append(got, rm)
		return nil
	})

	evt := newTestEvent("MSG1", chat, sender, false, ts, &waE2E.Message{
		Conversation: proto.String("hello there"),
	})
	h.HandleMessage(evt)

	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	rm := got[0]
	if rm.ExternalID != "MSG1" || rm.Platform != "whatsapp" {
		t.Errorf("unexpected id/platform: %+v", rm)
	}
	if rm.ConversationID != chat.String() || rm.SenderExternalID != sender.String() {
		t.Errorf("unexpected conversation/sender: %+v", rm)
	}
	if rm.MediaType != ingestion.MediaTypeText {
		t.Errorf("expected media type text, got %q", rm.MediaType)
	}
	if rm.Text == nil || *rm.Text != "hello there" {
		t.Errorf("unexpected text: %+v", rm.Text)
	}
	if rm.MediaURL != nil {
		t.Errorf("expected nil media url, got %v", *rm.MediaURL)
	}
	if !rm.Timestamp.Equal(ts) {
		t.Errorf("unexpected timestamp: %v", rm.Timestamp)
	}
}

func TestHandleMessage_ExtendedText(t *testing.T) {
	chat := mustJID("120363000000000000", "g.us")
	sender := mustJID("15551234567", "s.whatsapp.net")

	var got []ingestion.RawMessage
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		got = append(got, rm)
		return nil
	})

	evt := newTestEvent("MSG2", chat, sender, false, time.Now(), &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String("reply text"),
		},
	})
	h.HandleMessage(evt)

	if len(got) != 1 || got[0].MediaType != ingestion.MediaTypeText || *got[0].Text != "reply text" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestHandleMessage_Voice(t *testing.T) {
	chat := mustJID("15559999999", "s.whatsapp.net")
	sender := mustJID("15551234567", "s.whatsapp.net")

	var got []ingestion.RawMessage
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		got = append(got, rm)
		return nil
	})

	evt := newTestEvent("MSG3", chat, sender, true, time.Now(), &waE2E.Message{
		AudioMessage: &waE2E.AudioMessage{
			URL: proto.String("https://example.invalid/audio.opus"),
			PTT: proto.Bool(true),
		},
	})
	h.HandleMessage(evt)

	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	rm := got[0]
	if rm.MediaType != ingestion.MediaTypeVoice {
		t.Errorf("expected voice, got %q", rm.MediaType)
	}
	if rm.MediaURL == nil || *rm.MediaURL != "https://example.invalid/audio.opus" {
		t.Errorf("unexpected media url: %v", rm.MediaURL)
	}
	if rm.Text != nil {
		t.Errorf("expected nil text for voice message, got %v", *rm.Text)
	}
}

func TestHandleMessage_Image(t *testing.T) {
	chat := mustJID("15559999999", "s.whatsapp.net")
	sender := mustJID("15551234567", "s.whatsapp.net")

	var got []ingestion.RawMessage
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		got = append(got, rm)
		return nil
	})

	evt := newTestEvent("MSG4", chat, sender, false, time.Now(), &waE2E.Message{
		ImageMessage: &waE2E.ImageMessage{
			URL:     proto.String("https://example.invalid/photo.jpg"),
			Caption: proto.String("check this out"),
		},
	})
	h.HandleMessage(evt)

	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	rm := got[0]
	if rm.MediaType != ingestion.MediaTypeImage {
		t.Errorf("expected image, got %q", rm.MediaType)
	}
	if rm.Text == nil || *rm.Text != "check this out" {
		t.Errorf("unexpected caption: %v", rm.Text)
	}
	if rm.MediaURL == nil || *rm.MediaURL != "https://example.invalid/photo.jpg" {
		t.Errorf("unexpected media url: %v", rm.MediaURL)
	}
}

func TestHandleMessage_OutgoingFlowsThrough(t *testing.T) {
	chat := mustJID("15559999999", "s.whatsapp.net")
	sender := mustJID("15550001111", "s.whatsapp.net") // Marco's own JID

	var got []ingestion.RawMessage
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		got = append(got, rm)
		return nil
	})

	evt := newTestEvent("MSG5", chat, sender, true, time.Now(), &waE2E.Message{
		Conversation: proto.String("my own outgoing message"),
	})
	h.HandleMessage(evt)

	if len(got) != 1 {
		t.Fatalf("expected outgoing message to flow through, got %d messages", len(got))
	}
}

func TestHandleMessage_UnsupportedTypeDropped(t *testing.T) {
	chat := mustJID("15559999999", "s.whatsapp.net")
	sender := mustJID("15551234567", "s.whatsapp.net")

	var got []ingestion.RawMessage
	var loggedDrop bool
	h := &EventHandler{
		Sink: func(rm ingestion.RawMessage) error {
			got = append(got, rm)
			return nil
		},
		Logger: func(format string, args ...any) {
			loggedDrop = true
		},
	}

	// A sticker message has no counterpart in Pegasus's four media types.
	evt := newTestEvent("MSG6", chat, sender, false, time.Now(), &waE2E.Message{
		StickerMessage: &waE2E.StickerMessage{
			URL: proto.String("https://example.invalid/sticker.webp"),
		},
	})
	h.HandleMessage(evt)

	if len(got) != 0 {
		t.Fatalf("expected sticker message to be dropped, got %+v", got)
	}
	if !loggedDrop {
		t.Error("expected a log line for the dropped message")
	}
}

func TestHandleMessage_SinkErrorIsLoggedNotPanicked(t *testing.T) {
	chat := mustJID("15559999999", "s.whatsapp.net")
	sender := mustJID("15551234567", "s.whatsapp.net")

	var loggedErr bool
	h := &EventHandler{
		Sink: func(rm ingestion.RawMessage) error {
			return errors.New("boom")
		},
		Logger: func(format string, args ...any) {
			loggedErr = true
		},
	}

	evt := newTestEvent("MSG7", chat, sender, false, time.Now(), &waE2E.Message{
		Conversation: proto.String("hi"),
	})
	h.HandleMessage(evt)

	if !loggedErr {
		t.Error("expected sink error to be logged")
	}
}

func TestHandle_IgnoresNonMessageEvents(t *testing.T) {
	var called bool
	h := NewEventHandler(func(rm ingestion.RawMessage) error {
		called = true
		return nil
	})

	h.Handle(&events.Connected{})

	if called {
		t.Error("expected non-Message events to be ignored")
	}
}
