package whatsapp

import (
	"log"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// EventHandler adapts whatsmeow's live event stream into ingestion.RawMessage
// values, pushed onto an injected Sink. It has no opinion about what happens
// after normalization; routing, classification, and storage are the Message
// Router's job, not the adapter's.
//
// It does no identity canonicalization or filtering of its own: the live
// source (internal/ingestion/whatsapp/wasource) canonicalizes JIDs and
// drops unsupported chats/types before events reach it.
type EventHandler struct {
	Sink ingestion.Sink

	// Logger receives one line for every message dropped or failed to sink.
	// Defaults to log.Printf if nil.
	Logger func(format string, args ...any)
}

func NewEventHandler(sink ingestion.Sink) *EventHandler {
	return &EventHandler{Sink: sink}
}

// Handle matches whatsmeow's client.AddEventHandler(func(any)) signature —
// whatsmeow dispatches every event type through one handler, so this ignores
// anything that isn't *events.Message.
func (h *EventHandler) Handle(evt any) {
	msg, ok := evt.(*events.Message)
	if !ok {
		return
	}
	h.HandleMessage(msg)
}

// HandleMessage normalizes a single whatsmeow message event and pushes it
// onto the sink. Both incoming and outgoing messages are processed — Marco's
// own outgoing messages feed style-learning per proposal §6.4, so this does
// not filter on evt.Info.IsFromMe.
func (h *EventHandler) HandleMessage(evt *events.Message) {
	raw, ok := normalizeMessage(evt)
	if !ok {
		h.logf("whatsapp: dropping unsupported message type, id=%s chat=%s", evt.Info.ID, RedactJID(evt.Info.Chat))
		return
	}

	if err := h.Sink(raw); err != nil {
		h.logf("whatsapp: sink error for message id=%s: %v", evt.Info.ID, err)
	}
}

func (h *EventHandler) logf(format string, args ...any) {
	if h.Logger != nil {
		h.Logger(format, args...)
		return
	}
	log.Printf(format, args...)
}

// normalizeMessage maps a whatsmeow message event to a RawMessage. The
// second return value is false when the message's content doesn't map to
// one of Pegasus's four media types — those are dropped by the caller
// rather than forwarded with an invented media type.
func normalizeMessage(evt *events.Message) (ingestion.RawMessage, bool) {
	mediaType, text, mediaURL, ok := classify(evt.Message)
	if !ok {
		return ingestion.RawMessage{}, false
	}

	return ingestion.RawMessage{
		ExternalID:       evt.Info.ID,
		Platform:         "whatsapp",
		ConversationID:   evt.Info.Chat.String(),
		SenderExternalID: evt.Info.Sender.String(),
		Timestamp:        evt.Info.Timestamp,
		MediaType:        mediaType,
		Text:             text,
		MediaURL:         mediaURL,
	}, true
}

// classify maps a waE2E.Message's content to Pegasus's media_type. Anything
// whatsmeow reports that isn't plain/extended text, an image, a video, or an
// audio message — stickers, reactions, location shares, contact cards, etc.
// — is unsupported and reported via ok=false rather than invented a fifth
// media type or guessed at.
func classify(m *waE2E.Message) (mediaType string, text, mediaURL *string, ok bool) {
	switch {
	case m.GetConversation() != "":
		return ingestion.MediaTypeText, strPtr(m.GetConversation()), nil, true

	case m.GetExtendedTextMessage() != nil:
		return ingestion.MediaTypeText, strPtr(m.GetExtendedTextMessage().GetText()), nil, true

	case m.GetImageMessage() != nil:
		im := m.GetImageMessage()
		return ingestion.MediaTypeImage, optionalStrPtr(im.GetCaption()), strPtr(im.GetURL()), true

	case m.GetVideoMessage() != nil:
		vm := m.GetVideoMessage()
		return ingestion.MediaTypeVideo, optionalStrPtr(vm.GetCaption()), strPtr(vm.GetURL()), true

	case m.GetAudioMessage() != nil:
		am := m.GetAudioMessage()
		return ingestion.MediaTypeVoice, nil, strPtr(am.GetURL()), true

	default:
		return "", nil, nil, false
	}
}

// MediaTypeOf reports the Pegasus media type the adapter would assign to m,
// or ok=false for content it drops (reactions, protocol/revoke messages,
// stickers, ...). It is the same classification HandleMessage uses, exposed
// so the live source can count drops by reason before forwarding.
func MediaTypeOf(m *waE2E.Message) (mediaType string, ok bool) {
	mediaType, _, _, ok = classify(m)
	return mediaType, ok
}

// RedactJID shortens a JID for logs: the user part (a phone number or LID)
// is reduced to its last 4 characters, e.g. "…4567@s.whatsapp.net". Never
// log a full JID.
func RedactJID(j types.JID) string {
	if j.IsEmpty() {
		return "(none)"
	}
	return RedactUser(j.User) + "@" + j.Server
}

// RedactUser keeps only the last 4 characters of a JID user part.
func RedactUser(user string) string {
	if len(user) <= 4 {
		return "…" + user
	}
	return "…" + user[len(user)-4:]
}

func strPtr(s string) *string { return &s }

// optionalStrPtr returns nil for an empty string instead of a pointer to "",
// since captions on media messages are frequently absent.
func optionalStrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
