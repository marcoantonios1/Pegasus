// Package ingestion defines the shape every platform-specific adapter
// (WhatsApp, Instagram, ...) normalizes into. Adapters own translating a
// platform's native message format into RawMessage; everything downstream
// of that — classification, entity resolution, storage — is the Message
// Router's job (see Pegasus proposal, architecture diagram).
package ingestion

import "time"

const (
	MediaTypeText  = "text"  // see Pegasus proposal §6.3
	MediaTypeVoice = "voice" // see Pegasus proposal §6.3
	MediaTypeImage = "image" // see Pegasus proposal §6.3
	MediaTypeVideo = "video" // see Pegasus proposal §6.3
)

// RawMessage is the normalized shape every ingestion adapter produces.
// See Pegasus proposal §6.3.
type RawMessage struct {
	ExternalID       string    // see Pegasus proposal §6.3 — platform message ID, used for dedup
	Platform         string    // see Pegasus proposal §6.3 — e.g. "whatsapp"
	ConversationID   string    // see Pegasus proposal §6.3 — platform chat/thread ID
	SenderExternalID string    // see Pegasus proposal §6.3 — platform sender ID
	Timestamp        time.Time // see Pegasus proposal §6.3
	MediaType        string    // see Pegasus proposal §6.3 — MediaTypeText | MediaTypeVoice | MediaTypeImage | MediaTypeVideo
	Text             *string   // see Pegasus proposal §6.3 — optional
	MediaURL         *string   // see Pegasus proposal §6.3 — optional
}

// Sink receives normalized messages from an ingestion adapter. Adapters call
// it and do nothing else with the result — routing, classification, and
// storage happen downstream, not in the adapter.
type Sink func(RawMessage) error
