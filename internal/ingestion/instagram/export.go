// Package instagram normalizes Instagram's official data export ("Settings
// > Download Your Information") into ingestion.RawMessage. This is the
// second implementation of that contract — see internal/ingestion/whatsapp
// for the first. Structure here deliberately mirrors that package (same
// ExportParser shape, same classify-by-which-field-is-set dispatch, same
// synthetic external_id method) so the two read as one contract with two
// data sources, not two designs.
//
// There is no live-streaming path for Instagram the way whatsmeow provides
// for WhatsApp — proposal §6.3/§4.1 only lists "export/API" for Instagram,
// and building against Instagram's actual DM API would require Meta app
// review, well beyond this issue's scope. If live Instagram ingestion is
// wanted later via an unofficial API, that's a separate issue.
package instagram

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"
	"unicode/utf8"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// Export JSON shape, per Meta's "Download Your Information" format
// (Settings > Download Your Information > DMs, delivered as one JSON file
// per conversation thread under your_instagram_activity/messages/inbox/).
// This schema is long-documented and stable, but hasn't been checked
// against one of Marco's actual export files the way the WhatsApp .txt
// format was — validate against a real export before trusting this in
// production, the same way the WhatsApp adapter's NBSP handling was fixed
// after a real-file test caught it.
type exportThread struct {
	ThreadPath string          `json:"thread_path"`
	Title      string          `json:"title"`
	Messages   []exportMessage `json:"messages"`
}

type exportMessage struct {
	SenderName  string           `json:"sender_name"`
	TimestampMs int64            `json:"timestamp_ms"`
	Content     string           `json:"content"`
	IsUnsent    bool             `json:"is_unsent"`
	Photos      []exportMediaRef `json:"photos"`
	Videos      []exportMediaRef `json:"videos"`
	AudioFiles  []exportMediaRef `json:"audio_files"`
	Share       *exportShare     `json:"share"`
	Reactions   []exportReaction `json:"reactions"`
}

type exportMediaRef struct {
	URI string `json:"uri"`
}

type exportShare struct {
	Link      string `json:"link"`
	ShareText string `json:"share_text"`
}

type exportReaction struct {
	Reaction string `json:"reaction"`
	Actor    string `json:"actor"`
}

// ExportParser parses one Instagram DM export thread (one JSON file) into
// RawMessage values. Mirrors whatsapp.ExportParser: no classification or
// entity resolution beyond determining a message's shape.
type ExportParser struct {
	// Logger receives one line for every skipped/unsupported entry.
	// Defaults to log.Printf if nil.
	Logger func(format string, args ...any)
}

func NewExportParser() *ExportParser {
	return &ExportParser{}
}

// Parse reads one Instagram export thread JSON file and returns the
// messages it contains as RawMessage values, in file order.
//
// Unlike whatsapp.ExportParser.Parse, this does not take a conversationID
// parameter: Instagram's JSON export is self-describing and already carries
// a stable thread identifier (thread_path), so deriving it from the file
// avoids a caller having to keep an external ID in sync with the data.
func (p *ExportParser) Parse(r io.Reader) ([]ingestion.RawMessage, error) {
	var thread exportThread
	if err := json.NewDecoder(r).Decode(&thread); err != nil {
		return nil, fmt.Errorf("decode export: %w", err)
	}

	conversationID := thread.ThreadPath
	if conversationID == "" {
		conversationID = thread.Title
	}

	var out []ingestion.RawMessage
	for _, m := range thread.Messages {
		raw, ok := p.buildMessage(m, conversationID)
		if !ok {
			continue
		}
		out = append(out, raw)
	}

	return out, nil
}

func (p *ExportParser) buildMessage(m exportMessage, conversationID string) (ingestion.RawMessage, bool) {
	mediaType, text, mediaURL, ok := classify(m)
	if !ok {
		p.logf("instagram export: dropping unsupported/unresolvable entry from %s at %d", m.SenderName, m.TimestampMs)
		return ingestion.RawMessage{}, false
	}

	ts := time.UnixMilli(m.TimestampMs).UTC()

	return ingestion.RawMessage{
		ExternalID:       syntheticExternalID(conversationID, m.SenderName, ts, m.Content),
		Platform:         "instagram",
		ConversationID:   conversationID,
		SenderExternalID: m.SenderName,
		Timestamp:        ts,
		MediaType:        mediaType,
		Text:             text,
		MediaURL:         mediaURL,
	}, true
}

// classify determines media_type/text/media_url for one export message.
// Mirrors whatsapp.classify()'s shape: dispatch on which field is
// populated, known shapes map cleanly, anything ambiguous or unsupported
// (shared posts/reels, story replies, unsend markers, reaction-only
// entries) is reported via ok=false instead of guessed at.
func classify(m exportMessage) (mediaType string, text, mediaURL *string, ok bool) {
	switch {
	case m.IsUnsent:
		return "", nil, nil, false

	case len(m.Photos) > 0:
		return ingestion.MediaTypeImage, optionalStrPtr(fixMojibake(m.Content)), strPtr(m.Photos[0].URI), true

	case len(m.Videos) > 0:
		return ingestion.MediaTypeVideo, optionalStrPtr(fixMojibake(m.Content)), strPtr(m.Videos[0].URI), true

	case len(m.AudioFiles) > 0:
		return ingestion.MediaTypeVoice, nil, strPtr(m.AudioFiles[0].URI), true

	case m.Share != nil:
		// Shared posts/reels carry no Pegasus-shaped content of their own.
		return "", nil, nil, false

	case m.Content == "" && len(m.Reactions) > 0:
		// A reaction to another message, not a message itself.
		return "", nil, nil, false

	case m.Content != "":
		return ingestion.MediaTypeText, strPtr(fixMojibake(m.Content)), nil, true

	default:
		return "", nil, nil, false
	}
}

// fixMojibake reverses a long-standing bug in Meta's data export tool:
// exported text is the original UTF-8 bytes re-interpreted one byte at a
// time as Latin-1 code points and re-encoded as UTF-8, so emoji and
// accented characters come out mojibake'd. This is the one place Instagram
// genuinely needs adapter-specific logic that WhatsApp doesn't — it must
// not leak into the shared RawMessage contract.
//
// If the string doesn't look like it went through that bug (any rune above
// 0xFF, or the recovered bytes aren't valid UTF-8), it's returned unchanged
// rather than risk corrupting text that was already correct.
func fixMojibake(s string) string {
	if s == "" {
		return s
	}

	recovered := make([]byte, 0, len(s))
	for _, r := range s {
		if r > 0xFF {
			return s
		}
		recovered = append(recovered, byte(r))
	}

	if len(recovered) == 0 {
		return s
	}

	if !utf8.Valid(recovered) {
		return s
	}
	return string(recovered)
}

// syntheticExternalID mirrors whatsapp.syntheticExternalID exactly (same
// method, per the requirement that both adapters synthesize IDs the same
// way) — Instagram's JSON export does not include a native per-message ID
// in the documented schema, so this always runs.
func syntheticExternalID(conversationID, sender string, ts time.Time, content string) string {
	h := sha256.Sum256([]byte(conversationID + "|" + sender + "|" + ts.Format(time.RFC3339) + "|" + content))
	return "ig-export:" + hex.EncodeToString(h[:])
}

func strPtr(s string) *string { return &s }

// optionalStrPtr returns nil for an empty string instead of a pointer to
// "" — captions on media messages are frequently absent. Mirrors
// whatsapp.optionalStrPtr.
func optionalStrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (p *ExportParser) logf(format string, args ...any) {
	if p.Logger != nil {
		p.Logger(format, args...)
		return
	}
	log.Printf(format, args...)
}
