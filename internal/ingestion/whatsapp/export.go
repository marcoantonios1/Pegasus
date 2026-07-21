package whatsapp

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// Assumptions specific to WhatsApp's plain-text "Export Chat" format (as
// opposed to a linked-device JSON export — Marco confirmed .txt is what's
// actually being imported):
//
//   - Dates are parsed as DD/MM/YY(YY), matching a non-US phone locale. A
//     US-locale export (MM/DD/YY) would need this changed — there's no way
//     to tell the two apart from the file alone.
//   - Timestamps carry no timezone in the export; they're parsed as local
//     time (time.Local). Cross-timezone reconciliation, if ever needed, is
//     downstream work.
//   - Sender is whatever display name WhatsApp put in the export, not a
//     JID. It will not match a live adapter's SenderExternalID for the same
//     person without a separate identity-resolution step — that's Message
//     Router / entity-resolution territory, not this adapter's job.
//   - Line-based "sender:" detection can misparse a system notification
//     that happens to contain a colon in its own text. Rare at personal
//     scale; flagged here rather than silently accepted.

// ws is the whitespace class used throughout these patterns instead of \s.
// Go's regexp \s (RE2) is ASCII-only and does NOT match U+00A0 (NBSP) or
// U+202F (narrow NBSP) — and recent WhatsApp exports (confirmed against a
// real export) use U+202F between the time and its AM/PM marker. Missing
// this silently swallowed "pm"/"am" into the sender name and dropped it
// from the captured time, misparsing PM timestamps as AM.
const ws = ` \x{00A0}\x{202F}`

// headerRe matches a line that starts a new message: a timestamp, then a
// "Sender:" prefix. Handles both the iOS style ("[D/M/Y, H:MM:SS AM] Name: ")
// and the Android style ("D/M/Y, H:MM AM - Name: ").
var headerRe = regexp.MustCompile(`^\x{200E}?\[?(\d{1,2}/\d{1,2}/\d{2,4}),[` + ws + `]?(\d{1,2}:\d{2}(?::\d{2})?(?:[` + ws + `]?[APap][Mm])?)\]?[` + ws + `]*[-\x{2013}]?[` + ws + `]*([^:\n]{1,60}?):[` + ws + `](.*)$`)

// timestampRe matches a line that starts with a timestamp but has no
// "Sender:" attribution — a system notification ("Messages and calls are
// end-to-end encrypted...", "X added Y", etc.), not an actual message.
var timestampRe = regexp.MustCompile(`^\x{200E}?\[?(\d{1,2}/\d{1,2}/\d{2,4}),[` + ws + `]?(\d{1,2}:\d{2}(?::\d{2})?(?:[` + ws + `]?[APap][Mm])?)\]?[` + ws + `]*[-\x{2013}]?[` + ws + `]`)

// mediaOmittedType maps WhatsApp's type-specific omission markers (used in
// exports "without media") to Pegasus's media types. Markers that don't map
// to one of the four (sticker, GIF, contact card, document) are dropped
// rather than guessed at — same rule as the live adapter.
var mediaOmittedType = map[string]string{
	"image omitted": ingestion.MediaTypeImage,
	"video omitted": ingestion.MediaTypeVideo,
	"audio omitted": ingestion.MediaTypeVoice,
}

// attachedFileRe matches the "<filename> (file attached)" line used by
// exports "with media", where the referenced file ships alongside the .txt
// in the export bundle.
var attachedFileRe = regexp.MustCompile(`^\x{200E}?(\S+\.(\w+))\s\(file attached\)$`)

// genericOmittedRe catches omission markers this parser doesn't have a
// Pegasus media type for ("sticker omitted", "GIF omitted", "Contact card
// omitted", "document omitted", ...) so they get dropped instead of being
// forwarded as literal text. Limited to 1-2 leading words specifically to
// avoid misfiring on an ordinary sentence that happens to end in "omitted"
// ("the meeting agenda item was omitted") — a real but rare false-positive
// risk of line-based text matching, not worth over-engineering around here.
var genericOmittedRe = regexp.MustCompile(`(?i)^[a-z]+(?: [a-z]+)? omitted$`)

var attachedExtType = map[string]string{
	"jpg": ingestion.MediaTypeImage, "jpeg": ingestion.MediaTypeImage, "png": ingestion.MediaTypeImage, "webp": ingestion.MediaTypeImage,
	"mp4": ingestion.MediaTypeVideo, "mov": ingestion.MediaTypeVideo, "3gp": ingestion.MediaTypeVideo, "avi": ingestion.MediaTypeVideo,
	"opus": ingestion.MediaTypeVoice, "m4a": ingestion.MediaTypeVoice, "mp3": ingestion.MediaTypeVoice, "ogg": ingestion.MediaTypeVoice, "aac": ingestion.MediaTypeVoice, "wav": ingestion.MediaTypeVoice,
}

// ExportParser parses WhatsApp's plain-text chat export format into
// RawMessage values. It does no classification or entity resolution beyond
// what's needed to determine a message's shape (text vs. media, which media
// type) — that mirrors the live adapter's boundary.
type ExportParser struct {
	// Logger receives one line for every skipped/unsupported entry.
	// Defaults to log.Printf if nil.
	Logger func(format string, args ...any)
}

func NewExportParser() *ExportParser {
	return &ExportParser{}
}

type pendingMessage struct {
	dateStr, timeStr string
	sender           string
	lines            []string
}

// Parse reads a WhatsApp .txt export and returns the messages it contains as
// RawMessage values, in file order. conversationID is supplied by the
// caller (the export file itself carries no chat/group JID) — this is
// wiring, not a classification decision.
func (p *ExportParser) Parse(r io.Reader, conversationID string) ([]ingestion.RawMessage, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var out []ingestion.RawMessage
	var pending *pendingMessage

	flush := func() {
		if pending == nil {
			return
		}
		if raw, ok := p.buildMessage(*pending, conversationID); ok {
			out = append(out, raw)
		}
		pending = nil
	}

	for scanner.Scan() {
		line := scanner.Text()

		if m := headerRe.FindStringSubmatch(line); m != nil {
			flush()
			pending = &pendingMessage{
				dateStr: m[1],
				timeStr: m[2],
				sender:  strings.TrimSpace(m[3]),
				lines:   []string{m[4]},
			}
			continue
		}

		if timestampRe.MatchString(line) {
			// System notification, not a message from a sender — flush
			// whatever was open and drop this line without starting a new one.
			flush()
			continue
		}

		if pending != nil {
			// Continuation of a multi-line message (WhatsApp wraps these
			// without repeating the timestamp/sender prefix).
			pending.lines = append(pending.lines, line)
			continue
		}

		// Unattributed line before any message header (e.g. the leading
		// "Messages and calls are end-to-end encrypted..." notice). Nothing
		// to attach it to.
	}
	flush()

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan export: %w", err)
	}

	return out, nil
}

func (p *ExportParser) buildMessage(pm pendingMessage, conversationID string) (ingestion.RawMessage, bool) {
	ts, err := parseExportTimestamp(pm.dateStr, pm.timeStr)
	if err != nil {
		p.logf("whatsapp export: dropping message with unparseable timestamp %q %q: %v", pm.dateStr, pm.timeStr, err)
		return ingestion.RawMessage{}, false
	}

	body := strings.TrimRight(strings.Join(pm.lines, "\n"), "\n")

	mediaType, text, mediaURL, ok := classifyExportBody(body)
	if !ok {
		p.logf("whatsapp export: dropping unsupported/unresolvable entry from %s at %s %s", pm.sender, pm.dateStr, pm.timeStr)
		return ingestion.RawMessage{}, false
	}

	return ingestion.RawMessage{
		ExternalID:       syntheticExternalID(conversationID, pm.sender, ts, body),
		Platform:         "whatsapp",
		ConversationID:   conversationID,
		SenderExternalID: pm.sender,
		Timestamp:        ts,
		MediaType:        mediaType,
		Text:             text,
		MediaURL:         mediaURL,
	}, true
}

// classifyExportBody determines media_type/text/media_url for one flushed
// message body. Mirrors the live adapter's classify(): known shapes map
// cleanly, anything ambiguous (a bare "<Media omitted>" with no type hint,
// a sticker/GIF/contact/document omission, an attached file with an
// unrecognized extension) is reported via ok=false instead of guessed at.
func classifyExportBody(body string) (mediaType string, text, mediaURL *string, ok bool) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(body, "‎"))

	if mt, found := mediaOmittedType[strings.ToLower(trimmed)]; found {
		// Type is known but the actual file isn't in a text-only export.
		return mt, nil, nil, true
	}

	if genericOmittedRe.MatchString(trimmed) {
		// An omission marker Pegasus has no media type for (sticker, GIF,
		// contact card, document, ...) — drop rather than forward as text.
		return "", nil, nil, false
	}

	if lines := strings.SplitN(body, "\n", 2); true {
		firstLine := strings.TrimPrefix(lines[0], "‎")
		if m := attachedFileRe.FindStringSubmatch(firstLine); m != nil {
			ext := strings.ToLower(m[2])
			mt, found := attachedExtType[ext]
			if !found {
				return "", nil, nil, false
			}
			filename := m[1]
			var caption *string
			if len(lines) > 1 {
				if rest := strings.TrimSpace(lines[1]); rest != "" {
					caption = &rest
				}
			}
			return mt, caption, &filename, true
		}
	}

	if trimmed == "" || strings.EqualFold(trimmed, "<Media omitted>") {
		// Legacy/generic placeholder with no type information at all —
		// can't tell voice/image/video apart from text alone.
		return "", nil, nil, false
	}

	// Anything else is plain text.
	return ingestion.MediaTypeText, &body, nil, true
}

// parseExportTimestamp parses the date/time pair captured by headerRe.
// DD/MM/YY(YY) locale and no explicit timezone — see the package-level
// assumptions comment.
func parseExportTimestamp(dateStr, timeStr string) (time.Time, error) {
	dateParts := strings.Split(dateStr, "/")
	if len(dateParts) != 3 {
		return time.Time{}, fmt.Errorf("unrecognized date %q", dateStr)
	}
	day, err := strconv.Atoi(dateParts[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("day: %w", err)
	}
	month, err := strconv.Atoi(dateParts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("month: %w", err)
	}
	year, err := strconv.Atoi(dateParts[2])
	if err != nil {
		return time.Time{}, fmt.Errorf("year: %w", err)
	}
	if year < 100 {
		year += 2000
	}

	// Normalize NBSP/narrow-NBSP (see the ws const) to a plain space so the
	// "3:04 PM" layouts below — which expect an ASCII space — still match.
	timeStr = strings.NewReplacer(" ", " ", " ", " ").Replace(timeStr)
	timeStr = strings.TrimSpace(timeStr)
	layouts := []string{"15:04:05", "15:04", "3:04:05 PM", "3:04 PM"}
	var clock time.Time
	for _, layout := range layouts {
		clock, err = time.Parse(layout, strings.ToUpper(timeStr))
		if err == nil {
			break
		}
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("unrecognized time %q", timeStr)
	}

	return time.Date(year, time.Month(month), day, clock.Hour(), clock.Minute(), clock.Second(), 0, time.Local), nil
}

func syntheticExternalID(conversationID, sender string, ts time.Time, body string) string {
	h := sha256.Sum256([]byte(conversationID + "|" + sender + "|" + ts.Format(time.RFC3339) + "|" + body))
	return "wa-export:" + hex.EncodeToString(h[:])
}

func (p *ExportParser) logf(format string, args ...any) {
	if p.Logger != nil {
		p.Logger(format, args...)
		return
	}
	log.Printf(format, args...)
}
