package whatsapp

import (
	"strings"
	"testing"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

const sampleExport = `[05/01/26, 09:00:00] Messages and calls are end-to-end encrypted. No one outside of this chat, not even WhatsApp, can read or listen to them.
[05/01/26, 09:01:15] Marco: hey, how's it going?
[05/01/26, 09:01:40] Alice: pretty good!
this is a second line
and a third line, still the same message
[05/01/26, 09:02:00] Marco: image omitted
[05/01/26, 09:02:30] Alice: audio omitted
[05/01/26, 09:03:00] Marco: sticker omitted
[05/01/26, 09:03:10] Marco: <Media omitted>
[05/01/26, 09:04:00] Alice: IMG-20260105-WA0001.jpg (file attached)
nice shot!
[05/01/26, 09:05:00] Marco added Bob
`

func TestParseExport(t *testing.T) {
	p := NewExportParser()
	var dropped []string
	p.Logger = func(format string, args ...any) { dropped = append(dropped, format) }

	msgs, err := p.Parse(strings.NewReader(sampleExport), "conv-123")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Expect: Marco greeting, Alice multi-line, Marco image-omitted,
	// Alice audio-omitted, Alice attached-image-with-caption.
	// Dropped: encryption notice + "Marco added Bob" (system lines, no
	// RawMessage at all), sticker omitted, bare <Media omitted>.
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d: %+v", len(msgs), msgs)
	}

	greeting := msgs[0]
	if greeting.SenderExternalID != "Marco" || greeting.MediaType != ingestion.MediaTypeText {
		t.Errorf("unexpected greeting message: %+v", greeting)
	}
	if greeting.Text == nil || *greeting.Text != "hey, how's it going?" {
		t.Errorf("unexpected greeting text: %v", greeting.Text)
	}
	if greeting.ConversationID != "conv-123" {
		t.Errorf("unexpected conversation id: %v", greeting.ConversationID)
	}
	if greeting.ExternalID == "" {
		t.Error("expected a synthesized external id")
	}

	multiLine := msgs[1]
	wantMultiLine := "pretty good!\nthis is a second line\nand a third line, still the same message"
	if multiLine.SenderExternalID != "Alice" {
		t.Errorf("unexpected sender: %v", multiLine.SenderExternalID)
	}
	if multiLine.Text == nil || *multiLine.Text != wantMultiLine {
		t.Errorf("multi-line message not joined correctly, got: %v", multiLine.Text)
	}

	imageOmitted := msgs[2]
	if imageOmitted.MediaType != ingestion.MediaTypeImage {
		t.Errorf("expected image media type, got %q", imageOmitted.MediaType)
	}
	if imageOmitted.MediaURL != nil {
		t.Errorf("expected nil media url for omitted media, got %v", *imageOmitted.MediaURL)
	}

	audioOmitted := msgs[3]
	if audioOmitted.MediaType != ingestion.MediaTypeVoice {
		t.Errorf("expected voice media type, got %q", audioOmitted.MediaType)
	}

	attached := msgs[4]
	if attached.MediaType != ingestion.MediaTypeImage {
		t.Errorf("expected image media type for attached file, got %q", attached.MediaType)
	}
	if attached.MediaURL == nil || *attached.MediaURL != "IMG-20260105-WA0001.jpg" {
		t.Errorf("unexpected media url: %v", attached.MediaURL)
	}
	if attached.Text == nil || *attached.Text != "nice shot!" {
		t.Errorf("unexpected caption: %v", attached.Text)
	}

	// sticker omitted + bare <Media omitted> should have produced log lines.
	if len(dropped) < 2 {
		t.Errorf("expected at least 2 dropped-entry log lines, got %d: %v", len(dropped), dropped)
	}
}

func TestParseExport_ExternalIDsAreStableAndUnique(t *testing.T) {
	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(sampleExport), "conv-123")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	seen := map[string]bool{}
	for _, m := range msgs {
		if seen[m.ExternalID] {
			t.Errorf("duplicate external id: %v", m.ExternalID)
		}
		seen[m.ExternalID] = true
	}

	// Re-parsing the same export should synthesize identical IDs, so
	// historical and live imports can be deduplicated against each other.
	msgs2, err := p.Parse(strings.NewReader(sampleExport), "conv-123")
	if err != nil {
		t.Fatalf("Parse (second run): %v", err)
	}
	if len(msgs2) != len(msgs) {
		t.Fatalf("expected same message count on reparse, got %d vs %d", len(msgs2), len(msgs))
	}
	for i := range msgs {
		if msgs[i].ExternalID != msgs2[i].ExternalID {
			t.Errorf("external id not stable across reparse: %v vs %v", msgs[i].ExternalID, msgs2[i].ExternalID)
		}
	}
}

func TestParseExport_AndroidStyleHeader(t *testing.T) {
	const androidSample = `05/01/2026, 9:01 AM - Marco: good morning
05/01/2026, 9:02 AM - Alice: morning!
`
	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(androidSample), "conv-android")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].SenderExternalID != "Marco" || *msgs[0].Text != "good morning" {
		t.Errorf("unexpected first message: %+v", msgs[0])
	}
	if msgs[1].SenderExternalID != "Alice" || *msgs[1].Text != "morning!" {
		t.Errorf("unexpected second message: %+v", msgs[1])
	}
}

// TestParseExport_NarrowNoBreakSpaceBeforeAMPM regression-tests a real bug
// found by running the parser against an actual WhatsApp export: recent
// exports separate the time from its AM/PM marker with U+202F (narrow
// no-break space), not a plain ASCII space. Go's regexp \s doesn't match
// it, which used to swallow "pm"/"am" into the sender name and silently
// drop it from the parsed time (misparsing PM timestamps as AM).
func TestParseExport_NarrowNoBreakSpaceBeforeAMPM(t *testing.T) {
	sample := "29/07/2024, 1:38 pm - Kevin Azzi: hello\n"

	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(sample), "conv-nbsp")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}

	m := msgs[0]
	if m.SenderExternalID != "Kevin Azzi" {
		t.Errorf("expected sender %q, got %q (AM/PM marker leaked into sender name)", "Kevin Azzi", m.SenderExternalID)
	}
	if m.Timestamp.Hour() != 13 {
		t.Errorf("expected 1:38pm to parse as hour 13, got hour %d (PM marker was dropped)", m.Timestamp.Hour())
	}
}
