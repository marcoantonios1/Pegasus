package instagram

import (
	"strings"
	"testing"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// sampleExport mirrors whatsapp/export_test.go's sampleExport in spirit: one
// file covering text, each media type, an unsupported entry, and a
// multi-field message, so the two test files can be read side by side as
// the same contract exercised against two data sources.
const sampleExport = `{
	"thread_path": "inbox/marco_bugs_1234567890",
	"title": "Bugs",
	"messages": [
		{"sender_name": "marco.antonios", "timestamp_ms": 1735707600000, "content": "hey, how's it going?"},
		{"sender_name": "kevin.azzi", "timestamp_ms": 1735707660000, "content": "pretty good!"},
		{"sender_name": "marco.antonios", "timestamp_ms": 1735707720000, "content": "check this out", "photos": [{"uri": "messages/inbox/marco_bugs/photos/IMG_001.jpg"}]},
		{"sender_name": "kevin.azzi", "timestamp_ms": 1735707780000, "videos": [{"uri": "messages/inbox/marco_bugs/videos/VID_001.mp4"}]},
		{"sender_name": "marco.antonios", "timestamp_ms": 1735707840000, "audio_files": [{"uri": "messages/inbox/marco_bugs/audio/AUD_001.m4a"}]},
		{"sender_name": "kevin.azzi", "timestamp_ms": 1735707900000, "share": {"link": "https://instagram.com/p/xyz", "share_text": "check this post"}},
		{"sender_name": "marco.antonios", "timestamp_ms": 1735707960000, "content": "", "reactions": [{"reaction": "❤", "actor": "kevin.azzi"}]},
		{"sender_name": "kevin.azzi", "timestamp_ms": 1735708020000, "content": "this got unsent", "is_unsent": true}
	]
}`

func TestParseExport(t *testing.T) {
	p := NewExportParser()
	var dropped []string
	p.Logger = func(format string, args ...any) { dropped = append(dropped, format) }

	msgs, err := p.Parse(strings.NewReader(sampleExport))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Expect: greeting, reply, image w/ caption, video, voice.
	// Dropped: share, reaction-only, unsent.
	if len(msgs) != 5 {
		t.Fatalf("expected 5 messages, got %d: %+v", len(msgs), msgs)
	}

	greeting := msgs[0]
	if greeting.SenderExternalID != "marco.antonios" || greeting.MediaType != ingestion.MediaTypeText {
		t.Errorf("unexpected greeting message: %+v", greeting)
	}
	if greeting.Text == nil || *greeting.Text != "hey, how's it going?" {
		t.Errorf("unexpected greeting text: %v", greeting.Text)
	}
	if greeting.ConversationID != "inbox/marco_bugs_1234567890" {
		t.Errorf("unexpected conversation id: %v", greeting.ConversationID)
	}
	if greeting.Platform != "instagram" {
		t.Errorf("unexpected platform: %v", greeting.Platform)
	}
	if greeting.ExternalID == "" {
		t.Error("expected a synthesized external id")
	}
	if greeting.Timestamp.Unix() != 1735707600 {
		t.Errorf("unexpected timestamp: %v", greeting.Timestamp)
	}

	image := msgs[2]
	if image.MediaType != ingestion.MediaTypeImage {
		t.Errorf("expected image media type, got %q", image.MediaType)
	}
	if image.Text == nil || *image.Text != "check this out" {
		t.Errorf("unexpected caption: %v", image.Text)
	}
	if image.MediaURL == nil || *image.MediaURL != "messages/inbox/marco_bugs/photos/IMG_001.jpg" {
		t.Errorf("unexpected media url: %v", image.MediaURL)
	}

	video := msgs[3]
	if video.MediaType != ingestion.MediaTypeVideo {
		t.Errorf("expected video media type, got %q", video.MediaType)
	}
	if video.Text != nil {
		t.Errorf("expected nil text for captionless video, got %v", *video.Text)
	}
	if video.MediaURL == nil || *video.MediaURL != "messages/inbox/marco_bugs/videos/VID_001.mp4" {
		t.Errorf("unexpected media url: %v", video.MediaURL)
	}

	voice := msgs[4]
	if voice.MediaType != ingestion.MediaTypeVoice {
		t.Errorf("expected voice media type, got %q", voice.MediaType)
	}
	if voice.Text != nil {
		t.Errorf("expected nil text for voice message, got %v", *voice.Text)
	}
	if voice.MediaURL == nil || *voice.MediaURL != "messages/inbox/marco_bugs/audio/AUD_001.m4a" {
		t.Errorf("unexpected media url: %v", voice.MediaURL)
	}

	// share + reaction-only + unsent should have produced log lines.
	if len(dropped) != 3 {
		t.Errorf("expected 3 dropped-entry log lines, got %d: %v", len(dropped), dropped)
	}
}

func TestParseExport_ExternalIDsAreStableAndUnique(t *testing.T) {
	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(sampleExport))
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
	// historical imports can be deduplicated against reruns (and, in
	// principle, against a future live source using the same method).
	msgs2, err := p.Parse(strings.NewReader(sampleExport))
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

// TestParseExport_MojibakeText covers the one place Instagram genuinely
// needs adapter-specific logic that WhatsApp doesn't: Meta's export tool
// writes text as UTF-8 bytes re-interpreted one byte at a time as Latin-1
// and re-encoded as UTF-8, so emoji and accented characters come out
// mojibake'd. The content below is "Hi 👋 café" run through that exact
// transform (verified independently, not hand-transcribed) and embedded as
// \uXXXX escapes so the test file itself stays plain ASCII.
func TestParseExport_MojibakeText(t *testing.T) {
	const sample = `{
		"thread_path": "inbox/marco_bugs_1234567890",
		"title": "Bugs",
		"messages": [
			{"sender_name": "marco.antonios", "timestamp_ms": 1735707600000, "content": "Hi \u00f0\u009f\u0091\u008b caf\u00c3\u00a9"}
		]
	}`

	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %+v", len(msgs), msgs)
	}

	want := "Hi 👋 café"
	if msgs[0].Text == nil || *msgs[0].Text != want {
		got := "<nil>"
		if msgs[0].Text != nil {
			got = *msgs[0].Text
		}
		t.Errorf("mojibake not corrected: want %q, got %q", want, got)
	}
}

func TestParseExport_UnknownConversationFallsBackToTitle(t *testing.T) {
	const sample = `{
		"title": "Bugs",
		"messages": [
			{"sender_name": "marco.antonios", "timestamp_ms": 1735707600000, "content": "hi"}
		]
	}`

	p := NewExportParser()
	msgs, err := p.Parse(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].ConversationID != "Bugs" {
		t.Errorf("expected fallback to title, got %q", msgs[0].ConversationID)
	}
}
