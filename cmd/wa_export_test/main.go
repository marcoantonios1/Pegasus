// Throwaway manual-test CLI for the WhatsApp export parser. Not part of the
// adapter itself — delete after use.
//
// Usage:
//
//	go run ./cmd/wa_export_test path/to/export.txt
//
// With no argument, parses a small embedded sample instead.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp"
)

const sample = `[05/01/26, 09:00:00] Messages and calls are end-to-end encrypted. No one outside of this chat, not even WhatsApp, can read or listen to them.
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

func main() {
	var r io.Reader = strings.NewReader(sample)

	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "open:", err)
			os.Exit(1)
		}
		defer f.Close()
		r = f
	} else {
		fmt.Println("no file given, using embedded sample")
	}

	p := whatsapp.NewExportParser()

	msgs, err := p.Parse(r, "conv-manual-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse:", err)
		os.Exit(1)
	}

	fmt.Printf("parsed %d messages\n\n", len(msgs))
	for i, m := range msgs {
		text := "<nil>"
		if m.Text != nil {
			text = *m.Text
		}
		mediaURL := "<nil>"
		if m.MediaURL != nil {
			mediaURL = *m.MediaURL
		}
		fmt.Printf("[%d] external_id=%s sender=%s media_type=%s timestamp=%s\n    text=%q\n    media_url=%q\n\n",
			i, m.ExternalID, m.SenderExternalID, m.MediaType, m.Timestamp.Format("2006-01-02 15:04:05"), text, mediaURL)
	}
}
