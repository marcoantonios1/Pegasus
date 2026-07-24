// Throwaway manual-test CLI for the Instagram export parser. Not part of
// the adapter itself — delete after use.
//
// Unlike the adapter's Parse(io.Reader), which handles one thread's
// message_N.json per call by design, this walks an entire extracted export
// tree and runs every message_*.json it finds through the parser, so you
// don't have to invoke it once per conversation by hand.
//
// Usage:
//
//	go run ./cmd/ig_export_test /path/to/your_instagram_activity/messages/inbox
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/instagram"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ig_export_test <inbox dir>")
		os.Exit(1)
	}
	root := os.Args[1]

	totalMsgs := 0
	totalFiles := 0

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasPrefix(d.Name(), "message_") || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		totalFiles++

		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open %s: %v\n", path, err)
			return nil
		}
		defer f.Close()

		p := instagram.NewExportParser()
		msgs, err := p.Parse(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
			return nil
		}

		fmt.Printf("\n=== %s (%d messages) ===\n", path, len(msgs))
		for _, m := range msgs {
			text := "<nil>"
			if m.Text != nil {
				text = *m.Text
			}
			mediaURL := "<nil>"
			if m.MediaURL != nil {
				mediaURL = *m.MediaURL
			}
			fmt.Printf("  %-20s %-6s %-25s text=%-30q media_url=%q\n",
				m.SenderExternalID, m.MediaType, m.Timestamp.Format("2006-01-02 15:04:05"), text, mediaURL)

			// If a media_url was resolved, confirm it actually points at a
			// file that exists relative to the thread's own folder — this
			// is exactly the "does the JSON's uri match the real folder
			// layout" check worth doing against a real export.
			if m.MediaURL != nil {
				// root is .../your_instagram_activity/messages/inbox; media
				// URIs in the JSON are commonly relative to the export zip's
				// top level (".../your_instagram_activity/messages/inbox/...").
				exportRoot := filepath.Dir(filepath.Dir(filepath.Dir(root)))
				resolved := filepath.Join(exportRoot, *m.MediaURL)
				if _, statErr := os.Stat(resolved); statErr != nil {
					fmt.Printf("    !! media_url does not resolve to a real file: %s (%v)\n", resolved, statErr)
				}
			}
		}

		totalMsgs += len(msgs)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "walk:", err)
		os.Exit(1)
	}

	fmt.Printf("\n%d message_*.json files, %d messages total\n", totalFiles, totalMsgs)
}
