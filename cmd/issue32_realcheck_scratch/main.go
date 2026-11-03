// Throwaway real-model check for Issue 32's fix — NOT part of the
// committed codebase. Runs the real extraction model (via real
// Costguard) against two small correction-style windows: one stating
// the old value explicitly, one not. Prints the raw triples so
// replaces_object's real behavior can be eyeballed. This whole directory
// is deleted after the check — nothing from it is committed.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
)

func main() {
	client := extraction.NewCostguardClient("http://localhost:8080")
	client.Agent = "pegasus-issue32-replaces-object-check"
	extractor := extraction.NewExtractor(client)
	extractor.Logger = func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }

	base := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)

	windows := map[string]extraction.Window{
		"states_old_value": {
			Messages: []extraction.WindowMessage{
				{Speaker: "Marco", Timestamp: base, Text: "I'm thinking of moving to Dubai next year for work"},
				{Speaker: "Sara", Timestamp: base.Add(1 * time.Minute), Text: "oh nice, when do you leave?"},
				{Speaker: "Marco", Timestamp: base.Add(2 * time.Minute), Text: "actually wait, that's wrong — I already moved, and it was to Beirut, not Dubai"},
			},
		},
		"no_old_value_stated": {
			Messages: []extraction.WindowMessage{
				{Speaker: "Marco", Timestamp: base, Text: "my birthday is in April"},
				{Speaker: "Sara", Timestamp: base.Add(1 * time.Minute), Text: "got it, I'll remember that"},
				{Speaker: "Marco", Timestamp: base.Add(2 * time.Minute), Text: "actually no that's wrong, it's in March"},
			},
		},
	}

	for name, w := range windows {
		fmt.Printf("=== window: %s ===\n", name)
		triples, err := extractor.ExtractWindow(context.Background(), w)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			continue
		}
		for _, t := range triples {
			replaces := "<nil>"
			if t.ReplacesObject != nil {
				replaces = *t.ReplacesObject
			}
			idx := "<nil>"
			if t.CorrectionSourceIndex != nil {
				idx = fmt.Sprintf("%d", *t.CorrectionSourceIndex)
			}
			fmt.Printf("  subject=%q predicate=%q object=%q is_correction=%v correction_source_index=%s replaces_object=%s\n",
				t.Subject, t.Predicate, t.Object, t.IsCorrection, idx, replaces)
		}
		fmt.Println()
	}
}
