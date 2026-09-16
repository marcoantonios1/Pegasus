package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
)

// multiFlag collects a repeatable -in flag into a slice — report can
// merge several run.json files (e.g. one per conversation) into a single
// sample/summary, since a single ImportWhatsApp call only ever covers one
// conversation (see ImportWhatsApp's own doc comment) and Marco's "one
// real month" may span more than one chat.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	var inPaths multiFlag
	fs.Var(&inPaths, "in", "path to a run.json produced by `phase0_dryrun run` (repeatable, to merge multiple conversations)")
	sampleSize := fs.Int("sample-size", 75, "target sample size for manual review (proposal suggests 50-100)")
	seed := fs.Int64("seed", time.Now().UnixNano(), "random seed for sample selection — pass a fixed value to reproduce the same sample")
	totalMonths := fs.Float64("total-months", 0, "estimated total months of history for the full-import cost/time extrapolation (0 = report per-month rate only, no extrapolated total — supply this once you know your real backlog size)")
	outSummary := fs.String("out-summary", "", "output path for the summary report (markdown)")
	outSample := fs.String("out-sample", "", "output path for the manual-review sample (markdown)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(inPaths) == 0 || *outSummary == "" || *outSample == "" {
		return fmt.Errorf("--in (at least one), --out-summary, and --out-sample are required")
	}

	var runs []RunResult
	for _, p := range inPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %s: %w", p, err)
		}
		var r RunResult
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("parse %s: %w", p, err)
		}
		runs = append(runs, r)
	}

	if err := writeSummary(*outSummary, runs, *totalMonths); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	fmt.Printf("wrote %s\n", *outSummary)

	if err := writeSample(*outSample, runs, *sampleSize, *seed); err != nil {
		return fmt.Errorf("write sample: %w", err)
	}
	fmt.Printf("wrote %s\n", *outSample)

	return nil
}

// sampleItem is one candidate for the manual-review sample: an extracted
// triple's outcome plus the source window it came from, for showing the
// conversation context an annotator needs to judge it.
type sampleItem struct {
	SourceFile     string
	WindowIndex    int
	WindowMessages []extraction.WindowMessage
	Outcome        api.TripleOutcome
}

func flattenOutcomes(runs []RunResult) []sampleItem {
	var items []sampleItem
	for _, r := range runs {
		if r.Import == nil {
			continue
		}
		for _, w := range r.Import.Windows {
			for _, o := range w.Outcomes {
				items = append(items, sampleItem{
					SourceFile: r.ExportPath, WindowIndex: w.Index,
					WindowMessages: w.Messages, Outcome: o,
				})
			}
		}
	}
	return items
}

func edgeDescription(o api.TripleOutcome) string {
	object := o.Triple.Object
	confStr := fmt.Sprintf("%.2f", o.Triple.Confidence)
	return fmt.Sprintf("**%s** --%s--> %q (confidence=%s, is_correction=%v)", o.Triple.Subject, o.Triple.Predicate, object, confStr, o.Triple.IsCorrection)
}

func windowContext(msgs []extraction.WindowMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		fmt.Fprintf(&b, "> [%s] %s: %s\n", m.Timestamp.Format("2006-01-02 15:04"), m.Speaker, m.Text)
	}
	return b.String()
}
