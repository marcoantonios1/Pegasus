// phase0_dryrun is the tooling for Pegasus's Phase 0 validation dry-run
// (roadmap, closes Phase 0): running the full pipeline against one real
// month of WhatsApp history and producing evidence Marco can evaluate —
// this tool does NOT judge extraction accuracy or make a go/no-go call.
// It runs the real pipeline, captures metrics/cost/timing, and produces a
// sample for manual review plus a summary of the likely failure
// clusters. See NOTES.md for the design decisions behind each piece.
//
// Two modes, mirroring cmd/whisper_spotcheck's transcribe/report split
// (the expensive real-Costguard phase separate from formatting, so
// report generation can be iterated on without re-running the import):
//
//	go run ./cmd/phase0_dryrun run --export <whatsapp export .txt> --out run.json
//	go run ./cmd/phase0_dryrun report --in run.json --out-summary summary.md --out-sample sample.md
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: phase0_dryrun <run|report> [flags]")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "run":
		err = runRun(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (want run or report)\n", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
