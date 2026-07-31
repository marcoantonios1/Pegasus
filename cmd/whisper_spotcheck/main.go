// whisper_spotcheck is the tooling for the Phase 0 Whisper transcription
// spot-check (roadmap item: "Spot-check local Whisper transcription
// quality on real Lebanese Arabic/English/French code-switched voice
// notes before trusting bulk historical import").
//
// This tool does NOT judge transcription accuracy — it can't hear the
// audio it's transcribing. It builds the evidence (local vs. OpenAI
// transcripts side by side, plus whatever confidence signal the local
// model exposes) for Marco to judge by ear. See whisper_spotcheck_review.md
// for the actual findings and the accuracy judgment once produced.
//
// Two modes:
//
//	go run ./cmd/whisper_spotcheck transcribe --dir <audio dir> --base-url <costguard url> --label <local|openai> --out <results.json>
//	go run ./cmd/whisper_spotcheck report --local local.json --openai openai.json --out review.md
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: whisper_spotcheck <transcribe|report> [flags]")
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "transcribe":
		err = runTranscribe(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (want transcribe or report)\n", os.Args[1])
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
