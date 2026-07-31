# Whisper transcription spot-check — tooling notes

Roadmap Phase 0 item: "Spot-check local Whisper transcription quality on
real Lebanese Arabic/English/French code-switched voice notes before
trusting bulk historical import."

**Scope of this document**: the harness, what it found about the
*infrastructure* (response shapes, real bugs hit running it against real
files), and what's still open. It does **not** contain an accuracy
judgment or a go/no-go decision — Claude Code can't hear the audio. That
judgment happens in `review.md`, by Marco, by ear.

## What was built

`cmd/whisper_spotcheck` — two modes:

- `transcribe --dir <audio dir> --base-url <costguard> --label <local|openai> --out <results.json>`
  — sends every audio file in a directory through Costguard's real
  `/v1/audio/transcriptions` endpoint (not a mock, not calling Speaches/
  OpenAI directly) and captures the full `verbose_json` response plus a
  few derived summary numbers.
- `report --local local.json --openai openai.json --out review.md` —
  merges both legs into one markdown file, one section per audio file,
  with a blank rating table for manual annotation.

Kept as a real (non-throwaway) tool for now, unlike most of this repo's
other manual-test CLIs — this issue is explicitly two-phase (build +
gather evidence now, compute the aggregate analysis once Marco has
annotated `review.md`), so the tooling has ongoing use until that second
phase is done.

## Requirement 1's open question: does Speaches expose segment-level confidence?

**Yes**, confirmed by hitting the live local endpoint directly before
writing any code against it. `response_format=verbose_json` returns
`segments[]` with real `avg_logprob`, `no_speech_prob`, and
`compression_ratio` per segment — the same shape real OpenAI Whisper
returns. This is the signal proposal §5's escalation trigger ("low
per-segment transcription confidence") depends on existing, and it does.

## A real trade-off found while building this: word-level vs. segment-level confidence

Speaches also returns real per-word `probability` values when the request
includes `timestamp_granularities[]=word`. Tried using that as a second,
finer-grained confidence signal — but verified live against the *real*
OpenAI API that requesting word-level granularity there **suppresses
segment data entirely** (`segments: null`) and returns only placeholder
word probabilities (always exactly `0`, not real data). Without that flag,
OpenAI returns real segment-level `avg_logprob`/`no_speech_prob`.

Since §5's escalation design is specifically about segment-level
confidence, the harness does **not** request word-level granularity —
relying on segment `avg_logprob`/`no_speech_prob`, the one signal both
providers reliably populate with real values.

## Two real bugs/gaps found running this against real data (not synthetic)

1. **`.mp4` wasn't in the harness's audio-extension filter.** Instagram
   voice notes you *send* are `.mp4` containers (confirmed: `file` reports
   `ISO Media, MP4 Base Media`); ones you *receive* are `.ogg`
   (Opus-in-Ogg). The first real run silently only processed half the
   files (the `.ogg` ones) until this was caught and fixed.

2. **OpenAI's real API rejects WhatsApp's raw `.opus` extension outright**
   — confirmed directly against `api.openai.com`, bypassing Costguard, to
   rule out a Costguard-specific issue:
   `Invalid file format. Supported formats: ['flac', 'm4a', 'mp3', 'mp4',
   'mpeg', 'mpga', 'oga', 'ogg', 'wav', 'webm']`. `.opus` is not on that
   list — `.oga`/`.ogg` are. Verified the byte content is otherwise
   identical and valid: renaming the exact same file from `.opus` to
   `.ogg` (no transcoding, no byte changes) transcribes successfully.
   The harness now does this rename at the multipart-part-name level
   before sending (bytes on disk untouched).

   **This isn't just a harness bug** — if any future real Pegasus
   escalation path (§5: local confidence low → escalate to OpenAI) ever
   sends a WhatsApp voice note's raw `.opus` file to OpenAI without this
   rename, every single escalation would fail with a 400. Whoever builds
   that escalation call site needs to know this.

## An operational quirk worth knowing about, separate from transcription quality

The local Speaches instance (`infra` repo, `docker-compose.yml`) unloads
its model after 300s idle (`ttl=300` in its own logged config). Observed
twice: the *first* request after that idle-unload doesn't just reload the
model — the whole container process restarts (fresh "Application startup
complete" in its logs), and every in-flight/immediately-following request
during that restart window fails with a 502. Retrying once, a few seconds
later, succeeded both times. Not something this issue needs to fix, but
worth knowing if bulk historical import gets built against this same local
instance: back off and retry on the first call after any idle period,
don't treat a single 502 as a hard failure.

## What's genuinely NOT done yet

- **The accuracy judgment itself** (`review.md`'s rating columns are
  blank, by design).
- **Error rate / code-switch-boundary clustering analysis** (requirement
  6) — blocked on Marco's ratings existing. Also blocked on knowing which
  rating scale he actually used (asked, not assumed) before the numbers
  can be interpreted correctly.
- **The go/no-go decision on local-only sufficiency** (requirement 6) —
  depends on whether local's `avg_logprob`/`no_speech_prob` actually
  correlates with the manual ratings once they exist. Not evaluated yet;
  there's real reason to expect it might not correlate well specifically
  on code-switched content (a model can be confidently wrong), but that's
  a hypothesis to check against real ratings, not a conclusion.

## How to finish this

1. Open `review.md`, fill in the "Manual accuracy rating" and "Code-switch
   boundary error?" columns by ear for all 22 files, note which rating
   scale was used.
2. Tell Claude the ratings are ready — the aggregate analysis (error rate,
   code-switch clustering, confidence correlation, the go/no-go
   recommendation) gets built as a follow-up once there's real annotated
   data to compute it from, not before.
