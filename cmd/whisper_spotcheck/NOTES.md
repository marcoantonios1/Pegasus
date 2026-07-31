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

## Analysis of Marco's ratings (requirement 6)

**Rating scale used**: a single 0-100% accuracy estimate per file, filled
in by Marco in `review.md`. Confirmed with Marco this represents both
engines together except where a note overrides it — one file
(`4171339089673893.ogg`) has an explicit split ("0% for local" vs. the
90% listed, which is OpenAI's). 21 of 22 files were rated;
`984286304369025.ogg` was left blank and is excluded from the numbers
below rather than guessed at.

**Aggregate**: mean 92.5%, median 100%, min 0%. 15 of 21 files rated a
perfect 100% — every English-dominant file among them. All 6 files rated
below 100% involve Arabic, either alone or code-switched with English.
This part matches the proposal's underlying instinct: non-English content
is where the risk concentrates, and English transcription is essentially
solved by both engines already.

### Does errors cluster on code-switch boundaries specifically? Mixed — not cleanly.

Went through the 4 lowest-rated files directly (this part is Claude's
reading of the transcripts, not Marco's per-file judgment — the
"Code-switch boundary error?" column in `review.md` was left blank):

- **`4171339089673893.ogg` (0%)** — local completely misidentified the
  language as Yiddish and produced garbage in Hebrew script on a short
  (6.2s) clip; OpenAI correctly identified Arabic. **Not a code-switch
  error** — there's no English in this clip at all. A language-ID
  robustness failure on short/ambiguous audio.
- **`1569244804590020.ogg` (70%)** — both engines produced pure Arabic,
  zero English, with real word-level errors. **Not a code-switch error**
  — general Arabic transcription accuracy.
- **`1381303107441089.ogg` (85%)** — genuinely code-switch-related:
  OpenAI renders "bad news"/"BID" in Latin script; local transliterates
  the same words phonetically into Arabic script instead. A real
  divergence in how the two engines *handle* a switch, not obviously a
  clear-cut "one is right, one is wrong" case without hearing the audio.
- **`1026963569878900.ogg` (90%)** — the translation issue Marco flagged
  directly, at a heavily code-switched sentence.

**Conclusion**: only 2 of the 4 worst files are actually code-switch
related. General Arabic accuracy and language-ID robustness on short
clips are independent, comparably-sized risk factors — an escalation
design that only watches for code-switching specifically would miss the
other two failure modes entirely.

### Does local confidence predict accuracy? Weakly — not reliably enough on its own.

Pearson correlation between Marco's ratings and local's own confidence
signals (n=21, `4171339089673893.ogg` scored using its local=0%
override):

- rating vs. local `avg_logprob`: **r = 0.547** (moderate positive —
  right direction, not strong)
- rating vs. local `no_speech_prob`: **r = 0.172** (essentially no
  signal)

More important than the coefficient: **local confidence completely
missed the single worst failure.** `4171339089673893.ogg` (0%, total
language misidentification) had `avg_logprob = -0.483` — only the
3rd-worst value in the whole set, not flagged as the most alarming.
Meanwhile `997737946584480.mp4` (rated 100%) had a *worse*
`avg_logprob` (-0.409) than several files rated below it. This is exactly
the risk named in requirement 6 as a hypothesis to check, not assume: **a
model can be confidently wrong, and this run proves it actually happens**
— the worst mistake wasn't the least confident one.

### The decision: is local-only sufficient, or does the escalation trigger need rethinking?

**Rethinking needed — local-only as currently specified (escalate on low
`avg_logprob` alone) is not trustworthy enough for Arabic/code-switched
content, though it's fine for English.** Reasoning:

1. English content is effectively solved locally — 100% across every
   English-dominant file tested. No escalation needed there.
2. For Arabic/code-switched content, confidence-based escalation would
   need to be tuned very conservatively (escalate often) to catch real
   failures, given r=0.547 — undermining the cost-saving point of having
   a local-first tier at all for this content.
3. It would still miss the worst case. A threshold tuned to catch
   `4171339089673893.ogg` (the actual 0% failure) would also have to
   catch many higher-confidence files that were actually fine, given its
   `avg_logprob` wasn't even close to the worst in the set.
4. Escalating more Arabic content to OpenAI isn't a clean fix by itself
   either — the translation-vs-transcription problem (§ above) is a
   *different* failure mode that confidence scores wouldn't catch or fix,
   and could be worse for downstream extraction than a low-confidence but
   faithful local transcript, since it changes the actual words rather
   than just the certainty about them.

**A concrete, untested next step worth trying** (flagged as an idea, not
verified here — out of scope for this pass): OpenAI's API accepts an
explicit `language` parameter, which is a known mitigation for Whisper's
translate-instead-of-transcribe tendency. Worth testing whether passing
`language=ar` reduces the translation behavior seen on
`1026963569878900.ogg` before deciding OpenAI-escalation is unusable for
Arabic content.

**Practical recommendation for bulk import**: proceed with local-only for
English-dominant conversations now. For Arabic/code-switched
conversations, either escalate unconditionally (skip the confidence gate
entirely for non-English-detected segments, since confidence didn't
reliably separate good from catastrophic here) or hold off until the
`language=ar` mitigation above is tested — don't rely on the current
avg_logprob threshold alone for this content. This is a recommendation
grounded in the evidence above; the actual call is Marco's.
