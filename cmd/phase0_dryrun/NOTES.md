# Phase 0 validation dry-run — tooling notes

Roadmap Phase 0 item: run the full Pegasus pipeline against one real
month of WhatsApp history and produce evidence Marco can evaluate before
committing to a full historical import.

**Scope of this document**: the harness, the design decisions behind it,
and what it found running against small real-Costguard-backed data during
development. It does **not** contain an accuracy judgment or a go/no-go
decision — Claude Code can't judge extraction quality against Marco's
real conversations. That happens once Marco fills in `sample.md` by hand
and reads `summary.md`'s three decision inputs.

## What was built

`cmd/phase0_dryrun` — two modes, mirroring `cmd/whisper_spotcheck`'s
`transcribe`/`report` split (the expensive real-Costguard phase kept
separate from formatting, so the report can be regenerated — different
sample size, a fixed seed, a `--total-months` figure once known — without
re-running the actual import):

- `run --export <whatsapp export .txt> --out ~/pegasus-reports/run.json` — runs
  `api.ImportWhatsApp` exactly as built (no dry-run-specific code path,
  see below), against real Costguard and the real Pegasus database, and
  captures the full result plus Costguard's own usage data for the run's
  time window.
- `report --in ~/pegasus-reports/run.json [--in run2.json ...] --out-summary ~/pegasus-reports/summary.md
  --out-sample ~/pegasus-reports/sample.md [--sample-size N] [--seed N] [--total-months N]`
  — reads one or more `run.json` files (repeatable `--in`, in case Marco's
  "one month" spans more than one conversation export) and produces the
  two review artifacts.

**`--out`/`--out-summary`/`--out-sample` must point outside this repo** —
`internal/reportpath.EnsureOutsideRepo` refuses to write real personal
report output anywhere git could track it, regardless of gitignore
state. Added after real report output (voice transcripts, contacts'
names, extracted facts) was accidentally committed to this repo while it
was still public — this guard is the recurrence prevention, separate
from whatever was done about the already-exposed history. The example
paths above (`~/pegasus-reports/...`) aren't just a suggestion; a
repo-relative path like plain `run.json` will now error out.

## Requirement 2's constraint, and what it forced

"No special-cased dry-run mode that behaves differently from real bulk
import" ruled out the most obvious implementation (a wrapper that
re-derives the same decisions ImportWhatsApp already makes, just to
report on them). But `ImportWhatsApp(ctx, exportPath) (error)` — its
signature as the write-methods issue shipped it — gives a caller nothing
to observe beyond success/failure. There was no way to get triage-
candidate-rate, reinforcement-vs-new-edge counts, or per-window
provenance without either that forbidden special-cased path, or fragile
after-the-fact DB diffing (inferring "was this edge new or reinforced"
by comparing timestamps before/after a run — brittle, and blind to
anything CorrectMemory touches).

The fix: extend `ImportWhatsApp`'s return type from `error` to
`(*api.ImportResult, error)`, and `storeExtractedTriples`'s (unexported,
shared by both ProcessLiveMessage and ImportWhatsApp) from
`([]*memory.Edge, error)` to `([]api.TripleOutcome, error)`. Both changes
are additive — every existing call site either already discarded the old
return value (`if _, err := ...`) or now genuinely benefits from the
extra detail. This is real, first-integration-test signal exactly the
way this issue's own framing predicted: the write-methods issue's
`ImportWhatsApp` signature was under-specified for anyone who actually
needed to observe what it did, not just whether it succeeded. A real
production caller (a monitoring dashboard, a progress bar for a
multi-year import) would want this exact data too — it's not a
dry-run-only concern.

## Where the cost data actually comes from

Costguard's admin HTTP API (`/usage/summary`, `/usage/teams`,
`/usage/projects`, `/usage/agents` — `costguard/internal/server/admin/`)
does not expose a per-agent-per-model breakdown: `/usage/agents` gives
total spend per agent with no model dimension, and there's no
`/usage/models` route at all, even though the underlying `usage.Store`
interface (`costguard/internal/usage/store.go`) has `GetSpendByModel` —
it's just never wired to a route. This tool needs "cost for triage vs.
extraction vs. embeddings" (three models, one agent tag), which the
admin API genuinely cannot answer.

So `run` queries Costguard's `usage_records` table directly (read-only),
filtered by `agent` and the run's time window, grouped by `(model,
path)`. This required local filesystem access to the Costguard repo
checkout to read its actual schema
(`costguard/internal/usage/postgres_store.go`,
`costguard/internal/usage/record.go`) and its `docker-compose.yml` for
connection details — both read-only lookups against Marco's own local
Costguard instance, not a third-party system.

## A real finding from running this against real Costguard: `price_found`

Every usage row from local Ollama models (`llama3.2:3b`,
`qwen3-coder:30b`) came back `price_found=false`, `estimated_cost_usd=0`.
That is **not** the same as "confirmed free" — it means Costguard has no
price configured for these models at all, and defaults the cost to 0
rather than erroring. A naive report would show "$0.00" and a reader
would reasonably assume local models cost nothing. Both could be true
(self-hosted models plausibly have zero marginal per-token cost) or
Costguard's pricing table might simply be incomplete — `usage_records`
alone can't distinguish the two. `UsageRow` and the summary report both
now carry a `price_found` count so this is stated explicitly rather than
silently implied. If the true per-token economics ever matter (e.g. this
moves to a metered API, or local compute cost needs attributing somehow),
that needs Costguard pricing configuration, not a tooling fix here.

## What this dry-run's cost figure does NOT include

**Update — embeddings are now included.** The gap described below (this
section, as originally written) was closed by a follow-up issue: embedding
generation is now wired into `storeExtractedTriples`
(`internal/api/pipeline.go`'s `embedMessages`), one embedding per distinct
source message, called after that function's triple-storage loop
succeeds (log-and-continue on failure — see `embedMessages`' own doc
comment for the full reasoning). `run`'s existing usage query (grouped by
`model, path`) picked this up with zero changes needed — confirmed
against a live Costguard run, not assumed: `nomic-embed-text` /
`/v1/embeddings` shows up as its own row in `summary.md`'s cost table,
with the same `price_found` honesty already applied to the other local
models (it came back `price_found=false` too, during that same
confirmation run). What did need a small fix: `summary.md`'s "not
captured" caveat about embeddings was now stale once real embedding
calls started happening — see `summary.go`'s `writeCostAndTiming` for the
corrected wording.

**Update — voice transcription is now included too.** The gap described
below (this section, as originally written) was closed by a follow-up
issue: voice messages are transcribed (local Speaches, escalating to
OpenAI on low confidence or detected Arabic — see
`internal/api/voice_transcription.go`) before entering the same
triage/extraction/embedding path text does. `run`'s usage query picked
this up automatically too: `Systran/faster-whisper-large-v3` /
`/v1/audio/transcriptions` shows up as its own row in `summary.md`'s cost
table.

Confirmed against a REAL run, not a synthetic smoke test — see "Real
manual-test findings" below for what that run actually surfaced,
including two problems it's important to read before trusting a
transcription-heavy cost/completeness figure from this tool.

~~- **Voice transcription** — voice notes are stored (`processed=true`
  immediately) but never transcribed or extracted; no Whisper/audio call
  happens anywhere in `ImportWhatsApp`, so they never reach embedding
  either (`messageText` in `pipeline.go` has nothing to embed without a
  transcript). If Marco's real month includes voice notes, their real
  transcription (and embedding) cost is entirely absent from this tool's
  cost figures — a materially incomplete cost picture if voice volume is
  significant, not just a rounding gap.~~ (closed, see above)

Stated plainly in `summary.md` itself, not just here.

## Real manual-test findings: running `run` against Marco's actual Demi
## Vronen export with voice transcription wired in

Run against the real export (374 messages, 44 real voice notes) rather
than synthetic data, per this tool's own "no fabricated real data" rule.
Two real, unrelated problems surfaced:

1. **The Speaches idle-restart takes far longer than `whisper_spotcheck`
   originally observed.** `NOTES.md`'s spot-check (this same document,
   the sibling `cmd/whisper_spotcheck` one) found "a single retry a few
   seconds later succeeded both times" and the voice-transcription
   issue's retry logic (`extraction.Transcribe`) originally used a 3s
   backoff on that basis. This real run showed that estimate was too
   optimistic: 11 of the 44 real voice notes failed BOTH the local and
   escalated leg entirely. Investigating live (`docker inspect
   infra-speaches-1`, direct `curl` against its `/v1/audio/transcriptions`
   endpoint) confirmed Speaches doesn't just reload its model after 300s
   idle — the whole container process restarts (`RestartCount`
   incrementing on each post-idle request), and a full restart-to-ready
   cycle took 20-30+ seconds in this environment, not "a few seconds". Two
   direct curl calls in a row both timed out waiting for that restart.
   **Fixed**: `DefaultTranscriptionRetryBackoff` raised from 3s to 30s
   (`internal/extraction/transcribe.go`), now overridable per-client via
   `CostguardClient.TranscriptionRetryBackoff` so tests don't have to sit
   through it. The failure-HANDLING itself was already correct under this
   real, sustained failure — those 11 messages were logged clearly, left
   `transcript`/`processed` unset, and did not abort the batch — this was
   purely the backoff constant being wrong, not a correctness bug.

2. **A separate, pre-existing bug crashed the run at window 32/46 —
   nothing to do with voice.** `ImportWhatsApp: extract window 32/46:
   extraction call: parse costguard response: invalid character '\x1f'
   looking for beginning of value`. `\x1f` is gzip's magic byte:
   Costguard returned a gzip-encoded `/v1/chat/completions` response for
   that extraction (Pass 2) call, and `internal/extraction/client.go`'s
   `Complete`/`CompleteWithModel` didn't decompress it before JSON-parsing.
   This is in the general chat-completion HTTP path both triage and
   extraction share — unrelated to `internal/extraction/transcribe.go`'s
   audio call, which is a separate method — and pre-dates the
   voice-transcription issue entirely. Not fixed as part of voice
   transcription (different code path, out of that issue's scope).

   **Update — fixed in a follow-up issue
   (`fix-gzip-response-handling` branch).** Investigation (not assumption):
   `CompleteWithModel` sets no `Accept-Encoding` and uses no custom
   `Transport`/`DisableCompression`, ruling out the client breaking its own
   auto-decompression. Could NOT reproduce the failure live against a
   running Costguard instance — four direct curl tests (both models, small
   and large responses, with and without an explicit
   `Accept-Encoding: gzip`) all came back plain, uncompressed JSON.
   Read-only investigation of Costguard's own source found its
   upstream-facing client already relies on Go's standard transparent gzip
   decompression when talking to a provider (it has its own test proving
   this), and its cache-entry-cloning code (`internal/gateway/response.go`)
   deliberately strips `Content-Encoding` when copying headers — "cached
   bodies are always stored decoded... replaying a Content-Encoding header
   would be wrong" — meaning Costguard's own maintainers already treat
   this exact header/body mismatch as a real hazard elsewhere in that
   codebase. Conclusion: most likely an intermittent condition on
   Costguard's side (cache/retry/streaming-related, not audited further —
   out of scope for a Pegasus-side fix), not a deterministic "Costguard
   always gzips."

   Fix (`internal/extraction/client.go`'s `maybeDecompressGzip`): checks
   BOTH the `Content-Encoding: gzip` header AND the response body's own
   gzip magic bytes (0x1f 0x8b) as a fallback — the second check exists
   specifically because a response with compressed bytes but no
   announcing header is the scenario this investigation judged most
   likely, and a header-only check would silently miss exactly that case.
   Applied once, before the status-code check, so a compressed error body
   is also readable. `Embed`/`Transcribe` deliberately left untouched — an
   `/v1/embeddings` check during this investigation was inconclusive (an
   unrelated connection failure), not a confirmed Costguard-wide pattern.

   **Real end-to-end validation**: `cmd/phase0_dryrun run` against a real
   223-message slice of the Bugs export completed cleanly — 28 windows,
   45 real chat-completion round trips (28 triage + 17 extraction calls),
   zero errors, zero rejected triples. No `\x1f` crash. (The full,
   unsliced Bugs export is 16,596 messages — far too large to run
   end-to-end for this kind of validation; a real, unsliced-content slice
   of it, not synthetic data, was used instead, consistent with this
   tool's own "no fabricated data" rule — only the SIZE was reduced, not
   the source.) This doesn't prove the exact intermittent gzip condition
   was hit again during this specific run (nothing currently logs whether
   `maybeDecompressGzip`'s gzip branch actually fired) — only that the
   fix doesn't break the normal case across real traffic, and that no
   equivalent crash recurred.

## The triage-candidate-rate caveat

§5 estimates 10-20% of messages get flagged as extraction candidates.
This tool's number is a WINDOW-level rate (one triage call per ~8-message
batch, per §5's own cost-discipline note for historical import, not
per-message the way live triage works) — a window is all-or-nothing, so
one interesting message pulls its whole window in. That structurally
inflates the rate relative to a true per-message measurement. Comparing
this run's percentage directly against §5's estimate is comparing two
different units; `summary.md` says so rather than presenting a
misleadingly precise-looking single number.

## Explicitly out of scope: backfilling old messages' missing embeddings

Any message written before embedding generation was wired in (including
the real Demi Vronen and Bugs dry-run data from before this fix) has no
embedding row and stays that way — this issue does not backfill it. A
small batch utility (query for messages with no matching `embeddings`
row, call `Embed` for each) is a natural, likely-immediate follow-up
issue, made possible by `embedMessages`' error logging (every embed
failure is logged with its message ID specifically so it's
discoverable and re-embeddable later) — not built speculatively here.

## What still needs Marco

This tooling is built and mechanically verified (small synthetic
conversation, real Costguard, real Pegasus DB — see the smoke-test run
during development, not committed). It has **not** been run against
Marco's real one-month export — that's requirement 1's actual point, and
fabricating one would defeat it. Once he supplies a real export:

```
go run ./cmd/phase0_dryrun run --export <path> --out run.json
go run ./cmd/phase0_dryrun report --in run.json --out-summary summary.md --out-sample sample.md --total-months <real backlog size>
```

Then Marco fills in `sample.md`'s Correct? column by hand, and reads
`summary.md`'s three decision inputs (error rate, cost extrapolation,
failure clusters) to make the actual go/no-go call — not something this
tooling does for him.
