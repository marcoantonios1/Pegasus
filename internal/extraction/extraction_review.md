# Extraction prompt/vocabulary v1 — manual review findings

Validation run for proposal §8 acceptance criteria (items 7-8): the
finalized extraction prompt and predicate vocabulary v1, run against a real
WhatsApp export via a live Costguard → Ollama → `qwen3-coder:30b` pipeline
(not synthetic messages).

## Setup

- Source: a real WhatsApp group export ("Bugs" group chat with Marco,
  Kevin Azzi, Charbel Akl), `.txt` format, parsed via the existing
  `internal/ingestion/whatsapp` adapter.
- Sample: 96 consecutive text messages (offset 50, skipping the export's
  leading system/media noise), historical-path windowing
  (`WindowByCount`, size 8) → 12 windows.
- Model: `qwen3-coder:30b` via a locally running Costguard instance
  (`docker compose up`) proxying to a local Ollama.
- 12 windows / 96 messages processed in ~25-35s wall-clock — call latency
  is not a practical concern at this sample size.
- The conversation is heavily code-switched Lebanese Arabic-in-Latin-script
  ("Arabizi") mixed with English, which turned out to be a significant,
  unplanned stress test given §8's code-switching concerns were framed
  around voice transcription, not text extraction.

## What worked

- **Closed vocabulary enforcement held completely**: 0 rejections across
  three full runs (~90 triples total) — the model never emitted a
  predicate outside the closed list. This validates the proposal's premise
  that a coder-tuned model holds schema constraints reliably. Worth noting
  explicitly: this is a *separate axis* from factual accuracy — see below,
  the model stayed within the schema while still fabricating content on
  some windows.
- **Correctly returns `[]` for genuinely unextractable windows** — pure
  banter, prices/jokes with no factual content, an Instagram link share —
  in 4 of the 12 windows, with no forced/invented content. (One of those
  four regressed to hallucinating during a since-reverted prompt
  experiment — see below — confirming this behavior is real but not
  bulletproof against prompt changes.)
- **Extraction quality is strong on clear English text**: the gym/lockers
  story (windows 1-2) and the app-feasibility discussion (window 10)
  produced accurate, well-grounded triples with reasonable confidence.
- **A genuine nickname reveal was caught correctly**: `Marco --
  nickname_is --> Saniour` (window 11), directly grounded in Marco's own
  message.

## What didn't work — bugs found and fixed

### 1. Model echoed the prompt's own example phrases as fabricated facts (fixed)

`event_past`/`event_present`/`event_future`'s descriptions originally
included concrete quoted examples lifted straight from proposal §8.5
("went to Rome", "lives in Beirut", "going to Italy next week"). On windows
where the actual message content was untranslatable Arabizi slang, the
model didn't return `[]` — it fabricated triples using **those exact
phrases**, verbatim, as if they'd been extracted from the conversation
(`Charbel Akl -- event_past --> went to Rome`, confidence 0.30, on a window
that never mentions Rome, Italy, or anywhere else). Confirmed reproducible
across two separate windows with identical fabricated phrases and
confidence scores.

**Fix**: reworded the three event predicate descriptions to describe the
temporal semantics abstractly, with no concrete quotable example string.
Re-ran the identical sample after the fix — the Rome/Italy/Beirut
fabrication is completely gone (`grep -i "rome\|italy\|beirut"` on the
post-fix output: 0 matches). See `predicates.go`.

**Takeaway for future predicate additions**: don't put concrete quoted
example phrases in a predicate `Description` that gets injected into the
prompt — describe the pattern, don't give the model a copyable string.

### 2. `inside_joke_ref`'s object held a verbatim message quote, not a topic (fixed)

Window 1 produced `Marco -- inside_joke_ref --> "Brooo ho rehben"` — the
object was a literal quoted snippet of the triggering message rather than
a short description of what the joke is about. As written, that's useless
for later retrieval (a future mention of the same running joke wouldn't
match this object string).

**Fix**: tightened the description to explicitly state the object should
be a short description of the joke, not a quoted snippet. Not
independently re-validated against a fresh `inside_joke_ref` case in this
sample (none recurred with the same content) — flag this as worth
re-checking specifically once more inside-joke-bearing data is available.

## What didn't work — found, attempted, reverted

### 3. "Said 'X'" tautology on unparseable slang (documented, not fixed)

On windows where messages were untranslatable Lebanese Arabic slang the
model sometimes couldn't parse in a factual sense (e.g. "la nik harimoun"),
it fell back to a degenerate pattern: emitting an `event_past` triple for
*every message in the window*, with the object being a near-verbatim quote
of the message text (`Charbel Akl -- event_past --> said 'la nik
harimoun'`). This is content-free noise — "a message was sent" is
trivially true of every message and carries no signal.

Tried: adding an explicit prompt instruction against "said 'X'"
tautologies and against guessing on unparseable content. Result on
re-running the identical sample: the target window's tautology pattern
*persisted* (same "said 'X'" output, just confidence 0.90 → 0.80), and a
*different* window that had previously and correctly returned `[]`
regressed into hallucinating three low-value triples instead. Net effect
was neutral-to-negative, not a validated fix, so it was reverted. See the
comment above `promptTemplateText` in `prompt.go`.

**Open problem for v2**: this needs a different approach than a prompt
instruction — candidates worth trying separately (not bundled together,
so each can be evaluated on its own): (a) a stricter validation-layer
heuristic that rejects triples whose object is near-identical to the
source message text (catchable programmatically, unlike "is this
factual"), (b) pre-translating clearly non-English/non-standard content
before extraction, or (c) accepting that certain slang is simply outside
this model's competence and relying on the confidence score to keep it out
of retrieval — except finding 4 below shows confidence isn't a reliable
signal for exactly this failure mode.

## Confidence calibration

Not well calibrated for hallucinated content specifically: the fabricated
Rome/Italy triples (before the fix) carried confidence 0.30-0.40 — lower
than genuine extractions (0.80-0.90), so the model does seem to "know" it's
less sure, but 0.30-0.40 is still well above zero for content that was
**entirely fabricated**, not just uncertain. A downstream confidence floor
tuned to filter noise would need to be quite aggressive to catch this
specific failure mode, and would likely also filter legitimate
lower-confidence-but-real facts. This is a real tension for whoever tunes
`GetRelevantContext()`'s confidence floor later — not resolved here, just
surfaced.

## Windowing: helped and hurt, both observed

- **Hurt**: the gym/lockers complaint (Charbel's story) starts at the tail
  of window 1 and continues into window 2 as Marco's parallel retelling in
  English. Each window only saw its own half and produced separate,
  disconnected triples about what's really one continuous exchange — the
  exact "joke/story split across a window boundary" failure mode §8.3
  anticipates for non-overlapping windows.
- **Helped**: no clear case in this sample where a signal was *only*
  extractable because of window context (vs. would've worked
  per-message too) — the inside-joke case (finding 2) shows windowing
  *attempting* to capture multi-message context, just with a malformed
  object. Not a strong positive data point either way from this
  particular sample; worth watching for a clearer example in a larger
  validation pass.

## Vocabulary gaps discovered (not added in v1 — candidates for v2)

- **No predicate for simple personal attributes** (window 8: a blood-type
  exchange, "Ana O+" / "Eh ana"). Low-value/niche on its own; not adding a
  one-off predicate for it, but flagging in case a broader
  "attribute_is"-shaped need shows up again.
- **`object_type` usage is inconsistent**: the model frequently marked
  descriptive-phrase objects (e.g. "found the lockers full", "posting
  videos on social media") as `"entity"` rather than `"literal"`, even
  though these aren't things that could sensibly be a *subject* elsewhere
  — which is the definition given in the prompt. Not a closed-vocabulary
  violation (both values are valid), so `ValidateTriples` doesn't and
  shouldn't reject these, but it means `object_type` may need a clearer
  prompt definition or a downstream cleanup pass before it's trustworthy
  for whatever consumes it.

## Non-determinism note

Re-running the exact same 96-message sample against the exact same prompt
(temperature 0.1, not 0) produced minor wording differences window-to-window
across otherwise-identical runs (e.g. "bought 5 techour" vs. "gave 5
techour to Saro") — same predicates, similar confidence, different object
phrasing. Not a bug, just a fact worth knowing: extraction output is not
byte-reproducible run-to-run even at low temperature, which matters for
anyone trying to diff/test against exact extraction output later.

## Summary for vocabulary v2 planning

1. Don't put concrete quotable examples in predicate descriptions (learned
   the hard way, fixed).
2. `inside_joke_ref`'s object shape needs a clearer contract, ideally
   re-validated against a fresh inside-joke case.
3. The "said 'X'" tautology on unparseable slang is real, reproducible,
   and unresolved — needs a non-prompt-instruction approach.
4. Confidence is not a reliable filter for this specific hallucination
   mode — don't assume a confidence floor alone solves it.
5. Window-boundary story-splitting is real and observed, consistent with
   §8.3's own acknowledged tradeoff of non-overlapping windows.
6. `object_type` needs tightening before anything downstream trusts the
   entity/literal distinction.

## Addendum — findings from the bulk-import validation pass

Running the same real chat through the new `internal/bulkimport`
orchestration (a different, earlier 107-message slice of the same export,
14 windows) surfaced two more data points worth folding into v2 planning:

- **`inside_joke_ref`'s earlier fix (finding 2) traded one problem for
  another**: the object is no longer a verbatim quote, but it's now
  frequently too generic to be useful — e.g. `Charbel Akl --
  inside_joke_ref --> "shared laughter and joking"`. That description
  can't distinguish this joke from any other joke in the same
  conversation, so it's not obviously more useful for future retrieval
  than the quote it replaced. `inside_joke_ref` likely needs a stronger
  prompt example of what a *good* object looks like, not just an
  instruction about what to avoid — worth a dedicated small validation
  pass once there's a clearer candidate description to test.
- **A likely `nickname_is` false positive**: `Marco -- nickname_is -->
  "Awie"` — "Awie" is a Lebanese Arabic interjection (roughly "yeah" /
  "really") in the source message, not a nickname reveal. Unlike the
  Rome/Italy case, this doesn't look like example-echoing — it looks like
  a genuine misread of Arabizi content as a self-identification. Same root
  cause category as finding 3 (the model's handling of Arabizi is weaker
  than English), different predicate. Not fixed here — noted as another
  data point that this specific code-switched slang is a systematic weak
  spot for this model, not a one-off.
