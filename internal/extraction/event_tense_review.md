# Event tense classification — manual review findings

Validation for proposal §8.5's implementation: the prompt's new event
tense-classification instructions and the ambiguous-tense fallback rule,
run against a live Costguard → Ollama → `qwen3-coder:30b` pipeline with
realistic message phrasing (not synthetic/mocked). This is a focused
follow-up to `extraction_review.md` — read that first for the broader
extraction validation context (windowing, closed vocabulary, the
Rome/Italy example-echoing bug, etc.), which this doesn't repeat.

## Setup

8 test messages, one window each (single-message windows, to isolate tense
classification from windowing effects): the three required tenses from
realistic phrasing (past trip, present residence, present job status,
future trip, future job start), one deliberately ambiguous case, and —
the case this issue is actually about — a message timestamped **two years
before "now"** using relative future phrasing ("next week"), to test
requirement 1's core concern directly: is tense classified relative to the
message's own timestamp, or does the model get pulled toward treating it
as past just because real time has since elapsed?

## The most important result: historical backlog classification works correctly

```
[historical 'next week' (2 years old)] (msg timestamp: 2024-03-01)
  text: "Marco: flying to Italy next week"
  -> Marco --[event_future]--> flying to Italy (confidence=0.95)
  CORRECT (wanted event_future)
```

This is exactly the failure mode requirement 1 was written to prevent: a
message from years ago describing a near-term plan must be classified as
it would have been understood *at the time*, not reinterpreted through
today's date. The model got this right, correctly relative to the
message's own timestamp rather than the real current date. This was the
single highest-risk case in the whole validation and it passed.

## Clear-cut tenses: correct

- "went to Rome last month, it was amazing" → `event_past` ✓
- "flying to Italy next week" → `event_future` ✓
- "starting the new job in March" → `event_future` ✓
- "I live in Beirut now" → `event_present` ✓ (see note below on
  reproducibility)

## Noise, not a bug: one non-deterministic miss on rerun

The first pass classified "I live in Beirut now" as `event_past`
(wrong — should be `event_present`). Rerunning the *identical* input 3
more times produced `event_present` all 3 times. This matches the
non-determinism already documented in `extraction_review.md` (temperature
0.1, not 0) — treating the single original miss as a real classification
bug would have been wrong; a 3x rerun before concluding "the model gets
this case wrong" was necessary and changed the conclusion.

## Real, reproducible gap: the ambiguous-tense fallback isn't reliably followed

The prompt (item 2 in this issue) instructs: on genuinely ambiguous
tense, default to `event_present` rather than guessing past or future.
Tested with "been talking about Dubai with the team" — deliberately
unclear whether this is a settled plan, a current state, or just informal
chat with no real tense. Result, reproduced across 4 total runs (1
original + 3 explicit reruns): **`event_past`, consistently, never
`event_present`.**

This is not noise — it's a stable model preference that doesn't match
the instruction. The instruction exists in the prompt (verified via
`TestBuildPrompt_ContainsEventTenseInstructions`) but doesn't reliably
change behavior on genuinely ambiguous input. Worth noting: `event_past`
is arguably a defensible reading of "been talking about" in isolation
(present-perfect-continuous does describe something already underway) —
so this may be as much a "this phrasing wasn't as ambiguous to the model
as intended" finding as a "the fallback instruction is weak" finding.
Either way, the instruction did not do what requirement 2 asked it to do
on this input, and that's the honest result.

**Not fixed here** — consistent with how the earlier "said 'X'" tautology
finding was handled (`extraction_review.md`, finding 3): documenting a
real, reproduced gap rather than iterating prompt wording live against a
single test phrase until it happens to pass, which risks overfitting to
one example rather than actually strengthening the instruction. Worth
revisiting with a larger battery of genuinely-ambiguous phrasings before
trying another prompt change, so a fix can be validated against more than
one example.

## Unexpected but reasonable: `works_at` beat `event_present` for a job-status message

"still working at Costguard" was extracted as `Marco -- works_at -->
Costguard`, not `event_present`. This was scored as a mismatch against
this validation's own expectation, but on reflection the expectation was
the flawed part, not the model's choice: `works_at` is a more specific,
better-fitting predicate already in the closed vocabulary for exactly
this case, and the model picking the specific predicate over the generic
`event_present` is arguably the *better* answer. This surfaces a real
vocabulary overlap worth flagging for v2 planning: `works_at`/`role_is`
and `event_present` can both plausibly apply to an ongoing-employment
statement, and nothing currently tells the model which to prefer when
both fit. Not a tense-classification bug — a predicate-selection
ambiguity between two already-valid predicates.

## Summary for follow-up

1. Requirement 1's core concern (historical backlog + relative date
   phrasing) is validated working correctly — this was the highest-risk
   part of this issue and it passed.
2. The ambiguous-tense fallback (requirement 2) is real in the prompt but
   not reliably followed by the model — flagged as an open gap, not
   force-fixed against a single example.
3. Confirmed (again) that single-run results need a reproducibility check
   before being treated as a finding — one miss out of one run was noise
   here, not a bug.
4. New vocabulary v2 candidate: clarify precedence between `works_at` /
   `role_is` and `event_present` for ongoing-employment statements.
