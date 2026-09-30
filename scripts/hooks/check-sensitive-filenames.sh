#!/usr/bin/env bash
# check-sensitive-filenames.sh — blocks known review/report-output
# filename patterns from ever being committed again. Real personal data
# (voice transcripts, contacts' names, extracted facts about real people)
# was once committed to this repo under exactly these names —
# cmd/whisper_spotcheck/review.md, internal/extraction/
# extraction_review.md, internal/extraction/event_tense_review.md — while
# the repo was still public. internal/reportpath.EnsureOutsideRepo stops
# these tools from writing such output inside the repo going forward, but
# that only covers files THESE tools create; this script is the second,
# independent backstop against any file with one of these names landing
# in a commit by any means (a hand-written note, a copy-paste, a
# different tool entirely).
#
# Used by two callers, kept in one place so they can't drift out of sync
# with each other:
#   - scripts/hooks/pre-commit (the local, opt-in hook — pipes in
#     `git diff --cached --name-only`)
#   - .github/workflows/check-sensitive-filenames.yml (the CI backstop,
#     required because a local hook is opt-in and --no-verify bypasses it
#     — pipes in `git ls-files`, every tracked file at that ref)
#
# Reads file paths, one per line, on stdin. Exits 1 (and lists every
# match on stderr) if any match; exits 0 otherwise.
#
# Patterns (case-insensitive glob match against each path's basename —
# NOT the full path, so a match anywhere in the tree is caught):
#   *review*.md   review.md, extraction_review.md, event_tense_review.md,
#                 and dated/prefixed variants (review_2026-09-30.md,
#                 my_review.md, whisper_review.md, ...)
#   *sample*.md   sample.md and variants (sample_v2.md, sample_bugs.md,
#                 sample_bugs_v2.md, ...)
#   *summary*.md  summary.md and variants (summary_v2.md, summary_bugs.md,
#                 summary_bugs_v2.md, ...)
#   *run*.json    run.json and variants (run_bugs.json, run_v2.json,
#                 demi-vronen-run.json, bugs-slice-gzipfix-run.json, ...)
#
# These four patterns are deliberately broad (substring match, not exact
# name match) — a false positive here costs a rename; a false negative
# costs a repeat of the actual incident this exists to prevent. If a
# genuinely non-sensitive file needs one of these names, rename it rather
# than weakening these patterns.
set -euo pipefail

patterns=('*review*.md' '*sample*.md' '*summary*.md' '*run*.json')

matched=()
while IFS= read -r path; do
    [ -z "$path" ] && continue
    base=$(basename "$path")
    base_lower=$(printf '%s' "$base" | tr '[:upper:]' '[:lower:]')
    for pattern in "${patterns[@]}"; do
        # shellcheck disable=SC2053
        if [[ "$base_lower" == $pattern ]]; then
            matched+=("$path  (matches pattern: $pattern)")
            break
        fi
    done
done

if [ "${#matched[@]}" -gt 0 ]; then
    {
        echo "BLOCKED: the following file(s) match a known real-personal-data report-output pattern:"
        for m in "${matched[@]}"; do
            echo "  - $m"
        done
        echo
        echo "review*.md, sample*.md, summary*.md, and run*.json are how this repo's dry-run/"
        echo "spot-check tooling has historically named real personal-data output (voice"
        echo "transcripts, contacts' names, extracted facts) — see cmd/phase0_dryrun/NOTES.md"
        echo "and cmd/whisper_spotcheck/NOTES.md. That output must always be written outside"
        echo "this repo (internal/reportpath.EnsureOutsideRepo enforces this in the tools"
        echo "themselves); it must never be committed."
        echo
        echo "If this file genuinely isn't sensitive, rename it to avoid the pattern — don't"
        echo "bypass this check (--no-verify) instead."
    } >&2
    exit 1
fi

exit 0
