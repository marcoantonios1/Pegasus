#!/usr/bin/env bash
# Tests for check-sensitive-filenames.sh. Run locally or in CI:
#   ./scripts/hooks/test-check-sensitive-filenames.sh
set -euo pipefail

script="$(cd "$(dirname "$0")" && pwd)/check-sensitive-filenames.sh"
fail=0

expect_blocked() {
    if printf '%s\n' "$1" | "$script" >/dev/null 2>&1; then
        echo "FAIL: expected '$1' to be blocked"
        fail=1
    fi
}

expect_allowed() {
    if ! printf '%s\n' "$1" | "$script" >/dev/null 2>&1; then
        echo "FAIL: expected '$1' to be allowed"
        fail=1
    fi
}

# Report-output patterns.
for p in review.md cmd/x/extraction_review.md sample_v2.md SUMMARY.md run.json a/b-run.json; do
    expect_blocked "$p"
done

# WhatsApp session/credential patterns.
for p in session.db some/dir/session.db session.db-wal session.db-shm \
         store.sqlite store.sqlite3 Store.DB whatsmeow.db whatsmeow-session \
         deep/path/whatsmeow_store.bin; do
    expect_blocked "$p"
done

# Ordinary files stay allowed (including code that merely mentions whatsmeow
# in its directory path or contents, and Go files like db/embed.go).
for p in README.md go.mod db/embed.go internal/ingestion/whatsapp/live.go \
         internal/ingestion/whatsapp/wasource/session.go docs/database.md \
         db/migrations/000001_create_entities_table.up.sql dbutil.go; do
    expect_allowed "$p"
done

# Multiple paths: one bad one blocks the whole set.
if printf 'README.md\nsession.db\n' | "$script" >/dev/null 2>&1; then
    echo "FAIL: a set containing session.db should be blocked"
    fail=1
fi

# Empty input passes.
if ! printf '' | "$script" >/dev/null 2>&1; then
    echo "FAIL: empty input should pass"
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    exit 1
fi
echo "check-sensitive-filenames: all tests passed"
