// Package reportpath guards against the exact failure mode behind a real
// incident: dry-run/spot-check tooling (cmd/phase0_dryrun,
// cmd/whisper_spotcheck) writing report output — real voice transcripts,
// real contacts' names, real extracted facts — to a path that happened to
// live inside this git repository, which was later `git add`ed and
// committed as if it were ordinary source/documentation (see
// cmd/whisper_spotcheck/review.md, internal/extraction/
// extraction_review.md, internal/extraction/event_tense_review.md's own
// history for the concrete instance of this).
//
// A .gitignore entry alone does not prevent this: `git add -A` or
// `git add -f` can still stage an ignored path, and nothing stops a
// human (or an earlier Claude Code session) from choosing an in-repo
// --out path to begin with, since these tools' --out flags have never
// had a default that pointed anywhere, safe or not — they just require
// an explicit path (see each tool's own flag definition). The actual gap
// was that nothing validated the CHOSEN path at all. This package closes
// that gap at the source: every --out-style flag across these tools must
// be checked with EnsureOutsideRepo before a single byte is written,
// regardless of gitignore state.
package reportpath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// EnsureOutsideRepo returns an error if path resolves to a location at or
// inside the git repository containing the current working directory
// (found by walking up from cwd looking for a .git entry — the same
// discovery method `git` itself uses, not a hardcoded repo name). Returns
// nil, not an error, if no .git is found at all walking up from cwd —
// running one of these tools from outside any git checkout has nothing
// to guard against.
func EnsureOutsideRepo(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve absolute path for %q: %w", path, err)
	}
	// Resolve symlinks on both sides before comparing — cwd is often
	// reached through a symlinked ancestor (macOS: /tmp -> /private/tmp,
	// /var -> /private/var is the common case), and os.Getwd() (which
	// repoRoot below is derived from) does not consistently return the
	// same form as an absolute path built from a caller-supplied string.
	// Comparing an unresolved path against a resolved repoRoot (or vice
	// versa) makes filepath.Rel's "is it inside" computation silently
	// wrong instead of erroring — caught by this package's own tests
	// (TestEnsureOutsideRepo_RejectsPathInsideRepo's absolute-path case
	// failed before this fix, on exactly this macOS symlink mismatch).
	absPath = resolveSymlinksBestEffort(absPath)

	repoRoot, found, err := findRepoRoot()
	if err != nil {
		return fmt.Errorf("locate git repository root: %w", err)
	}
	if !found {
		return nil
	}
	repoRoot = resolveSymlinksBestEffort(repoRoot)

	rel, err := filepath.Rel(repoRoot, absPath)
	if err != nil {
		// Different volume or otherwise unrelated to repoRoot — can't be
		// inside it.
		return nil
	}

	insideRepo := rel == "." || !(rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)))
	if insideRepo {
		return fmt.Errorf(
			"refusing to write %q: it resolves to %s, inside the git repository at %s — "+
				"real personal data must never be written to a path git could track, regardless of "+
				"gitignore state (git add -A/-f can still stage an ignored file). Pick a location "+
				"outside the repo entirely, e.g. a sibling directory or a path under your home directory",
			path, absPath, repoRoot,
		)
	}
	return nil
}

// resolveSymlinksBestEffort resolves p's symlinks and returns the result,
// or p unchanged if that fails. p itself commonly doesn't exist yet (the
// normal case here: --out names a file this tool is about to CREATE), so
// this resolves the longest existing ancestor directory instead and
// rejoins the rest — filepath.EvalSymlinks requires every path component
// to exist, which a not-yet-created output file's own basename never
// does. Best-effort, not fail-loud: if nothing along the path resolves
// (e.g. the parent directory doesn't exist either), the plain absolute
// path is still a reasonable fallback for the inside/outside comparison
// in the common case (no symlinked ancestor at all).
func resolveSymlinksBestEffort(p string) string {
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return resolved
	}

	dir, base := filepath.Dir(p), filepath.Base(p)
	if dir == p {
		return p
	}
	return filepath.Join(resolveSymlinksBestEffort(dir), base)
}

// findRepoRoot walks up from the current working directory looking for a
// .git entry (directory for a normal checkout, file for a worktree/
// submodule — os.Stat succeeds either way, which is all this needs to
// know). Returns found=false, not an error, if none exists anywhere up
// to the filesystem root.
func findRepoRoot() (root string, found bool, err error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false, err
	}

	for {
		if _, statErr := os.Stat(filepath.Join(dir, ".git")); statErr == nil {
			return dir, true, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false, nil
		}
		dir = parent
	}
}
