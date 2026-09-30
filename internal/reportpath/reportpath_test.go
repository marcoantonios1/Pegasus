package reportpath

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withCwd runs fn with the process's working directory temporarily
// changed to dir, restoring it afterward — EnsureOutsideRepo/findRepoRoot
// both discover the repo root from os.Getwd(), the same way `git` itself
// does, so tests need to actually change directory rather than just
// passing a different path argument.
func withCwd(t *testing.T, dir string, fn func()) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir(%s): %v", dir, err)
	}
	defer func() {
		if err := os.Chdir(orig); err != nil {
			t.Fatalf("restore Chdir(%s): %v", orig, err)
		}
	}()
	fn()
}

// fakeRepo creates a temp directory with a .git marker (a plain file
// works fine — findRepoRoot only os.Stats it, matching how a worktree's
// .git is itself a file, not a directory) and a nested subdirectory, for
// tests to cd into.
func fakeRepo(t *testing.T) (repoRoot, nestedDir string) {
	t.Helper()
	repoRoot = t.TempDir()
	if err := os.WriteFile(filepath.Join(repoRoot, ".git"), []byte("gitdir: fake"), 0o600); err != nil {
		t.Fatalf("create fake .git: %v", err)
	}
	nestedDir = filepath.Join(repoRoot, "cmd", "sometool")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("mkdir nested dir: %v", err)
	}
	return repoRoot, nestedDir
}

func TestEnsureOutsideRepo_RejectsPathInsideRepo(t *testing.T) {
	repoRoot, nestedDir := fakeRepo(t)

	withCwd(t, nestedDir, func() {
		tests := []string{
			"review.md",                          // relative, same dir as cwd
			"../../review.md",                    // relative, repo root
			"../other/nested.json",               // relative, different subdir
			filepath.Join(repoRoot, "review.md"), // absolute, inside repo
		}
		for _, p := range tests {
			t.Run(p, func(t *testing.T) {
				if err := EnsureOutsideRepo(p); err == nil {
					t.Errorf("expected EnsureOutsideRepo(%q) to reject a path inside the repo, got nil", p)
				} else if !strings.Contains(err.Error(), "inside the git repository") {
					t.Errorf("expected an 'inside the git repository' error, got: %v", err)
				}
			})
		}
	})
}

func TestEnsureOutsideRepo_RejectsRepoRootItself(t *testing.T) {
	repoRoot, nestedDir := fakeRepo(t)

	withCwd(t, nestedDir, func() {
		if err := EnsureOutsideRepo(repoRoot); err == nil {
			t.Error("expected EnsureOutsideRepo(repoRoot) to reject the repo root itself, got nil")
		}
	})
}

func TestEnsureOutsideRepo_AllowsPathOutsideRepo(t *testing.T) {
	_, nestedDir := fakeRepo(t)
	outsideDir := t.TempDir() // a completely separate temp dir, not under repoRoot

	withCwd(t, nestedDir, func() {
		if err := EnsureOutsideRepo(filepath.Join(outsideDir, "run.json")); err != nil {
			t.Errorf("expected an outside-repo absolute path to be allowed, got: %v", err)
		}
	})
}

// TestEnsureOutsideRepo_AllowsSiblingDirectoryViaRelativeTraversal covers
// the realistic case these tools' own doc comments recommend (e.g.
// ../pegasus-reports/): a relative path using ".." enough times to climb
// out of the repo entirely must be allowed, not just an absolute path.
func TestEnsureOutsideRepo_AllowsSiblingDirectoryViaRelativeTraversal(t *testing.T) {
	repoRoot, nestedDir := fakeRepo(t)
	siblingDir := filepath.Join(filepath.Dir(repoRoot), "sibling-reports")
	if err := os.MkdirAll(siblingDir, 0o755); err != nil {
		t.Fatalf("mkdir sibling dir: %v", err)
	}

	withCwd(t, nestedDir, func() {
		// nestedDir is repoRoot/cmd/sometool, so climbing out to
		// repoRoot's own parent needs "../../../".
		if err := EnsureOutsideRepo("../../../sibling-reports/run.json"); err != nil {
			t.Errorf("expected a relative path climbing out to a sibling directory to be allowed, got: %v", err)
		}
	})
}

func TestEnsureOutsideRepo_NoGitFound_AllowsAnything(t *testing.T) {
	// A temp dir with no .git anywhere above it (t.TempDir() is under the
	// OS temp dir, which this repo's own .git is not an ancestor of).
	noRepoDir := t.TempDir()

	withCwd(t, noRepoDir, func() {
		if err := EnsureOutsideRepo("anything.json"); err != nil {
			t.Errorf("expected no error when no .git is found walking up from cwd, got: %v", err)
		}
	})
}
