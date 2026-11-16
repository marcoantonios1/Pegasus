package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestRunRun_FailsFastWithoutSelfIdentity: the dry-run tool refuses to
// import without the service's self identity, before touching a database.
func TestRunRun_FailsFastWithoutSelfIdentity(t *testing.T) {
	for _, k := range []string{"PEGASUS_SELF_NAME", "PEGASUS_SELF_ALIASES", "PEGASUS_SELF_WHATSAPP_IDS", "PEGASUS_SELF_INSTAGRAM_NAMES", "PEGASUS_CONFIG_FILE"} {
		t.Setenv(k, "")
	}
	err := runRun([]string{
		"--export", "unused.txt",
		"--out", filepath.Join(t.TempDir(), "out.json"),
		"--pegasus-db", "postgres://nobody@127.0.0.1:1/never?sslmode=disable",
	})
	if err == nil || !strings.Contains(err.Error(), "self identity") || !strings.Contains(err.Error(), "self name is required") {
		t.Fatalf("got %v", err)
	}
}
