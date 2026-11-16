package main

import (
	"os"
	"strings"
	"testing"
)

// TestRun_FailsFastWithoutSelfIdentity: the tool refuses to run without the
// service's self identity, before connecting to a database.
func TestRun_FailsFastWithoutSelfIdentity(t *testing.T) {
	for _, k := range []string{"PEGASUS_SELF_NAME", "PEGASUS_SELF_ALIASES", "PEGASUS_SELF_WHATSAPP_IDS", "PEGASUS_SELF_INSTAGRAM_NAMES", "PEGASUS_CONFIG_FILE"} {
		t.Setenv(k, "")
	}
	orig := os.Args
	t.Cleanup(func() { os.Args = orig })
	os.Args = []string{"backfill_embeddings", "--pegasus-db", "postgres://nobody@127.0.0.1:1/never?sslmode=disable"}

	err := run()
	if err == nil || !strings.Contains(err.Error(), "self identity") || !strings.Contains(err.Error(), "self name is required") {
		t.Fatalf("got %v", err)
	}
}
