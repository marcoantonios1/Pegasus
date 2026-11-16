package wasource

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// fakePair fills in what a real pairing would have stored.
func fakePair(dev *store.Device, user string) types.JID {
	id := types.NewADJID(user, 0, 3)
	dev.ID = &id
	dev.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte("d"), AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	return id
}

// TestOpenStore_DeviceSurvivesReopen proves the pure-Go modernc driver works
// with whatsmeow's "sqlite3" dialect: create a device, close, reopen, and
// find the same identity.
func TestOpenStore_DeviceSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wa", "session.db")

	c, err := OpenStore(ctx, path, nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if _, err := pairedDevice(ctx, c); !errors.Is(err, ErrNotPaired) {
		t.Fatalf("fresh store: got %v, want ErrNotPaired", err)
	}

	dev := c.NewDevice()
	id := fakePair(dev, "15551234567")
	dev.LID = types.NewJID("987654321", types.HiddenUserServer)
	dev.PushName = "test"
	if err := dev.Save(ctx); err != nil {
		t.Fatalf("save device: %v", err)
	}
	wantKey := dev.NoiseKey.Pub
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := OpenStore(ctx, path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	got, err := pairedDevice(ctx, c2)
	if err != nil {
		t.Fatalf("reopened store: %v", err)
	}
	if got.ID.String() != id.String() || *got.NoiseKey.Pub != *wantKey {
		t.Errorf("reopened device differs: id=%s", got.ID)
	}

	// Foreign keys must be on (whatsmeow relies on cascading deletes).
	db, err := sql.Open("sqlite", sessionDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var fk int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil || fk != 1 {
		t.Errorf("PRAGMA foreign_keys = %d (%v), want 1", fk, err)
	}
	if err := got.Delete(ctx); err != nil {
		t.Fatalf("delete device: %v", err)
	}
}

func TestOpenStore_FileModes(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "whatsmeow-dir")
	path := filepath.Join(dir, "session.db")

	c, err := OpenStore(ctx, path, nil)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	// Force WAL files to exist.
	dev := c.NewDevice()
	fakePair(dev, "15550000000")
	if err := dev.Save(ctx); err != nil {
		t.Fatal(err)
	}

	assertMode(t, dir, 0o700)
	assertMode(t, path, 0o600)
	for _, suffix := range []string{"-wal", "-shm"} {
		p := path + suffix
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s not created (WAL mode not active?): %v", suffix, err)
		}
		assertMode(t, p, 0o600)
	}
	c.Close()

	// A pre-existing world-readable file/dir gets tightened.
	os.Chmod(dir, 0o755)
	os.Chmod(path, 0o644)
	c, err = OpenStore(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	assertMode(t, dir, 0o700)
	assertMode(t, path, 0o600)
}

func assertMode(t *testing.T, p string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != want {
		t.Errorf("%s: mode %o, want %o", filepath.Base(p), got, want)
	}
}

func TestResolveSessionPath(t *testing.T) {
	// Inside the repo (tests run with cwd inside it): refused.
	if _, err := ResolveSessionPath("session.db"); err == nil || !strings.Contains(err.Error(), "inside the git repository") {
		t.Errorf("path inside repo: got %v, want refusal", err)
	}
	home, _ := os.UserHomeDir()
	got, err := ResolveSessionPath("~/.pegasus/whatsmeow/session.db")
	if err != nil || got != filepath.Join(home, ".pegasus/whatsmeow/session.db") {
		t.Errorf("~ expansion: got %q, %v", got, err)
	}
}

func TestOpen_UnpairedFailsWithHint(t *testing.T) {
	_, err := Open(context.Background(), Config{SessionPath: filepath.Join(t.TempDir(), "s.db"), Logf: t.Logf})
	if !errors.Is(err, ErrNotPaired) || !strings.Contains(err.Error(), "docker compose run --rm pegasus pair") {
		t.Fatalf("got %v", err)
	}
}
