// Package wasource is the live WhatsApp intake for the Pegasus service: a
// whatsmeow client wrapped as an internal/service LiveSource. It owns the
// session store, pairing, identity canonicalization, deliberate drops,
// reconnect supervision and fatal-state detection. Normalization of a
// message into an ingestion.RawMessage stays in the parent package's
// EventHandler.
package wasource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	// Pure-Go SQLite: keeps the build CGO_ENABLED=0 (mattn/go-sqlite3
	// would need CGO and a libc runtime image).
	_ "modernc.org/sqlite"

	"github.com/marcoantonios1/Pegasus/internal/reportpath"
)

// ErrNotPaired means the session store holds no logged-in device.
var ErrNotPaired = errors.New("no paired WhatsApp device in the session store")

// PairCommandHint is the action an operator must take when unpaired.
const PairCommandHint = "stop the service, then run `docker compose run --rm pegasus pair` (or `go run ./cmd/pegasus pair` on the host)"

// ResolveSessionPath expands a leading "~/" to the user's home directory,
// makes the path absolute, and refuses a path inside the git repository:
// the session file is a credential (anyone holding it can read and send
// as the account), so it must never be somewhere git could track it.
func ResolveSessionPath(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand ~ in WhatsApp session path: %w", err)
		}
		p = filepath.Join(home, strings.TrimPrefix(p, "~"))
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolve WhatsApp session path: %w", err)
	}
	if err := reportpath.EnsureOutsideRepo(abs); err != nil {
		return "", fmt.Errorf("WhatsApp session path: %w", err)
	}
	return abs, nil
}

// prepareSessionFile creates the session directory (0700) and an empty
// database file (0600) before SQLite ever opens it, so the file is never
// created with the process umask. SQLite creates the -wal/-shm companions
// with the database file's own permissions (session_test.go checks this).
func prepareSessionFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create session directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("restrict session directory to 0700: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("create session file: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Chmod(p, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("restrict %s to 0600: %w", filepath.Base(p), err)
		}
	}
	return nil
}

// OpenStore opens (creating if needed) the whatsmeow session store at path
// (already resolved by ResolveSessionPath). The modernc driver registers as
// "sqlite"; whatsmeow's dialect name is still "sqlite3". Foreign keys are
// enabled through the DSN pragma, as whatsmeow requires.
func OpenStore(ctx context.Context, path string, log waLog.Logger) (*sqlstore.Container, error) {
	if err := prepareSessionFile(path); err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	dsn := "file:" + path + "?" + q.Encode()

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open session store: %w", err)
	}
	container := sqlstore.NewWithDB(db, "sqlite3", log)
	if err := container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("upgrade session store schema: %w", err)
	}
	return container, nil
}

// pairedDevice returns the store's first device, or ErrNotPaired if it has
// never completed pairing (GetFirstDevice returns a fresh, ID-less device
// in that case).
func pairedDevice(ctx context.Context, c *sqlstore.Container) (*store.Device, error) {
	dev, err := c.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("load device from session store: %w", err)
	}
	if dev.ID == nil {
		return dev, ErrNotPaired
	}
	return dev, nil
}
