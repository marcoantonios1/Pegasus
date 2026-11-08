package memory

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRow is a pgx.Row whose Scan always returns a fixed, chosen error
// — the seam requirement 7 needs: lockEdgeForUpdate takes the queryer
// interface (edge_store.go), so a database is never required to prove
// how it classifies a given error.
type fakeRow struct {
	err error
}

func (r fakeRow) Scan(dest ...any) error { return r.err }

// fakeQueryer satisfies the queryer interface with a QueryRow that
// always returns fakeRow{err}. Exec is never called by lockEdgeForUpdate
// and is left unimplemented (panics if it ever is, which would itself
// be a signal something about the test's assumptions is wrong).
type fakeQueryer struct {
	err error
}

func (q fakeQueryer) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fakeRow{err: q.err}
}

func (q fakeQueryer) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	panic("fakeQueryer.Exec: lockEdgeForUpdate must never call Exec")
}

// TestLockEdgeForUpdate_RealErrorsAreNotMisclassifiedAsNotFound covers
// requirement 7: a generic connection error, context.Canceled,
// context.DeadlineExceeded, and a *pgconn.PgError with SQLSTATE 40P01
// (deadlock_detected) must all come back as their OWN distinct error —
// never matching ErrEdgeNotFound — while still being reachable via
// errors.Is/errors.As through lockEdgeForUpdate's wrapping, and with the
// edge ID present in the message for diagnosability.
func TestLockEdgeForUpdate_RealErrorsAreNotMisclassifiedAsNotFound(t *testing.T) {
	id := uuid.New()

	connErr := errors.New("connection reset by peer")
	deadlockErr := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

	cases := []struct {
		name string
		err  error
	}{
		{"generic connection error", connErr},
		{"context.Canceled", context.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"deadlock (SQLSTATE 40P01)", deadlockErr},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := lockEdgeForUpdate(context.Background(), fakeQueryer{err: c.err}, id)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if errors.Is(err, ErrEdgeNotFound) {
				t.Errorf("expected errors.Is(err, ErrEdgeNotFound) to be FALSE for a real error, got TRUE (err=%v)", err)
			}
			if !errors.Is(err, c.err) {
				t.Errorf("expected the original error still reachable via errors.Is, got %v", err)
			}
			if !strings.Contains(err.Error(), id.String()) {
				t.Errorf("expected the edge ID %s in the error message, got: %v", id, err)
			}
		})
	}

	t.Run("deadlock is reachable via errors.As(*pgconn.PgError)", func(t *testing.T) {
		_, err := lockEdgeForUpdate(context.Background(), fakeQueryer{err: deadlockErr}, id)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("expected errors.As(err, *pgconn.PgError) to succeed, got: %v", err)
		}
		if pgErr.Code != "40P01" {
			t.Errorf("expected SQLSTATE 40P01, got %q", pgErr.Code)
		}
	})
}
