package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeQuerier answers the two queries checkMigrations issues, without a
// database (the shared dev schema must not be altered to test these).
type fakeQuerier struct {
	tableExists bool
	hasRow      bool
	version     int64
	dirty       bool
}

type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *bool:
			*p = r.vals[i].(bool)
		case *int64:
			*p = r.vals[i].(int64)
		default:
			return errors.New("fakeRow: unsupported dest type")
		}
	}
	return nil
}

func (f fakeQuerier) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "to_regclass"):
		return fakeRow{vals: []any{f.tableExists}}
	case strings.Contains(sql, "FROM schema_migrations"):
		if !f.hasRow {
			return fakeRow{err: pgx.ErrNoRows}
		}
		return fakeRow{vals: []any{f.version, f.dirty}}
	}
	return fakeRow{err: errors.New("unexpected query: " + sql)}
}

func TestCheckMigrations(t *testing.T) {
	const want = 8
	cases := []struct {
		name    string
		q       fakeQuerier
		wantErr string
	}{
		{"up to date", fakeQuerier{tableExists: true, hasRow: true, version: 8}, ""},
		{"behind", fakeQuerier{tableExists: true, hasRow: true, version: 7}, "behind: database is at version 7, this binary expects 8"},
		{"ahead", fakeQuerier{tableExists: true, hasRow: true, version: 9}, "ahead"},
		{"dirty", fakeQuerier{tableExists: true, hasRow: true, version: 8, dirty: true}, "dirty at version 8"},
		{"missing table", fakeQuerier{tableExists: false}, "schema_migrations table is missing"},
		{"empty table", fakeQuerier{tableExists: true, hasRow: false}, "no version row"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMigrations(context.Background(), tc.q, want)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected success, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), "docker compose up migrate") {
				t.Errorf("error is not actionable (no `docker compose up migrate`): %v", err)
			}
			var me *MigrationError
			if !errors.As(err, &me) {
				t.Errorf("expected *MigrationError, got %T", err)
			}
		})
	}
}
