// Package testdb gives DB-backed tests a fresh, fully migrated database of
// their own, for tests that need control over global state the shared dev
// database can't offer (the single is_self entity, above all).
//
// It follows the repo's DB-test convention for finding a server
// (PEGASUS_TEST_DATABASE_URL, else a localhost DSN from POSTGRES_PASSWORD):
// it skips when none is configured or reachable, and fails on an
// authentication error so a rotated password can't silently skip a suite.
// It never alters the database the DSN names: it CREATEs a separate
// pegasus_t_<random> database (the compose user is a superuser), applies
// the embedded db/migrations/*.up.sql in order, and DROPs it on cleanup.
package testdb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"

	"github.com/marcoantonios1/Pegasus/db"
)

// ServerDSN returns the configured test server DSN, skipping the test if
// none is configured or reachable (failing on auth errors).
func ServerDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("PEGASUS_TEST_DATABASE_URL")
	if dsn == "" {
		if pw := os.Getenv("POSTGRES_PASSWORD"); pw != "" {
			dsn = "postgres://pegasus:" + pw + "@localhost:5432/pegasus?sslmode=disable"
		}
	}
	if dsn == "" {
		t.Skip("no test database configured — set PEGASUS_TEST_DATABASE_URL, or POSTGRES_PASSWORD (`set -a; source .env; set +a`), to enable this test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		if isAuthFailure(err) {
			t.Fatalf("authentication failed connecting to the test database (%v) — the Postgres password was likely rotated; set PEGASUS_TEST_DATABASE_URL to the current DSN, or re-export POSTGRES_PASSWORD before running go test", err)
		}
		t.Skipf("no test database available (%v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}
	conn.Close(ctx)
	return dsn
}

// Fresh creates a new, migrated database and returns its DSN. It is
// dropped when the test ends.
func Fresh(t testing.TB) string {
	t.Helper()
	server := ServerDSN(t)
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, server)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)

	name := "pegasus_t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create scratch database (the test user needs CREATEDB): %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), server)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(server)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to scratch database: %v", err)
	}
	defer conn.Close(ctx)
	if err := migrate(ctx, conn); err != nil {
		t.Fatalf("migrate scratch database: %v", err)
	}
	return dsn
}

// FreshPool is Fresh plus a pgx pool with the pgvector codec registered,
// closed when the test ends.
func FreshPool(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := Fresh(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { return pgxvec.RegisterTypes(ctx, c) }
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

// migrate applies every embedded *.up.sql in version order and records the
// final version the way golang-migrate does, so the service's migration
// check accepts the database.
func migrate(ctx context.Context, conn *pgx.Conn) error {
	entries, err := fs.ReadDir(db.Migrations, "migrations")
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, n := range names {
		sql, err := fs.ReadFile(db.Migrations, "migrations/"+n)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("%s: %w", n, err)
		}
	}
	latest, err := db.LatestMigrationVersion()
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `CREATE TABLE schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL);
		INSERT INTO schema_migrations VALUES (`+fmt.Sprint(latest)+`, false)`)
	return err
}

func isAuthFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "28P01" {
		return true
	}
	return strings.Contains(err.Error(), "password authentication failed")
}
