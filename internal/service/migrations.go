package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// migrateHint is appended to every migration-check failure: the service
// never applies migrations itself; compose's migrate service is the only
// thing that changes the schema.
const migrateHint = "run `docker compose up migrate` (the only supported way to apply migrations), then start pegasus again"

// rowQuerier is the one pgx method the migration check needs — satisfied
// by *pgxpool.Pool and by the fake in migrations_test.go.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// MigrationError is a failed migration check. Its message is safe to show
// in /readyz (no data, just versions).
type MigrationError struct{ Msg string }

func (e *MigrationError) Error() string { return e.Msg }

// checkMigrations compares golang-migrate's schema_migrations row against
// want (the highest embedded *.up.sql version). It only reads; a schema
// that is missing, dirty, behind, or ahead fails with an actionable
// message.
func checkMigrations(ctx context.Context, q rowQuerier, want uint64) error {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("migration check: look up schema_migrations: %w", err)
	}
	if !exists {
		return &MigrationError{fmt.Sprintf("migration check: schema_migrations table is missing (database has never been migrated; this binary expects version %d) — %s", want, migrateHint)}
	}

	var (
		version int64
		dirty   bool
	)
	err := q.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations LIMIT 1`).Scan(&version, &dirty)
	if errors.Is(err, pgx.ErrNoRows) {
		return &MigrationError{fmt.Sprintf("migration check: schema_migrations has no version row (this binary expects version %d) — %s", want, migrateHint)}
	}
	if err != nil {
		return fmt.Errorf("migration check: read schema_migrations: %w", err)
	}

	switch {
	case dirty:
		return &MigrationError{fmt.Sprintf("migration check: schema is dirty at version %d (a previous migration failed part-way; fix it by hand with golang-migrate `force`) — then %s", version, migrateHint)}
	case version < 0 || uint64(version) < want:
		return &MigrationError{fmt.Sprintf("migration check: schema is behind: database is at version %d, this binary expects %d — %s", version, want, migrateHint)}
	case uint64(version) > want:
		return &MigrationError{fmt.Sprintf("migration check: schema is ahead: database is at version %d, this binary only knows up to %d (running an older build against a newer schema) — deploy the matching build, or %s", version, want, migrateHint)}
	}
	return nil
}
