// Package db embeds the schema migrations so a running binary knows which
// schema version it was built against. It never APPLIES them — compose's
// `migrate` service (golang-migrate) stays the only thing that changes the
// schema; internal/service only compares this embedded version against the
// database's schema_migrations row at startup (see
// internal/service/migrations.go).
package db

import (
	"embed"
	"fmt"
	"io/fs"
	"path"
	"strconv"
	"strings"
)

// Migrations holds every db/migrations/*.up.sql file.
//
//go:embed migrations/*.up.sql
var Migrations embed.FS

// LatestMigrationVersion returns the highest version number among the
// embedded *.up.sql files, parsed from golang-migrate's
// "<version>_<name>.up.sql" filename convention.
func LatestMigrationVersion() (uint64, error) {
	return latestVersion(Migrations, "migrations")
}

func latestVersion(fsys fs.FS, dir string) (uint64, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return 0, fmt.Errorf("read embedded migrations: %w", err)
	}
	var latest uint64
	found := false
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		prefix, _, ok := strings.Cut(path.Base(name), "_")
		if !ok {
			return 0, fmt.Errorf("migration %q: filename does not follow <version>_<name>.up.sql", name)
		}
		v, err := strconv.ParseUint(prefix, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("migration %q: version prefix %q is not a number: %w", name, prefix, err)
		}
		found = true
		if v > latest {
			latest = v
		}
	}
	if !found {
		return 0, fmt.Errorf("no embedded *.up.sql migrations found")
	}
	return latest, nil
}
