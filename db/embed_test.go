package db

import (
	"testing"
	"testing/fstest"
)

func TestLatestMigrationVersion_Embedded(t *testing.T) {
	v, err := LatestMigrationVersion()
	if err != nil {
		t.Fatalf("LatestMigrationVersion: %v", err)
	}
	if v == 0 {
		t.Fatalf("expected a non-zero latest version")
	}
}

func TestLatestVersion_PicksHighestAndRejectsMalformed(t *testing.T) {
	fsys := fstest.MapFS{
		"m/000002_b.up.sql":   {},
		"m/000010_c.up.sql":   {},
		"m/000001_a.up.sql":   {},
		"m/000010_c.down.sql": {},
	}
	v, err := latestVersion(fsys, "m")
	if err != nil || v != 10 {
		t.Fatalf("got %d, %v; want 10, nil", v, err)
	}

	bad := fstest.MapFS{"m/abc_x.up.sql": {}}
	if _, err := latestVersion(bad, "m"); err == nil {
		t.Fatalf("expected an error for a non-numeric version prefix")
	}
}
