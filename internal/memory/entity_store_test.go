package memory

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// --- Entity-resolution race fix: EntityStore.GetOrCreateByCanonicalName/
// GetOrCreateSelf, and EntityStore.Create's ErrEntityExists ---
//
// These exercise the actual database write directly against real
// Postgres, with migration 000008's unique index
// (entities_canonical_name_lower_uidx) already applied — without it,
// these tests wouldn't be able to tell "the race was resolved
// correctly" apart from "the race just didn't happen to fire this
// time," since the pre-fix code's bug was silent (two rows, no error)
// rather than loud.

// TestEntityStore_GetOrCreateByCanonicalName_ConcurrentCallsResolveToOneRow
// covers requirement 7 — the actual regression reproducer. For each of
// many rounds, a FRESH, never-used-before name, resolved by 24
// goroutines released together by a barrier, using case variants
// ("Name", "name", "NAME") round-robin across the goroutines. Repeated
// over enough rounds that a race is near-certain to be exercised at
// least once if the implementation has one.
//
// Confirmed to FAIL against the old GetByCanonicalName-then-Create
// shape (api.resolveEntity's previous implementation, before this fix):
// with migration 000008's unique index already in place, running that
// old two-step pattern under this exact concurrent load reliably
// produced caller-visible unique-violation errors from whichever
// goroutine's Create() lost the race (see this fix's own PR
// description — reproduced by temporarily reverting resolveEntity to
// that shape and running this test against it).
func TestEntityStore_GetOrCreateByCanonicalName_ConcurrentCallsResolveToOneRow(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	store := NewEntityStore(pool)

	const goroutines = 24
	const rounds = 50

	for round := 0; round < rounds; round++ {
		baseName := "ConcurrentEntity-" + uuid.New().String()
		variants := []string{baseName, strings.ToLower(baseName), strings.ToUpper(baseName)}

		var wg sync.WaitGroup
		start := make(chan struct{})
		ids := make([]uuid.UUID, goroutines)
		created := make([]bool, goroutines)
		errs := make([]error, goroutines)

		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			i := i
			name := variants[i%len(variants)]
			go func() {
				defer wg.Done()
				<-start
				e, c, err := store.GetOrCreateByCanonicalName(ctx, name, "person")
				errs[i] = err
				if err == nil {
					ids[i] = e.ID
					created[i] = c
				}
			}()
		}
		close(start)
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d (name=%q): goroutine %d: unexpected error: %v", round, baseName, i, err)
			}
		}

		firstID := ids[0]
		for i, id := range ids {
			if id != firstID {
				t.Fatalf("round %d (name=%q): goroutine %d resolved to ID %s, want %s (every caller for the same name must resolve to the same entity)", round, baseName, i, id, firstID)
			}
		}

		createdCount := 0
		for _, c := range created {
			if c {
				createdCount++
			}
		}
		if createdCount != 1 {
			t.Fatalf("round %d (name=%q): expected exactly 1 goroutine to report created=true, got %d", round, baseName, createdCount)
		}

		var rowCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE lower(canonical_name) = lower($1)`, baseName).Scan(&rowCount); err != nil {
			t.Fatalf("round %d: count rows: %v", round, err)
		}
		if rowCount != 1 {
			t.Fatalf("round %d (name=%q): expected exactly 1 entities row, got %d", round, baseName, rowCount)
		}
	}
}

// TestEntityStore_GetOrCreateByCanonicalName_DifferentTypesStillOneRow
// covers requirement 8: two concurrent calls for the SAME name but
// DIFFERENT entityType must still resolve to one row and one ID — the
// unique index is on lower(canonical_name) alone, never (name, type),
// so there is nothing for the two different types to even conflict on
// independently. Only ID equality is asserted, deliberately: WHICH
// caller's type actually won is not a contract this method makes (see
// its own doc comment) — it's whoever's INSERT the database happened to
// commit first.
func TestEntityStore_GetOrCreateByCanonicalName_DifferentTypesStillOneRow(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	store := NewEntityStore(pool)

	name := "DifferentTypes-" + uuid.New().String()

	var wg sync.WaitGroup
	start := make(chan struct{})
	var id1, id2 uuid.UUID
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		e, _, err := store.GetOrCreateByCanonicalName(ctx, name, "person")
		err1 = err
		if err == nil {
			id1 = e.ID
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		e, _, err := store.GetOrCreateByCanonicalName(ctx, name, "object")
		err2 = err
		if err == nil {
			id2 = e.ID
		}
	}()
	close(start)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("goroutine 1 (type=person): %v", err1)
	}
	if err2 != nil {
		t.Fatalf("goroutine 2 (type=object): %v", err2)
	}
	if id1 != id2 {
		t.Fatalf("expected both calls to resolve to the same entity ID, got %s (person) and %s (object)", id1, id2)
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE lower(canonical_name) = lower($1)`, name).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 entities row, got %d", rowCount)
	}
}

// TestEntityStore_Create_ExistingNameReturnsErrEntityExists covers
// requirement 10: a direct Create() call (not GetOrCreateByCanonicalName)
// against an already-existing (case-insensitive) canonical_name must
// come back as the typed ErrEntityExists, not a raw driver error a
// caller would have to pattern-match on.
func TestEntityStore_Create_ExistingNameReturnsErrEntityExists(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	store := NewEntityStore(pool)

	name := "ExistingName-" + uuid.New().String()
	first := &Entity{Type: "person", CanonicalName: name}
	if err := store.Create(ctx, first); err != nil {
		t.Fatalf("create first: %v", err)
	}

	// Case-insensitive collision, not an exact string repeat — proves
	// the translation keys off the same lower(canonical_name) comparison
	// the unique index itself uses, not a literal string match.
	second := &Entity{Type: "person", CanonicalName: strings.ToUpper(name)}
	err := store.Create(ctx, second)
	if !errors.Is(err, ErrEntityExists) {
		t.Fatalf("expected ErrEntityExists, got %v", err)
	}
}
