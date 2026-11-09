package memory

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrEntityExists is returned by Create when canonical_name already
// exists, case-insensitively (a *pgconn.PgError unique violation,
// SQLSTATE 23505, on entities_canonical_name_lower_uidx — migration
// 000008) — translated into a typed sentinel so a direct caller can
// handle "this name is taken" without matching on a raw driver error.
// Wraps the original *pgconn.PgError via %w, so errors.As still reaches
// it (e.g. to inspect ConstraintName) if a caller needs more than this
// sentinel alone.
//
// Most callers should reach for GetOrCreateByCanonicalName instead,
// which never raises this at all (it resolves the conflict internally
// rather than surfacing it) — Create stays a plain, unconditional INSERT
// for a caller that already knows the name is new, or that wants to
// detect a collision explicitly rather than silently resolve it.
var ErrEntityExists = errors.New("memory: entity with this canonical_name already exists")

type EntityStore struct {
	db *pgxpool.Pool
}

func NewEntityStore(db *pgxpool.Pool) *EntityStore {
	return &EntityStore{db: db}
}

func (s *EntityStore) Create(ctx context.Context, e *Entity) error {
	// metadata is NOT NULL DEFAULT '{}'::jsonb; a nil Go map encodes as SQL
	// NULL rather than the JSON object the column requires, so normalize it.
	if e.Metadata == nil {
		e.Metadata = map[string]any{}
	}

	err := s.db.QueryRow(ctx, `
		INSERT INTO entities (type, canonical_name, is_self, metadata)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at, updated_at
	`,
		e.Type,
		e.CanonicalName,
		e.IsSelf,
		e.Metadata,
	).Scan(&e.ID, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return fmt.Errorf("%w: %w", ErrEntityExists, err)
		}
		return err
	}

	return nil
}

func (s *EntityStore) GetByID(ctx context.Context, id uuid.UUID) (*Entity, error) {
	var e Entity

	err := s.db.QueryRow(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE id = $1
	`, id).Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &e, nil
}

// GetByIDs returns every entity in ids, in no particular order — used by
// WhyDoWeBelieveThis (internal/api, proposal §12) to resolve message
// sender_ids to canonical_name for the "Mentioned by" breakdown. Returns
// an empty slice, not an error, for an empty or nil ids.
func (s *EntityStore) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*Entity, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := s.db.Query(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE id = ANY($1)
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entities []*Entity
	for rows.Next() {
		var e Entity
		if err := rows.Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		entities = append(entities, &e)
	}

	return entities, rows.Err()
}

// GetByCanonicalName returns the entity whose canonical_name matches name
// (case-insensitive exact match), or (nil, nil) if none exists — used by
// the write pipeline (internal/api) to resolve an extracted triple's raw
// subject/object string (e.g. "Sara") to an existing entity before
// falling back to creating a new one.
//
// Case-insensitive exact match, not fuzzy/alias matching: the same real
// person mentioned as "sara" vs "Sara" across two extractions should
// resolve to one entity, but genuine name-variant resolution ("Sara" vs
// "Sarah" vs a nickname) is a harder problem this method does not attempt
// — a known simplification, not a silent guess dressed up as more than it
// is.
func (s *EntityStore) GetByCanonicalName(ctx context.Context, name string) (*Entity, error) {
	var e Entity

	err := s.db.QueryRow(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE lower(canonical_name) = lower($1)
		LIMIT 1
	`, name).Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &e, nil
}

// maxGetOrCreateRetries bounds GetOrCreateByCanonicalName/GetOrCreateSelf's
// retry loop for the one genuine edge case where an insert-then-select
// pair can both come back empty: the conflicting row is DELETED between
// this call's own INSERT (which sees a conflict and inserts nothing) and
// its follow-up SELECT (which now also finds nothing, since the row
// that "won" the conflict no longer exists). Nothing in this codebase
// deletes entities in a way that races like this today, but retrying a
// few times costs nothing and turns a vanishingly rare timing window
// into "try again" rather than a caller-visible failure for no reason.
const maxGetOrCreateRetries = 3

// GetOrCreateByCanonicalName atomically resolves name to an entity: if
// one already exists (case-insensitively), returns it with
// created=false; otherwise creates one with entityType and returns it
// with created=true. The race-free replacement for the old
// GetByCanonicalName-then-Create pattern (api.resolveEntity's previous
// shape) — see migration 000008's own comment for the concurrent-import
// bug this closes: two callers resolving the same name at once could
// both miss the lookup and both insert.
//
// INSERT ... ON CONFLICT (lower(canonical_name)) DO NOTHING RETURNING is
// the core move: if this call's own INSERT wins, RETURNING hands back
// the newly-created row directly — no separate read needed, no window
// between "insert succeeded" and "read it back" for anything else to
// land in. If it LOSES the race (another call already holds this name,
// or is concurrently inserting it right now — the unique index's own
// conflict check blocks until that other transaction commits or rolls
// back before deciding there's a conflict), ON CONFLICT DO NOTHING
// inserts nothing and RETURNING produces no row — not an error — so this
// falls back to a plain SELECT by lower(name) to fetch whichever row
// actually won.
//
// Whoever's INSERT lands first determines the entity's type for good:
// if entityType differs from what the winning row already has, the
// LOSING caller's entityType is silently NOT applied — the existing
// row is returned exactly as it stands. This is correct for this
// schema's name-only resolution (migration 000008's own comment has the
// full reasoning: extraction's objectEntityTypeForPredicate gives a NEW
// object-side entity a default type that often differs from the same
// name's existing subject-side entity, and a type-keyed unique
// constraint would wrongly split those into two entities that
// correctly unify into one today). Type is a creation-time default,
// not part of this schema's identity — there is nothing to reconcile
// when two callers' default-type guesses for the same name disagree.
func (s *EntityStore) GetOrCreateByCanonicalName(ctx context.Context, name, entityType string) (*Entity, bool, error) {
	for attempt := 0; attempt < maxGetOrCreateRetries; attempt++ {
		var e Entity
		err := s.db.QueryRow(ctx, `
			INSERT INTO entities (type, canonical_name, is_self, metadata)
			VALUES ($1, $2, false, '{}'::jsonb)
			ON CONFLICT (lower(canonical_name)) DO NOTHING
			RETURNING id, type, canonical_name, is_self, metadata, created_at, updated_at
		`, entityType, name).Scan(
			&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt,
		)
		if err == nil {
			return &e, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, fmt.Errorf("get or create entity %q: %w", name, err)
		}

		// ON CONFLICT DO NOTHING inserted nothing — someone else already
		// holds (or just won) this name. Fetch it fresh.
		existing, getErr := s.GetByCanonicalName(ctx, name)
		if getErr != nil {
			return nil, false, fmt.Errorf("get or create entity %q: load existing row: %w", name, getErr)
		}
		if existing != nil {
			return existing, false, nil
		}
		// existing == nil: the conflicting row was deleted between our
		// own INSERT and this SELECT — retry.
	}

	return nil, false, fmt.Errorf("get or create entity %q: gave up after %d attempts, the conflicting row kept disappearing between insert and lookup", name, maxGetOrCreateRetries)
}

// GetOrCreateSelf atomically resolves THE is_self entity: if one already
// exists, returns it; otherwise creates one named canonicalName with
// IsSelf=true. The race-free replacement for resolveSender's own
// previous GetSelf-then-Create shape.
//
// Unlike GetOrCreateByCanonicalName, this can lose its race on EITHER of
// two different unique indexes — entities_single_self_idx (another
// concurrent call already created its OWN is_self row, possibly under a
// different name) or entities_canonical_name_lower_uidx (a NON-self
// entity already holds canonicalName — e.g. extraction already resolved
// a plain 'person' entity keyed by Marco's own external ID, from a
// message not yet recognized as self, before this ever ran). The INSERT
// below uses an untargeted ON CONFLICT DO NOTHING specifically so BOTH
// conflicts fall through to the same recovery path, rather than needing
// two different targeted clauses: either way, the right next step is
// the same — re-fetch via GetSelf and see who actually won.
//
//   - If GetSelf now finds a row, that's the winner — return it,
//     regardless of whether its CanonicalName matches canonicalName:
//     the same "name is a creation-time default, not an identity"
//     reasoning GetOrCreateByCanonicalName's own doc comment states.
//     Once ANY is_self row exists, that IS Marco's entity, whatever
//     name it happened to be created under.
//   - If GetSelf STILL finds nothing after bounded retries, the INSERT
//     must have been conflicting on the NAME specifically (a non-self
//     entity already owns canonicalName) — retrying the exact same
//     INSERT would conflict the same way forever, so this stops and
//     returns a clear error naming the conflict, rather than looping on
//     something that will never resolve itself.
func (s *EntityStore) GetOrCreateSelf(ctx context.Context, canonicalName string) (*Entity, error) {
	for attempt := 0; attempt < maxGetOrCreateRetries; attempt++ {
		var e Entity
		err := s.db.QueryRow(ctx, `
			INSERT INTO entities (type, canonical_name, is_self, metadata)
			VALUES ('person', $1, true, '{}'::jsonb)
			ON CONFLICT DO NOTHING
			RETURNING id, type, canonical_name, is_self, metadata, created_at, updated_at
		`, canonicalName).Scan(
			&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt,
		)
		if err == nil {
			return &e, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("get or create self entity %q: %w", canonicalName, err)
		}

		self, getErr := s.GetSelf(ctx)
		if getErr != nil {
			return nil, fmt.Errorf("get or create self entity %q: load existing self: %w", canonicalName, getErr)
		}
		if self != nil {
			return self, nil
		}
		// self == nil after our own INSERT conflicted: the conflict was
		// on the NAME, not on is_self (a non-self entity already owns
		// canonicalName). One more bounded retry in case this was
		// instead a deleted-row timing window (GetOrCreateByCanonicalName's
		// own doc comment); if it keeps happening, the loop below
		// returns a clear error rather than spinning forever.
	}

	return nil, fmt.Errorf("get or create self entity %q: no is_self entity exists after %d attempts, but creating one under this name kept failing — most likely a non-self entity already owns this canonical_name", canonicalName, maxGetOrCreateRetries)
}

// GetSelf returns the single is_self=true entity (entities_single_self_idx
// guarantees at most one exists), or (nil, nil) if none has been created
// yet — used by the write pipeline to resolve Marco's own messages to his
// entity row without needing his platform sender ID matched by name.
func (s *EntityStore) GetSelf(ctx context.Context) (*Entity, error) {
	var e Entity

	err := s.db.QueryRow(ctx, `
		SELECT id, type, canonical_name, is_self, metadata, created_at, updated_at
		FROM entities
		WHERE is_self = true
		LIMIT 1
	`).Scan(&e.ID, &e.Type, &e.CanonicalName, &e.IsSelf, &e.Metadata, &e.CreatedAt, &e.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &e, nil
}

func (s *EntityStore) Update(ctx context.Context, e *Entity) error {
	if e.Metadata == nil {
		e.Metadata = map[string]any{}
	}

	return s.db.QueryRow(ctx, `
		UPDATE entities
		SET type = $1, canonical_name = $2, is_self = $3, metadata = $4, updated_at = now()
		WHERE id = $5
		RETURNING updated_at
	`,
		e.Type,
		e.CanonicalName,
		e.IsSelf,
		e.Metadata,
		e.ID,
	).Scan(&e.UpdatedAt)
}

func (s *EntityStore) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Exec(ctx, `DELETE FROM entities WHERE id = $1`, id)
	return err
}
