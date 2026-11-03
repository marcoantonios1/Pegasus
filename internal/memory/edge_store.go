package memory

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EdgeStore struct {
	db *pgxpool.Pool
}

func NewEdgeStore(db *pgxpool.Pool) *EdgeStore {
	return &EdgeStore{db: db}
}

// queryer is the subset of *pgxpool.Pool and pgx.Tx this package
// needs. Both satisfy it with identical method signatures, so the same
// SQL-executing helper can run either directly against the pool or inside
// an explicit transaction — see insertEdge, used by both Create (against
// s.db) and CorrectMemory (against a transaction, correct_memory.go), so
// the INSERT itself is defined once, not duplicated between them.
type queryer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// insertEdge writes e. FirstSeen/LastReinforced, if e sets them (non-zero
// time.Time), are used AS GIVEN — the actual fix for edges defaulting to
// "now" regardless of when the underlying fact was actually said: callers
// that know a real observed time (storeExtractedTriples, deriving it from
// the source messages' own timestamps; CorrectMemory, via
// CorrectionInput.ObservedAt) now pass it through here instead of relying
// on the column's own DB-side default. A zero e.FirstSeen/e.LastReinforced
// (every caller before this fix, and any caller with no real observed
// time to supply) binds NULL and falls back to COALESCE(..., now()) —
// unchanged from the previous behavior, so this is purely additive for
// every existing call site.
func insertEdge(ctx context.Context, q queryer, e *Edge) error {
	// source_message_ids is NOT NULL DEFAULT '{}'; a nil Go slice encodes as
	// SQL NULL rather than an empty array, so normalize it.
	if e.SourceMessageIDs == nil {
		e.SourceMessageIDs = []uuid.UUID{}
	}

	var firstSeen, lastReinforced *time.Time
	if !e.FirstSeen.IsZero() {
		firstSeen = &e.FirstSeen
	}
	if !e.LastReinforced.IsZero() {
		lastReinforced = &e.LastReinforced
	}

	return q.QueryRow(ctx, `
		INSERT INTO edges (
			subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			decay_rate, decay_locked, superseded_by, is_correction,
			first_seen, last_reinforced
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			COALESCE($14, now()), COALESCE($15, now())
		)
		RETURNING id, first_seen, last_reinforced
	`,
		e.SubjectID,
		e.Predicate,
		e.ObjectID,
		e.ObjectLiteral,
		e.Confidence,
		e.Importance,
		e.SourceType,
		e.SourceWeight,
		e.SourceMessageIDs,
		e.DecayRate,
		e.DecayLocked,
		e.SupersededBy,
		e.IsCorrection,
		firstSeen,
		lastReinforced,
	).Scan(&e.ID, &e.FirstSeen, &e.LastReinforced)
}

func (s *EdgeStore) Create(ctx context.Context, e *Edge) error {
	return insertEdge(ctx, s.db, e)
}

// GetByID is a direct lookup by primary key — deliberately unfiltered,
// including tombstones (memory.IsTombstone) and superseded edges alike.
// A caller who already has a specific edge ID (WhyDoWeBelieveThis,
// describeEdge's SupersededBy label, UpdateMemory) is asking about THAT
// row, not asking for curated default retrieval — IsPruneCandidate and
// the delete-exclusion fix's searchFiltersWhere both apply only to the
// search/retrieval methods built on SearchRelevant/SearchByText, never
// to this one. See GetBySubjectAndPredicate's own doc comment for the
// same rule applied to a different direct-lookup shape.
func (s *EdgeStore) GetByID(ctx context.Context, id uuid.UUID) (*Edge, error) {
	var e Edge

	err := s.db.QueryRow(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE id = $1
	`, id).Scan(
		&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
		&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
		&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
		&e.SupersededBy, &e.IsCorrection,
	)
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (s *EdgeStore) Update(ctx context.Context, e *Edge) error {
	if e.SourceMessageIDs == nil {
		e.SourceMessageIDs = []uuid.UUID{}
	}

	_, err := s.db.Exec(ctx, `
		UPDATE edges
		SET
			subject_id = $1, predicate = $2, object_id = $3, object_literal = $4,
			confidence = $5, importance = $6, source_type = $7, source_weight = $8,
			source_message_ids = $9, last_reinforced = $10, decay_rate = $11,
			decay_locked = $12, superseded_by = $13, is_correction = $14
		WHERE id = $15
	`,
		e.SubjectID,
		e.Predicate,
		e.ObjectID,
		e.ObjectLiteral,
		e.Confidence,
		e.Importance,
		e.SourceType,
		e.SourceWeight,
		e.SourceMessageIDs,
		e.LastReinforced,
		e.DecayRate,
		e.DecayLocked,
		e.SupersededBy,
		e.IsCorrection,
		e.ID,
	)

	return err
}

// GetBySubjectAndPredicate uses the (subject_id, predicate) index from
// migration 000002. This is the retrieval pattern GetRelevantContext() will
// build on.
//
// Deliberately unfiltered — includes superseded edges AND tombstones
// (memory.IsTombstone) both. This is the base method FindSupersededMatches
// and reflection.countExplicitSignalEdges both call directly; each of
// those callers makes its OWN, context-appropriate decision about
// tombstones (FindSupersededMatches: a tombstone is a legitimate
// superseding edge for the stale-resurrection guard to load, see
// isStaleResurrection; countExplicitSignalEdges: explicitly excludes
// them, since a deleted nickname isn't an "explicit signal" — see that
// function's own doc comment) rather than this shared method making that
// call for every caller. Do not add a tombstone filter here; add it at
// the specific caller that needs it, the same way FindLiveEdgesBySubject
// AndPredicate is a SEPARATE method rather than a parameter on this one.
func (s *EdgeStore) GetBySubjectAndPredicate(ctx context.Context, subjectID uuid.UUID, predicate string) ([]*Edge, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE subject_id = $1 AND predicate = $2
	`, subjectID, predicate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var edges []*Edge
	for rows.Next() {
		var e Edge

		if err := rows.Scan(
			&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
			&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
			&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
			&e.SupersededBy, &e.IsCorrection,
		); err != nil {
			return nil, err
		}

		edges = append(edges, &e)
	}

	return edges, rows.Err()
}

// Reinforce updates last_reinforced and first_seen — nothing else. Kept
// separate from Update() specifically so reinforcing an edge can never
// accidentally rewrite confidence, predicate, or any other field via a
// stray full-object update; this method's whole contract is "touch the
// two observation-timestamp columns, nothing more."
//
// at and earliest are both taken as explicit parameters (matching
// Update's existing pattern of binding timestamps directly rather than
// relying on SQL now()) rather than letting Postgres own the value — this
// keeps the method deterministically testable without wall-clock sleeps.
//
// Order-independence (the edge-timestamp fix's Part 3): a reinforcing
// message is not guaranteed to arrive, or be processed, in real-world
// order — a re-import, a reprocessed window, or a voice note whose
// transcription succeeds on a retry can all reinforce an edge with a
// message OLDER or NEWER than whatever the edge currently has. This
// method therefore never simply overwrites either column:
//
//	last_reinforced = GREATEST(last_reinforced, at)
//	first_seen       = LEAST(first_seen, earliest)
//
// last_reinforced only ever moves FORWARD (or stays put), first_seen only
// ever moves EARLIER (or stays put) — reinforcement can retroactively
// pull first_seen back if an out-of-order message turns out to predate
// what was previously recorded as the earliest sighting, but can never
// push either column the wrong direction. For the common case (a single
// reinforcing message, not a multi-message window), callers pass the
// same timestamp for both at and earliest — see storeExtractedTriples'
// own call sites (internal/api/pipeline.go) for window-level vs.
// single-message usage.
//
// decay_locked edges ARE reinforced by this method: last_reinforced still
// updates, since a correction being independently reconfirmed later is
// still real information worth recording. What decay_locked actually
// protects is confidence, which this method never touches regardless of
// the flag — see EffectiveConfidence, which short-circuits on
// DecayLocked before ever consulting LastReinforced. Reinforcing a locked
// edge's timestamps doesn't weaken that protection.
func (s *EdgeStore) Reinforce(ctx context.Context, edgeID uuid.UUID, at, earliest time.Time) error {
	_, err := s.db.Exec(ctx, `
		UPDATE edges
		SET last_reinforced = GREATEST(last_reinforced, $1), first_seen = LEAST(first_seen, $2)
		WHERE id = $3
	`, at, earliest, edgeID)
	return err
}

// FindLiveEdgesBySubjectAndPredicate is GetBySubjectAndPredicate narrowed
// to LIVE, non-tombstone edges only (superseded_by IS NULL AND
// source_type != SourceTypeUserDeletion) — the correction-target lookup
// internal/api's storeExtractedTriples needs (Issue 32's fix: that
// function used to call FindMatchingEdge with the triple's own NEW
// object, which by definition can't already exist as a live edge, so
// every correction was silently skipped — see storeExtractedTriples' own
// doc comment for the full history).
//
// Deliberately a separate method from GetBySubjectAndPredicate (rather
// than, say, a boolean parameter): a correction can only ever sensibly
// supersede a currently-live fact — an already-superseded edge isn't
// "what's currently believed" regardless of which value it used to hold —
// so this query excludes it at the SQL level (same (subject_id,
// predicate) index GetBySubjectAndPredicate already uses) rather than
// filtering in Go after fetching rows this caller never wants.
//
// Tombstone exclusion (the delete-exclusion fix) is the same reasoning
// applied to a deletion marker specifically: a tombstone is live by
// every OTHER criterion this method checks (superseded_by IS NULL), but
// it isn't "a currently-live fact" in any sense a correction should be
// able to target — there is nothing true there to correct, only a record
// that something used to be true. Without this, a tombstone could be
// picked as selectCorrectionTarget's correction target (internal/api/
// pipeline.go), which makes no sense: superseding a deletion marker with
// a "correction" conflates two different operations this codebase keeps
// deliberately distinct (CorrectMemory vs. SoftDelete).
//
// As of the stale-resurrection fix, FindMatchingEdge is ALSO implemented
// in terms of this method (it used to go through GetBySubjectAndPredicate
// directly and deliberately include superseded edges — see its own doc
// comment for why that turned out to be a bug, not a feature) — so this
// same tombstone exclusion automatically covers FindMatchingEdge's own
// normal-reinforcement-match use too: without it, a fact the user later
// reasserts after deleting it would get "reinforced" into the zero-
// confidence tombstone (bumping its timestamps, never its confidence)
// instead of creating a fresh, genuinely live edge — permanently unable
// to be relearned.
func (s *EdgeStore) FindLiveEdgesBySubjectAndPredicate(ctx context.Context, subjectID uuid.UUID, predicate string) ([]*Edge, error) {
	rows, err := s.db.Query(ctx, `
		SELECT
			id, subject_id, predicate, object_id, object_literal, confidence,
			importance, source_type, source_weight, source_message_ids,
			first_seen, last_reinforced, decay_rate, decay_locked,
			superseded_by, is_correction
		FROM edges
		WHERE subject_id = $1 AND predicate = $2 AND superseded_by IS NULL
			AND source_type != '`+SourceTypeUserDeletion+`'
	`, subjectID, predicate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var edges []*Edge
	for rows.Next() {
		var e Edge

		if err := rows.Scan(
			&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
			&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
			&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
			&e.SupersededBy, &e.IsCorrection,
		); err != nil {
			return nil, err
		}

		edges = append(edges, &e)
	}

	return edges, rows.Err()
}

// FindMatchingEdge looks for a LIVE existing edge (superseded_by IS NULL)
// with the same subject_id, predicate, and object (object_id or
// object_literal — exact match only; near-duplicate fuzzy merging of
// similar-but-not-identical objects is the Reflection Engine's job, §9.2,
// not this) as the given values. Returns (nil, nil) if no match exists.
// Used to decide whether a newly extracted triple should reinforce an
// existing edge (Reinforce) or become a new one (Create) — that decision
// itself is the caller's, not this method's.
//
// Live-only as of the stale-resurrection fix (previously this included
// superseded edges on purpose — see git history/TestFindMatchingEdge_
// IncludesSupersededEdges, now deleted). That was found to be a real bug,
// not just a surprising-but-intentional tradeoff: a corrected-away value
// re-extracted from an OLDER, out-of-order message (a retried voice-note
// transcription, a re-import) would silently reinforce the dead edge —
// bumping its last_reinforced and making a long-superseded, wrong fact
// look recently reconfirmed — instead of being evaluated as what it
// actually is, a new claim about a subject+predicate that currently has a
// different live answer. Reinforcing a dead edge was never a deliberate
// feature; it was a side effect of this method's exact-match loop running
// over every candidate GetBySubjectAndPredicate returned, superseded or
// not.
//
// The timestamp-ordering judgment this change now requires — is a
// corrected-away value being re-extracted a stale, out-of-order replay (no
// live match here, and older than the edge that superseded it: skip,
// don't resurrect) or a genuine fresh reassertion (no live match here, but
// NEWER than what superseded it: a real new claim, let it through to
// Create) — is storeExtractedTriples' job (internal/api/pipeline.go),
// using FindSupersededMatches below. This method only ever answers "is
// there a live edge with this exact object right now" — it does not,
// and must not, reach into superseded edges at all; that's a decision
// requiring the message timestamps this method has no access to.
//
// Also excludes tombstones (FindLiveEdgesBySubjectAndPredicate's own doc
// comment has the full reasoning): a deletion marker is live by every
// other criterion, but matching one here would "reinforce" a
// zero-confidence tombstone instead of creating a fresh edge when the
// user reasserts a fact they'd previously deleted — the same
// stale-resurrection machinery (FindSupersededMatches/
// isStaleResurrection) that decides whether a corrected-away value gets
// relearned handles a deleted one too, once this method correctly stops
// matching the tombstone itself.
func (s *EdgeStore) FindMatchingEdge(ctx context.Context, subjectID uuid.UUID, predicate string, objectID *uuid.UUID, objectLiteral *string) (*Edge, error) {
	candidates, err := s.FindLiveEdgesBySubjectAndPredicate(ctx, subjectID, predicate)
	if err != nil {
		return nil, err
	}

	for _, e := range candidates {
		if edgeObjectMatches(e, objectID, objectLiteral) {
			return e, nil
		}
	}

	return nil, nil
}

// FindSupersededMatches looks for edges with the same subject_id,
// predicate, and object as FindMatchingEdge's exact-match rule, but
// restricted to edges that HAVE been superseded (superseded_by IS NOT
// NULL) — the counterpart lookup storeExtractedTriples needs once
// FindMatchingEdge (now live-only) comes back empty, to tell a stale,
// out-of-order replay of a corrected-away value apart from a genuine new
// claim about the same subject+predicate (see FindMatchingEdge's own doc
// comment for the full reasoning, and storeExtractedTriples for how the
// timestamp comparison that actually makes that call is done — this
// method only finds the candidates, it doesn't decide anything about
// them).
//
// Can return more than one match: supersession chains (a value corrected,
// then corrected again) and the Reflection Engine's dedup pass (§9.2, same
// superseded_by field, different reason — see MergeDuplicates' own doc
// comment on that overload) can both leave more than one dead edge with
// the same object text behind. Returning all of them, unfiltered, is
// deliberate — picking which one(s) matter for the timestamp comparison is
// the caller's job, not this method's.
func (s *EdgeStore) FindSupersededMatches(ctx context.Context, subjectID uuid.UUID, predicate string, objectID *uuid.UUID, objectLiteral *string) ([]*Edge, error) {
	candidates, err := s.GetBySubjectAndPredicate(ctx, subjectID, predicate)
	if err != nil {
		return nil, err
	}

	var matches []*Edge
	for _, e := range candidates {
		if e.SupersededBy != nil && edgeObjectMatches(e, objectID, objectLiteral) {
			matches = append(matches, e)
		}
	}

	return matches, nil
}

// edgeObjectMatches reports whether e's object (object_id or
// object_literal — the schema's XOR constraint guarantees exactly one of
// e's two is set) exactly matches the given objectID/objectLiteral. Exact
// match only, by design — see FindMatchingEdge.
func edgeObjectMatches(e *Edge, objectID *uuid.UUID, objectLiteral *string) bool {
	if objectID != nil && e.ObjectID != nil {
		return *objectID == *e.ObjectID
	}
	if objectLiteral != nil && e.ObjectLiteral != nil {
		return *objectLiteral == *e.ObjectLiteral
	}
	return false
}

// Delete is intentionally not implemented: edges are never hard-deleted per
// the proposal's data model. Superseding an edge is done via SupersededBy,
// set through Update.
