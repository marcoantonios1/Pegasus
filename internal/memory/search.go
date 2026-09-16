package memory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
)

// escapeLikePattern escapes '\', '%', and '_' in s so it can be safely
// wrapped in a LIKE/ILIKE '%'...'%' pattern (with ESCAPE '\') and match
// only literal occurrences of s — without this, a caller's search term
// containing '%' or '_' would be silently treated as a wildcard instead
// of the literal character they typed. The backslash itself must be
// escaped first, or escaping '%' into '\%' would then have its own
// backslash re-escaped on a second pass.
func escapeLikePattern(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// EdgeCandidate pairs an edge with its similarity score against whatever
// query vector produced it (0 when no query vector was given — see
// SearchRelevant). An internal building block for weighted ranking
// (internal/api's GetRelevantContext, proposal §11), not itself part of
// any store's public read/write contract.
type EdgeCandidate struct {
	Edge       *Edge
	Similarity float64
}

// SearchFilters are the structured, SQL-expressible filters SearchRelevant
// and SearchByText both take. Deliberately narrower than internal/api's
// own Filters type (which additionally carries IncludePruned and ranking
// weights) — those two extra concerns depend on EffectiveConfidence decay
// math and a caller-facing ranking formula, neither of which belongs at
// the SQL layer. See SearchRelevant's doc comment for why.
type SearchFilters struct {
	SubjectID   *uuid.UUID
	SourceTypes []string
	Since       *time.Time
}

const searchFiltersWhere = `
	e.superseded_by IS NULL
	AND ($1::uuid IS NULL OR e.subject_id = $1)
	AND ($2::text[] IS NULL OR e.source_type = ANY($2))
	AND ($3::timestamptz IS NULL OR e.first_seen >= $3)
`

const edgeColumns = `
	e.id, e.subject_id, e.predicate, e.object_id, e.object_literal, e.confidence,
	e.importance, e.source_type, e.source_weight, e.source_message_ids,
	e.first_seen, e.last_reinforced, e.decay_rate, e.decay_locked,
	e.superseded_by, e.is_correction
`

func scanEdgeRow(row interface {
	Scan(dest ...any) error
}, e *Edge) error {
	return row.Scan(
		&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
		&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
		&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
		&e.SupersededBy, &e.IsCorrection,
	)
}

// SearchRelevant is the hybrid-retrieval query behind GetRelevantContext
// (proposal §11): one SQL statement combining structured filters
// (subject_id, source_type, recency) with pgvector cosine similarity,
// rather than two separate queries stitched together in Go — the whole
// point of keeping embeddings in the same database (§6.1) is lost if
// retrieval instead round-trips through Go to combine them.
//
// An edge has no embedding of its own — only messages do (the embeddings
// table). This joins each candidate edge to its SOURCE messages'
// embeddings via source_message_ids and scores the edge by its best
// (highest-similarity) matching source message, via a LATERAL join so
// each edge contributes at most one row regardless of how many source
// messages it has.
//
// queryVector may be nil — SearchRelevant is also how GetContact fetches
// "top edges about this person" with no free-text query at all (see
// api.GetContact). Without a query vector there's nothing to rank
// similarity against, so this skips the embeddings join entirely and
// falls back to ORDER BY last_reinforced DESC; every returned
// EdgeCandidate.Similarity is 0 in that case, and the caller's ranking
// formula naturally reduces to its confidence/importance terms.
//
// Always excludes superseded edges (superseded_by IS NULL) — every read
// method in internal/api excludes them by default except
// GetRelationshipHistory, which needs the full history on purpose (see
// that method's own doc comment for why it's the exception).
//
// Deliberately applies no confidence/importance pruning filter here —
// IsPruneCandidate (prune_candidate.go) depends on EffectiveConfidence, a
// decay computation that would have to be duplicated in SQL to run inside
// this query, and this package has consistently avoided reimplementing
// that formula a second place (see ApplyBatchDecay's and
// RecomputeRelationshipStats' own doc comments for the same reasoning).
// Structured filters that don't need decay math (subject_id, source_type,
// recency) run here, in SQL, alongside the similarity ordering;
// confidence/importance-based pruning is the caller's job, applied
// against this method's (deliberately generous) candidate pool — see
// api.GetRelevantContext.
//
// candidatePoolSize bounds how many rows this query returns. Callers are
// expected to request more than their final topN, since Go-side
// prune-gating and re-weighting can reorder or drop rows within the pool,
// but still far short of the whole table.
//
// Requires the pgvector pgx codec registered on the pool (see
// EmbeddingStore's own doc comment) whenever queryVector is non-nil —
// EdgeStore did not previously depend on that registration; this method
// is what introduces the dependency, since it binds a pgvector.Vector
// query parameter directly against the embeddings table's vector column.
func (s *EdgeStore) SearchRelevant(ctx context.Context, queryVector []float32, filters SearchFilters, candidatePoolSize int) ([]EdgeCandidate, error) {
	var rows interface {
		Next() bool
		Scan(dest ...any) error
		Err() error
		Close()
	}
	var err error

	if queryVector == nil {
		rows, err = s.db.Query(ctx, `
			SELECT `+edgeColumns+`, NULL::float8 AS similarity
			FROM edges e
			WHERE `+searchFiltersWhere+`
			ORDER BY e.last_reinforced DESC
			LIMIT $4
		`, filters.SubjectID, filters.SourceTypes, filters.Since, candidatePoolSize)
	} else {
		rows, err = s.db.Query(ctx, `
			SELECT `+edgeColumns+`, best.similarity
			FROM edges e
			LEFT JOIN LATERAL (
				SELECT 1 - (em.vector <=> $4) AS similarity
				FROM messages m
				JOIN embeddings em ON em.message_id = m.id
				WHERE m.id = ANY(e.source_message_ids)
				ORDER BY em.vector <=> $4
				LIMIT 1
			) best ON true
			WHERE `+searchFiltersWhere+`
			ORDER BY best.similarity DESC NULLS LAST, e.last_reinforced DESC
			LIMIT $5
		`, filters.SubjectID, filters.SourceTypes, filters.Since, pgvector.NewVector(queryVector), candidatePoolSize)
	}
	if err != nil {
		return nil, fmt.Errorf("search relevant edges: %w", err)
	}
	defer rows.Close()

	var candidates []EdgeCandidate
	for rows.Next() {
		var e Edge
		var similarity *float64

		if err := rows.Scan(
			&e.ID, &e.SubjectID, &e.Predicate, &e.ObjectID, &e.ObjectLiteral, &e.Confidence,
			&e.Importance, &e.SourceType, &e.SourceWeight, &e.SourceMessageIDs,
			&e.FirstSeen, &e.LastReinforced, &e.DecayRate, &e.DecayLocked,
			&e.SupersededBy, &e.IsCorrection,
			&similarity,
		); err != nil {
			return nil, fmt.Errorf("scan relevant edge row: %w", err)
		}

		c := EdgeCandidate{Edge: &e}
		if similarity != nil {
			c.Similarity = *similarity
		}
		candidates = append(candidates, c)
	}

	return candidates, rows.Err()
}

// SearchByText implements SearchMemory's keyword/structured search
// (proposal §13): edges whose subject's canonical_name, predicate, or
// object_literal contains queryText (case-insensitive substring match).
// This is deliberately simple — ILIKE, not a dedicated full-text-search
// index or ranking — matching this build's established "don't
// overengineer past what a personal-scale system needs" bar (see
// dedup.go's literalSimilarity choice for the same reasoning). Distinct
// from SearchRelevant/vector search: this finds edges matching specific
// terms, not edges related in meaning without keyword overlap.
//
// queryText is escaped (escapeLikePattern) before being wrapped in
// '%'...'%' — ILIKE treats a bare '%' or '_' in the search term as a
// wildcard, not a literal character, so an unescaped query for e.g. "50%"
// would match far more than intended. The query is still fully
// parameterized either way (no SQL injection risk existed here); this is
// purely about matching what the caller actually typed.
//
// Always excludes superseded edges, same as SearchRelevant, and for the
// same reason.
func (s *EdgeStore) SearchByText(ctx context.Context, queryText string, filters SearchFilters, limit int) ([]*Edge, error) {
	escaped := escapeLikePattern(queryText)

	rows, err := s.db.Query(ctx, `
		SELECT `+edgeColumns+`
		FROM edges e
		JOIN entities subj ON subj.id = e.subject_id
		WHERE `+searchFiltersWhere+`
			AND (
				subj.canonical_name ILIKE '%' || $4 || '%' ESCAPE '\'
				OR e.predicate ILIKE '%' || $4 || '%' ESCAPE '\'
				OR e.object_literal ILIKE '%' || $4 || '%' ESCAPE '\'
			)
		ORDER BY e.importance DESC, e.first_seen DESC
		LIMIT $5
	`, filters.SubjectID, filters.SourceTypes, filters.Since, escaped, limit)
	if err != nil {
		return nil, fmt.Errorf("search edges by text: %w", err)
	}
	defer rows.Close()

	var edges []*Edge
	for rows.Next() {
		var e Edge
		if err := scanEdgeRow(rows, &e); err != nil {
			return nil, fmt.Errorf("scan text-search edge row: %w", err)
		}
		edges = append(edges, &e)
	}

	return edges, rows.Err()
}

// GetBySubjectOrObject returns every edge where entityID is either the
// subject or the object — proposal §10.1's relationship-history query:
// a fact where the contact is the OBJECT (e.g. "Marco -> likes ->
// [contact]'s cooking") is still relationship-relevant, not just facts
// where the contact is the subject.
//
// Unlike every other query method in this file, this one does NOT
// exclude superseded edges — see api.GetRelationshipHistory's doc
// comment, the sole caller this method exists for: reconstructing how a
// relationship's facts changed over time requires the superseded history,
// not just current state. Ordered by first_seen ascending (oldest first)
// for exactly that reason — this is meant to be read as a timeline.
func (s *EdgeStore) GetBySubjectOrObject(ctx context.Context, entityID uuid.UUID) ([]*Edge, error) {
	rows, err := s.db.Query(ctx, `
		SELECT `+edgeColumns+`
		FROM edges e
		WHERE e.subject_id = $1 OR e.object_id = $1
		ORDER BY e.first_seen ASC
	`, entityID)
	if err != nil {
		return nil, fmt.Errorf("get edges by subject or object: %w", err)
	}
	defer rows.Close()

	var edges []*Edge
	for rows.Next() {
		var e Edge
		if err := scanEdgeRow(rows, &e); err != nil {
			return nil, fmt.Errorf("scan subject-or-object edge row: %w", err)
		}
		edges = append(edges, &e)
	}

	return edges, rows.Err()
}
