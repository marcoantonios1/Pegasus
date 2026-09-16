package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// ErrNotAPerson is returned by GetContact when contactID resolves to an
// entity whose Type is not "person" — GetContact is specifically about
// people Marco has a relationship with, not topics/places/projects/
// events/objects that also live in the entities table.
var ErrNotAPerson = errors.New("api: entity is not a person")

// ErrRelationshipStatsNotComputed is returned by GetRelationship when no
// consolidation pass has ever computed relationship_stats for this
// contact yet (wraps pgx.ErrNoRows from RelationshipStatsStore, so
// callers can match on either). This is an expected, not exceptional,
// state for a newly-seen contact — the Reflection Engine only computes
// relationship_stats during consolidation (reflection.RecomputeRelationshipStats),
// which may not have run yet for someone Marco just started talking to.
var ErrRelationshipStatsNotComputed = errors.New("api: relationship stats not yet computed for this contact")

// contactTopEdgesCount is how many edges GetContact includes as "top
// edges about this person" — deliberately larger than
// GetRelevantContext's own DefaultTopN (5-8 per §11), since GetContact is
// an explicit "tell me about this person" call where a caller likely
// wants a fuller picture than a single relevance-ranked query result.
// Unvalidated; a reasonable starting guess like every other unvalidated
// constant in this build.
const contactTopEdgesCount = 10

// ContactSummary is GetContact's composite result: the entity itself,
// its current relationship stats (nil if consolidation hasn't computed
// them yet — see ErrRelationshipStatsNotComputed's doc comment; GetContact
// does NOT treat this as an error, since "no stats yet" is expected for a
// new contact), and a small set of top edges about them.
type ContactSummary struct {
	Entity   *memory.Entity
	Stats    *memory.RelationshipStats
	TopEdges []*memory.Edge
}

// GetContact implements proposal §13's "everything about this person"
// call: the entity row (must be type='person'), its current
// relationship_stats (RelationshipStatsStore), and a small ranked set of
// top edges about them.
//
// Top edges are fetched via GetRelevantContext with an empty query and
// SubjectID set to contactID, rather than a second, separate filtering/
// ranking implementation — GetRelevantContext already does exactly the
// confidence-gating and importance-weighting this needs (see its own doc
// comment for what an empty query does: skips vector similarity, ranks
// purely by decayed confidence and importance). Reusing it here means
// GetContact's notion of "top edges" can never silently drift from
// GetRelevantContext's, since there is only one ranking/pruning
// implementation, not two.
func (a *API) GetContact(ctx context.Context, contactID uuid.UUID) (*ContactSummary, error) {
	entity, err := a.entities.GetByID(ctx, contactID)
	if err != nil {
		return nil, fmt.Errorf("GetContact: %w", err)
	}
	if entity.Type != "person" {
		return nil, fmt.Errorf("GetContact(%s): %w (type=%q)", contactID, ErrNotAPerson, entity.Type)
	}

	stats, err := a.relStats.GetByContactID(ctx, contactID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("GetContact: relationship stats: %w", err)
		}
		stats = nil // not yet computed — not an error for GetContact's purposes
	}

	topEdges, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &contactID}, contactTopEdgesCount)
	if err != nil {
		return nil, fmt.Errorf("GetContact: top edges: %w", err)
	}

	return &ContactSummary{Entity: entity, Stats: stats, TopEdges: topEdges}, nil
}

// GetRelationship implements proposal §13's narrower "just the
// relationship score" call: closeness, humor_level, frequency, and
// reply_speed for contactID, with no entity/edge data attached.
//
// This is a thin wrapper over RelationshipStatsStore.GetByContactID, not
// redundant with GetContact: GetContact assembles "everything about this
// person" (entity + stats + top edges, three queries), while
// GetRelationship is the cheap, single-query path for a caller that only
// ever needs the relationship numbers — e.g. Hermes deciding tone/warmth
// for a reply doesn't need the full contact profile on every message.
// Both exist deliberately, calling the same underlying store method
// rather than each having their own copy of it.
func (a *API) GetRelationship(ctx context.Context, contactID uuid.UUID) (*memory.RelationshipStats, error) {
	stats, err := a.relStats.GetByContactID(ctx, contactID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("GetRelationship(%s): %w", contactID, ErrRelationshipStatsNotComputed)
		}
		return nil, fmt.Errorf("GetRelationship: %w", err)
	}
	return stats, nil
}

// GetRelationshipHistory implements proposal §10.1: every edge for this
// relationship, subject_id = contactID OR object_id = contactID (a fact
// where the contact is the OBJECT — e.g. "Marco -> likes ->
// [contact]'s cooking" — is still relationship-relevant, not just facts
// where they're the subject), ordered by first_seen ascending.
//
// This is the one method in this whole API that does NOT exclude
// superseded edges — deliberately, and unlike every other read method
// here (GetRelevantContext, SearchMemory both filter superseded_by IS
// NULL by default; see EdgeStore.SearchRelevant's doc comment). The point
// of a relationship HISTORY is reconstructing how things changed over
// time — a corrected or contradicted fact is part of that story, not
// noise to hide. See EdgeStore.GetBySubjectOrObject's own doc comment,
// the query this method is a thin wrapper over.
func (a *API) GetRelationshipHistory(ctx context.Context, contactID uuid.UUID) ([]*memory.Edge, error) {
	edges, err := a.edges.GetBySubjectOrObject(ctx, contactID)
	if err != nil {
		return nil, fmt.Errorf("GetRelationshipHistory(%s): %w", contactID, err)
	}
	return edges, nil
}
