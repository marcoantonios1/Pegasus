package api

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// StoreMemory implements proposal §13's direct write path: a caller
// (Hermes) that already has a fully-formed edge to store, bypassing
// extraction entirely. A thin wrapper over EdgeStore.Create with one
// validation this package adds: predicate must be in the closed
// vocabulary. Reuses extraction.IsValidPredicate — the same check
// ValidateTriples applies to model output (internal/extraction/parse.go)
// — rather than a second, possibly-drifting vocabulary check maintained
// here.
//
// edge is taken by value and returned by pointer with ID/FirstSeen/
// LastReinforced filled in by the write, matching EdgeStore.Create's own
// contract — StoreMemory adds no computation beyond the predicate check;
// it does not apply ApplySourceWeight or compute DecayRate for the
// caller, since a caller using this direct path already has a
// fully-formed edge and is responsible for its own field values.
func (a *API) StoreMemory(ctx context.Context, edge memory.Edge) (*memory.Edge, error) {
	if !extraction.IsValidPredicate(edge.Predicate) {
		return nil, fmt.Errorf("StoreMemory: predicate %q is not in the closed vocabulary", edge.Predicate)
	}

	if err := a.edges.Create(ctx, &edge); err != nil {
		return nil, fmt.Errorf("StoreMemory: %w", err)
	}

	return &edge, nil
}

// UpdateMemoryFields is the ONLY set of fields UpdateMemory is allowed to
// change — see UpdateMemory's own doc comment for why this is
// deliberately narrow rather than a general partial-edge-update type.
type UpdateMemoryFields struct {
	// Importance, if non-nil, replaces the edge's Importance.
	Importance *float64
}

// UpdateMemory implements proposal §13's narrow mutable-field update —
// e.g. Hermes or Marco manually reweighting an edge's importance —
// WITHOUT going through CorrectMemory's correction machinery. This is
// NOT for fixing wrong facts: that is CorrectMemory's job specifically
// (§7.6 — supersession, decay-lock, an audit trail via IsCorrection),
// and UpdateMemory must never be a side door around it.
//
// Scoped to Importance only, by construction rather than by a runtime
// field-allowlist check: UpdateMemoryFields simply has no Confidence,
// Predicate, or ObjectLiteral/ObjectID field to set in the first place,
// so there is nothing for a caller to even attempt to bypass
// CorrectMemory with through this type — a stronger guarantee than
// validating and rejecting disallowed fields at runtime would be.
// Confidence/predicate/object changes belong to CorrectMemory precisely
// because those are what its supersession/decay-lock/audit-trail
// machinery exists to handle correctly; letting UpdateMemory silently
// change them would bypass all of that.
func (a *API) UpdateMemory(ctx context.Context, edgeID uuid.UUID, fields UpdateMemoryFields) (*memory.Edge, error) {
	edge, err := a.edges.GetByID(ctx, edgeID)
	if err != nil {
		return nil, fmt.Errorf("UpdateMemory(%s): %w", edgeID, err)
	}

	if fields.Importance != nil {
		edge.Importance = *fields.Importance
	}

	if err := a.edges.Update(ctx, edge); err != nil {
		return nil, fmt.Errorf("UpdateMemory(%s): %w", edgeID, err)
	}

	return edge, nil
}

// DeleteMemory implements proposal §13's delete path as a soft delete —
// see memory.EdgeStore.SoftDelete for the actual mechanism (a zero-
// confidence, zero-importance tombstone edge superseding edgeID).
//
// There is no exception here, stated plainly rather than implied: edges
// are never hard-deleted anywhere in this system, including through this
// API. EdgeStore.Delete is intentionally unimplemented (edge_store.go),
// and DeleteMemory does not add a different path to real removal — a
// "deleted" edge remains a real row, still directly queryable by ID
// (EdgeStore.GetByID) or via GetRelationshipHistory (which deliberately
// includes superseded edges), and only disappears from
// GetRelevantContext's default results because its zero confidence/
// importance put it below IsPruneCandidate's floors, the same mechanism
// that excludes any other low-value edge — not because it was special-
// cased as "deleted."
func (a *API) DeleteMemory(ctx context.Context, edgeID uuid.UUID) error {
	if _, err := a.edges.SoftDelete(ctx, edgeID); err != nil {
		return fmt.Errorf("DeleteMemory(%s): %w", edgeID, err)
	}
	return nil
}
