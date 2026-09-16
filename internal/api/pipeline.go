package api

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// conversationIDNamespace anchors deterministic UUID derivation for
// messages.conversation_id (UUID-typed) from a platform's own conversation
// identifier (a string — a WhatsApp chat JID, an Instagram thread path,
// etc. — see ingestion.RawMessage.ConversationID). Any fixed UUID works as
// a namespace; this one has no meaning beyond being constant across every
// call, so the same platform conversation always maps to the same UUID —
// which is what lets live windowing (per-conversation state, keyed by
// this UUID) and historical import agree about which messages belong to
// the same thread, without needing a lookup table anywhere.
var conversationIDNamespace = uuid.MustParse("d29049a1-9c60-4b0e-8d1f-9c2d9a6e6b2a")

// resolveConversationID deterministically derives a UUID from
// (platform, externalConversationID) via UUIDv5 (SHA1-based, namespaced).
func resolveConversationID(platform, externalConversationID string) uuid.UUID {
	return uuid.NewSHA1(conversationIDNamespace, []byte(platform+":"+externalConversationID))
}

// resolveSender resolves a message's platform sender ID to an entity:
// Marco's own is_self entity if senderExternalID is in the configured
// SelfExternalIDs set (WithPipeline), otherwise a "person" entity keyed
// by the raw external ID itself.
//
// This is a real, documented simplification, not an oversight:
// ingestion.RawMessage carries no separate display-name field for a
// sender, only SenderExternalID (a phone number/JID) — whatsmeow's own
// evt.Info.IsFromMe signal doesn't even survive normalization into
// RawMessage (see internal/ingestion/whatsapp/live.go), so there is no
// content-based way to tell self from contact at this layer, only
// configuration (SelfExternalIDs). One consequence: a contact entity
// created here (keyed by "+9611234567", say) and an entity later resolved
// by extraction from the model's own output (keyed by "Sara", a display
// name — see resolveEntity) may end up as two separate Entity rows for
// the same real person, since these are two different identity
// namespaces (platform ID vs. display name) this pipeline does not
// unify. A real identity-resolution/linking step across those namespaces
// is out of this issue's scope — flagged here rather than silently
// producing duplicate entities without explanation.
func (a *API) resolveSender(ctx context.Context, senderExternalID string) (*memory.Entity, error) {
	if a.selfExternalIDs[senderExternalID] {
		self, err := a.entities.GetSelf(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolve self entity: %w", err)
		}
		if self != nil {
			return self, nil
		}
		// No is_self entity exists yet — create one, keyed by the raw
		// external ID for now (a real deployment would presumably set
		// Marco's own display name at setup time; that setup flow is out
		// of scope here).
		e := &memory.Entity{Type: "person", CanonicalName: senderExternalID, IsSelf: true}
		if err := a.entities.Create(ctx, e); err != nil {
			return nil, fmt.Errorf("create self entity: %w", err)
		}
		return e, nil
	}

	return a.resolveEntity(ctx, senderExternalID, "person")
}

// resolveEntity finds an existing entity by canonical_name, or creates a
// new one with entityType if none exists. Exact (case-insensitive) name
// matching only — no fuzzy/alias resolution ("Sara" vs "Sarah" vs a
// nickname); see EntityStore.GetByCanonicalName's own doc comment for why
// that's a deliberate, documented simplification rather than a silent
// gap.
func (a *API) resolveEntity(ctx context.Context, name, entityType string) (*memory.Entity, error) {
	existing, err := a.entities.GetByCanonicalName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("look up entity %q: %w", name, err)
	}
	if existing != nil {
		return existing, nil
	}

	e := &memory.Entity{Type: entityType, CanonicalName: name}
	if err := a.entities.Create(ctx, e); err != nil {
		return nil, fmt.Errorf("create entity %q: %w", name, err)
	}
	return e, nil
}

// objectEntityTypeForPredicate returns the semantic entity type a NEW
// entity-typed object should be created with, based on predicate alone.
// extraction.ExtractedTriple does not classify entity type itself — only
// ObjectType ("entity" vs "literal") — so this is a necessary but
// necessarily incomplete heuristic: a handful of predicates unambiguously
// imply a type (family_member_of/pet_of -> the object is a person
// too); everything else defaults to the schema's generic "object" type
// rather than guessing further (e.g. works_at's object is really an
// organization, which has no matching value in entities.type's enum —
// 'person'|'topic'|'place'|'project'|'event'|'object' — at all). A real
// entity-type classifier is out of this issue's scope — flagged here,
// not silently invented.
func objectEntityTypeForPredicate(predicate string) string {
	switch predicate {
	case "family_member_of", "pet_of":
		return "person"
	default:
		return "object"
	}
}

// DefaultExtractedImportance is every newly-created edge's Importance
// until a real importance-scoring step exists. extraction.ExtractedTriple
// carries no importance signal at all (only Confidence) — nothing in this
// codebase computes importance from extracted content today, the same
// category of gap as relationship-stats' humor_level or the style-profile
// method: no fabricated per-predicate heuristic pretending to be
// principled, just an honest neutral default, flagged here rather than
// silently invented.
const DefaultExtractedImportance = 0.5

// TripleOutcome pairs an extracted triple with what storeExtractedTriples
// actually did with it. Both existing callers (ProcessLiveMessage,
// ImportWhatsApp) previously discarded storeExtractedTriples' return
// value entirely — surfacing this detail changes neither caller's
// behavior, it only makes already-happening decisions observable. Added
// for the Phase 0 dry-run tooling (cmd/phase0_dryrun), which needs
// reinforcement-vs-new-edge counts and per-triple provenance for its
// report; kept here rather than in a dry-run-only wrapper because any
// real caller benefits from knowing this (e.g. a production import
// wanting the same progress/stats reporting), and because there is no
// other way to observe it without either a special-cased dry-run code
// path (which this issue's own instructions forbid) or fragile
// after-the-fact DB diffing.
type TripleOutcome struct {
	Triple extraction.ExtractedTriple
	// Edge is the edge this triple resulted in — the created, reinforced,
	// or corrected(new) edge — or nil if Action is
	// TripleActionSkippedNoCorrectionTarget.
	Edge   *memory.Edge
	Action TripleAction
}

// TripleAction is the specific thing storeExtractedTriples did for one
// triple.
type TripleAction string

const (
	TripleActionCreated                   TripleAction = "created"
	TripleActionReinforced                TripleAction = "reinforced"
	TripleActionCorrected                 TripleAction = "corrected"
	TripleActionSkippedNoCorrectionTarget TripleAction = "skipped_no_correction_target"
)

// storeExtractedTriples resolves entities, applies source weighting,
// checks for reinforcement-vs-new, and writes/reinforces edges for
// triples extracted from sourceMessageIDs, attributed to sourceType. This
// is the ONE function both ProcessLiveMessage and ImportWhatsApp's
// batched path call to turn ExtractedTriple values into memory.Edge
// writes — the actual mechanism (not just a comment's claim) by which
// both are guaranteed to produce the same shape of output for equivalent
// content: there is exactly one place this happens, not two parallel
// implementations that happen to agree.
//
// A triple with IsCorrection=true is routed through
// EdgeStore.CorrectMemory instead of a normal Create/Reinforce — see
// extraction.ExtractedTriple.IsCorrection's own doc comment for why this
// field exists at all (this issue's minimal extension to extraction's
// output shape, not a second classifier). CorrectMemory needs an existing
// edge to supersede; if FindMatchingEdge finds no current edge for this
// (subject, predicate) to correct, the triple is skipped
// (TripleActionSkippedNoCorrectionTarget) rather than either fabricating
// a "corrected" edge with nothing to supersede or silently falling back
// to a normal Create, which would misrepresent an explicit correction
// signal as an ordinary new fact.
func (a *API) storeExtractedTriples(ctx context.Context, triples []extraction.ExtractedTriple, sourceType string, sourceMessageIDs []uuid.UUID, now time.Time) ([]TripleOutcome, error) {
	var outcomes []TripleOutcome

	for _, triple := range triples {
		subject, err := a.resolveEntity(ctx, triple.Subject, "person")
		if err != nil {
			return outcomes, fmt.Errorf("resolve subject %q: %w", triple.Subject, err)
		}

		var objectID *uuid.UUID
		var objectLiteral *string
		if triple.ObjectType == "entity" {
			obj, err := a.resolveEntity(ctx, triple.Object, objectEntityTypeForPredicate(triple.Predicate))
			if err != nil {
				return outcomes, fmt.Errorf("resolve object %q: %w", triple.Object, err)
			}
			objectID = &obj.ID
		} else {
			lit := triple.Object
			objectLiteral = &lit
		}

		if triple.IsCorrection {
			existing, err := a.edges.FindMatchingEdge(ctx, subject.ID, triple.Predicate, objectID, objectLiteral)
			if err != nil {
				return outcomes, fmt.Errorf("find edge to correct (subject=%q predicate=%q): %w", triple.Subject, triple.Predicate, err)
			}
			if existing == nil {
				// Nothing to supersede — see this function's doc comment
				// for why this is a skip, not a fallback to Create.
				outcomes = append(outcomes, TripleOutcome{Triple: triple, Action: TripleActionSkippedNoCorrectionTarget})
				continue
			}

			corrected, err := a.edges.CorrectMemory(ctx, existing.ID, memory.CorrectionInput{
				SubjectID:        subject.ID,
				Predicate:        triple.Predicate,
				ObjectID:         objectID,
				ObjectLiteral:    objectLiteral,
				SourceMessageIDs: sourceMessageIDs,
			})
			if err != nil {
				return outcomes, fmt.Errorf("correct edge %s: %w", existing.ID, err)
			}
			outcomes = append(outcomes, TripleOutcome{Triple: triple, Edge: corrected, Action: TripleActionCorrected})
			continue
		}

		existing, err := a.edges.FindMatchingEdge(ctx, subject.ID, triple.Predicate, objectID, objectLiteral)
		if err != nil {
			return outcomes, fmt.Errorf("find matching edge (subject=%q predicate=%q): %w", triple.Subject, triple.Predicate, err)
		}
		if existing != nil {
			if err := a.edges.Reinforce(ctx, existing.ID, now); err != nil {
				return outcomes, fmt.Errorf("reinforce edge %s: %w", existing.ID, err)
			}
			outcomes = append(outcomes, TripleOutcome{Triple: triple, Edge: existing, Action: TripleActionReinforced})
			continue
		}

		confidence, err := memory.ApplySourceWeight(triple.Confidence, sourceType)
		if err != nil {
			return outcomes, fmt.Errorf("apply source weight (source_type=%q): %w", sourceType, err)
		}

		edge := &memory.Edge{
			SubjectID:        subject.ID,
			Predicate:        triple.Predicate,
			ObjectID:         objectID,
			ObjectLiteral:    objectLiteral,
			Confidence:       confidence,
			Importance:       DefaultExtractedImportance,
			SourceType:       sourceType,
			SourceWeight:     memory.SourceWeights[sourceType],
			SourceMessageIDs: sourceMessageIDs,
			DecayRate:        memory.DefaultDecayRate(triple.Predicate),
		}
		if err := a.edges.Create(ctx, edge); err != nil {
			return outcomes, fmt.Errorf("create edge (subject=%q predicate=%q): %w", triple.Subject, triple.Predicate, err)
		}
		outcomes = append(outcomes, TripleOutcome{Triple: triple, Edge: edge, Action: TripleActionCreated})
	}

	return outcomes, nil
}
