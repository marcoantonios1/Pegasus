package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	// TripleActionSkippedAmbiguousCorrectionTarget is Issue 32's fix's own
	// new outcome (selectCorrectionTarget's own doc comment has the full
	// selection rules): several live candidate edges exist for the
	// triple's (subject, predicate) and nothing — neither the triple
	// already matching one of them, nor a stated ReplacesObject — narrows
	// it down to exactly one. Distinct from
	// TripleActionSkippedNoCorrectionTarget (zero valid candidates)
	// specifically so "we found candidates but refused to guess among
	// them" is discoverable and distinguishable from "there was nothing
	// to correct at all," not silently folded into the same bucket.
	TripleActionSkippedAmbiguousCorrectionTarget TripleAction = "skipped_ambiguous_correction_target"
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
// A triple with IsCorrection=true (and that passes the self-sender gate
// below) is routed through EdgeStore.CorrectMemory instead of a normal
// Create/Reinforce — see extraction.ExtractedTriple.IsCorrection's own
// doc comment for why this field exists at all. Which live edge (if any)
// actually gets superseded is selectCorrectionTarget's job (Issue 32's
// fix) — its own doc comment has the full selection rules; the short
// version: FindLiveEdgesBySubjectAndPredicate finds this subject+
// predicate's currently-live candidates, and the triple's own
// ReplacesObject (the stated old value) disambiguates among them when
// there's more than one. No valid target, or an ambiguous one nothing
// narrows down, is a skip (TripleActionSkippedNoCorrectionTarget /
// TripleActionSkippedAmbiguousCorrectionTarget) rather than either
// fabricating a "corrected" edge with nothing to supersede or silently
// falling back to a normal Create, which would misrepresent an explicit
// correction signal as an ordinary new fact.
//
// Issue 32, for context on why this needed its own fix separate from the
// self-sender gate below: the ORIGINAL correction path called
// FindMatchingEdge with the triple's own NEW object — which, by
// construction, cannot already exist as a live edge (if it did, the
// "correction" would just be restating an already-current fact) — so
// every genuine correction silently fell through to
// TripleActionSkippedNoCorrectionTarget and the wrong fact stayed live,
// completely undetected (no pipeline-level test exercised this path).
// The self-sender gate decides WHETHER a correction is honored at all;
// this fix is what makes an honored correction actually apply.
//
// Self-sender gate (prompt-injection / forged-attribution fix, Part 3):
// is_correction=true, on its own, is just the model's say-so — a
// correction's whole effect (confidence forced to 1.0, decay_locked so
// it's NEVER pruned, superseding an existing edge outright) makes it the
// single highest-trust, hardest-to-undo write path in this entire
// pipeline, and extraction.ExtractedTriple.CorrectionSourceIndex's own
// doc comment is explicit that it's exactly as untrusted as every other
// model-reported field — parsed out of a response to a prompt built from
// untrusted conversation content. A contact's message, structurally
// forged to look like a message from Marco (Part 1's prompt-encoding fix
// narrows this, Part 2's parser heuristic narrows it further for
// historical imports, but neither claims to make it impossible — see
// each one's own doc comment) could otherwise talk the model into
// emitting is_correction=true and getting it trusted outright.
//
// The actual gate: before honoring is_correction at all, resolve
// CorrectionSourceIndex to one of sourceMessageIDs, load THAT MESSAGE'S
// OWN STORED sender_id (not anything the model said), resolve it to an
// entity, and require entity.IsSelf — true identity, independent of
// whatever a model-generated triple claims. Fail safe
// (isCorrectionFromSelf's own doc comment has the exact conditions): a
// missing index, an out-of-range index, a sender that isn't self, or any
// lookup failure all DOWNGRADE the triple to an ordinary claim rather
// than erroring the whole call out — the triple still falls through to
// the exact same Create/Reinforce path every non-correction triple uses
// below, so "downgraded" and "never claimed a correction at all" are
// LITERALLY the same code path, not two implementations that need to be
// kept in sync by hand.
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

		honorCorrection := triple.IsCorrection
		if honorCorrection {
			ok, reason := a.isCorrectionFromSelf(ctx, triple.CorrectionSourceIndex, sourceMessageIDs)
			if !ok {
				a.logf("storeExtractedTriples: downgrading is_correction triple to a normal claim (subject=%q predicate=%q object=%q): %s",
					triple.Subject, triple.Predicate, triple.Object, reason)
				honorCorrection = false
			}
		}

		if honorCorrection {
			candidates, err := a.edges.FindLiveEdgesBySubjectAndPredicate(ctx, subject.ID, triple.Predicate)
			if err != nil {
				return outcomes, fmt.Errorf("find live edges for correction (subject=%q predicate=%q): %w", triple.Subject, triple.Predicate, err)
			}

			selection, err := a.selectCorrectionTarget(ctx, triple, objectID, objectLiteral, candidates)
			if err != nil {
				return outcomes, fmt.Errorf("select correction target (subject=%q predicate=%q): %w", triple.Subject, triple.Predicate, err)
			}

			switch {
			case selection.reinforceEdge != nil:
				// A live edge already holds the triple's new value —
				// nothing to correct; touch it the same way an ordinary
				// matching extraction would (see this function's doc
				// comment: "a live candidate already holds the NEW
				// value" is requirement 4(b), not a correction at all).
				if err := a.edges.Reinforce(ctx, selection.reinforceEdge.ID, now); err != nil {
					return outcomes, fmt.Errorf("reinforce edge %s: %w", selection.reinforceEdge.ID, err)
				}
				outcomes = append(outcomes, TripleOutcome{Triple: triple, Edge: selection.reinforceEdge, Action: TripleActionReinforced})

			case selection.supersedeEdge != nil:
				corrected, err := a.edges.CorrectMemory(ctx, selection.supersedeEdge.ID, memory.CorrectionInput{
					SubjectID:        subject.ID,
					Predicate:        triple.Predicate,
					ObjectID:         objectID,
					ObjectLiteral:    objectLiteral,
					SourceMessageIDs: sourceMessageIDs,
				})
				if err != nil {
					if errors.Is(err, memory.ErrEdgeAlreadySuperseded) {
						// A concurrent correction won the race
						// (requirement 6) — log and skip this ONE
						// triple; the window's other triples still
						// store normally.
						a.logf("storeExtractedTriples: correction target %s was already superseded by a concurrent correction (subject=%q predicate=%q) — skipping",
							selection.supersedeEdge.ID, triple.Subject, triple.Predicate)
						outcomes = append(outcomes, TripleOutcome{Triple: triple, Action: TripleActionSkippedNoCorrectionTarget})
						continue
					}
					return outcomes, fmt.Errorf("correct edge %s: %w", selection.supersedeEdge.ID, err)
				}
				outcomes = append(outcomes, TripleOutcome{Triple: triple, Edge: corrected, Action: TripleActionCorrected})

			default:
				// A skip — either no live candidate existed at all, or
				// several did and nothing narrowed it down to exactly
				// one (selectCorrectionTarget's own doc comment has the
				// exact rules). Logged distinctly per case rather than
				// one generic line, so "ambiguous, refused to guess" and
				// "genuinely nothing to correct" stay distinguishable in
				// logs the same way they are in TripleAction.
				if selection.skipAction == TripleActionSkippedAmbiguousCorrectionTarget {
					a.logf("storeExtractedTriples: ambiguous correction target (subject=%q predicate=%q), %d live candidate edge(s) %v — skipping, not guessing",
						triple.Subject, triple.Predicate, len(selection.skipCandidateIDs), selection.skipCandidateIDs)
				} else {
					a.logf("storeExtractedTriples: no live correction target found (subject=%q predicate=%q new_object=%q replaces_object=%v)",
						triple.Subject, triple.Predicate, triple.Object, derefOrNil(triple.ReplacesObject))
				}
				outcomes = append(outcomes, TripleOutcome{Triple: triple, Action: selection.skipAction})
			}
			continue
		}

		// Reached either because the model never claimed is_correction,
		// or because it did but the self-sender gate above downgraded it
		// — exactly one normal-claim code path either way.
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

	// Closes the gap the Phase 0 dry-run tooling surfaced (cmd/phase0_dryrun/NOTES.md):
	// CostguardClient.Embed and EmbeddingStore.Create both existed and were
	// tested in isolation, but nothing in production ever called either —
	// the embeddings table was empty in any real deployment. Run only on
	// the success path (after every triple above has been written) — see
	// embedMessages' own doc comment for why embedding failures must never
	// roll back or block the structured knowledge this function just wrote.
	a.embedMessages(ctx, sourceMessageIDs)

	return outcomes, nil
}

// isCorrectionFromSelf is the actual trust check behind Part 3's
// self-sender gate (storeExtractedTriples' own doc comment has the full
// threat-model reasoning) — resolves idx (the model-reported
// CorrectionSourceIndex) to one of sourceMessageIDs, loads THAT
// MESSAGE'S OWN STORED sender_id, resolves it to an entity, and reports
// whether that entity IsSelf. ok=false covers every way this can fail to
// qualify — never an error return, since the caller's job either way is
// the same "downgrade to a normal claim" (storeExtractedTriples), not a
// different code path per failure reason; reason is a log-friendly
// explanation of WHICH way it failed, for storeExtractedTriples' own
// downgrade log line.
//
// Lockstep assumption this relies on — confirmed, not assumed: idx is a
// window-local position (extraction.conversationLine's "i"), and
// sourceMessageIDs[idx] must be the message that produced
// conversationLine i. For ImportWhatsApp this holds by construction
// (windowWithMessageIDs partitions messageIDs using the exact same
// offsets WindowByCount itself uses — see that function's own doc
// comment). For ProcessLiveMessage this also holds: extraction.
// LiveWindower.Add appends to w.pending and hands back that same slice
// as the completed Window's Messages (window.go) — strict FIFO, the same
// order ProcessLiveMessage appends to its own parallel pendingIDs slice
// in, so completedIDs[i] and completed.Messages[i] always refer to the
// same real message.
func (a *API) isCorrectionFromSelf(ctx context.Context, idx *int, sourceMessageIDs []uuid.UUID) (ok bool, reason string) {
	if idx == nil {
		return false, "correction_source_index missing"
	}
	if *idx < 0 || *idx >= len(sourceMessageIDs) {
		return false, fmt.Sprintf("correction_source_index %d out of range [0,%d)", *idx, len(sourceMessageIDs))
	}

	msgID := sourceMessageIDs[*idx]
	msg, err := a.messages.GetByID(ctx, msgID)
	if err != nil {
		return false, fmt.Sprintf("load message %s: %v", msgID, err)
	}

	sender, err := a.entities.GetByID(ctx, msg.SenderID)
	if err != nil {
		return false, fmt.Sprintf("load sender entity for message %s: %v", msgID, err)
	}
	if !sender.IsSelf {
		return false, fmt.Sprintf("message %s's sender (%s) is not the self entity", msgID, sender.CanonicalName)
	}

	return true, ""
}

// correctionSelection is selectCorrectionTarget's decision for one
// correction triple. Exactly one of reinforceEdge, supersedeEdge, or
// skipAction is meaningful — storeExtractedTriples' own switch on this
// struct is a single, exhaustive dispatch over those three, not several
// independent booleans that could disagree with each other.
type correctionSelection struct {
	// reinforceEdge is set when a live candidate already holds the
	// triple's NEW value (requirement 4(b)) — nothing to correct.
	reinforceEdge *memory.Edge
	// supersedeEdge is the live edge selectCorrectionTarget chose to
	// supersede via CorrectMemory (requirements 4(c)/4(d)).
	supersedeEdge *memory.Edge
	// skipAction is set (to one of the two SkippedXxx TripleActions) when
	// neither of the above applies — see selectCorrectionTarget's own
	// doc comment for exactly which skip and why.
	skipAction TripleAction
	// skipCandidateIDs is populated only for an ambiguous skip — the
	// live candidate edges that couldn't be narrowed to one, for
	// storeExtractedTriples' own log line (requirement 4(e): "log the
	// candidate edge IDs, so these are discoverable rather than
	// silent").
	skipCandidateIDs []uuid.UUID
}

// selectCorrectionTarget implements Issue 32's fix (requirement 4):
// given candidates (the triple's subject+predicate's LIVE edges, from
// FindLiveEdgesBySubjectAndPredicate — already confirmed non-empty by
// the caller... actually checked here too, see (a) below) decide what a
// correction triple should do. Matching is exact-after-normalization
// (trimmed, case-insensitive), never fuzzy, by design: a fuzzy match
// could supersede the WRONG live edge, and wrongly superseding a live
// fact is worse than skipping a real correction — ambiguity is reported
// (skipCandidateIDs, logged by the caller), never guessed through.
//
// Rules, applied in order:
//
//	(a) Zero live candidates → skip, TripleActionSkippedNoCorrectionTarget.
//	(b) A live candidate already holds the triple's NEW object (exact
//	    match, same rule FindMatchingEdge's own edgeObjectMatches uses) →
//	    nothing to correct; reinforce it. Checked before (c)/(d) — if the
//	    "correction" is actually just restating the current live value,
//	    that's not a correction at all, regardless of how many OTHER
//	    candidates might also exist.
//	(c) triple.ReplacesObject is stated → match it (trimmed,
//	    case-insensitive) against each candidate's object: object_literal
//	    directly, or for an entity-object edge, against that object
//	    entity's canonical_name (looked up ONCE via EntityStore.
//	    GetByCanonicalName — read-only; resolveEntity is never used here,
//	    since it creates entities as a side effect and must not run just
//	    to check an old value that might not even correspond to a real
//	    entity). Exactly one match → supersede it. Zero matches → skip,
//	    TripleActionSkippedNoCorrectionTarget (the stated old value
//	    doesn't correspond to anything live — still "nothing valid to
//	    correct," not "ambiguous"). Several matches → skip,
//	    TripleActionSkippedAmbiguousCorrectionTarget.
//	(d) ReplacesObject absent (only reached if (b) didn't already resolve
//	    it) → supersede the sole live candidate if there is EXACTLY one;
//	    with several, skip, TripleActionSkippedAmbiguousCorrectionTarget
//	    — with no stated old value, there's no way to tell which one.
func (a *API) selectCorrectionTarget(ctx context.Context, triple extraction.ExtractedTriple, newObjectID *uuid.UUID, newObjectLiteral *string, candidates []*memory.Edge) (correctionSelection, error) {
	if len(candidates) == 0 {
		return correctionSelection{skipAction: TripleActionSkippedNoCorrectionTarget}, nil
	}

	for _, c := range candidates {
		if edgeHasObject(c, newObjectID, newObjectLiteral) {
			return correctionSelection{reinforceEdge: c}, nil
		}
	}

	if old := strings.TrimSpace(derefOrEmpty(triple.ReplacesObject)); old != "" {
		matches, err := a.candidatesMatchingOldValue(ctx, candidates, old)
		if err != nil {
			return correctionSelection{}, err
		}
		switch len(matches) {
		case 0:
			return correctionSelection{skipAction: TripleActionSkippedNoCorrectionTarget}, nil
		case 1:
			return correctionSelection{supersedeEdge: matches[0]}, nil
		default:
			return correctionSelection{skipAction: TripleActionSkippedAmbiguousCorrectionTarget, skipCandidateIDs: edgeIDs(matches)}, nil
		}
	}

	if len(candidates) == 1 {
		return correctionSelection{supersedeEdge: candidates[0]}, nil
	}
	return correctionSelection{skipAction: TripleActionSkippedAmbiguousCorrectionTarget, skipCandidateIDs: edgeIDs(candidates)}, nil
}

// candidatesMatchingOldValue returns the subset of candidates whose
// object matches oldValue (already trimmed, non-empty) — trimmed,
// case-insensitive exact match on object_literal; for an entity-object
// edge, match against the object entity's canonical_name by resolving
// oldValue itself to an entity ONCE (read-only — GetByCanonicalName, not
// resolveEntity) and comparing IDs, rather than resolving every
// candidate's own object entity individually.
func (a *API) candidatesMatchingOldValue(ctx context.Context, candidates []*memory.Edge, oldValue string) ([]*memory.Edge, error) {
	oldAsEntity, err := a.entities.GetByCanonicalName(ctx, oldValue)
	if err != nil {
		return nil, fmt.Errorf("look up old value %q as an entity: %w", oldValue, err)
	}

	var matches []*memory.Edge
	for _, c := range candidates {
		if c.ObjectLiteral != nil && strings.EqualFold(strings.TrimSpace(*c.ObjectLiteral), oldValue) {
			matches = append(matches, c)
			continue
		}
		if c.ObjectID != nil && oldAsEntity != nil && *c.ObjectID == oldAsEntity.ID {
			matches = append(matches, c)
		}
	}
	return matches, nil
}

// edgeHasObject mirrors memory.EdgeStore's own (unexported)
// edgeObjectMatches — reimplemented here rather than exported across the
// package boundary for one call site, over Edge's already-exported
// ObjectID/ObjectLiteral fields.
func edgeHasObject(e *memory.Edge, objectID *uuid.UUID, objectLiteral *string) bool {
	if objectID != nil && e.ObjectID != nil {
		return *objectID == *e.ObjectID
	}
	if objectLiteral != nil && e.ObjectLiteral != nil {
		return *objectLiteral == *e.ObjectLiteral
	}
	return false
}

func edgeIDs(edges []*memory.Edge) []uuid.UUID {
	ids := make([]uuid.UUID, len(edges))
	for i, e := range edges {
		ids[i] = e.ID
	}
	return ids
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// derefOrNil formats a *string for a log line as either its value or the
// literal "<nil>" — %v on a *string prints a pointer address, not the
// pointed-to value or a readable nil, which is useless in a log line.
func derefOrNil(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

// messageText returns the text content to embed for msg: raw_text if
// present, else transcript, else ok=false if neither is populated. This
// mirrors the same decision ProcessLiveMessage/ImportWhatsApp already
// make implicitly by only windowing messages with MediaType==text (see
// their own "not text-extractable" branches) — a voice/image message has
// no transcription pipeline wired in anywhere in this codebase yet, so
// both RawText and Transcript are nil for it, and there is nothing to
// embed, not an error.
func messageText(m *memory.Message) (string, bool) {
	if m.RawText != nil && *m.RawText != "" {
		return *m.RawText, true
	}
	if m.Transcript != nil && *m.Transcript != "" {
		return *m.Transcript, true
	}
	return "", false
}

// EmbedMessagesResult tallies what embedMessages actually did across one
// call's messageIDs — added for cmd/backfill_embeddings' summary output
// (embedded vs. already-embedded vs. skipped-no-text vs. failed counts),
// which embedMessages didn't used to return since its only caller before
// this issue (storeExtractedTriples) never needed per-call counts, only
// the side effect. Purely additive: storeExtractedTriples's own call
// (`a.embedMessages(ctx, sourceMessageIDs)`) already discards whatever
// embedMessages returns via a bare call statement, so this doesn't change
// its behavior or require touching that call site.
type EmbedMessagesResult struct {
	Embedded        int
	AlreadyEmbedded int
	SkippedNoText   int
	Failed          int
}

// embedMessages generates and stores an embedding for each distinct
// message in messageIDs that doesn't already have one (see
// EmbeddingStore.GetByMessageID's own doc comment on the idempotency
// check this relies on).
//
// Runs per MESSAGE, not per triple, deliberately: storeExtractedTriples
// calls this once, after its triple loop, with the same sourceMessageIDs
// every triple in that call shares — not once per triple — so a message
// backing several triples (or reinforcing existing ones) only reaches
// Embed() once per call, and the seen-set below still catches duplicate
// IDs within messageIDs itself as a second layer. A message can produce
// zero, one, or several triples (zero is a normal outcome — e.g. triage
// flagged a window a candidate but extraction found nothing from it, see
// TripleActionSkippedNoCorrectionTarget for a different zero-triple case)
// — it still needs exactly one embedding regardless, since semantic
// search over raw content (SearchSemantic / EmbeddingStore.
// SearchSimilarMessages) and structured extraction are two different
// downstream consumers of the same message, not the same concern.
//
// Scope note: this only reaches messages that make it as far as
// storeExtractedTriples — i.e. messages in a window triage already
// flagged as a candidate. A message in a window triage flagged as NOISE
// never calls this function at all (see ProcessLiveMessage/
// ImportWhatsApp, which only call storeExtractedTriples for candidate
// windows) and stays unembedded. That's this issue's actual scope, not
// an oversight: the insertion point is this shared function, not a
// broader change to either caller's triage-gating; embedding every
// message regardless of triage outcome would be a different, larger
// change made at a different call site.
//
// Failure behavior, decided explicitly: log-and-continue, not retry-
// with-backoff and not fail-the-whole-write. By the time this runs, every
// triple's edge has already been written successfully (this is called
// after the triple loop, not before or interleaved with it) — failing
// storeExtractedTriples' whole call because Costguard's embeddings
// endpoint had a transient error would block or appear to roll back
// real, already-correct structured knowledge over a failure in a
// completely separate downstream concern. This is NOT silent
// swallow-and-forget, which the acceptance criteria explicitly rule out:
// every failure (existing-embedding check, message load, the Embed call
// itself, or the store write) is logged via a.logf — the same Logger
// hook WithPipeline already wires for ImportWhatsApp's progress lines,
// not a print statement only visible to someone watching stdout — with
// the message ID and error, so a message that fails to embed is
// discoverable and re-embeddable later. A batch "backfill missing
// embeddings" utility (querying for messages with no embeddings row and
// re-running this) is a natural follow-up issue this logging makes
// possible; it is not built here.
func (a *API) embedMessages(ctx context.Context, messageIDs []uuid.UUID) EmbedMessagesResult {
	var result EmbedMessagesResult
	if a.embedder == nil {
		return result
	}

	seen := make(map[uuid.UUID]bool, len(messageIDs))
	for _, id := range messageIDs {
		if seen[id] {
			continue
		}
		seen[id] = true

		existing, err := a.embeddings.GetByMessageID(ctx, id)
		if err != nil {
			a.logf("embedMessages: check existing embedding for message %s: %v", id, err)
			result.Failed++
			continue
		}
		if existing != nil {
			result.AlreadyEmbedded++
			continue
		}

		msg, err := a.messages.GetByID(ctx, id)
		if err != nil {
			a.logf("embedMessages: load message %s: %v", id, err)
			result.Failed++
			continue
		}

		text, ok := messageText(msg)
		if !ok {
			result.SkippedNoText++
			continue
		}

		vec, err := a.embedder.Embed(ctx, text)
		if err != nil {
			a.logf("embedMessages: embed message %s: %v", id, err)
			result.Failed++
			continue
		}

		if err := a.embeddings.Create(ctx, &memory.Embedding{MessageID: id, Vector: vec}); err != nil {
			a.logf("embedMessages: store embedding for message %s: %v", id, err)
			result.Failed++
			continue
		}

		result.Embedded++
	}

	return result
}

// BackfillEmbeddings embeds every message in messageIDs that doesn't
// already have one — the exported entry point cmd/backfill_embeddings
// (and any future caller outside this package) uses to reuse
// embedMessages' exact logic — idempotency via EmbeddingStore.
// GetByMessageID, log-and-continue failure handling via a.logf,
// messageText's raw_text-then-transcript fallback, "no text" as a normal
// skip rather than a failure — rather than reimplementing any of it. See
// embedMessages' own doc comment for the full behavior this inherits by
// construction, including its failure philosophy.
//
// Idempotent by construction, not by separate logic here: running this
// twice against the same messageIDs relies entirely on embedMessages'
// own GetByMessageID check (every message from the first run now has a
// row, so the second run's EmbedMessagesResult.AlreadyEmbedded absorbs
// all of them and Embedded is 0) — nothing in this method or
// cmd/backfill_embeddings needs its own idempotency tracking.
//
// Unlike embedMessages (an internal step storeExtractedTriples always
// calls after a successful triple write, where a nil Embedder is a valid,
// silent no-op — most read methods work fine without one), a nil
// Embedder here is a real caller misconfiguration worth surfacing:
// backfilling embeddings IS the entire point of calling this, so silently
// returning a zero-value, all-zero-counts result would look identical to
// "ran successfully, nothing needed embedding" instead of "wasn't
// configured to run at all" — a caller has no way to tell those apart
// from EmbedMessagesResult alone.
func (a *API) BackfillEmbeddings(ctx context.Context, messageIDs []uuid.UUID) (EmbedMessagesResult, error) {
	if a.embedder == nil {
		return EmbedMessagesResult{}, fmt.Errorf("BackfillEmbeddings: API has no Embedder configured — construct via New(..., embedder) with a non-nil embedder")
	}
	return a.embedMessages(ctx, messageIDs), nil
}
