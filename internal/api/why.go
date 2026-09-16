package api

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// SenderBreakdown is one sender's contribution to an edge's "mentioned
// by" count — WhyDoWeBelieveThis' per-sender breakdown of its source
// messages.
type SenderBreakdown struct {
	SenderID      uuid.UUID
	CanonicalName string
	Count         int
}

// MediaTypeBreakdown is one media_type's count among an edge's source
// messages. See Belief.SourceTypes' doc comment for why this is
// media_type, not edge.SourceType.
type MediaTypeBreakdown struct {
	MediaType string
	Count     int
}

// Belief is WhyDoWeBelieveThis' output shape (proposal §12).
type Belief struct {
	EdgeID uuid.UUID

	// Confidence is the raw stored value (extracted_confidence *
	// source_weight, §7.5) — not decayed; WhyDoWeBelieveThis is
	// explaining the edge's provenance/write-time reasoning, not its
	// current retrieval-time strength (that's EffectiveConfidence's job,
	// used elsewhere — GetRelevantContext, IsPruneCandidate — not here).
	Confidence   float64
	SourceWeight float64

	// ExtractedConfidenceReconstructed is Confidence / SourceWeight —
	// NOT a separately stored value. §7.5's write-time formula
	// (ApplySourceWeight) collapses extracted_confidence * source_weight
	// into the single stored Confidence column and never persists
	// extracted_confidence on its own; dividing back out only recovers it
	// exactly if nothing else has touched Confidence since write time
	// (true today — nothing mutates Confidence post-write — but this is
	// reconstructed arithmetic, not retrieved original data, and would
	// silently misrepresent extracted_confidence if that ever changed).
	// Presented as its own field, clearly named, rather than folded into
	// Confidence as if it were independently stored.
	ExtractedConfidenceReconstructed float64

	Importance float64

	// Seen is the distinct calendar dates (UTC, time truncated to
	// midnight) this edge's source messages were sent on, ascending.
	Seen []time.Time

	MentionedBy []SenderBreakdown

	// SourceTypes is a per-message media_type breakdown (messages.media_type:
	// 'text'/'voice'/'image'/'video'), NOT the edge's own SourceType field
	// relabeled per message. §12's own worked example shows counts like
	// "whatsapp_text (4), voice_transcript (1)" — those are SourceType's
	// vocabulary (whatsapp_text/voice_transcript/instagram_dm/
	// instagram_caption/instagram_story), which exists ONCE per edge, not
	// per source message; the schema has no per-message field that stores
	// that vocabulary at all. media_type is the only per-message
	// categorical field that actually exists, so that's the real
	// breakdown this implements — under its own honest name, not
	// relabeled to imitate source_type's example strings, which would
	// mean fabricating a value never actually stored per message. See
	// EdgeSourceType for the edge's own single value.
	SourceTypes []MediaTypeBreakdown

	// EdgeSourceType is the edge's own single SourceType field — an
	// edge-level label (see SourceTypes' doc comment for why it is not a
	// per-message breakdown).
	EdgeSourceType string

	IsCorrection bool

	// SupersededBy is the superseding edge's ID, nil if this edge is
	// current.
	SupersededBy *uuid.UUID
	// SupersededByLabel is a human-readable reference to the superseding
	// edge ("predicate: object"), or "none" if SupersededBy is nil.
	SupersededByLabel string
}

// WhyDoWeBelieveThis implements proposal §12 as a pure assembly/read
// query over data that already exists — no new confidence/importance
// computation happens here, only formatting and joins against messages/
// entities to explain an edge's provenance.
func (a *API) WhyDoWeBelieveThis(ctx context.Context, edgeID uuid.UUID) (*Belief, error) {
	edge, err := a.edges.GetByID(ctx, edgeID)
	if err != nil {
		return nil, fmt.Errorf("WhyDoWeBelieveThis(%s): %w", edgeID, err)
	}

	sourceMessages, err := a.messages.GetByIDs(ctx, edge.SourceMessageIDs)
	if err != nil {
		return nil, fmt.Errorf("WhyDoWeBelieveThis(%s): load source messages: %w", edgeID, err)
	}

	seenDates := map[time.Time]bool{}
	senderCounts := map[uuid.UUID]int{}
	mediaTypeCounts := map[string]int{}
	for _, m := range sourceMessages {
		day := time.Date(m.Timestamp.Year(), m.Timestamp.Month(), m.Timestamp.Day(), 0, 0, 0, 0, time.UTC)
		seenDates[day] = true
		senderCounts[m.SenderID]++
		mediaTypeCounts[m.MediaType]++
	}

	seen := make([]time.Time, 0, len(seenDates))
	for d := range seenDates {
		seen = append(seen, d)
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i].Before(seen[j]) })

	senderIDs := make([]uuid.UUID, 0, len(senderCounts))
	for id := range senderCounts {
		senderIDs = append(senderIDs, id)
	}
	senders, err := a.entities.GetByIDs(ctx, senderIDs)
	if err != nil {
		return nil, fmt.Errorf("WhyDoWeBelieveThis(%s): resolve sender names: %w", edgeID, err)
	}
	nameByID := make(map[uuid.UUID]string, len(senders))
	for _, s := range senders {
		nameByID[s.ID] = s.CanonicalName
	}

	mentionedBy := make([]SenderBreakdown, 0, len(senderCounts))
	for id, count := range senderCounts {
		mentionedBy = append(mentionedBy, SenderBreakdown{SenderID: id, CanonicalName: nameByID[id], Count: count})
	}
	sort.Slice(mentionedBy, func(i, j int) bool { return mentionedBy[i].Count > mentionedBy[j].Count })

	sourceTypes := make([]MediaTypeBreakdown, 0, len(mediaTypeCounts))
	for mt, count := range mediaTypeCounts {
		sourceTypes = append(sourceTypes, MediaTypeBreakdown{MediaType: mt, Count: count})
	}
	sort.Slice(sourceTypes, func(i, j int) bool { return sourceTypes[i].Count > sourceTypes[j].Count })

	var extractedReconstructed float64
	if edge.SourceWeight != 0 {
		extractedReconstructed = edge.Confidence / edge.SourceWeight
	}

	supersededByLabel := "none"
	if edge.SupersededBy != nil {
		label, err := a.describeEdge(ctx, *edge.SupersededBy)
		if err != nil {
			return nil, fmt.Errorf("WhyDoWeBelieveThis(%s): describe superseding edge: %w", edgeID, err)
		}
		supersededByLabel = label
	}

	return &Belief{
		EdgeID:                           edge.ID,
		Confidence:                       edge.Confidence,
		SourceWeight:                     edge.SourceWeight,
		ExtractedConfidenceReconstructed: extractedReconstructed,
		Importance:                       edge.Importance,
		Seen:                             seen,
		MentionedBy:                      mentionedBy,
		SourceTypes:                      sourceTypes,
		EdgeSourceType:                   edge.SourceType,
		IsCorrection:                     edge.IsCorrection,
		SupersededBy:                     edge.SupersededBy,
		SupersededByLabel:                supersededByLabel,
	}, nil
}

// describeEdge builds a short human-readable "predicate: object"
// reference to edgeID — used for Belief.SupersededByLabel. Resolves an
// entity object via its canonical_name; a literal object is used as-is.
func (a *API) describeEdge(ctx context.Context, edgeID uuid.UUID) (string, error) {
	e, err := a.edges.GetByID(ctx, edgeID)
	if err != nil {
		return "", err
	}

	object := ""
	switch {
	case e.ObjectLiteral != nil:
		object = *e.ObjectLiteral
	case e.ObjectID != nil:
		ent, err := a.entities.GetByID(ctx, *e.ObjectID)
		if err != nil {
			return "", err
		}
		object = ent.CanonicalName
	}

	return fmt.Sprintf("%s: %s", e.Predicate, object), nil
}
