package reflection

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// Tuning constants for RecomputeRelationshipStats (proposal §10). Every
// one of these is a first-guess starting value, not validated against
// real data — the same caveat this package's other consolidation stages
// (dedup.go's SimilarityThreshold/Window, decay.go's decay rates) already
// carry. They need real tuning once RecomputeRelationshipStats has run
// against Marco's actual contacts and its output can be spot-checked
// against his own judgment (§10's own stated acceptance path, not
// something this code can verify itself).
const (
	// DefaultRelationshipStatsWindow bounds how far back frequency and
	// reply-speed pairing draw messages from. 4 weeks: long enough to
	// smooth out a single unusually quiet or busy week, short enough to
	// reflect the relationship's CURRENT rhythm rather than its entire
	// history — a contact who talked constantly two years ago but has
	// gone quiet since shouldn't still score as high-frequency today.
	DefaultRelationshipStatsWindow = 4 * 7 * 24 * time.Hour

	// replySpeedHalfLifeSeconds is the characteristic timescale used to
	// squash a raw median reply latency (seconds) into closeness's 0-1
	// reply-speed component: replySpeedNorm = 1 / (1 +
	// median/replySpeedHalfLifeSeconds). At exactly this many seconds the
	// component scores 0.5. 1800s (30 minutes) is a first guess: replies
	// on that order of magnitude read as "fast" for a close relationship,
	// replies on the order of many hours read as "slow".
	replySpeedHalfLifeSeconds = 1800.0

	// recencyHalfLifeDays is the timescale for recencyNorm's exponential
	// decay: recencyNorm = exp(-daysSinceLastMessage / recencyHalfLifeDays).
	// 14 days is a first guess: a contact heard from within the last
	// couple of weeks should still read as "recent"; one not heard from
	// in over a month should read as meaningfully less so.
	recencyHalfLifeDays = 14.0

	// explicitSignalCap is how many nickname_is/inside_joke_ref edges are
	// needed to saturate the explicit-signal closeness component at 1.0.
	// 5 is a first guess — a handful of named inside jokes or nicknames
	// already signals real closeness for a personal-scale contact list;
	// requiring dozens before saturating would leave this component near
	// 0 for nearly everyone.
	explicitSignalCap = 5.0
)

// Closeness composite weights (§10: "weights are set manually first" —
// explicitly deferred ML/weight-learning tuning is out of scope here, see
// this file's package-level doc comment). Sum to 1.0 by construction. As
// much a first guess as the constants above, carrying the same caveat.
const (
	weightFrequency      = 0.30
	weightReplySpeed     = 0.25
	weightHumor          = 0.20
	weightRecency        = 0.15
	weightExplicitSignal = 0.10
)

// noReplyPairsSentinel is ReplySpeed's value when zero reply pairs were
// observed in the window. Deliberately not 0: 0 seconds is itself a real,
// meaningful value (an instant reply), so reusing it for "no data" would
// make a contact with no observed replies indistinguishable from one who
// always replies instantly. -1 is never a legitimate latency.
const noReplyPairsSentinel = -1.0

// RecomputeRelationshipStats implements proposal §10's per-contact
// relationship-stat recomputation — frequency, reply_speed, humor_level,
// and the closeness composite — for a single contact, writing the result
// via store.Upsert and returning it.
//
// This is one stage of §9.2's consolidation pipeline (merge → summaries →
// stats → decay → pruning → embeddings), called once per non-self
// 'person' entity by the consolidation orchestrator — the same
// not-yet-built orchestrator this package's other stages (dedup.go's
// MergeDuplicates, decay.go's ApplyBatchDecay) already note they plug
// into. contactID must be a 'person' entity that is not Marco's own
// is_self row; filtering that out of the candidate set is the orchestrator
// loop's job, not this function's — same division of responsibility as
// MergeDuplicates taking a caller-selected candidate set rather than
// querying for one itself. This function runs only during that batch
// pass, never live per-message — nothing here is wired into ingestion.
//
// 1:1-conversation assumption: every signal below treats "sender_id ==
// contactID" as an inbound message and any other sender appearing in that
// same conversation_id as Marco's own outbound message. That holds for
// Pegasus's current scope — WhatsApp/Instagram DMs, one contact per
// conversation_id (see internal/bulkimport's "a window must never mix
// messages from two different chats") — but would silently misattribute
// messages in a group chat with multiple non-Marco participants. No
// group-chat support exists anywhere in this pipeline yet, so this is a
// known, stated limitation rather than a guarded error case.
//
// humor_level is a known gap, not a fabricated number: §10 defines it as
// "ratio of messages classified humorous (sentiment tier) combined with
// meme-send frequency," but no sentiment/humor classification exists
// anywhere upstream of this function — not on memory.Message, not in
// extraction.Predicates' closed vocabulary. Building that classifier is a
// meaningfully different task (an NLP pass over message content) than
// recomputing a stat from data that already exists, and doing it inside
// the Reflection Engine would mean this consolidation stage running its
// own ad hoc classification pass rather than consuming an
// already-processed signal the way every other §10 input does. This
// function always returns HumorLevel = 0 with this comment as the
// TODO marker: implement humor classification as an extraction-pipeline
// addition (a separate, scoped issue), then wire its output in here.
func RecomputeRelationshipStats(ctx context.Context, store *memory.RelationshipStatsStore, edgeStore *memory.EdgeStore, msgStore *memory.MessageStore, contactID uuid.UUID, now time.Time) (*memory.RelationshipStats, error) {
	since := now.Add(-DefaultRelationshipStatsWindow)

	conversationIDs, err := msgStore.ConversationIDsForSender(ctx, contactID)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: resolve conversations for contact %s: %w", contactID, err)
	}

	windowCounts, err := msgStore.CountByConversationSince(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: count messages since %s: %w", since, err)
	}
	frequency := computeFrequency(conversationIDs, windowCounts)

	var replyLatencies []float64
	for _, cid := range conversationIDs {
		msgs, err := msgStore.GetByConversationSince(ctx, cid, since)
		if err != nil {
			return nil, fmt.Errorf("relationship stats: load conversation %s: %w", cid, err)
		}
		replyLatencies = append(replyLatencies, replyLatenciesSeconds(contactID, msgs)...)
	}
	replySpeed := median(replyLatencies)

	lastMessageAt, err := msgStore.LastMessageTime(ctx, conversationIDs)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: last message time for contact %s: %w", contactID, err)
	}

	explicitSignalCount, err := countExplicitSignalEdges(ctx, edgeStore, contactID)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: count explicit signal edges for contact %s: %w", contactID, err)
	}

	// See this function's doc comment: no upstream humor/sentiment
	// classification signal exists yet, so this is a placeholder, not a
	// computed value.
	humorLevel := 0.0

	closeness := computeCloseness(closenessInputs{
		frequency:           frequency,
		replySpeedSeconds:   replySpeed,
		humorLevel:          humorLevel,
		lastMessageAt:       lastMessageAt,
		now:                 now,
		explicitSignalCount: explicitSignalCount,
	})

	stats := &memory.RelationshipStats{
		ContactID:  contactID,
		Frequency:  frequency,
		ReplySpeed: replySpeed,
		HumorLevel: humorLevel,
		Closeness:  closeness,
	}

	if err := store.Upsert(ctx, stats); err != nil {
		return nil, fmt.Errorf("relationship stats: upsert for contact %s: %w", contactID, err)
	}

	return stats, nil
}

// computeFrequency implements §10 point 1: this contact's message count in
// the window (summed across every conversation_id ConversationIDsForSender
// resolved for them) divided by the mean message count across every
// conversation active in the same window, including the contact's own —
// a contact merely average for Marco scores ~1.0, not near 0.
//
// "messages/week" per §10's wording and this ratio are the same number:
// both the numerator and denominator cover the identical window, so
// converting each to a per-week rate before dividing would multiply and
// divide by the same constant — the window length cancels out of the
// ratio algebraically. This computes the ratio directly from raw window
// counts rather than performing that redundant conversion.
func computeFrequency(conversationIDs []uuid.UUID, windowCounts map[uuid.UUID]int) float64 {
	if len(windowCounts) == 0 {
		return 0
	}

	var contactCount int
	for _, cid := range conversationIDs {
		contactCount += windowCounts[cid]
	}

	var total int
	for _, c := range windowCounts {
		total += c
	}
	mean := float64(total) / float64(len(windowCounts))
	if mean == 0 {
		return 0
	}

	return float64(contactCount) / mean
}

// replyLatenciesSeconds implements §10 point 2's reply-pairing rule for a
// single conversation's messages (ordered by timestamp ascending, per
// MessageStore.GetByConversationSince): a "reply" is Marco's first message
// following one or more inbound messages from contactID, with latency
// measured from the immediately preceding inbound message — not the first
// of a possible inbound streak, the last one, since that's the message
// actually being replied to. Consecutive Marco messages with no
// intervening inbound message are continuations, not replies, and are
// excluded, per §10's explicit requirement.
func replyLatenciesSeconds(contactID uuid.UUID, msgs []*memory.Message) []float64 {
	var latencies []float64
	var lastInboundAt time.Time
	haveLastInbound := false
	lastSenderWasContact := false

	for _, m := range msgs {
		if m.SenderID == contactID {
			lastInboundAt = m.Timestamp
			haveLastInbound = true
			lastSenderWasContact = true
			continue
		}

		// Any other sender in this contact's conversation is treated as
		// Marco's own outbound message — see the 1:1-conversation
		// assumption in RecomputeRelationshipStats' doc comment.
		if lastSenderWasContact && haveLastInbound {
			latencies = append(latencies, m.Timestamp.Sub(lastInboundAt).Seconds())
		}
		lastSenderWasContact = false
	}

	return latencies
}

// median returns the median of values, or noReplyPairsSentinel if values
// is empty.
func median(values []float64) float64 {
	if len(values) == 0 {
		return noReplyPairsSentinel
	}

	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

// countExplicitSignalEdges counts current (non-superseded) nickname_is and
// inside_joke_ref edges with contactID as subject — §10's "explicit
// signals" component, drawn from the predicate vocabulary already built
// (internal/extraction.Predicates).
//
// Only checks contactID as subject, not object: EdgeStore's
// GetBySubjectAndPredicate is subject-only by design (edge_store.go), and
// EdgeStore has no "either side" query — adding one just for this
// component (10% of the closeness weight) isn't worth the new query
// surface. Known simplification: an inside_joke_ref edge extracted with
// contactID only as the object, not the subject, won't be counted here.
func countExplicitSignalEdges(ctx context.Context, edgeStore *memory.EdgeStore, contactID uuid.UUID) (int, error) {
	count := 0
	for _, predicate := range [...]string{"nickname_is", "inside_joke_ref"} {
		edges, err := edgeStore.GetBySubjectAndPredicate(ctx, contactID, predicate)
		if err != nil {
			return 0, err
		}
		for _, e := range edges {
			if e.SupersededBy == nil {
				count++
			}
		}
	}
	return count, nil
}

// closenessInputs carries computeCloseness's inputs as a struct rather
// than a long positional parameter list, since several are same-typed
// float64s where argument-order mistakes at a call site would compile
// silently but score every contact wrong.
type closenessInputs struct {
	frequency           float64
	replySpeedSeconds   float64
	humorLevel          float64
	lastMessageAt       time.Time
	now                 time.Time
	explicitSignalCount int
}

// computeCloseness implements §10 point 4's hand-weighted composite.
// Every component is normalized into a comparable 0-1-ish range before
// weighting — summing raw reply-speed seconds and a raw frequency ratio
// directly would let whichever happens to have the larger raw magnitude
// dominate the sum regardless of its assigned weight, which is not what
// "weighted composite" means. See each component's normalization inline,
// and replySpeedHalfLifeSeconds/recencyHalfLifeDays/explicitSignalCap's
// doc comments for the specific constants chosen.
func computeCloseness(in closenessInputs) float64 {
	// frequency (see computeFrequency) is a baseline-relative ratio,
	// unbounded above (≈1.0 = average, 3.0 = 3x average, etc.) — squashed
	// here via ratio/(ratio+1) purely for this weighted sum: 0 at no
	// activity, 0.5 at exactly average, approaching 1 as activity grows
	// far above average. This squashed value is internal to this
	// function; only the raw ratio is stored (memory.RelationshipStats.Frequency).
	frequencyNorm := in.frequency / (in.frequency + 1)

	// replySpeedSeconds carries noReplyPairsSentinel (-1) when no reply
	// pairs were observed — that case scores 0, not "fast": a
	// relationship with no observed replies this window shouldn't read
	// as maximally responsive just because there's no latency to measure.
	replySpeedNorm := 0.0
	if in.replySpeedSeconds >= 0 {
		replySpeedNorm = 1 / (1 + in.replySpeedSeconds/replySpeedHalfLifeSeconds)
	}

	recencyNorm := 0.0
	if !in.lastMessageAt.IsZero() {
		daysSinceLast := in.now.Sub(in.lastMessageAt).Hours() / 24
		if daysSinceLast < 0 {
			// A last-message timestamp in the future relative to now
			// shouldn't happen in practice; clamp rather than let recency
			// exceed 1, mirroring memory.EffectiveConfidence's same
			// defensive clamp for an analogous "elapsed time" computation.
			daysSinceLast = 0
		}
		recencyNorm = math.Exp(-daysSinceLast / recencyHalfLifeDays)
	}
	// lastMessageAt.IsZero() (no messages found at all) leaves recencyNorm
	// at 0 — no observed contact ever is the minimum possible recency.

	explicitSignalNorm := math.Min(1, float64(in.explicitSignalCount)/explicitSignalCap)

	return weightFrequency*frequencyNorm +
		weightReplySpeed*replySpeedNorm +
		weightHumor*in.humorLevel +
		weightRecency*recencyNorm +
		weightExplicitSignal*explicitSignalNorm
}
