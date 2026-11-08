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

	// groupActivityWeight discounts a group-chat member's OWN message
	// count before it counts toward their frequency (§10 rule C,
	// group-chat-attribution fix). First guess: a member active in a
	// group chat is presumably getting less 1:1 attention from Marco per
	// message than the same volume of direct messages would represent,
	// so group activity counts for something, but less than equivalent
	// 1:1 volume — not the group's WHOLE volume (that was the bug this
	// fix closes: every member used to get credited with everyone else's
	// messages too, including Marco's own).
	groupActivityWeight = 0.25

	// groupReplyMaxLatency caps how long after a group member's message
	// a later Marco message can still count as a reply to THEM (§10 rule
	// D). Group threads interleave multiple people's messages, so unlike
	// a 1:1 thread (where literally every inbound message is from the
	// same one contact, so there's no ambiguity about who an eventual
	// reply is "to"), a long-delayed Marco message in a group could
	// plausibly be answering something else entirely, or just restarting
	// a conversation, rather than actually replying to whoever spoke
	// last. 30 minutes is a first guess at "still clearly the same
	// exchange" for a group thread; pairs exceeding it are excluded
	// entirely (not clamped to the cap), since a latency that long isn't
	// good evidence of A reply at all, let alone a fast one.
	groupReplyMaxLatency = 30 * time.Minute
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
// Group-chat attribution (§10, group-chat-attribution fix): a
// conversation_id can now hold more than one non-self participant (a
// WhatsApp/Instagram group), and every signal below handles that
// explicitly rather than assuming "sender_id == contactID is inbound,
// anything else is Marco" the way this function used to. selfID (Marco's
// own is_self entity) is now a required parameter precisely because
// telling a group chat's OTHER members apart from Marco requires knowing
// which sender actually IS Marco — the old 1:1-only code never needed
// that distinction at all.
//
// A-F below are this fix's own decided semantics, each a first guess
// like every other tuning constant in this file (groupActivityWeight,
// groupReplyMaxLatency):
//
//	A. Group detection is derived from data, not configured: a
//	   conversation is a group if it has ever had >=2 DISTINCT non-self
//	   senders, across all time — not just this window, so a group that's
//	   gone quiet doesn't misclassify as 1:1 just because this window is
//	   sparse. Known limit: a group where only one other person has EVER
//	   spoken is indistinguishable from a real 1:1 by this rule. The real
//	   fix is recording conversation kind at ingestion time (a WhatsApp
//	   @g.us JID, an Instagram participant count) — deliberately not
//	   built here; this data-derived heuristic is a stand-in until that
//	   exists.
//	B. 1:1 conversations: UNCHANGED from before this fix — frequency
//	   uses the whole conversation's volume (both directions), reply
//	   pairing uses the existing contact<->Marco pairing with no latency
//	   cap, recency uses the last message in either direction.
//	C. Group conversations, frequency: the member is credited ONLY their
//	   own messages, scaled by groupActivityWeight. Marco's messages and
//	   every OTHER member's messages are never credited to anyone but
//	   whoever actually sent them.
//	D. Group conversations, reply pairing: a Marco message counts as a
//	   reply to member X only when it's Marco's FIRST message since the
//	   most recent non-Marco message, that most-recent message was from
//	   X, and the latency is within groupReplyMaxLatency. A later
//	   member's message updates "most recent inbound," so a subsequent
//	   Marco message attributes to THEM, not to X. Pairs over the cap are
//	   excluded outright, not clamped down to it. Group-derived pairs and
//	   1:1 pairs feed the same overall median.
//	E. Group conversations, recency: the member's OWN last message in
//	   that conversation — not the group's overall last message, which
//	   would credit a silent member with everyone else's activity.
//	F. Cross-contact normalization: frequency = attributed(contact) /
//	   mean(attributed over every contact active in the window), where
//	   attributed(contact) sums, over their 1:1 conversations, each
//	   conversation's total count, plus groupActivityWeight times their
//	   own message count in each of their groups. For 1:1-only data this
//	   reduces exactly to the pre-fix computation (one conversation per
//	   contact, so "mean per contact" and the old "mean per conversation"
//	   are the same number). The per-(conversation, sender) counts behind
//	   this are one grouped query (MessageStore.
//	   CountByConversationAndSenderSince), not one query per contact —
//	   this function recomputes that population-wide data on every call,
//	   the same way it already recomputed the plain per-conversation mean
//	   on every call before this fix; batching that across contacts is
//	   the not-yet-built orchestrator's concern, not this function's.
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
func RecomputeRelationshipStats(ctx context.Context, store *memory.RelationshipStatsStore, edgeStore *memory.EdgeStore, msgStore *memory.MessageStore, selfID, contactID uuid.UUID, now time.Time) (*memory.RelationshipStats, error) {
	// No fallback to the old "any other sender in the conversation is
	// Marco" assumption: that assumption is exactly what misattributed
	// group-chat activity to every member in the first place (see this
	// function's own doc comment), so a caller that can't resolve a real
	// is_self entity gets a clear error instead of silently guessing
	// again.
	if selfID == uuid.Nil {
		return nil, fmt.Errorf("relationship stats: no self entity configured — refusing to guess which sender is Marco")
	}

	since := now.Add(-DefaultRelationshipStatsWindow)

	conversationIDs, err := msgStore.ConversationIDsForSender(ctx, contactID)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: resolve conversations for contact %s: %w", contactID, err)
	}

	// Population-wide data for rule F's cross-contact mean: EVERY
	// conversation active in the window, and (one grouped query, not one
	// per contact) each one's per-sender counts within it.
	perConvSenderCounts, err := msgStore.CountByConversationAndSenderSince(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: count messages by conversation and sender since %s: %w", since, err)
	}

	// Group/1:1 classification needs covering both the population-wide
	// set (for rule F's mean) AND this specific contact's own
	// conversations (for reply-pairing and recency below, including any
	// of THEIR conversations that happen to be quiet this window and so
	// wouldn't otherwise appear in perConvSenderCounts at all) — one more
	// single query over the union of both, all-time, per rule A.
	classifyIDs := make(map[uuid.UUID]struct{}, len(perConvSenderCounts)+len(conversationIDs))
	for cid := range perConvSenderCounts {
		classifyIDs[cid] = struct{}{}
	}
	for _, cid := range conversationIDs {
		classifyIDs[cid] = struct{}{}
	}
	classifyIDList := make([]uuid.UUID, 0, len(classifyIDs))
	for cid := range classifyIDs {
		classifyIDList = append(classifyIDList, cid)
	}
	senderSets, err := msgStore.DistinctSendersByConversation(ctx, classifyIDList)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: resolve distinct senders for conversations: %w", err)
	}
	isGroup, oneOnOneOwner := classifyConversations(selfID, senderSets)

	frequency := computeFrequency(selfID, contactID, perConvSenderCounts, isGroup, oneOnOneOwner)

	var replyLatencies []float64
	var oneOnOneConvIDs, groupConvIDs []uuid.UUID
	for _, cid := range conversationIDs {
		msgs, err := msgStore.GetByConversationSince(ctx, cid, since)
		if err != nil {
			return nil, fmt.Errorf("relationship stats: load conversation %s: %w", cid, err)
		}
		replyLatencies = append(replyLatencies, replyLatenciesSeconds(selfID, contactID, msgs, isGroup[cid])...)

		if isGroup[cid] {
			groupConvIDs = append(groupConvIDs, cid)
		} else {
			oneOnOneConvIDs = append(oneOnOneConvIDs, cid)
		}
	}
	replySpeed := median(replyLatencies)

	// Rule E: 1:1 recency is unchanged (last message either direction);
	// group recency is the contact's OWN last message in their groups,
	// not whoever in the group spoke most recently.
	lastMessageAt, err := msgStore.LastMessageTime(ctx, oneOnOneConvIDs)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: last message time for contact %s: %w", contactID, err)
	}
	lastGroupMessageAt, err := msgStore.LastMessageTimeBySender(ctx, contactID, groupConvIDs)
	if err != nil {
		return nil, fmt.Errorf("relationship stats: last group message time for contact %s: %w", contactID, err)
	}
	if lastGroupMessageAt.After(lastMessageAt) {
		lastMessageAt = lastGroupMessageAt
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

// classifyConversations implements §10 rule A (group detection) for
// every conversation_id in senderSets: a conversation is a group if it
// has ever had (all-time, per senderSets' own contract — see
// MessageStore.DistinctSendersByConversation) >=2 distinct non-self
// senders. For a conversation that is NOT a group, oneOnOneOwner records
// its one non-self participant — the contact every signal for that
// conversation attributes to, used both to build rule F's population-wide
// attributed map (for a conversation that doesn't happen to belong to
// the specific contact RecomputeRelationshipStats was called for) and,
// degenerate case aside, to confirm a conversation really does belong to
// the contact it's being scored for. A conversation where self is the
// ONLY sender that has ever appeared (should not happen in practice —
// every conversation_id in this pipeline comes from a two-or-more-party
// chat) gets no owner and is simply excluded from frequency attribution
// by computeFrequency, rather than attributed to nobody incorrectly.
func classifyConversations(selfID uuid.UUID, senderSets map[uuid.UUID]map[uuid.UUID]bool) (isGroup map[uuid.UUID]bool, oneOnOneOwner map[uuid.UUID]uuid.UUID) {
	isGroup = make(map[uuid.UUID]bool, len(senderSets))
	oneOnOneOwner = make(map[uuid.UUID]uuid.UUID, len(senderSets))

	for cid, senders := range senderSets {
		var nonSelf []uuid.UUID
		for senderID := range senders {
			if senderID != selfID {
				nonSelf = append(nonSelf, senderID)
			}
		}

		if len(nonSelf) >= 2 {
			isGroup[cid] = true
			continue
		}
		if len(nonSelf) == 1 {
			oneOnOneOwner[cid] = nonSelf[0]
		}
	}

	return isGroup, oneOnOneOwner
}

// computeFrequency implements §10 point 1 as reworked by rule F: a
// contact's attributed activity in the window (their own 1:1
// conversations' total volume, plus groupActivityWeight times their own
// message count in each of their groups — rules B/C) divided by the mean
// attributed activity across every contact active in the same window,
// including the contact's own. A contact merely average for Marco scores
// ~1.0, not near 0 — unchanged from before this fix, just computed over
// contacts now instead of conversations (the two coincide exactly when
// every conversation is 1:1, which is why this still reduces to the
// pre-fix computation for 1:1-only data).
//
// perConvSenderCounts, isGroup, and oneOnOneOwner all come from
// RecomputeRelationshipStats' own single pair of grouped queries over
// the WHOLE active population, not just this contact — rule F's mean is
// explicitly a cross-contact quantity, so computing it requires seeing
// every contact's attributed activity, not just the one this call is
// scoring.
//
// "messages/week" per §10's wording and this ratio are the same number:
// both the numerator and denominator cover the identical window, so
// converting each to a per-week rate before dividing would multiply and
// divide by the same constant — the window length cancels out of the
// ratio algebraically. This computes the ratio directly from raw window
// counts rather than performing that redundant conversion.
func computeFrequency(selfID, contactID uuid.UUID, perConvSenderCounts map[uuid.UUID]map[uuid.UUID]int, isGroup map[uuid.UUID]bool, oneOnOneOwner map[uuid.UUID]uuid.UUID) float64 {
	attributed := make(map[uuid.UUID]float64, len(perConvSenderCounts))

	for cid, senderCounts := range perConvSenderCounts {
		if isGroup[cid] {
			// Rule C: each member credited ONLY their own messages,
			// scaled down. Marco's own messages in the group (senderID ==
			// selfID) are never credited to anyone.
			for senderID, count := range senderCounts {
				if senderID == selfID {
					continue
				}
				attributed[senderID] += groupActivityWeight * float64(count)
			}
			continue
		}

		// Rule B: unchanged — the WHOLE conversation's volume (both
		// directions) attributed to its one owning contact.
		owner, ok := oneOnOneOwner[cid]
		if !ok {
			// No non-self sender has EVER appeared in this conversation
			// (the degenerate case classifyConversations' own doc comment
			// notes) — nothing to attribute it to.
			continue
		}
		var total int
		for _, count := range senderCounts {
			total += count
		}
		attributed[owner] += float64(total)
	}

	if len(attributed) == 0 {
		return 0
	}

	var sum float64
	for _, v := range attributed {
		sum += v
	}
	mean := sum / float64(len(attributed))
	if mean == 0 {
		return 0
	}

	return attributed[contactID] / mean
}

// replyLatenciesSeconds implements §10 point 2's reply-pairing rule
// (rules B and D) for a single conversation's messages (ordered by
// timestamp ascending, per MessageStore.GetByConversationSince): a
// "reply" is Marco's first message following one or more inbound
// messages, with latency measured from the immediately preceding inbound
// message — not the first of a possible inbound streak, the last one,
// since that's the message actually being replied to. Consecutive Marco
// messages with no intervening inbound message are continuations, not
// replies, and are excluded, per §10's explicit requirement.
//
// In a 1:1 conversation (isGroup false) every inbound message is from
// contactID by construction (classifyConversations only calls a
// conversation 1:1 when it has exactly one non-self participant ever),
// so this reduces exactly to the pre-fix 1:1 behavior: every reply pairs
// with contactID, no latency cap.
//
// In a group conversation (isGroup true), "the immediately preceding
// inbound message" can be from ANY member, not just contactID — rule D's
// "most recent inbound updates on every member's message, not just
// contactID's" requirement, which is why this tracks the most recent
// inbound sender unconditionally rather than only tracking contactID's
// own messages the way the pre-fix version did. A reply only counts
// toward contactID specifically when that most-recent inbound sender WAS
// contactID, and (group-only) when the latency is within
// groupReplyMaxLatency — a pair exceeding the cap is dropped entirely,
// not clamped to it.
func replyLatenciesSeconds(selfID, contactID uuid.UUID, msgs []*memory.Message, isGroup bool) []float64 {
	var latencies []float64
	var lastInboundAt time.Time
	var lastInboundSender uuid.UUID
	haveLastInbound := false

	for _, m := range msgs {
		if m.SenderID != selfID {
			// Inbound from whoever actually sent it — contactID in a 1:1
			// conversation, possibly a different member in a group. This
			// unconditionally overwrites the "most recent inbound" state
			// per rule D, even when it's not contactID.
			lastInboundAt = m.Timestamp
			lastInboundSender = m.SenderID
			haveLastInbound = true
			continue
		}

		// Marco's own message: a reply to lastInboundSender if there IS a
		// pending inbound message to reply to, and a continuation
		// (excluded) otherwise. Either way, the pending inbound is
		// consumed now — a LATER Marco message with no new inbound in
		// between is a continuation of THIS reply, not a second one.
		if haveLastInbound && lastInboundSender == contactID {
			latency := m.Timestamp.Sub(lastInboundAt).Seconds()
			if !isGroup || latency <= groupReplyMaxLatency.Seconds() {
				latencies = append(latencies, latency)
			}
		}
		haveLastInbound = false
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

// countExplicitSignalEdges counts current (non-superseded,
// non-tombstone) nickname_is and inside_joke_ref edges with contactID as
// subject — §10's "explicit signals" component, drawn from the predicate
// vocabulary already built (internal/extraction.Predicates).
//
// Only checks contactID as subject, not object: EdgeStore's
// GetBySubjectAndPredicate is subject-only by design (edge_store.go), and
// EdgeStore has no "either side" query — adding one just for this
// component (10% of the closeness weight) isn't worth the new query
// surface. Known simplification: an inside_joke_ref edge extracted with
// contactID only as the object, not the subject, won't be counted here.
//
// Excludes tombstones (memory.IsTombstone) as well as superseded edges —
// the delete-exclusion audit's one real finding in this package
// (requirement 9/15): GetBySubjectAndPredicate is deliberately
// unfiltered (see its own doc comment) and SoftDelete leaves a LIVE
// tombstone with the SAME predicate as whatever it replaced, so checking
// only SupersededBy == nil used to still count a deleted nickname as an
// "explicit signal" Marco had explicitly asked to remove.
func countExplicitSignalEdges(ctx context.Context, edgeStore *memory.EdgeStore, contactID uuid.UUID) (int, error) {
	count := 0
	for _, predicate := range [...]string{"nickname_is", "inside_joke_ref"} {
		edges, err := edgeStore.GetBySubjectAndPredicate(ctx, contactID, predicate)
		if err != nil {
			return 0, err
		}
		for _, e := range edges {
			if e.SupersededBy == nil && !memory.IsTombstone(*e) {
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
