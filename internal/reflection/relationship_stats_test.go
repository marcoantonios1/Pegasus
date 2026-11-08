package reflection

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func floatsClose(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}

// --- Requirement 7: unit tests for each formula component in isolation ---

// TestComputeFrequency covers rule F's reworked formula: 1:1 attribution
// (rule B, unchanged), group attribution (rule C: own messages only,
// scaled by groupActivityWeight, Marco never credited), and the
// cross-contact mean now being computed per CONTACT rather than per
// CONVERSATION.
func TestComputeFrequency(t *testing.T) {
	self := uuid.New()

	t.Run("1:1 only — above/below-average contacts, zero activity, empty population", func(t *testing.T) {
		closeContact := uuid.New()
		moderateContact := uuid.New()
		distantContact := uuid.New() // never appears in perConvSenderCounts: zero activity this window
		closeConv := uuid.New()
		moderateConv := uuid.New()

		perConvSenderCounts := map[uuid.UUID]map[uuid.UUID]int{
			closeConv:    {self: 20, closeContact: 20}, // total 40
			moderateConv: {self: 4, moderateContact: 4}, // total 8
		}
		isGroup := map[uuid.UUID]bool{closeConv: false, moderateConv: false}
		oneOnOneOwner := map[uuid.UUID]uuid.UUID{closeConv: closeContact, moderateConv: moderateContact}

		// mean = (40+8)/2 contacts = 24.
		if got, want := computeFrequency(self, closeContact, perConvSenderCounts, isGroup, oneOnOneOwner), 40.0/24.0; !floatsClose(got, want, 1e-9) {
			t.Errorf("above-average contact: computeFrequency() = %v, want %v", got, want)
		}
		if got, want := computeFrequency(self, moderateContact, perConvSenderCounts, isGroup, oneOnOneOwner), 8.0/24.0; !floatsClose(got, want, 1e-9) {
			t.Errorf("below-average contact: computeFrequency() = %v, want %v", got, want)
		}
		// distantContact never appears anywhere in perConvSenderCounts —
		// attributed[distantContact] defaults to 0.
		if got := computeFrequency(self, distantContact, perConvSenderCounts, isGroup, oneOnOneOwner); got != 0 {
			t.Errorf("zero activity in window: computeFrequency() = %v, want 0", got)
		}
		if got := computeFrequency(self, closeContact, map[uuid.UUID]map[uuid.UUID]int{}, map[uuid.UUID]bool{}, map[uuid.UUID]uuid.UUID{}); got != 0 {
			t.Errorf("empty population: computeFrequency() = %v, want 0", got)
		}
	})

	t.Run("contact with multiple 1:1 conversation_ids sums across them", func(t *testing.T) {
		// Same real person reachable through two conversation_ids (e.g.
		// WhatsApp and Instagram) — summed into ONE contact's attributed
		// total, not treated as two separate population members. A
		// second, different contact with an equal-sized single
		// conversation keeps the population at 2 distinct contacts, so
		// the mean reflects contacts, not raw conversation count (rule
		// F's actual point — see this file's own doc comment on why that
		// differs from the pre-fix per-conversation mean whenever a
		// contact has more than one conversation).
		multiContact := uuid.New()
		otherContact := uuid.New()
		conv1 := uuid.New()
		conv2 := uuid.New()
		otherConv := uuid.New()

		perConvSenderCounts := map[uuid.UUID]map[uuid.UUID]int{
			conv1:     {self: 5, multiContact: 5},   // total 10
			conv2:     {self: 7, multiContact: 7},   // total 14
			otherConv: {self: 12, otherContact: 12}, // total 24
		}
		isGroup := map[uuid.UUID]bool{conv1: false, conv2: false, otherConv: false}
		oneOnOneOwner := map[uuid.UUID]uuid.UUID{conv1: multiContact, conv2: multiContact, otherConv: otherContact}

		// attributed = {multiContact: 24, otherContact: 24} -> mean = 24.
		if got, want := computeFrequency(self, multiContact, perConvSenderCounts, isGroup, oneOnOneOwner), 1.0; !floatsClose(got, want, 1e-9) {
			t.Errorf("computeFrequency() = %v, want %v", got, want)
		}
	})

	t.Run("group conversation: own messages only, scaled, Marco never credited", func(t *testing.T) {
		alice := uuid.New()
		bob := uuid.New()
		groupConv := uuid.New()

		perConvSenderCounts := map[uuid.UUID]map[uuid.UUID]int{
			groupConv: {self: 50, alice: 3, bob: 2}, // Marco's 50 messages must never be credited to anyone
		}
		isGroup := map[uuid.UUID]bool{groupConv: true}
		oneOnOneOwner := map[uuid.UUID]uuid.UUID{}

		// attributed = {alice: 0.25*3=0.75, bob: 0.25*2=0.5} -> mean = 0.625.
		if got, want := computeFrequency(self, alice, perConvSenderCounts, isGroup, oneOnOneOwner), 0.75/0.625; !floatsClose(got, want, 1e-9) {
			t.Errorf("alice: computeFrequency() = %v, want %v", got, want)
		}
		if got, want := computeFrequency(self, bob, perConvSenderCounts, isGroup, oneOnOneOwner), 0.5/0.625; !floatsClose(got, want, 1e-9) {
			t.Errorf("bob: computeFrequency() = %v, want %v", got, want)
		}
		// Marco's own entity ID was never a group member being scored,
		// but confirm it isn't even present in the attributed population:
		// if it had leaked in, bob's mean (and thus his frequency) would
		// be wrong.
		if got := computeFrequency(self, self, perConvSenderCounts, isGroup, oneOnOneOwner); got != 0 {
			t.Errorf("Marco's own ID must never accumulate attributed frequency, got %v", got)
		}
	})

	t.Run("mixed: a 1:1 DM plus a group, for the same contact", func(t *testing.T) {
		david := uuid.New()
		eve := uuid.New()
		dmConv := uuid.New()
		groupConv := uuid.New()

		perConvSenderCounts := map[uuid.UUID]map[uuid.UUID]int{
			dmConv:    {self: 6, david: 6},         // total 12
			groupConv: {self: 10, david: 8, eve: 4}, // david's own group count: 8, eve's: 4
		}
		isGroup := map[uuid.UUID]bool{dmConv: false, groupConv: true}
		oneOnOneOwner := map[uuid.UUID]uuid.UUID{dmConv: david}

		// attributed = {david: 12 + 0.25*8 = 14, eve: 0.25*4 = 1} -> mean = 7.5.
		if got, want := computeFrequency(self, david, perConvSenderCounts, isGroup, oneOnOneOwner), 14.0/7.5; !floatsClose(got, want, 1e-9) {
			t.Errorf("david: computeFrequency() = %v, want %v", got, want)
		}
	})
}

// TestClassifyConversations covers rule A's group-detection threshold
// (>=2 distinct non-self senders, all-time) and the 1:1 owner lookup
// rule B/C/E all depend on.
func TestClassifyConversations(t *testing.T) {
	self := uuid.New()
	alice := uuid.New()
	bob := uuid.New()

	oneOnOneConv := uuid.New()
	groupConv := uuid.New()
	selfOnlyConv := uuid.New() // degenerate: self messaging itself, should not happen in practice

	senderSets := map[uuid.UUID]map[uuid.UUID]bool{
		oneOnOneConv: {self: true, alice: true},
		groupConv:    {self: true, alice: true, bob: true},
		selfOnlyConv: {self: true},
	}

	isGroup, oneOnOneOwner := classifyConversations(self, senderSets)

	if isGroup[oneOnOneConv] {
		t.Error("a conversation with exactly 1 non-self sender must not be classified as a group")
	}
	if owner, ok := oneOnOneOwner[oneOnOneConv]; !ok || owner != alice {
		t.Errorf("expected alice as the 1:1 owner, got %v (ok=%v)", owner, ok)
	}

	if !isGroup[groupConv] {
		t.Error("a conversation with 2 distinct non-self senders must be classified as a group")
	}
	if _, ok := oneOnOneOwner[groupConv]; ok {
		t.Error("a group conversation must not get a 1:1 owner")
	}

	if isGroup[selfOnlyConv] {
		t.Error("a conversation where self is the only sender ever must not be classified as a group")
	}
	if _, ok := oneOnOneOwner[selfOnlyConv]; ok {
		t.Error("a self-only conversation has no owner to attribute to")
	}
}

// TestReplyLatenciesSeconds_And_Median covers requirement 7's own worked
// example: known messages with known timestamps -> assert exact median
// reply_speed. Also proves the pairing rule's core exclusion: consecutive
// Marco messages with no intervening inbound message are continuations,
// not replies, per §10 point 2.
func TestReplyLatenciesSeconds_And_Median(t *testing.T) {
	contact := uuid.New()
	self := uuid.New()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	msgs := []*memory.Message{
		{SenderID: contact, Timestamp: base},                        // inbound
		{SenderID: contact, Timestamp: base.Add(60 * time.Second)},  // inbound again — no reply yet, this is now "the preceding inbound"
		{SenderID: self, Timestamp: base.Add(120 * time.Second)},    // reply: latency = 120-60 = 60s (from the LAST inbound, not the first)
		{SenderID: self, Timestamp: base.Add(150 * time.Second)},    // continuation, not a reply — must be excluded
		{SenderID: contact, Timestamp: base.Add(200 * time.Second)}, // inbound
		{SenderID: self, Timestamp: base.Add(260 * time.Second)},    // reply: latency = 260-200 = 60s
	}

	latencies := replyLatenciesSeconds(self, contact, msgs, false)
	want := []float64{60, 60}
	if len(latencies) != len(want) {
		t.Fatalf("replyLatenciesSeconds() = %v, want %v", latencies, want)
	}
	for i := range want {
		if latencies[i] != want[i] {
			t.Errorf("latency[%d] = %v, want %v", i, latencies[i], want[i])
		}
	}

	if got := median(latencies); got != 60 {
		t.Errorf("median(%v) = %v, want 60", latencies, got)
	}

	// Odd count.
	if got := median([]float64{10, 30, 20}); got != 20 {
		t.Errorf("median([10,30,20]) = %v, want 20", got)
	}
	// Even count: average of the two middle values.
	if got := median([]float64{10, 20, 30, 40}); got != 25 {
		t.Errorf("median([10,20,30,40]) = %v, want 25", got)
	}
	// No reply pairs at all: the sentinel, not 0.
	if got := median(nil); got != noReplyPairsSentinel {
		t.Errorf("median(nil) = %v, want sentinel %v", got, noReplyPairsSentinel)
	}
}

func TestReplyLatenciesSeconds_NoLeadingReplyWithoutInbound(t *testing.T) {
	contact := uuid.New()
	self := uuid.New()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Marco messages the contact first, unprompted — there is no preceding
	// inbound message, so this must not be counted as a reply.
	msgs := []*memory.Message{
		{SenderID: self, Timestamp: base},
		{SenderID: self, Timestamp: base.Add(30 * time.Second)},
	}

	latencies := replyLatenciesSeconds(self, contact, msgs, false)
	if len(latencies) != 0 {
		t.Errorf("expected no latencies when there's no preceding inbound message, got %v", latencies)
	}
}

// TestReplyLatenciesSeconds_Group covers rule D directly: Bob speaks,
// Marco replies -> attributed to Bob, never to Alice (the regression
// this whole fix closes — this exact case used to count toward
// whichever contact's own conversationIDs happened to include this
// group, regardless of who actually sent the inbound message); a later
// member's message updates "most recent inbound" so a subsequent Marco
// reply attributes to THEM; and a pair exceeding groupReplyMaxLatency is
// excluded outright, not clamped.
func TestReplyLatenciesSeconds_Group(t *testing.T) {
	self := uuid.New()
	alice := uuid.New()
	bob := uuid.New()
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	msgs := []*memory.Message{
		{SenderID: bob, Timestamp: base},                                            // inbound from Bob
		{SenderID: self, Timestamp: base.Add(5 * time.Minute)},                       // reply to Bob: latency 300s
		{SenderID: alice, Timestamp: base.Add(6 * time.Minute)},                      // inbound from Alice — updates "most recent inbound"
		{SenderID: self, Timestamp: base.Add(7 * time.Minute)},                       // reply to Alice: latency 60s
		{SenderID: bob, Timestamp: base.Add(8 * time.Minute)},                        // inbound from Bob again
		{SenderID: self, Timestamp: base.Add(8*time.Minute + 40*time.Minute)},        // reply to Bob, but 40min > the 30min cap -> excluded
	}

	bobLatencies := replyLatenciesSeconds(self, bob, msgs, true)
	if len(bobLatencies) != 1 || bobLatencies[0] != 300 {
		t.Errorf("bob: replyLatenciesSeconds() = %v, want [300]", bobLatencies)
	}

	aliceLatencies := replyLatenciesSeconds(self, alice, msgs, true)
	if len(aliceLatencies) != 1 || aliceLatencies[0] != 60 {
		t.Errorf("alice: replyLatenciesSeconds() = %v, want [60] (the reply to Bob must NOT be attributed to Alice)", aliceLatencies)
	}
}

// TestComputeCloseness covers requirement 7 for the closeness composite
// itself: hand-computable inputs, exact expected weighted sum.
func TestComputeCloseness(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("hand-computed midpoint inputs", func(t *testing.T) {
		in := closenessInputs{
			frequency:           1.0,                       // frequencyNorm = 1/(1+1) = 0.5
			replySpeedSeconds:   replySpeedHalfLifeSeconds, // replySpeedNorm = 1/(1+1) = 0.5
			humorLevel:          0.0,
			lastMessageAt:       now, // daysSinceLast = 0 -> recencyNorm = 1
			now:                 now,
			explicitSignalCount: 5, // saturates explicitSignalCap -> 1.0
		}
		want := weightFrequency*0.5 + weightReplySpeed*0.5 + weightHumor*0 + weightRecency*1.0 + weightExplicitSignal*1.0
		got := computeCloseness(in)
		if !floatsClose(got, want, 1e-9) {
			t.Errorf("computeCloseness() = %v, want %v", got, want)
		}
	})

	t.Run("no reply pairs scores 0 on that component, not 1", func(t *testing.T) {
		in := closenessInputs{
			frequency:           0,
			replySpeedSeconds:   noReplyPairsSentinel,
			humorLevel:          0,
			lastMessageAt:       time.Time{}, // never observed
			now:                 now,
			explicitSignalCount: 0,
		}
		got := computeCloseness(in)
		if got != 0 {
			t.Errorf("expected closeness 0 for a completely unobserved contact, got %v", got)
		}
	})

	t.Run("explicit signal count above cap does not exceed 1.0 contribution", func(t *testing.T) {
		base := closenessInputs{now: now, lastMessageAt: now, replySpeedSeconds: noReplyPairsSentinel}
		atCap := base
		atCap.explicitSignalCount = int(explicitSignalCap)
		aboveCap := base
		aboveCap.explicitSignalCount = int(explicitSignalCap) * 10

		if got1, got2 := computeCloseness(atCap), computeCloseness(aboveCap); got1 != got2 {
			t.Errorf("expected explicit-signal contribution to saturate at the cap: at-cap=%v, above-cap=%v", got1, got2)
		}
	})
}

// --- Requirement 8: integration-style test with distinct synthetic profiles ---

func createTestMessage(t *testing.T, ctx context.Context, store *memory.MessageStore, conversationID, senderID uuid.UUID, ts time.Time) *memory.Message {
	t.Helper()
	m := &memory.Message{
		ConversationID: conversationID,
		SenderID:       senderID,
		MediaType:      "text",
		Timestamp:      ts,
	}
	if err := store.Create(ctx, m); err != nil {
		t.Fatalf("create test message: %v", err)
	}
	return m
}

// TestRecomputeRelationshipStats_OrdersContactsSensibly seeds three
// synthetic contacts with deliberately different profiles — high-
// frequency/fast-reply/close, moderate, and sparse/slow-reply/distant —
// and asserts the resulting closeness scores order the way a human would
// expect (close > moderate > distant). This is the closest thing to §10's
// "spot-checked against Marco's own judgment" acceptance criterion that
// can be automated here; validating the actual numbers against Marco's
// real contacts is his manual judgment call once this runs against real
// data, not something this test can stand in for.
func TestRecomputeRelationshipStats_OrdersContactsSensibly(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	// IsSelf deliberately left false: entities_single_self_idx allows at
	// most one is_self=true row in the whole database, and this test
	// suite runs repeatedly against a persistent local dev DB with no
	// per-test reset — a second run would collide on that constraint.
	// RecomputeRelationshipStats never reads the entity's IsSelf column
	// itself (it takes selfID as a plain uuid.UUID parameter — see its
	// doc comment), so this field's DB value doesn't affect what's under
	// test here; self.ID is passed to RecomputeRelationshipStats directly
	// below.
	self := createTestEntity(t, ctx, entityStore, "Marco")

	closeContact := createTestEntity(t, ctx, entityStore, "Close Contact")
	moderateContact := createTestEntity(t, ctx, entityStore, "Moderate Contact")
	distantContact := createTestEntity(t, ctx, entityStore, "Distant Contact")

	now := time.Now()

	// Close contact: 20 reply pairs over the last ~5 days, each replied to
	// within 2 minutes, plus an explicit nickname signal. Last message
	// only hours ago.
	closeConv := uuid.New()
	for i := 0; i < 20; i++ {
		inboundAt := now.Add(-time.Duration(20-i) * 6 * time.Hour)
		createTestMessage(t, ctx, msgStore, closeConv, closeContact.ID, inboundAt)
		createTestMessage(t, ctx, msgStore, closeConv, self.ID, inboundAt.Add(2*time.Minute))
	}
	nickname := "Bestie"
	nicknameEdge := &memory.Edge{
		SubjectID: closeContact.ID, Predicate: "nickname_is", ObjectLiteral: &nickname,
		Confidence: 1.0, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := edgeStore.Create(ctx, nicknameEdge); err != nil {
		t.Fatalf("create nickname edge: %v", err)
	}

	// Moderate contact: 4 reply pairs spread across the window, each
	// replied to within 2 hours. Last message about a week ago.
	moderateConv := uuid.New()
	for i := 0; i < 4; i++ {
		inboundAt := now.Add(-28*24*time.Hour + time.Duration(i)*7*24*time.Hour)
		createTestMessage(t, ctx, msgStore, moderateConv, moderateContact.ID, inboundAt)
		createTestMessage(t, ctx, msgStore, moderateConv, self.ID, inboundAt.Add(2*time.Hour))
	}

	// Distant contact: a single exchange well outside the trailing
	// window (40 days ago), replied to after 2 days. No explicit signals.
	distantConv := uuid.New()
	distantInboundAt := now.Add(-40 * 24 * time.Hour)
	createTestMessage(t, ctx, msgStore, distantConv, distantContact.ID, distantInboundAt)
	createTestMessage(t, ctx, msgStore, distantConv, self.ID, distantInboundAt.Add(2*24*time.Hour))

	closeStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, closeContact.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(close): %v", err)
	}
	moderateStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, moderateContact.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(moderate): %v", err)
	}
	distantStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, distantContact.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(distant): %v", err)
	}

	// --- Ordering: the actual acceptance criterion this test stands in for ---
	if !(closeStats.Closeness > moderateStats.Closeness) {
		t.Errorf("expected close contact's closeness (%v) > moderate contact's (%v)", closeStats.Closeness, moderateStats.Closeness)
	}
	if !(moderateStats.Closeness > distantStats.Closeness) {
		t.Errorf("expected moderate contact's closeness (%v) > distant contact's (%v)", moderateStats.Closeness, distantStats.Closeness)
	}

	// --- Sanity bounds, loose enough to survive future weight/threshold retuning ---
	if closeStats.Closeness <= 0.4 {
		t.Errorf("expected close contact's closeness comfortably high, got %v", closeStats.Closeness)
	}
	if distantStats.Closeness >= 0.15 {
		t.Errorf("expected distant contact's closeness to stay low, got %v", distantStats.Closeness)
	}

	// --- Component-level sanity, not just the final composite ---
	if closeStats.Frequency <= moderateStats.Frequency {
		t.Errorf("expected close contact's frequency (%v) > moderate's (%v)", closeStats.Frequency, moderateStats.Frequency)
	}
	if distantStats.Frequency != 0 {
		t.Errorf("expected distant contact's frequency 0 (zero activity in window), got %v", distantStats.Frequency)
	}
	if distantStats.ReplySpeed != noReplyPairsSentinel {
		t.Errorf("expected distant contact's reply_speed to be the no-data sentinel %v (its only exchange is outside the window), got %v", noReplyPairsSentinel, distantStats.ReplySpeed)
	}
	if closeStats.ReplySpeed <= 0 || closeStats.ReplySpeed >= moderateStats.ReplySpeed {
		t.Errorf("expected close contact's reply_speed (%v) to be a small positive number, faster than moderate's (%v)", closeStats.ReplySpeed, moderateStats.ReplySpeed)
	}
	for _, s := range []*memory.RelationshipStats{closeStats, moderateStats, distantStats} {
		if s.HumorLevel != 0 {
			t.Errorf("expected humor_level to always be the 0 placeholder (no upstream classifier), got %v for contact %s", s.HumorLevel, s.ContactID)
		}
	}

	// --- Upsert actually persisted a readable row ---
	reloaded, err := statsStore.GetByContactID(ctx, closeContact.ID)
	if err != nil {
		t.Fatalf("GetByContactID(close): %v", err)
	}
	if reloaded.Closeness != closeStats.Closeness {
		t.Errorf("reloaded closeness %v does not match returned value %v", reloaded.Closeness, closeStats.Closeness)
	}
}

// TestRecomputeRelationshipStats_UpsertOverwritesNotAccumulates confirms
// this table holds one current row per contact (migration 000006's
// design decision): recomputing twice for the same contact must not
// create a second row.
func TestRecomputeRelationshipStats_UpsertOverwritesNotAccumulates(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	self := &memory.Entity{Type: "person", CanonicalName: "Marco Two", IsSelf: false} // not is_self, avoids the single-is_self unique index colliding across tests
	if err := entityStore.Create(ctx, self); err != nil {
		t.Fatalf("create self entity: %v", err)
	}
	contact := createTestEntity(t, ctx, entityStore, "Repeat Contact")

	conv := uuid.New()
	now := time.Now()
	createTestMessage(t, ctx, msgStore, conv, contact.ID, now.Add(-time.Hour))
	createTestMessage(t, ctx, msgStore, conv, self.ID, now.Add(-50*time.Minute))

	first, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, contact.ID, now)
	if err != nil {
		t.Fatalf("first RecomputeRelationshipStats: %v", err)
	}
	second, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, contact.ID, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("second RecomputeRelationshipStats: %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("expected the same relationship_stats row (upsert, not insert) across two recompute passes, got IDs %s and %s", first.ID, second.ID)
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relationship_stats WHERE contact_id = $1`, contact.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 relationship_stats row for the contact after two recompute passes, got %d", rowCount)
	}
}

// TestCountExplicitSignalEdges_ExcludesTombstone covers the delete-
// exclusion audit's one real finding in this package (requirement 9/15):
// countExplicitSignalEdges only checks SupersededBy == nil (live), with
// no tombstone check — a soft-deleted nickname_is edge leaves a LIVE
// tombstone (same subject/predicate/object, source_type=user_deletion)
// that this function would otherwise still count as an "explicit
// signal," even though the user explicitly deleted it.
func TestCountExplicitSignalEdges_ExcludesTombstone(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)

	contact := createTestEntity(t, ctx, entityStore, "Tombstone Nickname Contact")

	nickname := "Bestie"
	nicknameEdge := &memory.Edge{
		SubjectID: contact.ID, Predicate: "nickname_is", ObjectLiteral: &nickname,
		Confidence: 1.0, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}
	if err := edgeStore.Create(ctx, nicknameEdge); err != nil {
		t.Fatalf("create nickname edge: %v", err)
	}

	before, err := countExplicitSignalEdges(ctx, edgeStore, contact.ID)
	if err != nil {
		t.Fatalf("countExplicitSignalEdges (before delete): %v", err)
	}
	if before != 1 {
		t.Fatalf("expected 1 explicit signal before deletion, got %d", before)
	}

	if _, err := edgeStore.SoftDelete(ctx, nicknameEdge.ID); err != nil {
		t.Fatalf("SoftDelete: %v", err)
	}

	after, err := countExplicitSignalEdges(ctx, edgeStore, contact.ID)
	if err != nil {
		t.Fatalf("countExplicitSignalEdges (after delete): %v", err)
	}
	if after != 0 {
		t.Errorf("expected 0 explicit signals after deleting the nickname (the tombstone must not count), got %d", after)
	}
}
