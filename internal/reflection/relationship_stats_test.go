package reflection

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func floatsClose(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}

// futureTestNow anchors a test's synthetic "now" at a RANDOM point far
// in the future (hundreds to tens of thousands of days out) rather than
// at the real wall-clock time.Now(), purely so the exact-hand-computed-
// value tests for rule F's cross-contact mean
// (TestRecomputeRelationshipStats_GroupChat and its siblings below)
// don't collide with this suite's own shared, never-reset dev database:
// rule F's mean is explicitly computed over EVERY contact with any
// activity in the trailing DefaultRelationshipStatsWindow, globally, by
// design (not scoped to one test's own conversations — see
// RecomputeRelationshipStats' own doc comment) — so a test anchored at
// the real time.Now() would have its expected mean silently polluted by
// whatever other tests, run at any point in this same session, happened
// to insert messages timestamped within the last 28 real days.
// Anchoring in the future sidesteps that without needing to scope or
// mock the query itself: nothing else in this database has ever been
// written with a future timestamp, so a window computed from a future
// "now" is guaranteed to contain only this test's own messages.
//
// RANDOM, not a fixed per-test constant — confirmed necessary, not just
// defensive: an earlier version of this used fixed offsets (one shared
// constant, then one distinct constant per test) and got bitten twice —
// first by every test polluting every OTHER test's population by
// sharing one offset, then, after giving each test its own fixed
// offset, by a PREVIOUS run of this same test file (during this fix's
// own development) leaving behind rows at that same fixed offset, which
// the NEXT run then collided with. A random offset avoids both: the
// draw space (hundreds of years) makes any two draws landing within
// DefaultRelationshipStatsWindow (28 days) of each other astronomically
// unlikely, across tests in one run AND across repeated runs over the
// life of this database — the same reasoning internal/api's
// randomBasisIndex already established for an analogous shared-DB
// collision risk.
//
// Confirmed safe to use future timestamps at all: MessageStore.Create
// has no future-timestamp clamp (that only exists in internal/api's
// write pipeline, for edges' FirstSeen/LastReinforced — see
// clampFutureTimestamp's own doc comment — and these tests write
// messages directly via msgStore.Create, never through that pipeline).
func futureTestNow(t *testing.T) time.Time {
	t.Helper()
	days := 400 + rand.Intn(100000)
	return time.Now().Add(time.Duration(days) * 24 * time.Hour)
}

// cleanupFutureTestMessages registers a t.Cleanup that deletes every
// message in conversationIDs once the test finishes — belt-and-braces
// alongside futureTestNow's own random draw: confirmed necessary, not
// just defensive, after this fix's own development left over a
// thousand future-dated rows from earlier iterations of these same
// tests, which then collided with later runs' own random draws (the
// draw space is huge, but not infinite, and debris that never gets
// cleaned up only grows across however many times this suite runs over
// this database's lifetime). Deleting by conversation_id, not by a
// timestamp range: safe even if a real future-dated fixture is ever
// legitimately added elsewhere, since this only ever touches the exact
// conversations this test itself created.
func cleanupFutureTestMessages(t *testing.T, pool *pgxpool.Pool, conversationIDs ...uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM messages WHERE conversation_id = ANY($1)`, conversationIDs); err != nil {
			t.Logf("cleanupFutureTestMessages: %v", err)
		}
	})
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
			closeConv:    {self: 20, closeContact: 20},  // total 40
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
			dmConv:    {self: 6, david: 6},          // total 12
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
		{SenderID: bob, Timestamp: base},                                      // inbound from Bob
		{SenderID: self, Timestamp: base.Add(5 * time.Minute)},                // reply to Bob: latency 300s
		{SenderID: alice, Timestamp: base.Add(6 * time.Minute)},               // inbound from Alice — updates "most recent inbound"
		{SenderID: self, Timestamp: base.Add(7 * time.Minute)},                // reply to Alice: latency 60s
		{SenderID: bob, Timestamp: base.Add(8 * time.Minute)},                 // inbound from Bob again
		{SenderID: self, Timestamp: base.Add(8*time.Minute + 40*time.Minute)}, // reply to Bob, but 40min > the 30min cap -> excluded
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

	// IsSelf: false (avoids entities_single_self_idx colliding across
	// tests) and a uuid-suffixed name (avoids entities_canonical_name_lower_uidx
	// colliding — the entity-resolution race fix, migration 000008 —
	// since this bypasses the createTestEntity helper that otherwise
	// handles that automatically).
	self := &memory.Entity{Type: "person", CanonicalName: "Marco Two " + uuid.New().String(), IsSelf: false}
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

// TestRecomputeRelationshipStats_MissingSelfEntityReturnsError covers
// requirement 11: selfID == uuid.Nil (the caller found no is_self
// entity) must return a clear error, never fall back to guessing. No
// database needed — the check happens before any store call.
func TestRecomputeRelationshipStats_MissingSelfEntityReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := RecomputeRelationshipStats(ctx, nil, nil, nil, uuid.Nil, uuid.New(), time.Now())
	if err == nil {
		t.Fatal("expected an error when selfID is uuid.Nil (no self entity configured)")
	}
}

// TestRecomputeRelationshipStats_GroupChat covers requirement 7: a
// 3+-person group chat (Marco, Alice, Bob, Carol), with every claim
// hand-computed exactly rather than just checked for ordering, since the
// whole point of this fix is that specific numbers used to be wrong.
//
//   - Alice never sends a message within the trailing window (her one
//     message is 30 days old, outside DefaultRelationshipStatsWindow) —
//     frequency reflects that (0), not the group's general activity.
//     Before this fix, her conversationIDs would include this group
//     conversation and she'd be credited with the WHOLE window's volume
//     (Marco's and Bob's messages too) despite having said nothing
//     herself this window.
//   - Bob sends 5 messages within the window: frequency reflects ONLY
//     his own count (scaled by groupActivityWeight), and since he's the
//     only contact with any attributed activity this window, the
//     cross-contact mean collapses to exactly his own value, giving
//     frequency == 1.0 exactly.
//   - Bob speaks, Marco replies (4 pairs, each fast; this is the exact
//     repro of the bug: before this fix, that reply counted toward
//     EVERY contact whose conversationIDs included this group, Alice
//     included) — asserted NOT counted toward Alice's reply speed
//     (hers is the no-pairs sentinel) and IS counted toward Bob's
//     (median of the 4 fast pairs).
//   - A 5th Bob/Marco pair exceeds groupReplyMaxLatency and is excluded
//     from Bob's median.
//   - Carol never sends anything at all — frequency 0, reply speed the
//     sentinel.
//   - Recency: rule E — Alice's OWN last message (30 days ago) drives
//     her closeness's recency component, not the group's overall last
//     message (which is effectively "yesterday," via Bob). Asserted via
//     an exact hand-computed Closeness value: if the bug's group-wide
//     recency were still in effect, Alice's Closeness would be far
//     higher than this.
func TestRecomputeRelationshipStats_GroupChat(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	self := createTestEntity(t, ctx, entityStore, "Group Chat Marco "+uuid.New().String())
	alice := createTestEntity(t, ctx, entityStore, "Group Chat Alice "+uuid.New().String())
	bob := createTestEntity(t, ctx, entityStore, "Group Chat Bob "+uuid.New().String())
	carol := createTestEntity(t, ctx, entityStore, "Group Chat Carol "+uuid.New().String())

	now := futureTestNow(t)
	groupConv := uuid.New()
	cleanupFutureTestMessages(t, pool, groupConv)

	// Alice's only message: 30 days ago, outside the 28-day window.
	aliceMessageAt := now.Add(-30 * 24 * time.Hour)
	createTestMessage(t, ctx, msgStore, groupConv, alice.ID, aliceMessageAt)

	// Bob: 4 fast reply pairs (within groupReplyMaxLatency), then a 5th
	// pair whose reply is too slow — all within the window. Bob's LAST
	// message (the final pair's inbound) lands ~1 day ago, so the group
	// (via Bob) is "active yesterday" while Alice has been silent for 30
	// days.
	for i := 3; i >= 0; i-- {
		bobAt := now.Add(-time.Duration(2+i) * 24 * time.Hour)
		createTestMessage(t, ctx, msgStore, groupConv, bob.ID, bobAt)
		createTestMessage(t, ctx, msgStore, groupConv, self.ID, bobAt.Add(3*time.Minute))
	}
	slowBobAt := now.Add(-1 * 24 * time.Hour)
	createTestMessage(t, ctx, msgStore, groupConv, bob.ID, slowBobAt)
	createTestMessage(t, ctx, msgStore, groupConv, self.ID, slowBobAt.Add(40*time.Minute))

	// Carol never sends anything — present in entities only, like a real
	// group member who never happens to speak.
	_ = carol

	aliceStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, alice.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(alice): %v", err)
	}
	bobStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, bob.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(bob): %v", err)
	}
	carolStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, carol.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(carol): %v", err)
	}

	// --- Frequency: only Bob has any attributed activity this window,
	// so the cross-contact mean collapses to exactly his own value. ---
	if !floatsClose(aliceStats.Frequency, 0, 1e-9) {
		t.Errorf("alice: expected frequency 0 (silent this window), got %v — the bug would credit her with the group's general activity instead", aliceStats.Frequency)
	}
	if !floatsClose(bobStats.Frequency, 1.0, 1e-9) {
		t.Errorf("bob: expected frequency exactly 1.0 (sole active contact this window), got %v", bobStats.Frequency)
	}
	if !floatsClose(carolStats.Frequency, 0, 1e-9) {
		t.Errorf("carol: expected frequency 0 (never spoke), got %v", carolStats.Frequency)
	}

	// --- Reply speed: Bob's 4 fast pairs, never counted toward Alice. ---
	if aliceStats.ReplySpeed != noReplyPairsSentinel {
		t.Errorf("alice: expected the no-reply-pairs sentinel (Bob's replies must not count toward her), got %v", aliceStats.ReplySpeed)
	}
	if !floatsClose(bobStats.ReplySpeed, 180, 1e-9) {
		t.Errorf("bob: expected median reply latency 180s (4 fast pairs), got %v — the slow 5th pair must be excluded by groupReplyMaxLatency, not just averaged in", bobStats.ReplySpeed)
	}
	if carolStats.ReplySpeed != noReplyPairsSentinel {
		t.Errorf("carol: expected the no-reply-pairs sentinel (silent), got %v", carolStats.ReplySpeed)
	}

	// --- Recency (rule E), via an exact hand-computed Closeness: Alice's
	// OWN last message (30 days ago) must drive her recency component,
	// not the group's overall last message (~1 day ago, via Bob). ---
	wantAliceCloseness := weightRecency * math.Exp(-30.0/recencyHalfLifeDays)
	if !floatsClose(aliceStats.Closeness, wantAliceCloseness, 1e-6) {
		t.Errorf("alice: expected closeness %v (recency from her own 30-day-old message), got %v — the bug would use the group's ~1-day-old recency instead, scoring her far higher", wantAliceCloseness, aliceStats.Closeness)
	}
}

// TestRecomputeRelationshipStats_MixedOneOnOneAndGroup covers requirement
// 8: a contact (David) with both a 1:1 DM and a group — attributed
// activity is the DM's total plus groupActivityWeight times his own
// group message count, hand-computed exactly.
func TestRecomputeRelationshipStats_MixedOneOnOneAndGroup(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	self := createTestEntity(t, ctx, entityStore, "Mixed Marco "+uuid.New().String())
	david := createTestEntity(t, ctx, entityStore, "Mixed David "+uuid.New().String())
	eve := createTestEntity(t, ctx, entityStore, "Mixed Eve "+uuid.New().String())

	now := futureTestNow(t)
	dmConv := uuid.New()
	groupConv := uuid.New()
	cleanupFutureTestMessages(t, pool, dmConv, groupConv)

	// David's 1:1 DM: 6 messages each direction (total 12), all within
	// the window.
	for i := 0; i < 6; i++ {
		at := now.Add(-time.Duration(i+1) * time.Hour)
		createTestMessage(t, ctx, msgStore, dmConv, david.ID, at)
		createTestMessage(t, ctx, msgStore, dmConv, self.ID, at.Add(time.Minute))
	}

	// A group David and Eve are both in: David sends 8, Eve sends 4,
	// Marco sends some too (never credited to either).
	for i := 0; i < 8; i++ {
		createTestMessage(t, ctx, msgStore, groupConv, david.ID, now.Add(-time.Duration(i+1)*time.Hour))
	}
	for i := 0; i < 4; i++ {
		createTestMessage(t, ctx, msgStore, groupConv, eve.ID, now.Add(-time.Duration(i+1)*time.Hour))
	}
	for i := 0; i < 3; i++ {
		createTestMessage(t, ctx, msgStore, groupConv, self.ID, now.Add(-time.Duration(i+1)*time.Hour))
	}

	davidStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, david.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(david): %v", err)
	}

	// attributed(david) = 12 (DM total) + 0.25*8 (own group messages) = 14.
	// attributed(eve) = 0.25*4 = 1.
	// mean = (14+1)/2 = 7.5.
	want := 14.0 / 7.5
	if !floatsClose(davidStats.Frequency, want, 1e-9) {
		t.Errorf("david: expected frequency %v (DM total + weighted own group messages), got %v", want, davidStats.Frequency)
	}
}

// TestRecomputeRelationshipStats_HighVolumeGroupNotMember covers
// requirement 9: a high-volume group the contact is not in must not
// change their 1:1 frequency beyond what rule F's mean predicts — the
// group's members dilute the cross-contact mean (as they should, being
// genuinely active contacts themselves), but never touch the contact's
// own numerator.
func TestRecomputeRelationshipStats_HighVolumeGroupNotMember(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	self := createTestEntity(t, ctx, entityStore, "HighVol Marco "+uuid.New().String())
	frank := createTestEntity(t, ctx, entityStore, "HighVol Frank "+uuid.New().String())
	grace := createTestEntity(t, ctx, entityStore, "HighVol Grace "+uuid.New().String())
	henry := createTestEntity(t, ctx, entityStore, "HighVol Henry "+uuid.New().String())

	now := futureTestNow(t)
	frankConv := uuid.New()
	busyGroupConv := uuid.New()
	cleanupFutureTestMessages(t, pool, frankConv, busyGroupConv)

	// Frank: an ordinary 1:1 DM, total window count 20.
	for i := 0; i < 10; i++ {
		at := now.Add(-time.Duration(i+1) * time.Hour)
		createTestMessage(t, ctx, msgStore, frankConv, frank.ID, at)
		createTestMessage(t, ctx, msgStore, frankConv, self.ID, at.Add(time.Minute))
	}

	// A high-volume group Frank is NOT a member of at all: Grace and
	// Henry, 100 messages each within the window.
	for i := 0; i < 100; i++ {
		createTestMessage(t, ctx, msgStore, busyGroupConv, grace.ID, now.Add(-time.Duration(i+1)*time.Minute))
		createTestMessage(t, ctx, msgStore, busyGroupConv, henry.ID, now.Add(-time.Duration(i+1)*time.Minute))
	}

	frankStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, frank.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(frank): %v", err)
	}

	// attributed(frank) = 20. attributed(grace) = 0.25*100 = 25.
	// attributed(henry) = 0.25*100 = 25. mean = (20+25+25)/3 = 70/3.
	want := 20.0 / (70.0 / 3.0)
	if !floatsClose(frankStats.Frequency, want, 1e-9) {
		t.Errorf("frank: expected frequency %v (unchanged numerator, mean diluted by the group's own real members), got %v", want, frankStats.Frequency)
	}
}

// TestRecomputeRelationshipStats_GroupClassifiedByAllTimeNotWindow
// covers requirement 10: a conversation with 2 distinct non-self senders
// ALL-TIME, but only 1 of them active THIS window, must still classify
// as a group — not fall back to 1:1 behavior just because the window is
// quiet for everyone but one member.
//
// Bob's only message is 60 days old (outside the window) — just enough
// to make this conversation a group by rule A's all-time rule, without
// contributing anything to this window's counts. Alice is the only
// member active this window. A second, genuinely-1:1 contact (Zoe) gives
// the cross-contact population 2 members, so a misclassification (this
// conversation treated as 1:1 this window, crediting Alice with its
// WHOLE window volume instead of just her own, scaled) would produce a
// clearly different, wrong frequency value rather than one that
// happens to coincide by accident.
func TestRecomputeRelationshipStats_GroupClassifiedByAllTimeNotWindow(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := memory.NewEntityStore(pool)
	edgeStore := memory.NewEdgeStore(pool)
	msgStore := memory.NewMessageStore(pool)
	statsStore := memory.NewRelationshipStatsStore(pool)

	self := createTestEntity(t, ctx, entityStore, "AllTime Marco "+uuid.New().String())
	alice := createTestEntity(t, ctx, entityStore, "AllTime Alice "+uuid.New().String())
	bob := createTestEntity(t, ctx, entityStore, "AllTime Bob "+uuid.New().String())
	zoe := createTestEntity(t, ctx, entityStore, "AllTime Zoe "+uuid.New().String())

	now := futureTestNow(t)
	groupConv := uuid.New()
	zoeConv := uuid.New()
	cleanupFutureTestMessages(t, pool, groupConv, zoeConv)

	// Bob's one message: 60 days ago, well outside the window — present
	// only so this conversation has 2 distinct non-self senders all-time.
	createTestMessage(t, ctx, msgStore, groupConv, bob.ID, now.Add(-60*24*time.Hour))
	// Alice: 4 messages this window, each followed by a Marco reply (6
	// Marco messages total — more than Alice's own, so a 1:1
	// misclassification and a correct group classification would produce
	// visibly different numbers).
	for i := 0; i < 4; i++ {
		createTestMessage(t, ctx, msgStore, groupConv, alice.ID, now.Add(-time.Duration(i+1)*time.Hour))
	}
	for i := 0; i < 6; i++ {
		createTestMessage(t, ctx, msgStore, groupConv, self.ID, now.Add(-time.Duration(i+1)*time.Minute))
	}

	// Zoe: an ordinary 1:1 DM, total window count 5.
	createTestMessage(t, ctx, msgStore, zoeConv, zoe.ID, now.Add(-30*time.Minute))
	for i := 0; i < 4; i++ {
		createTestMessage(t, ctx, msgStore, zoeConv, self.ID, now.Add(-time.Duration(i+1)*time.Minute))
	}

	aliceStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, self.ID, alice.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(alice): %v", err)
	}

	// Correct (group): attributed(alice) = 0.25*4 = 1. attributed(zoe) = 5.
	// mean = 6/2 = 3. frequency = 1/3.
	// If misclassified as 1:1, attributed(alice) would instead be the
	// WHOLE window total (4+6=10), giving a visibly different frequency
	// (10/7.5 = 1.333...) — asserting the correct value rules that out.
	want := 1.0 / 3.0
	if !floatsClose(aliceStats.Frequency, want, 1e-9) {
		t.Errorf("alice: expected frequency %v (classified as a group by all-time participant count), got %v — a window-based classification would misclassify this as 1:1 and produce a different value", want, aliceStats.Frequency)
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
