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

func TestComputeFrequency(t *testing.T) {
	closeConv := uuid.New()
	moderateConv := uuid.New()
	distantConv := uuid.New() // never appears in windowCounts: zero activity this window

	windowCounts := map[uuid.UUID]int{
		closeConv:    40,
		moderateConv: 8,
	}

	cases := []struct {
		name            string
		conversationIDs []uuid.UUID
		windowCounts    map[uuid.UUID]int
		want            float64
	}{
		{"above-average contact", []uuid.UUID{closeConv}, windowCounts, 40.0 / 24.0},
		{"below-average contact", []uuid.UUID{moderateConv}, windowCounts, 8.0 / 24.0},
		{"zero activity in window", []uuid.UUID{distantConv}, windowCounts, 0},
		{"empty population", []uuid.UUID{closeConv}, map[uuid.UUID]int{}, 0},
		{"contact with multiple conversation_ids sums across them", []uuid.UUID{closeConv, moderateConv}, windowCounts, 48.0 / 24.0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := computeFrequency(c.conversationIDs, c.windowCounts)
			if !floatsClose(got, c.want, 1e-9) {
				t.Errorf("computeFrequency() = %v, want %v", got, c.want)
			}
		})
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

	latencies := replyLatenciesSeconds(contact, msgs)
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

	latencies := replyLatenciesSeconds(contact, msgs)
	if len(latencies) != 0 {
		t.Errorf("expected no latencies when there's no preceding inbound message, got %v", latencies)
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
	// RecomputeRelationshipStats never reads IsSelf itself (see its doc
	// comment: "sender_id == contactID" is inbound, anything else in that
	// conversation is Marco — no entity lookup involved), so this field's
	// value doesn't affect what's under test here.
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

	closeStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, closeContact.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(close): %v", err)
	}
	moderateStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, moderateContact.ID, now)
	if err != nil {
		t.Fatalf("RecomputeRelationshipStats(moderate): %v", err)
	}
	distantStats, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, distantContact.ID, now)
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

	first, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, contact.ID, now)
	if err != nil {
		t.Fatalf("first RecomputeRelationshipStats: %v", err)
	}
	second, err := RecomputeRelationshipStats(ctx, statsStore, edgeStore, msgStore, contact.ID, now.Add(time.Minute))
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
