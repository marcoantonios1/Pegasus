package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// TestWhyDoWeBelieveThis_BreakdownMatchesExactly covers requirement 12:
// an edge with known source_message_ids from multiple senders/media
// types, asserting the breakdown matches expected counts exactly.
func TestWhyDoWeBelieveThis_BreakdownMatchesExactly(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Why Test Subject")
	marco := createTestEntity(t, ctx, ts, "person", "Why Test Marco")
	friend := createTestEntity(t, ctx, ts, "person", "Why Test Friend")
	conv := uuid.New()

	day1 := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 3, 3, 18, 30, 0, 0, time.UTC)

	// 4 messages from marco: 3 text on day1, 1 text later same day1
	// (still one distinct date), 1 voice on day2. Plus 1 text from friend
	// on day2. Deliberately uneven counts so "matches exactly" is a real
	// assertion, not a coincidence of symmetric data.
	m1 := createTestMessage(t, ctx, ts, conv, marco.ID, "text", day1)
	m2 := createTestMessage(t, ctx, ts, conv, marco.ID, "text", day1.Add(time.Hour))
	m3 := createTestMessage(t, ctx, ts, conv, marco.ID, "text", day1.Add(2*time.Hour))
	m4 := createTestMessage(t, ctx, ts, conv, marco.ID, "voice", day2)
	m5 := createTestMessage(t, ctx, ts, conv, friend.ID, "text", day2.Add(time.Hour))

	lit := "goes to the gym regularly"
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "routine_is", ObjectLiteral: &lit,
		Confidence: 0.72, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 0.9,
		DecayRate:        memory.DecayRateSlow,
		SourceMessageIDs: []uuid.UUID{m1.ID, m2.ID, m3.ID, m4.ID, m5.ID},
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	belief, err := a.WhyDoWeBelieveThis(ctx, edge.ID)
	if err != nil {
		t.Fatalf("WhyDoWeBelieveThis: %v", err)
	}

	if belief.Confidence != 0.72 {
		t.Errorf("expected raw Confidence 0.72, got %v", belief.Confidence)
	}
	if belief.SourceWeight != 0.9 {
		t.Errorf("expected SourceWeight 0.9, got %v", belief.SourceWeight)
	}
	wantReconstructed := 0.72 / 0.9
	if diff := belief.ExtractedConfidenceReconstructed - wantReconstructed; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("expected ExtractedConfidenceReconstructed = %v, got %v", wantReconstructed, belief.ExtractedConfidenceReconstructed)
	}
	if belief.Importance != 0.6 {
		t.Errorf("expected Importance 0.6, got %v", belief.Importance)
	}
	if belief.EdgeSourceType != "whatsapp_text" {
		t.Errorf("expected EdgeSourceType %q, got %q", "whatsapp_text", belief.EdgeSourceType)
	}
	if belief.IsCorrection {
		t.Error("expected IsCorrection false")
	}
	if belief.SupersededBy != nil || belief.SupersededByLabel != "none" {
		t.Errorf("expected no supersession, got SupersededBy=%v label=%q", belief.SupersededBy, belief.SupersededByLabel)
	}

	// Seen: exactly 2 distinct dates.
	if len(belief.Seen) != 2 {
		t.Fatalf("expected 2 distinct seen dates, got %d: %v", len(belief.Seen), belief.Seen)
	}
	wantDay1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	wantDay2 := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)
	if !belief.Seen[0].Equal(wantDay1) || !belief.Seen[1].Equal(wantDay2) {
		t.Errorf("expected seen dates [%v, %v], got %v", wantDay1, wantDay2, belief.Seen)
	}

	// Mentioned by: marco x4, friend x1.
	mentionedByCount := map[string]int{}
	for _, s := range belief.MentionedBy {
		mentionedByCount[s.CanonicalName] = s.Count
	}
	if mentionedByCount["Why Test Marco"] != 4 {
		t.Errorf("expected marco mentioned 4 times, got %d", mentionedByCount["Why Test Marco"])
	}
	if mentionedByCount["Why Test Friend"] != 1 {
		t.Errorf("expected friend mentioned 1 time, got %d", mentionedByCount["Why Test Friend"])
	}

	// Source types (media_type breakdown): text x4, voice x1.
	mediaCount := map[string]int{}
	for _, s := range belief.SourceTypes {
		mediaCount[s.MediaType] = s.Count
	}
	if mediaCount["text"] != 4 {
		t.Errorf("expected 4 text messages, got %d", mediaCount["text"])
	}
	if mediaCount["voice"] != 1 {
		t.Errorf("expected 1 voice message, got %d", mediaCount["voice"])
	}
}

func TestWhyDoWeBelieveThis_SupersededByResolvesToLabel(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Superseded Why Subject")
	now := time.Now()
	lit := func(s string) *string { return &s }

	oldEdge := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: lit("OldCo"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now.Add(-10*24*time.Hour))
	newEdge := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: lit("NewCo"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now)
	oldEdge.SupersededBy = &newEdge.ID
	if err := ts.edges.Update(ctx, oldEdge); err != nil {
		t.Fatalf("mark superseded: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	belief, err := a.WhyDoWeBelieveThis(ctx, oldEdge.ID)
	if err != nil {
		t.Fatalf("WhyDoWeBelieveThis: %v", err)
	}
	if belief.SupersededBy == nil || *belief.SupersededBy != newEdge.ID {
		t.Fatalf("expected SupersededBy %s, got %v", newEdge.ID, belief.SupersededBy)
	}
	if belief.SupersededByLabel != "works_at: NewCo" {
		t.Errorf("expected SupersededByLabel %q, got %q", "works_at: NewCo", belief.SupersededByLabel)
	}
}
