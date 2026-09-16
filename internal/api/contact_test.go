package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func TestGetContact_ComposesEntityStatsAndTopEdges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contact := createTestEntity(t, ctx, ts, "person", "Get Contact Subject")
	lit := func(s string) *string { return &s }
	now := time.Now()

	createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: contact.ID, Predicate: "likes", ObjectLiteral: lit("chess"),
		Confidence: 0.9, Importance: 0.7, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, now)

	stats := &memory.RelationshipStats{ContactID: contact.ID, Frequency: 1.2, ReplySpeed: 300, HumorLevel: 0, Closeness: 0.6}
	if err := ts.relStats.Upsert(ctx, stats); err != nil {
		t.Fatalf("seed relationship stats: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	summary, err := a.GetContact(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if summary.Entity.ID != contact.ID {
		t.Errorf("expected entity %s, got %s", contact.ID, summary.Entity.ID)
	}
	if summary.Stats == nil || summary.Stats.Closeness != 0.6 {
		t.Errorf("expected stats.Closeness = 0.6, got %+v", summary.Stats)
	}
	if len(summary.TopEdges) != 1 || summary.TopEdges[0].Predicate != "likes" {
		t.Errorf("expected 1 top edge (likes/chess), got %+v", summary.TopEdges)
	}
}

func TestGetContact_NoStatsYetIsNotAnError(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contact := createTestEntity(t, ctx, ts, "person", "No Stats Yet Contact")
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	summary, err := a.GetContact(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetContact: %v", err)
	}
	if summary.Stats != nil {
		t.Errorf("expected nil Stats for a contact with no consolidation pass yet, got %+v", summary.Stats)
	}
}

func TestGetContact_RejectsNonPersonEntity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	topic := createTestEntity(t, ctx, ts, "topic", "Not A Person")
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	if _, err := a.GetContact(ctx, topic.ID); !errors.Is(err, ErrNotAPerson) {
		t.Errorf("expected ErrNotAPerson for a topic entity, got %v", err)
	}
}

func TestGetRelationship_NotComputedYet(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contact := createTestEntity(t, ctx, ts, "person", "Relationship Not Computed")
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	if _, err := a.GetRelationship(ctx, contact.ID); !errors.Is(err, ErrRelationshipStatsNotComputed) {
		t.Errorf("expected ErrRelationshipStatsNotComputed, got %v", err)
	}
}

func TestGetRelationship_ReturnsComputedStats(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contact := createTestEntity(t, ctx, ts, "person", "Relationship Computed")
	stats := &memory.RelationshipStats{ContactID: contact.ID, Frequency: 2.0, ReplySpeed: 60, HumorLevel: 0, Closeness: 0.8}
	if err := ts.relStats.Upsert(ctx, stats); err != nil {
		t.Fatalf("seed relationship stats: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	got, err := a.GetRelationship(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if got.Closeness != 0.8 {
		t.Errorf("expected Closeness 0.8, got %v", got.Closeness)
	}
}

// TestGetRelationshipHistory_IncludesSuperseded_ContrastsGetRelevantContext
// covers requirement 11: on the SAME seeded data, GetRelationshipHistory
// includes a superseded edge while GetRelevantContext excludes it — this
// pair is what actually proves the two methods handle supersession
// differently on purpose, not by accident (see EdgeStore.SearchRelevant
// and EdgeStore.GetBySubjectOrObject's own doc comments for the design
// decision this locks in).
func TestGetRelationshipHistory_IncludesSuperseded_ContrastsGetRelevantContext(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contact := createTestEntity(t, ctx, ts, "person", "History Contrast Subject")
	lit := func(s string) *string { return &s }
	now := time.Now()

	survivor := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: contact.ID, Predicate: "works_at", ObjectLiteral: lit("NewCo"),
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now)
	superseded := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: contact.ID, Predicate: "works_at", ObjectLiteral: lit("OldCo"),
		Confidence: 0.9, Importance: 0.6, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now.Add(-30*24*time.Hour))
	superseded.SupersededBy = &survivor.ID
	if err := ts.edges.Update(ctx, superseded); err != nil {
		t.Fatalf("mark superseded: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	history, err := a.GetRelationshipHistory(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetRelationshipHistory: %v", err)
	}
	assertContainsEdge(t, history, survivor.ID, true)
	assertContainsEdge(t, history, superseded.ID, true) // the whole point of this method

	relevant, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &contact.ID}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext: %v", err)
	}
	assertContainsEdge(t, relevant, survivor.ID, true)
	assertContainsEdge(t, relevant, superseded.ID, false) // excluded by default, unlike GetRelationshipHistory
}

// TestGetRelationshipHistory_IncludesObjectSideEdges covers §10.1's
// "contact as object" case: a fact where the contact is the OBJECT, not
// the subject, is still relationship-relevant.
func TestGetRelationshipHistory_IncludesObjectSideEdges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	marco := createTestEntity(t, ctx, ts, "person", "Marco (object-side test)")
	contact := createTestEntity(t, ctx, ts, "person", "Object Side Contact")
	now := time.Now()

	// Marco -> likes -> contact: contact is the OBJECT here, not the
	// subject, and must still show up in their relationship history.
	objectSide := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: marco.ID, Predicate: "likes", ObjectID: &contact.ID,
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, now)

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	history, err := a.GetRelationshipHistory(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetRelationshipHistory: %v", err)
	}
	assertContainsEdge(t, history, objectSide.ID, true)
}
