package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func TestSearchMemory_MatchesByKeyword(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Keyword Search Subject")
	lit := func(s string) *string { return &s }
	now := time.Now()

	matching := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "goal_is", ObjectLiteral: lit("run a marathon in Beirut"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now)
	nonMatching := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: lit("cooking"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable,
	}, now)

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	results, err := a.SearchMemory(ctx, "marathon", Filters{SubjectID: &subject.ID})
	if err != nil {
		t.Fatalf("SearchMemory: %v", err)
	}
	assertContainsEdge(t, results, matching.ID, true)
	assertContainsEdge(t, results, nonMatching.ID, false)
}

func TestSearchMemory_ExcludesSupersededEdges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Keyword Superseded Subject")
	lit := func(s string) *string { return &s }
	now := time.Now()

	survivor := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: lit("Acme"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now)
	superseded := createTestEdge(t, ctx, ts, &memory.Edge{
		SubjectID: subject.ID, Predicate: "works_at", ObjectLiteral: lit("OldCo"),
		Confidence: 0.9, Importance: 0.5, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateSlow,
	}, now)
	superseded.SupersededBy = &survivor.ID
	if err := ts.edges.Update(ctx, superseded); err != nil {
		t.Fatalf("mark superseded: %v", err)
	}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	results, err := a.SearchMemory(ctx, "works_at", Filters{SubjectID: &subject.ID})
	if err != nil {
		t.Fatalf("SearchMemory: %v", err)
	}
	assertContainsEdge(t, results, survivor.ID, true)
	assertContainsEdge(t, results, superseded.ID, false)
}

func TestSearchSemantic_ReturnsNearestMessages(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Semantic Search Sender")
	now := time.Now()

	queryIndex := randomBasisIndex()
	farIndex := (queryIndex + 1) % embeddingDim

	near := createTestMessage(t, ctx, ts, uuid.New(), sender.ID, "text", now)
	createTestEmbedding(t, ctx, ts, near.ID, nearVector(queryIndex))

	far := createTestMessage(t, ctx, ts, uuid.New(), sender.ID, "text", now)
	createTestEmbedding(t, ctx, ts, far.ID, basisVector(farIndex, 1))

	embedder := &fakeEmbedder{vector: basisVector(queryIndex, 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	results, err := a.SearchSemantic(ctx, "some query", 1)
	if err != nil {
		t.Fatalf("SearchSemantic: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (topN=1), got %d", len(results))
	}
	if results[0].ID != near.ID {
		t.Errorf("expected the nearest message (%s) first, got %s", near.ID, results[0].ID)
	}
	_ = far
}

func TestSearchSemantic_NoEmbedderReturnsError(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	if _, err := a.SearchSemantic(ctx, "anything", 5); err == nil {
		t.Error("expected an error when SearchSemantic is called with no embedder configured")
	}
}
