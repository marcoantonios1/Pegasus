package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

func TestRegressionCheck_UpdateMemoryConcurrentWithReinforce(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	subject := createTestEntity(t, ctx, ts, "person", "Concurrent UpdateMemory Subject")
	lit := "concurrent update test"
	originalMsgIDs := []uuid.UUID{uuid.New(), uuid.New()}
	edge := &memory.Edge{
		SubjectID: subject.ID, Predicate: "likes", ObjectLiteral: &lit,
		Confidence: 0.8, Importance: 0.1, SourceType: "whatsapp_text", SourceWeight: 1.0,
		DecayRate: memory.DecayRateStable, SourceMessageIDs: originalMsgIDs,
	}
	if err := ts.edges.Create(ctx, edge); err != nil {
		t.Fatalf("create edge: %v", err)
	}

	const writers = 8
	const perWriter = 25 // 200 total each side
	base := time.Now()
	maxTimestamp := base.Add(time.Duration(writers*perWriter-1) * time.Second)

	var wg sync.WaitGroup
	wg.Add(2 * writers)

	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				importance := float64((w*perWriter+i)%100) / 100.0
				if _, err := a.UpdateMemory(ctx, edge.ID, UpdateMemoryFields{Importance: &importance}); err != nil {
					t.Errorf("UpdateMemory: %v", err)
					return
				}
			}
		}()
	}
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				seq := w*perWriter + i
				at := base.Add(time.Duration(seq) * time.Second)
				if err := ts.edges.Reinforce(ctx, edge.ID, at, at); err != nil {
					t.Errorf("Reinforce: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()

	reloaded, err := ts.edges.GetByID(ctx, edge.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}

	if !reloaded.LastReinforced.Equal(maxTimestamp) {
		t.Errorf("expected last_reinforced == max issued timestamp %v, got %v (a concurrent UpdateMemory clobbered it with a stale snapshot)", maxTimestamp, reloaded.LastReinforced)
	}
}
