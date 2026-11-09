package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Entity-resolution race fix, at the API layer ---
//
// internal/memory/entity_store_test.go already covers the store-level
// primitives (GetOrCreateByCanonicalName/GetOrCreateSelf) directly.
// These two cover the two REAL production callers that compose them —
// resolveSender's self branch, and resolveEntity via the actual write
// pipeline (storeExtractedTriples) — proving the fix holds all the way
// up, not just at the store method in isolation.

// TestResolveSender_ConcurrentSelfResolutionProducesOneEntity covers
// requirement 9: many concurrent resolveSender calls for the same self
// external ID must all resolve to the SAME is_self entity, and exactly
// ONE is_self=true row must exist afterward.
//
// Does not assert "no self entity exists beforehand" or "exactly 1
// goroutine reports creation" the way the store-level concurrency test
// does for ordinary names: entities_single_self_idx permits at most ONE
// is_self=true row in the WHOLE table, ever, and this suite runs
// against a real, persistent, shared dev database other tests in this
// same package may also create a self entity in — the established
// convention this whole suite already lives with (see
// correct_memory_pipeline_test.go's own resolveTestSelfEntity doc
// comment: "a self entity may already exist from an earlier test/run").
// The two assertions below hold regardless of whether THIS test is the
// one that actually creates it: same ID for every concurrent caller,
// and exactly one is_self row total.
func TestResolveSender_ConcurrentSelfResolutionProducesOneEntity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "concurrent-self-" + uuid.New().String()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		SelfExternalIDs: []string{selfExternalID},
	})

	const goroutines = 24
	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make([]uuid.UUID, goroutines)
	errs := make([]error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			e, err := a.resolveSender(ctx, selfExternalID)
			errs[i] = err
			if err == nil {
				ids[i] = e.ID
			}
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, err)
		}
	}

	firstID := ids[0]
	for i, id := range ids {
		if id != firstID {
			t.Fatalf("goroutine %d resolved to self entity %s, want %s (every caller must resolve to the same self entity)", i, id, firstID)
		}
	}

	var selfRowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE is_self = true`).Scan(&selfRowCount); err != nil {
		t.Fatalf("count is_self rows: %v", err)
	}
	if selfRowCount != 1 {
		t.Fatalf("expected exactly 1 is_self=true row across the whole table, got %d", selfRowCount)
	}
}

// TestStoreExtractedTriples_ConcurrentNewSubjectProducesOneEntity
// covers requirement 11: two goroutines, each running storeExtractedTriples
// for a DIFFERENT message but the SAME brand-new subject name, must both
// succeed (no error, each produces its own edge) and resolve to a
// SINGLE subject entity — the real write-pipeline path resolveEntity
// feeds into, not just the store primitive in isolation.
func TestStoreExtractedTriples_ConcurrentNewSubjectProducesOneEntity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	sender := createTestEntity(t, ctx, ts, "person", "Pipeline Race Sender")
	subjectName := "Pipeline Race Subject " + uuid.New().String()

	text1 := "message one"
	msg1 := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text1, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg1); err != nil {
		t.Fatalf("create message 1: %v", err)
	}
	text2 := "message two"
	msg2 := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text", RawText: &text2, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg2); err != nil {
		t.Fatalf("create message 2: %v", err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	var err1, err2 error
	var subjectID1, subjectID2 uuid.UUID

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		triples := []extraction.ExtractedTriple{
			{Subject: subjectName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.9},
		}
		outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg1.ID}, time.Now())
		err1 = err
		if err == nil && len(outcomes) == 1 && outcomes[0].Edge != nil {
			subjectID1 = outcomes[0].Edge.SubjectID
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		triples := []extraction.ExtractedTriple{
			{Subject: subjectName, Predicate: "likes", Object: "sailing", ObjectType: "literal", Confidence: 0.9},
		}
		outcomes, err := a.storeExtractedTriples(ctx, triples, memory.SourceTypeWhatsAppText, []uuid.UUID{msg2.ID}, time.Now())
		err2 = err
		if err == nil && len(outcomes) == 1 && outcomes[0].Edge != nil {
			subjectID2 = outcomes[0].Edge.SubjectID
		}
	}()
	close(start)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("goroutine 1 (storeExtractedTriples): %v", err1)
	}
	if err2 != nil {
		t.Fatalf("goroutine 2 (storeExtractedTriples): %v", err2)
	}
	if subjectID1 == uuid.Nil || subjectID2 == uuid.Nil {
		t.Fatalf("expected both goroutines to produce an edge with a resolved subject, got %s and %s", subjectID1, subjectID2)
	}
	if subjectID1 != subjectID2 {
		t.Fatalf("expected both triples' subject to resolve to the SAME entity, got %s and %s", subjectID1, subjectID2)
	}

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE lower(canonical_name) = lower($1)`, subjectName).Scan(&rowCount); err != nil {
		t.Fatalf("count entities: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 entities row for %q, got %d", subjectName, rowCount)
	}
}
