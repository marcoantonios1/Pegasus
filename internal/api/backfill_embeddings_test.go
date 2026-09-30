package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Requirement 8: mixed already-embedded / missing / no-text messages ---

// TestBackfillEmbeddings_OnlyEmbedsGenuinelyMissingMessages covers
// requirement 8: seed a mix of messages — one already embedded (direct
// EmbeddingStore.Create, simulating pre-existing data), one with no
// embedding, and one with no embeddable text — and assert the backfill
// only creates a NEW embedding for the genuinely-missing one, leaves the
// already-embedded one untouched (no duplicate row, same vector), and
// skips the no-text one without counting it as a failure.
func TestBackfillEmbeddings_OnlyEmbedsGenuinelyMissingMessages(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Backfill Sender "+uuid.New().String())
	conversationID := uuid.New()

	alreadyText := "already embedded before this backfill ran"
	alreadyEmbeddedMsg := &memory.Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &alreadyText, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, alreadyEmbeddedMsg); err != nil {
		t.Fatalf("create already-embedded message: %v", err)
	}
	preExistingVector := basisVector(randomBasisIndex(), 1)
	createTestEmbedding(t, ctx, ts, alreadyEmbeddedMsg.ID, preExistingVector)

	missingText := "never embedded, this is what the backfill should catch"
	missingMsg := &memory.Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &missingText, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, missingMsg); err != nil {
		t.Fatalf("create missing-embedding message: %v", err)
	}

	// A voice message with no transcript — messageText returns ok=false
	// for this, the same "nothing to embed, not an error" case
	// embedMessages already handles.
	noTextMsg := &memory.Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "voice",
		Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, noTextMsg); err != nil {
		t.Fatalf("create no-text message: %v", err)
	}

	newVector := basisVector(randomBasisIndex(), 1)
	embedder := &fakeEmbedder{vector: newVector}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	unembedded, err := ts.messages.ListWithoutEmbeddings(ctx)
	if err != nil {
		t.Fatalf("ListWithoutEmbeddings: %v", err)
	}
	unembeddedIDs := make(map[uuid.UUID]bool, len(unembedded))
	for _, m := range unembedded {
		unembeddedIDs[m.ID] = true
	}
	if unembeddedIDs[alreadyEmbeddedMsg.ID] {
		t.Error("expected ListWithoutEmbeddings to exclude the already-embedded message")
	}
	if !unembeddedIDs[missingMsg.ID] {
		t.Error("expected ListWithoutEmbeddings to include the missing-embedding message")
	}
	if !unembeddedIDs[noTextMsg.ID] {
		t.Error("expected ListWithoutEmbeddings to include the no-text message (it's still unembedded — text availability isn't this query's concern)")
	}

	ids := make([]uuid.UUID, len(unembedded))
	for i, m := range unembedded {
		ids[i] = m.ID
	}
	result, err := a.BackfillEmbeddings(ctx, ids)
	if err != nil {
		t.Fatalf("BackfillEmbeddings: %v", err)
	}

	if result.Embedded != 1 {
		t.Errorf("expected exactly 1 newly-embedded message, got %d", result.Embedded)
	}
	if result.SkippedNoText != 1 {
		t.Errorf("expected exactly 1 skipped-no-text message, got %d", result.SkippedNoText)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failures, got %d", result.Failed)
	}

	// The genuinely-missing message now has an embedding with the
	// embedder's vector.
	newEmb, err := ts.embeds.GetByMessageID(ctx, missingMsg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID(missingMsg): %v", err)
	}
	if newEmb == nil {
		t.Fatal("expected the missing-embedding message to now have an embedding")
	}
	if newEmb.Vector[0] != newVector[0] {
		t.Errorf("expected the new embedding to use the embedder's vector, got a different one")
	}

	// The already-embedded message's row is untouched — same vector,
	// still exactly one row.
	reloaded, err := ts.embeds.GetByMessageID(ctx, alreadyEmbeddedMsg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID(alreadyEmbeddedMsg): %v", err)
	}
	if reloaded == nil {
		t.Fatal("expected the already-embedded message to still have its embedding")
	}
	if reloaded.Vector[0] != preExistingVector[0] {
		t.Errorf("expected the pre-existing embedding's vector unchanged, got a different one — it was overwritten")
	}
	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, alreadyEmbeddedMsg.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count embeddings for alreadyEmbeddedMsg: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 embeddings row for the already-embedded message, got %d (duplicated)", rowCount)
	}

	// The no-text message still has no embedding — nothing to embed it
	// with, and that's expected, not a failure.
	noTextEmb, err := ts.embeds.GetByMessageID(ctx, noTextMsg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID(noTextMsg): %v", err)
	}
	if noTextEmb != nil {
		t.Error("expected the no-text message to remain unembedded")
	}
}

// --- Requirement 9: idempotency across two full runs ---

// TestBackfillEmbeddings_IdempotentAcrossTwoRuns covers requirement 9
// directly: running the backfill twice against the same seeded data must
// embed the missing messages on the first run, then embed ZERO additional
// messages on the second (everything now already has a row), without
// erroring either time.
func TestBackfillEmbeddings_IdempotentAcrossTwoRuns(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Idempotent Backfill Sender "+uuid.New().String())
	conversationID := uuid.New()

	var seededIDs []uuid.UUID
	for i := 0; i < 3; i++ {
		text := fmt.Sprintf("idempotency test message %d", i)
		msg := &memory.Message{
			ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
			RawText: &text, Timestamp: time.Now(),
		}
		if err := ts.messages.Create(ctx, msg); err != nil {
			t.Fatalf("create seeded message %d: %v", i, err)
		}
		seededIDs = append(seededIDs, msg.ID)
	}

	embedder := &fakeEmbedder{vector: basisVector(randomBasisIndex(), 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	firstUnembedded, err := ts.messages.ListWithoutEmbeddings(ctx)
	if err != nil {
		t.Fatalf("ListWithoutEmbeddings (first): %v", err)
	}
	firstIDs := idsOf(firstUnembedded, seededIDs)

	firstResult, err := a.BackfillEmbeddings(ctx, firstIDs)
	if err != nil {
		t.Fatalf("BackfillEmbeddings (first run): %v", err)
	}
	if firstResult.Embedded != len(firstIDs) {
		t.Fatalf("expected the first run to embed all %d seeded messages, got %d", len(firstIDs), firstResult.Embedded)
	}

	// Second run: query again (mirrors cmd/backfill_embeddings' own
	// behavior of re-querying ListWithoutEmbeddings on every invocation,
	// not reusing the first run's ID list) — none of the seeded messages
	// should come back this time.
	secondUnembedded, err := ts.messages.ListWithoutEmbeddings(ctx)
	if err != nil {
		t.Fatalf("ListWithoutEmbeddings (second): %v", err)
	}
	for _, m := range secondUnembedded {
		for _, id := range seededIDs {
			if m.ID == id {
				t.Errorf("expected seeded message %s to be excluded from ListWithoutEmbeddings after the first backfill run", id)
			}
		}
	}

	// Also directly re-run BackfillEmbeddings against the SAME ID list
	// the first run used — the literal idempotency claim: calling it
	// again with messages that already have embeddings must embed zero
	// additional ones and not error.
	secondResult, err := a.BackfillEmbeddings(ctx, firstIDs)
	if err != nil {
		t.Fatalf("BackfillEmbeddings (second run, same IDs): %v", err)
	}
	if secondResult.Embedded != 0 {
		t.Errorf("expected the second run to embed 0 additional messages, got %d", secondResult.Embedded)
	}
	if secondResult.AlreadyEmbedded != len(firstIDs) {
		t.Errorf("expected the second run to report all %d messages as already embedded, got %d", len(firstIDs), secondResult.AlreadyEmbedded)
	}

	for _, id := range seededIDs {
		var rowCount int
		if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, id).Scan(&rowCount); err != nil {
			t.Fatalf("count embeddings for %s: %v", id, err)
		}
		if rowCount != 1 {
			t.Errorf("expected exactly 1 embeddings row for message %s after two runs, got %d", id, rowCount)
		}
	}
}

// idsOf returns the subset of msgs' IDs that appear in wantIDs — used to
// scope a real ListWithoutEmbeddings query result (which, against this
// suite's shared persistent DB, may include leftover rows from other
// tests) down to just the IDs this specific test seeded.
func idsOf(msgs []*memory.Message, wantIDs []uuid.UUID) []uuid.UUID {
	want := make(map[uuid.UUID]bool, len(wantIDs))
	for _, id := range wantIDs {
		want[id] = true
	}
	var out []uuid.UUID
	for _, m := range msgs {
		if want[m.ID] {
			out = append(out, m.ID)
		}
	}
	return out
}

// --- Requirement 10: failure logging, log-and-continue ---

// TestBackfillEmbeddings_FailureIsLoggedAndRunContinues covers
// requirement 10: an embedder that fails for one message must be logged
// with that message's ID, and — critically — must not abort the whole
// backfill call (log-and-continue, the same behavior embedMessages
// itself already guarantees; this backfill must not regress to
// fail-fast).
func TestBackfillEmbeddings_FailureIsLoggedAndRunContinues(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Backfill Failure Sender "+uuid.New().String())
	conversationID := uuid.New()

	failText := "this message's embedding call will fail"
	failMsg := &memory.Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &failText, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, failMsg); err != nil {
		t.Fatalf("create fail message: %v", err)
	}

	okText := "this message's embedding call will succeed"
	okMsg := &memory.Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &okText, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, okMsg); err != nil {
		t.Fatalf("create ok message: %v", err)
	}

	embedder := &fakeEmbedder{err: fmt.Errorf("simulated costguard embeddings outage")}
	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	result, err := a.BackfillEmbeddings(ctx, []uuid.UUID{failMsg.ID, okMsg.ID})
	if err != nil {
		t.Fatalf("expected BackfillEmbeddings to complete (log-and-continue) despite a failing embedder, got error: %v", err)
	}
	if result.Failed != 2 {
		// Both messages use the SAME fakeEmbedder configured to always
		// error, so both fail — this asserts the run processed both
		// rather than stopping after the first failure.
		t.Errorf("expected both messages to fail with this always-erroring embedder, got Failed=%d", result.Failed)
	}
	if result.Embedded != 0 {
		t.Errorf("expected 0 embedded with an always-erroring embedder, got %d", result.Embedded)
	}

	foundFail := false
	foundOK := false
	for _, line := range logLines {
		if strings.Contains(line, failMsg.ID.String()) {
			foundFail = true
		}
		if strings.Contains(line, okMsg.ID.String()) {
			foundOK = true
		}
	}
	if !foundFail {
		t.Errorf("expected a log line naming the failed message's ID (%s), got: %v", failMsg.ID, logLines)
	}
	if !foundOK {
		t.Errorf("expected the run to have also attempted (and logged failing) the second message %s — log-and-continue, not fail-fast; got: %v", okMsg.ID, logLines)
	}
}

// TestBackfillEmbeddings_NilEmbedderReturnsError covers the deliberate
// difference from embedMessages' own silent-no-op-on-nil-embedder
// behavior: BackfillEmbeddings is the exported entry point whose entire
// purpose is embedding messages, so a misconfigured (nil) Embedder must
// surface as an error, not a quietly successful zero-count result.
func TestBackfillEmbeddings_NilEmbedderReturnsError(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	_, err := a.BackfillEmbeddings(ctx, []uuid.UUID{uuid.New()})
	if err == nil {
		t.Fatal("expected an error when no Embedder is configured, got nil")
	}
}
