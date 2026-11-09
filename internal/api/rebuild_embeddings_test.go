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

// --- RebuildEmbeddings (requirement 12): re-embed every message whose
// existing vector was generated without nomic-embed-text's required
// task prefix — see extraction.EmbeddingDocumentPrefix's own doc
// comment, and RebuildEmbeddings' own doc comment for the full
// before/after-this-fix-shipped reasoning. ---

// TestRebuildEmbeddings_ReplacesOldVectorViaDocumentPath covers the core
// contract: seed messages with OLD embedding rows, run
// RebuildEmbeddings, and confirm exactly ONE row per message survives —
// the NEW one, produced by the document path specifically, not the
// query path. documentVector and queryVector are deliberately distinct
// so a bug that routed this through EmbedQuery instead would be caught
// by the vector mismatch, not just by (the also-asserted) call count.
func TestRebuildEmbeddings_ReplacesOldVectorViaDocumentPath(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Rebuild Sender "+uuid.New().String())
	conversationID := uuid.New()

	var msgIDs []uuid.UUID
	var oldVectors [][]float32
	for i := 0; i < 2; i++ {
		text := fmt.Sprintf("rebuild test message %d", i)
		msg := &memory.Message{
			ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
			RawText: &text, Timestamp: time.Now(),
		}
		if err := ts.messages.Create(ctx, msg); err != nil {
			t.Fatalf("create message %d: %v", i, err)
		}
		oldVector := basisVector(randomBasisIndex(), 1)
		createTestEmbedding(t, ctx, ts, msg.ID, oldVector)
		msgIDs = append(msgIDs, msg.ID)
		oldVectors = append(oldVectors, oldVector)
	}

	documentIndex := randomBasisIndex()
	queryIndex := (documentIndex + 1) % embeddingDim
	embedder := &fakeEmbedder{
		documentVector: basisVector(documentIndex, 1),
		queryVector:    basisVector(queryIndex, 1),
	}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	result, err := a.RebuildEmbeddings(ctx, msgIDs)
	if err != nil {
		t.Fatalf("RebuildEmbeddings: %v", err)
	}
	if result.Replaced != len(msgIDs) {
		t.Errorf("expected Replaced=%d, got %d", len(msgIDs), result.Replaced)
	}
	if result.Failed != 0 || result.SkippedNoText != 0 {
		t.Errorf("expected no failures/skips, got %+v", result)
	}

	if embedder.documentCalls != len(msgIDs) {
		t.Errorf("expected %d EmbedDocument calls, got %d", len(msgIDs), embedder.documentCalls)
	}
	if embedder.queryCalls != 0 {
		t.Errorf("expected EmbedQuery never called by a rebuild, got %d calls", embedder.queryCalls)
	}

	for i, id := range msgIDs {
		var rowCount int
		if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, id).Scan(&rowCount); err != nil {
			t.Fatalf("count embeddings for %s: %v", id, err)
		}
		if rowCount != 1 {
			t.Errorf("expected exactly 1 embeddings row for message %s after rebuild, got %d", id, rowCount)
		}

		emb, err := ts.embeds.GetByMessageID(ctx, id)
		if err != nil {
			t.Fatalf("GetByMessageID(%s): %v", id, err)
		}
		if emb == nil {
			t.Fatalf("expected an embedding for message %s, got none", id)
		}
		if emb.Vector[0] != embedder.documentVector[0] {
			t.Errorf("message %s: expected the NEW document-mode vector, got a different one (old vector was %v)", id, oldVectors[i])
		}
	}
}

// TestRebuildEmbeddings_FailedEmbedKeepsOldRowAndIsCounted covers the
// "a message never has zero embeddings mid-run" guarantee directly: an
// embedder that always fails must leave the message's EXISTING row
// completely untouched (ReplaceForMessage is never even called, since
// EmbedDocument fails before it), counted as Failed and logged with the
// message ID — not silently dropped, and not left with zero rows.
func TestRebuildEmbeddings_FailedEmbedKeepsOldRowAndIsCounted(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Rebuild Failure Sender "+uuid.New().String())
	text := "this message's rebuild will fail"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text",
		RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create message: %v", err)
	}
	oldVector := basisVector(randomBasisIndex(), 1)
	createTestEmbedding(t, ctx, ts, msg.ID, oldVector)

	embedder := &fakeEmbedder{err: fmt.Errorf("simulated costguard embeddings outage")}
	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	result, err := a.RebuildEmbeddings(ctx, []uuid.UUID{msg.ID})
	if err != nil {
		t.Fatalf("expected RebuildEmbeddings to complete (log-and-continue) despite a failing embedder, got error: %v", err)
	}
	if result.Failed != 1 {
		t.Errorf("expected Failed=1, got %+v", result)
	}
	if result.Replaced != 0 {
		t.Errorf("expected Replaced=0, got %d", result.Replaced)
	}

	// The message was never, at any point, down to zero rows: its OLD
	// row is still there, unchanged, exactly once.
	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, msg.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 embeddings row (the untouched old one) after a failed rebuild, got %d", rowCount)
	}
	emb, err := ts.embeds.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil || emb.Vector[0] != oldVector[0] {
		t.Errorf("expected the old vector to survive untouched after the failed rebuild, got %v", emb)
	}

	found := false
	for _, line := range logLines {
		if strings.Contains(line, msg.ID.String()) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected a log line naming the failed message's ID (%s), got: %v", msg.ID, logLines)
	}
}

// TestRebuildEmbeddings_SkipsMessagesWithoutText covers the no-text
// case: a voice message with no transcript has nothing to embed —
// counted as SkippedNoText, never calls the embedder, and (having no
// row to begin with) stays with none.
func TestRebuildEmbeddings_SkipsMessagesWithoutText(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Rebuild No Text Sender "+uuid.New().String())
	noTextMsg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "voice",
		Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, noTextMsg); err != nil {
		t.Fatalf("create no-text message: %v", err)
	}

	embedder := &fakeEmbedder{documentVector: basisVector(randomBasisIndex(), 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	result, err := a.RebuildEmbeddings(ctx, []uuid.UUID{noTextMsg.ID})
	if err != nil {
		t.Fatalf("RebuildEmbeddings: %v", err)
	}
	if result.SkippedNoText != 1 {
		t.Errorf("expected SkippedNoText=1, got %+v", result)
	}
	if result.Replaced != 0 || result.Failed != 0 {
		t.Errorf("expected no replacements or failures, got %+v", result)
	}
	if embedder.documentCalls != 0 {
		t.Errorf("expected the embedder never called for a no-text message, got %d calls", embedder.documentCalls)
	}

	emb, err := ts.embeds.GetByMessageID(ctx, noTextMsg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb != nil {
		t.Error("expected the no-text message to remain unembedded")
	}
}

// TestRebuildEmbeddings_NilEmbedderReturnsError mirrors
// TestBackfillEmbeddings_NilEmbedderReturnsError's own reasoning:
// rebuilding IS the entire point of calling this, so a misconfigured
// (nil) Embedder must surface as an error, not a quiet zero-count no-op.
func TestRebuildEmbeddings_NilEmbedderReturnsError(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	_, err := a.RebuildEmbeddings(ctx, []uuid.UUID{uuid.New()})
	if err == nil {
		t.Fatal("expected an error when no Embedder is configured, got nil")
	}
}

// TestRebuildEmbeddings_SafeToRerun covers the "safe to re-run" claim
// (requirement 7): running it twice against the same message must
// leave exactly one row, matching whatever the embedder most recently
// returned, with no duplicate accumulation.
func TestRebuildEmbeddings_SafeToRerun(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	sender := createTestEntity(t, ctx, ts, "person", "Rebuild Rerun Sender "+uuid.New().String())
	text := "rerun test message"
	msg := &memory.Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text",
		RawText: &text, Timestamp: time.Now(),
	}
	if err := ts.messages.Create(ctx, msg); err != nil {
		t.Fatalf("create message: %v", err)
	}
	createTestEmbedding(t, ctx, ts, msg.ID, basisVector(randomBasisIndex(), 1))

	embedder := &fakeEmbedder{documentVector: basisVector(randomBasisIndex(), 1)}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder)

	if _, err := a.RebuildEmbeddings(ctx, []uuid.UUID{msg.ID}); err != nil {
		t.Fatalf("first RebuildEmbeddings: %v", err)
	}
	if _, err := a.RebuildEmbeddings(ctx, []uuid.UUID{msg.ID}); err != nil {
		t.Fatalf("second RebuildEmbeddings: %v", err)
	}

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, msg.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 embeddings row after two rebuild runs, got %d", rowCount)
	}
	if embedder.documentCalls != 2 {
		t.Errorf("expected 2 EmbedDocument calls (a rebuild always re-embeds, no skip-if-present check), got %d", embedder.documentCalls)
	}
}
