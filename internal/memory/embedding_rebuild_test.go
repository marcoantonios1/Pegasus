package memory

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- EmbeddingStore.ReplaceForMessage / MessageStore.ListWithText —
// the task-prefix fix's rebuild-mode building blocks ---

func TestEmbeddingStore_ReplaceForMessage_SwapsAtomically(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)
	embeddingStore := NewEmbeddingStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "ReplaceForMessage Sender "+uuid.New().String())
	text := "replace for message test"
	msg := &Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text",
		RawText: &text, Timestamp: time.Now(),
	}
	if err := messageStore.Create(ctx, msg); err != nil {
		t.Fatalf("create message: %v", err)
	}

	oldVector := make([]float32, 768)
	oldVector[0] = 1
	if err := embeddingStore.Create(ctx, &Embedding{MessageID: msg.ID, Vector: oldVector}); err != nil {
		t.Fatalf("create old embedding: %v", err)
	}

	newVector := make([]float32, 768)
	newVector[1] = 1
	if err := embeddingStore.ReplaceForMessage(ctx, msg.ID, newVector); err != nil {
		t.Fatalf("ReplaceForMessage: %v", err)
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM embeddings WHERE message_id = $1`, msg.ID).Scan(&rowCount); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 embeddings row after ReplaceForMessage, got %d", rowCount)
	}

	emb, err := embeddingStore.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil {
		t.Fatal("expected an embedding row, got none")
	}
	if emb.Vector[0] != 0 || emb.Vector[1] != 1 {
		t.Errorf("expected the OLD vector replaced by the NEW one, got vector starting %v", emb.Vector[:2])
	}
}

// TestEmbeddingStore_ReplaceForMessage_NoExistingRowStillInserts covers
// the simplest case: a message with NO prior embedding at all still
// gets its new row inserted (the delete is a no-op; the insert still
// happens) — RebuildEmbeddings can legitimately be run against messages
// that were never embedded before at all, not only ones being
// corrected.
func TestEmbeddingStore_ReplaceForMessage_NoExistingRowStillInserts(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)
	embeddingStore := NewEmbeddingStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "ReplaceForMessage No-Prior Sender "+uuid.New().String())
	text := "no prior embedding test"
	msg := &Message{
		ConversationID: uuid.New(), SenderID: sender.ID, MediaType: "text",
		RawText: &text, Timestamp: time.Now(),
	}
	if err := messageStore.Create(ctx, msg); err != nil {
		t.Fatalf("create message: %v", err)
	}

	vector := make([]float32, 768)
	vector[5] = 1
	if err := embeddingStore.ReplaceForMessage(ctx, msg.ID, vector); err != nil {
		t.Fatalf("ReplaceForMessage: %v", err)
	}

	emb, err := embeddingStore.GetByMessageID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetByMessageID: %v", err)
	}
	if emb == nil {
		t.Fatal("expected an embedding row to have been inserted, got none")
	}
}

// TestMessageStore_ListWithText covers ListWithText's own contract:
// returns every message with embeddable text REGARDLESS of processed or
// embedded state (unlike ListWithoutEmbeddings/ListUnprocessedWithText,
// each of which adds its own extra filter this one deliberately
// doesn't) — the rebuild's candidate set.
func TestMessageStore_ListWithText(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)
	embeddingStore := NewEmbeddingStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "ListWithText Sender "+uuid.New().String())
	conversationID := uuid.New()

	// Already processed AND already embedded — ListWithoutEmbeddings and
	// ListUnprocessedWithText would both exclude this one; ListWithText
	// must still include it (a rebuild needs to re-touch it).
	processedEmbeddedText := "processed and embedded"
	processedEmbedded := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &processedEmbeddedText, Timestamp: time.Now(), Processed: true,
	}
	if err := messageStore.Create(ctx, processedEmbedded); err != nil {
		t.Fatalf("create processedEmbedded: %v", err)
	}
	vec := make([]float32, 768)
	vec[0] = 1
	if err := embeddingStore.Create(ctx, &Embedding{MessageID: processedEmbedded.ID, Vector: vec}); err != nil {
		t.Fatalf("create embedding: %v", err)
	}

	// Unprocessed, never embedded, has text.
	unprocessedText := "unprocessed with text"
	unprocessed := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		RawText: &unprocessedText, Timestamp: time.Now(), Processed: false,
	}
	if err := messageStore.Create(ctx, unprocessed); err != nil {
		t.Fatalf("create unprocessed: %v", err)
	}

	// No usable text at all (a voice message with no transcript) — must
	// be excluded regardless of processed state.
	noText := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "voice",
		Timestamp: time.Now(), Processed: true,
	}
	if err := messageStore.Create(ctx, noText); err != nil {
		t.Fatalf("create noText: %v", err)
	}

	msgs, err := messageStore.ListWithText(ctx)
	if err != nil {
		t.Fatalf("ListWithText: %v", err)
	}
	ids := make(map[uuid.UUID]bool, len(msgs))
	for _, m := range msgs {
		ids[m.ID] = true
	}

	if !ids[processedEmbedded.ID] {
		t.Error("expected ListWithText to include a message that's already processed AND embedded")
	}
	if !ids[unprocessed.ID] {
		t.Error("expected ListWithText to include an unprocessed message with text")
	}
	if ids[noText.ID] {
		t.Error("expected ListWithText to exclude a message with no usable text")
	}
}
