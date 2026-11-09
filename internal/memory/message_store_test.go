package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// --- Message ingestion idempotency ---
//
// CreateIfAbsent is the atomic create-if-absent write MessageStore needed
// (requirements 2-3 of the message-ingestion idempotency fix) so a
// re-import or a redelivered live message detects "this exact message
// was already stored" via each adapter's own deterministic
// RawMessage.ExternalID, instead of silently creating a duplicate row
// every time. These tests exercise it directly against real Postgres.

// createTestMessageEntity always suffixes name with a fresh UUID — see
// createTestEntity's own doc comment (edge_store_reinforce_test.go, same
// package) for why: the entity-resolution race fix's new unique index on
// canonical_name turns a fixed literal name into a Create() failure
// (ErrEntityExists) the moment any other test (or an earlier run of
// this one) already used it, not just a harmless duplicate row the way
// it used to be.
func createTestMessageEntity(t *testing.T, ctx context.Context, store *EntityStore, name string) *Entity {
	t.Helper()
	e := &Entity{Type: "person", CanonicalName: name + " " + uuid.New().String()}
	if err := store.Create(ctx, e); err != nil {
		t.Fatalf("create test entity: %v", err)
	}
	return e
}

func TestMessageStore_CreateIfAbsent_FirstCallCreates(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "CreateIfAbsent Test Sender "+uuid.New().String())
	conversationID := uuid.New()
	extID := "ext-" + uuid.New().String()

	msg := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
		Processed: false, Timestamp: time.Now(), ExternalID: &extID,
	}
	created, err := messageStore.CreateIfAbsent(ctx, msg)
	if err != nil {
		t.Fatalf("CreateIfAbsent: %v", err)
	}
	if !created {
		t.Fatal("expected created=true on the first call")
	}
	if msg.ID == uuid.Nil {
		t.Error("expected msg.ID to be populated")
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id = $2`, conversationID, extID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 row, got %d", rowCount)
	}
}

// TestMessageStore_CreateIfAbsent_SecondCallLoadsExisting covers the
// core idempotency contract: a second CreateIfAbsent for the same
// (conversation_id, external_id) inserts nothing new, returns
// created=false, and overwrites the passed message with what's actually
// stored — including fields the caller's new attempt didn't carry
// (Processed=true, a Transcript), proving a resumed import sees the
// REAL stored state, not its own stale attempt.
func TestMessageStore_CreateIfAbsent_SecondCallLoadsExisting(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "CreateIfAbsent Resume Sender "+uuid.New().String())
	conversationID := uuid.New()
	extID := "ext-" + uuid.New().String()
	transcript := "hello from a prior run"

	first := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "voice",
		Processed: true, Transcript: &transcript, Timestamp: time.Now(), ExternalID: &extID,
	}
	if created, err := messageStore.CreateIfAbsent(ctx, first); err != nil || !created {
		t.Fatalf("first CreateIfAbsent: created=%v err=%v", created, err)
	}

	// A second attempt with DIFFERENT (stale/default) field values —
	// exactly what a fresh re-import pass would construct before
	// knowing anything already exists.
	second := &Message{
		ConversationID: conversationID, SenderID: sender.ID, MediaType: "voice",
		Processed: false, Timestamp: time.Now(), ExternalID: &extID,
	}
	created, err := messageStore.CreateIfAbsent(ctx, second)
	if err != nil {
		t.Fatalf("second CreateIfAbsent: %v", err)
	}
	if created {
		t.Error("expected created=false on the second call")
	}
	if second.ID != first.ID {
		t.Errorf("expected the same message ID, got %s vs %s", second.ID, first.ID)
	}
	if !second.Processed {
		t.Error("expected second.Processed to reflect the EXISTING row's true value, not the caller's stale false")
	}
	if second.Transcript == nil || *second.Transcript != transcript {
		t.Errorf("expected second.Transcript to reflect the existing row's transcript, got %v", second.Transcript)
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id = $2`, conversationID, extID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 row (no duplicate insert), got %d", rowCount)
	}
}

// TestMessageStore_CreateIfAbsent_NilExternalIDNeverDedupes covers
// requirement 4/18: a message with no ExternalID (nil — the caller's own
// job to convert an empty RawMessage.ExternalID into nil, see
// api.messageExternalID) always inserts, and two SEPARATE such messages
// are never merged with each other — NULL never matches NULL under the
// partial unique index.
func TestMessageStore_CreateIfAbsent_NilExternalIDNeverDedupes(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "CreateIfAbsent Nil ExtID Sender "+uuid.New().String())
	conversationID := uuid.New()

	msg1 := &Message{ConversationID: conversationID, SenderID: sender.ID, MediaType: "text", Timestamp: time.Now(), ExternalID: nil}
	created1, err := messageStore.CreateIfAbsent(ctx, msg1)
	if err != nil || !created1 {
		t.Fatalf("msg1: created=%v err=%v", created1, err)
	}

	msg2 := &Message{ConversationID: conversationID, SenderID: sender.ID, MediaType: "text", Timestamp: time.Now(), ExternalID: nil}
	created2, err := messageStore.CreateIfAbsent(ctx, msg2)
	if err != nil || !created2 {
		t.Fatalf("msg2: created=%v err=%v", created2, err)
	}

	if msg1.ID == msg2.ID {
		t.Error("expected two distinct rows for two nil-ExternalID messages, got the same ID")
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id IS NULL`, conversationID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected exactly 2 rows (never merged), got %d", rowCount)
	}
}

// TestMessageStore_CreateIfAbsent_ConcurrentCallsYieldOneRow covers
// requirement 17: two genuinely concurrent CreateIfAbsent calls for the
// SAME (conversation_id, external_id) — Whatsmeow redelivering a live
// message while a historical re-import of the same conversation also
// runs is the real scenario this guards against — must produce exactly
// one row, both callers ending up with the same ID, neither erroring.
// Relies on the database's own unique-index conflict handling for
// atomicity, not application-level locking — see CreateIfAbsent's own
// doc comment for why a check-then-insert approach would race here.
func TestMessageStore_CreateIfAbsent_ConcurrentCallsYieldOneRow(t *testing.T) {
	pool := testDB(t)
	ctx := context.Background()
	entityStore := NewEntityStore(pool)
	messageStore := NewMessageStore(pool)

	sender := createTestMessageEntity(t, ctx, entityStore, "CreateIfAbsent Race Sender "+uuid.New().String())
	conversationID := uuid.New()
	extID := "ext-" + uuid.New().String()

	var wg sync.WaitGroup
	results := make([]*Message, 2)
	errs := make([]error, 2)
	createdFlags := make([]bool, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := &Message{
				ConversationID: conversationID, SenderID: sender.ID, MediaType: "text",
				Timestamp: time.Now(), ExternalID: &extID,
			}
			created, err := messageStore.CreateIfAbsent(ctx, m)
			results[i], errs[i], createdFlags[i] = m, err, created
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: CreateIfAbsent error: %v", i, err)
		}
	}
	if results[0].ID != results[1].ID {
		t.Errorf("expected both goroutines to end up with the same message ID, got %s vs %s", results[0].ID, results[1].ID)
	}
	if createdFlags[0] == createdFlags[1] {
		t.Errorf("expected exactly one goroutine to report created=true and the other false, got %v and %v", createdFlags[0], createdFlags[1])
	}

	var rowCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id = $2`, conversationID, extID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 row despite the concurrent race, got %d", rowCount)
	}
}
