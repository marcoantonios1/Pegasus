package api

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/ingestion"
)

// TestSelfIdentity_ImportAndLiveBothResolveToSameSelfEntity is the
// group-chat-attribution fix's own required precondition check
// (requirement 5): RecomputeRelationshipStats now needs a real selfID
// to tell Marco's own messages apart from everyone else's in a group —
// if ImportWhatsApp and ProcessLiveMessage ever resolved Marco's own
// messages to two DIFFERENT entities (or to a separate "Marco" person
// entity instead of the is_self row), every relationship stat computed
// downstream would be wrong in a way this fix could not detect or
// correct.
//
// This is NOT a hypothetical worth guessing about — both paths are
// exercised for real here (ImportWhatsApp against a real export file,
// ProcessLiveMessage against a real live message), with Marco's sender
// identified only by his platform ID (SelfExternalIDs), the same way a
// real deployment configures it — never by hand-seeding a sender_id
// directly. Confirmed: both paths resolve to the exact same entity, and
// that entity is the one EntityStore.GetSelf() returns.
func TestSelfIdentity_ImportAndLiveBothResolveToSameSelfEntity(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	selfExternalID := "self-identity-check-" + uuid.New().String()
	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, SelfExternalIDs: []string{selfExternalID},
	})

	// --- Import path: a real WhatsApp export file, sender name == the
	// configured self external ID. ---
	importConvExternalID := "self-identity-import-" + uuid.New().String()
	exportPath := t.TempDir() + "/" + importConvExternalID + ".txt"
	writeExportFile(t, exportPath, selfExternalID, "1/1/26", []string{"hello from import"})
	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}
	importConvID := resolveConversationID("whatsapp", importConvExternalID)
	importSenderID := soleSenderID(t, ctx, ts, importConvID)

	// --- Live path: ProcessLiveMessage, same configured self external
	// ID, a completely different conversation. ---
	liveConvExternalID := "self-identity-live-" + uuid.New().String()
	raw := ingestion.RawMessage{
		ExternalID: "self-identity-msg-" + uuid.New().String(), Platform: "whatsapp",
		ConversationID: liveConvExternalID, SenderExternalID: selfExternalID,
		Timestamp: time.Now(), MediaType: ingestion.MediaTypeText, Text: textPtr("hello from live"),
	}
	if err := a.ProcessLiveMessage(ctx, raw); err != nil {
		t.Fatalf("ProcessLiveMessage: %v", err)
	}
	liveConvID := resolveConversationID("whatsapp", liveConvExternalID)
	liveSenderID := soleSenderID(t, ctx, ts, liveConvID)

	if importSenderID != liveSenderID {
		t.Fatalf("ImportWhatsApp and ProcessLiveMessage resolved Marco's own messages to DIFFERENT entities (%s vs %s) — every relationship stat computed from this data would misattribute activity", importSenderID, liveSenderID)
	}

	self, err := ts.entities.GetSelf(ctx)
	if err != nil {
		t.Fatalf("GetSelf: %v", err)
	}
	if self == nil {
		t.Fatal("expected a self entity to exist after resolving a configured self external ID through both paths")
	}
	if self.ID != importSenderID {
		t.Fatalf("expected both paths to resolve to EntityStore.GetSelf()'s own entity (%s), got %s", self.ID, importSenderID)
	}
	if !self.IsSelf {
		t.Errorf("expected the resolved self entity to have IsSelf=true, got false")
	}
}

// soleSenderID returns the one sender_id used in conversationID,
// failing the test if there's more than one or none at all — these test
// conversations each carry exactly one message.
func soleSenderID(t *testing.T, ctx context.Context, ts *testStores, conversationID uuid.UUID) uuid.UUID {
	t.Helper()
	rows, err := ts.pool.Query(ctx, `SELECT DISTINCT sender_id FROM messages WHERE conversation_id = $1`, conversationID)
	if err != nil {
		t.Fatalf("query sender_id for conversation %s: %v", conversationID, err)
	}
	defer rows.Close()

	var senderID uuid.UUID
	n := 0
	for rows.Next() {
		if err := rows.Scan(&senderID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 distinct sender_id in conversation %s, got %d", conversationID, n)
	}
	return senderID
}
