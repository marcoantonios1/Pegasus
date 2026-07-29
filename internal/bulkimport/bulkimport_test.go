package bulkimport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
)

// fakeExtractor lets tests exercise the orchestration logic without a live
// Costguard/model dependency.
type fakeExtractor struct {
	calls      int
	err        error
	tripleFunc func(w extraction.Window) []extraction.ExtractedTriple
}

func (f *fakeExtractor) ExtractWindow(ctx context.Context, w extraction.Window) ([]extraction.ExtractedTriple, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.tripleFunc != nil {
		return f.tripleFunc(w), nil
	}
	return []extraction.ExtractedTriple{
		{Subject: "Marco", Predicate: "likes", Object: "Go", ObjectType: "literal", Confidence: 0.9},
	}, nil
}

const sampleWhatsAppExport = `[05/01/26, 09:01:15] Marco: hey, how's it going?
[05/01/26, 09:01:40] Alice: pretty good!
[05/01/26, 09:02:00] Marco: nice
[05/01/26, 09:02:30] Alice: yep
`

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestImportWhatsAppDir_MultipleFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "alice-chat.txt"), sampleWhatsAppExport)
	writeFile(t, filepath.Join(dir, "bob-chat.txt"), sampleWhatsAppExport)
	writeFile(t, filepath.Join(dir, "not-a-chat.jpg"), "ignore me")

	extractor := &fakeExtractor{}
	var sunk []string
	sink := func(conversationID string, w extraction.Window, triples []extraction.ExtractedTriple) error {
		sunk = append(sunk, conversationID)
		return nil
	}

	result, err := ImportWhatsAppDir(context.Background(), dir, extractor, sink)
	if err != nil {
		t.Fatalf("ImportWhatsAppDir: %v", err)
	}

	if len(result.Conversations) != 2 {
		t.Fatalf("expected 2 conversations (non-.txt file ignored), got %d: %+v", len(result.Conversations), result.Conversations)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %+v", result.Errors)
	}

	gotIDs := map[string]bool{}
	for _, c := range result.Conversations {
		gotIDs[c.ConversationID] = true
		if c.MessagesIngested != 4 {
			t.Errorf("conversation %s: expected 4 messages ingested, got %d", c.ConversationID, c.MessagesIngested)
		}
		if c.WindowsProcessed != 1 {
			t.Errorf("conversation %s: expected 1 window (4 msgs < default size 8), got %d", c.ConversationID, c.WindowsProcessed)
		}
	}
	if !gotIDs["alice-chat"] || !gotIDs["bob-chat"] {
		t.Errorf("expected conversation IDs derived from filenames, got %+v", gotIDs)
	}

	if len(sunk) != 2 {
		t.Errorf("expected the sink to be called once per conversation's single window, got %d calls: %+v", len(sunk), sunk)
	}

	if result.TotalMessages() != 8 {
		t.Errorf("expected TotalMessages()=8, got %d", result.TotalMessages())
	}
	if result.TotalTriples() != 2 {
		t.Errorf("expected TotalTriples()=2 (1 per conversation), got %d", result.TotalTriples())
	}
}

func TestImportWhatsAppDir_OneBadFileDoesNotAbortBatch(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "good-chat.txt"), sampleWhatsAppExport)
	writeFile(t, filepath.Join(dir, "empty-chat.txt"), "")

	extractor := &fakeExtractor{err: errors.New("costguard unavailable")}

	result, err := ImportWhatsAppDir(context.Background(), dir, extractor, nil)
	if err != nil {
		t.Fatalf("ImportWhatsAppDir: %v", err)
	}

	// Both files parse fine (an empty export is valid — 0 messages), but
	// the extractor is set to always fail, so both should show up as
	// per-conversation errors, not abort the whole batch or panic.
	if len(result.Errors) != 1 {
		// empty-chat.txt produces 0 windows, so ExtractWindow is never
		// called for it and it succeeds trivially with 0 triples.
		t.Fatalf("expected exactly 1 conversation-level error (good-chat.txt), got %d: %+v", len(result.Errors), result.Errors)
	}
	if _, ok := result.Errors["good-chat.txt"]; !ok {
		t.Errorf("expected good-chat.txt to be recorded as failed, got %+v", result.Errors)
	}
}

func TestImportWhatsAppDir_NonexistentDir(t *testing.T) {
	_, err := ImportWhatsAppDir(context.Background(), "/no/such/directory", &fakeExtractor{}, nil)
	if err == nil {
		t.Fatal("expected an error for a nonexistent directory, got nil")
	}
}

func TestImportWhatsAppDir_SinkErrorRecordedPerConversation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "chat.txt"), sampleWhatsAppExport)

	extractor := &fakeExtractor{}
	sink := func(conversationID string, w extraction.Window, triples []extraction.ExtractedTriple) error {
		return errors.New("downstream write failed")
	}

	result, err := ImportWhatsAppDir(context.Background(), dir, extractor, sink)
	if err != nil {
		t.Fatalf("ImportWhatsAppDir: %v", err)
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected a sink error to be recorded per-conversation, got %+v", result.Errors)
	}
}

const sampleInstagramExport = `{
	"thread_path": "inbox/alice_12345",
	"title": "Alice",
	"messages": [
		{"sender_name": "marco.antonios", "timestamp_ms": 1735707600000, "content": "hey there"},
		{"sender_name": "alice", "timestamp_ms": 1735707660000, "content": "hi!"}
	]
}`

func TestImportInstagramDir_WalksNestedThreads(t *testing.T) {
	dir := t.TempDir()
	aliceDir := filepath.Join(dir, "inbox", "alice_12345")
	bobDir := filepath.Join(dir, "inbox", "bob_67890")
	if err := os.MkdirAll(aliceDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bobDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(aliceDir, "message_1.json"), sampleInstagramExport)
	writeFile(t, filepath.Join(bobDir, "message_1.json"), `{"thread_path":"inbox/bob_67890","messages":[{"sender_name":"bob","timestamp_ms":1735707600000,"content":"yo"}]}`)
	writeFile(t, filepath.Join(aliceDir, "photos"), "") // not a message_*.json — must be ignored, not just non-.json

	extractor := &fakeExtractor{}
	result, err := ImportInstagramDir(context.Background(), dir, extractor, nil)
	if err != nil {
		t.Fatalf("ImportInstagramDir: %v", err)
	}

	if len(result.Conversations) != 2 {
		t.Fatalf("expected 2 threads processed, got %d: %+v", len(result.Conversations), result.Conversations)
	}

	gotIDs := map[string]int{}
	for _, c := range result.Conversations {
		gotIDs[c.ConversationID] = c.MessagesIngested
	}
	if gotIDs["inbox/alice_12345"] != 2 {
		t.Errorf("expected alice thread conversation_id from thread_path with 2 messages, got %+v", gotIDs)
	}
	if gotIDs["inbox/bob_67890"] != 1 {
		t.Errorf("expected bob thread conversation_id from thread_path with 1 message, got %+v", gotIDs)
	}
}

func TestImportInstagramDir_ExtractorErrorRecordedPerThread(t *testing.T) {
	dir := t.TempDir()
	threadDir := filepath.Join(dir, "inbox", "alice_12345")
	if err := os.MkdirAll(threadDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(threadDir, "message_1.json"), sampleInstagramExport)

	extractor := &fakeExtractor{err: errors.New("model call failed")}
	result, err := ImportInstagramDir(context.Background(), dir, extractor, nil)
	if err != nil {
		t.Fatalf("ImportInstagramDir: %v", err)
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 recorded error, got %+v", result.Errors)
	}
}
