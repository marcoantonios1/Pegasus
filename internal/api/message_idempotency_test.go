package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Message ingestion idempotency ---
//
// Before this fix, ImportWhatsApp/ProcessLiveMessage discarded each
// adapter's own deterministic RawMessage.ExternalID and always called
// MessageStore.Create — re-importing the same export (or Whatsmeow
// redelivering a live message) silently created duplicate rows and
// double-reinforced edges. These tests exercise the fix through the real
// pipeline (ImportWhatsApp, ProcessLiveMessage) against real Postgres.

// TestImportWhatsApp_SecondImportIsIdempotent covers requirement 11: run
// ImportWhatsApp twice on the IDENTICAL export. Message count, edge
// count, and the resulting edge's last_reinforced/source_message_ids
// must be identical to a single import — and the fake triager,
// extractor, and embedder must each see ZERO additional calls on the
// second run, the direct proof nothing downstream reran. (The fake
// transcriber trivially sees zero calls both times too — this export has
// no voice messages; requirement 15's own test covers transcript reuse
// specifically.)
func TestImportWhatsApp_SecondImportIsIdempotent(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subjectName := "Idempotent Import Subject " + uuid.New().String()
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.8},
	}}
	triager := &fakeTriager{candidate: true}
	embedder := &fakeEmbedder{vector: basisVector(0, 1)}
	transcriber := &fakeTranscriber{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, embedder).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, Transcriber: transcriber,
	})

	dir := t.TempDir()
	exportFileName := "IdempotentImport-" + uuid.New().String() + ".txt"
	exportPath := filepath.Join(dir, exportFileName)
	lines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range lines {
		lines[i] = fmt.Sprintf("message %d", i)
	}
	writeExportFile(t, exportPath, "John", "1/1/26", lines)

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (first): %v", err)
	}
	if triager.calls != 1 || extractor.calls != 1 {
		t.Fatalf("expected 1 triage call and 1 extraction call after the first import, got triager=%d extractor=%d", triager.calls, extractor.calls)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, subjectName)
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edgesBefore, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edgesBefore) != 1 {
		t.Fatalf("expected exactly 1 edge after the first import, got %d (err=%v)", len(edgesBefore), err)
	}

	conversationID := resolveConversationID("whatsapp", strings.TrimSuffix(exportFileName, ".txt"))
	messagesBefore, err := ts.messages.GetByConversationSince(ctx, conversationID, time.Time{})
	if err != nil {
		t.Fatalf("GetByConversationSince: %v", err)
	}
	embedderCallsBefore, triagerCallsBefore, extractorCallsBefore := embedder.calls, triager.calls, extractor.calls

	// The identical export, imported again.
	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (second): %v", err)
	}

	if triager.calls != triagerCallsBefore {
		t.Errorf("expected ZERO new triage calls on the second import, got %d (was %d)", triager.calls, triagerCallsBefore)
	}
	if extractor.calls != extractorCallsBefore {
		t.Errorf("expected ZERO new extraction calls on the second import, got %d (was %d)", extractor.calls, extractorCallsBefore)
	}
	if embedder.calls != embedderCallsBefore {
		t.Errorf("expected ZERO new embedding calls on the second import, got %d (was %d)", embedder.calls, embedderCallsBefore)
	}
	if len(transcriber.calls) != 0 {
		t.Errorf("expected ZERO transcriber calls (no voice messages in this export), got %d", len(transcriber.calls))
	}

	messagesAfter, err := ts.messages.GetByConversationSince(ctx, conversationID, time.Time{})
	if err != nil {
		t.Fatalf("GetByConversationSince (after): %v", err)
	}
	if len(messagesAfter) != len(messagesBefore) {
		t.Errorf("expected message count unchanged at %d, got %d", len(messagesBefore), len(messagesAfter))
	}

	edgesAfter, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edgesAfter) != 1 {
		t.Fatalf("expected exactly 1 edge after the second import (no double-reinforcement), got %d (err=%v)", len(edgesAfter), err)
	}
	if edgesAfter[0].ID != edgesBefore[0].ID {
		t.Errorf("expected the same edge ID, got %s vs %s", edgesBefore[0].ID, edgesAfter[0].ID)
	}
	if !edgesAfter[0].LastReinforced.Equal(edgesBefore[0].LastReinforced) {
		t.Errorf("expected last_reinforced unchanged at %v, got %v", edgesBefore[0].LastReinforced, edgesAfter[0].LastReinforced)
	}
	if !reflect.DeepEqual(edgesAfter[0].SourceMessageIDs, edgesBefore[0].SourceMessageIDs) {
		t.Errorf("expected source_message_ids unchanged, got %v vs %v", edgesBefore[0].SourceMessageIDs, edgesAfter[0].SourceMessageIDs)
	}
}

// TestImportWhatsApp_OverlappingExportOnlyProcessesNewMessages covers
// requirement 13: import a chat, then re-import a SUPERSET export under
// the same filename (so it resolves to the same conversation_id) adding
// older-content... actually here, additional NEWER content after the
// original window — only the new messages get windowed/extracted; the
// original window's messages (and its edge) are completely untouched,
// not merely unduplicated.
func TestImportWhatsApp_OverlappingExportOnlyProcessesNewMessages(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subjectName := "Overlap Subject " + uuid.New().String()
	firstTriples := []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.8},
	}
	extractor := &fakeExtractor{triples: firstTriples}
	triager := &fakeTriager{candidate: true}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	dir := t.TempDir()
	exportFileName := "OverlapConv-" + uuid.New().String() + ".txt"
	exportPath := filepath.Join(dir, exportFileName)
	originalLines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range originalLines {
		originalLines[i] = fmt.Sprintf("original message %d", i)
	}
	writeExportFile(t, exportPath, "John", "1/1/26", originalLines)

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (original): %v", err)
	}
	if extractor.calls != 1 {
		t.Fatalf("expected 1 extraction call after the original import, got %d", extractor.calls)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, subjectName)
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	originalEdges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(originalEdges) != 1 {
		t.Fatalf("expected exactly 1 'likes' edge, got %d (err=%v)", len(originalEdges), err)
	}
	originalEdgeLastReinforced := originalEdges[0].LastReinforced

	// Superset export, SAME filename (same conversation_id). The
	// original lines are reproduced with their EXACT ORIGINAL date (so
	// they recompute identical external IDs and are correctly
	// recognized as already-existing), followed by one more window's
	// worth of genuinely new content on a later date — writeExportFile
	// itself only supports one dateStr for a whole batch, so this is
	// built directly rather than via two separate calls (which would
	// overwrite, not append).
	extractor.triples = []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "works_at", Object: "Acme Corp", ObjectType: "literal", Confidence: 0.8},
	}
	newLines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range newLines {
		newLines[i] = fmt.Sprintf("new message %d", i)
	}
	var superset strings.Builder
	for i, text := range originalLines {
		fmt.Fprintf(&superset, "1/1/26, 9:%02d AM - John: %s\n", i, text)
	}
	for i, text := range newLines {
		fmt.Fprintf(&superset, "2/1/26, 9:%02d AM - John: %s\n", i, text)
	}
	if err := os.WriteFile(exportPath, []byte(superset.String()), 0o644); err != nil {
		t.Fatalf("write superset export file: %v", err)
	}

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (superset): %v", err)
	}

	if extractor.calls != 2 {
		t.Errorf("expected exactly 1 ADDITIONAL extraction call (for the new window only), got %d total calls", extractor.calls)
	}

	conversationID := resolveConversationID("whatsapp", strings.TrimSuffix(exportFileName, ".txt"))
	allMessages, err := ts.messages.GetByConversationSince(ctx, conversationID, time.Time{})
	if err != nil {
		t.Fatalf("GetByConversationSince: %v", err)
	}
	if len(allMessages) != 2*extraction.DefaultHistoricalWindowSize {
		t.Errorf("expected %d total messages (no duplicates), got %d", 2*extraction.DefaultHistoricalWindowSize, len(allMessages))
	}

	reloadedOriginal, err := ts.edges.GetByID(ctx, originalEdges[0].ID)
	if err != nil {
		t.Fatalf("GetByID(original edge): %v", err)
	}
	if !reloadedOriginal.LastReinforced.Equal(originalEdgeLastReinforced) {
		t.Errorf("expected the original edge completely untouched (last_reinforced unchanged), got %v want %v",
			reloadedOriginal.LastReinforced, originalEdgeLastReinforced)
	}

	newEdges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "works_at")
	if err != nil || len(newEdges) != 1 {
		t.Fatalf("expected exactly 1 'works_at' edge from the new content, got %d (err=%v)", len(newEdges), err)
	}
}

// TestImportWhatsApp_ResumeOnlyReprocessesUnprocessedMessages covers
// requirement 14: a prior run left SOME messages processed=true (its
// completed windows) and SOME processed=false (a window it crashed
// before finishing) — simulated faithfully by running a real import to
// completion, then flipping one window's messages back to
// processed=false directly (exactly what a crash between
// storeExtractedTriples and MarkProcessed leaves, per requirement 8's own
// documented limitation). Re-importing the same export must windows/
// extract ONLY the unprocessed window, leaving the already-processed one
// untouched.
func TestImportWhatsApp_ResumeOnlyReprocessesUnprocessedMessages(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subjectName := "Resume Subject " + uuid.New().String()
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "likes", Object: "chess", ObjectType: "literal", Confidence: 0.8},
	}}
	triager := &fakeTriager{candidate: true}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	dir := t.TempDir()
	exportFileName := "ResumeConv-" + uuid.New().String() + ".txt"
	exportPath := filepath.Join(dir, exportFileName)
	lines := make([]string, 2*extraction.DefaultHistoricalWindowSize)
	for i := range lines {
		lines[i] = fmt.Sprintf("message %d", i)
	}
	writeExportFile(t, exportPath, "John", "1/1/26", lines)

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (initial): %v", err)
	}
	if extractor.calls != 2 {
		t.Fatalf("expected 2 extraction calls (2 windows), got %d", extractor.calls)
	}

	conversationID := resolveConversationID("whatsapp", strings.TrimSuffix(exportFileName, ".txt"))
	allMessages, err := ts.messages.GetByConversationSince(ctx, conversationID, time.Time{})
	if err != nil || len(allMessages) != 2*extraction.DefaultHistoricalWindowSize {
		t.Fatalf("expected %d messages, got %d (err=%v)", 2*extraction.DefaultHistoricalWindowSize, len(allMessages), err)
	}

	// Simulate a crash that left the SECOND window's messages
	// unprocessed — flip the later half (by timestamp) back to
	// processed=false directly.
	sortMessagesByTimestamp(allMessages)
	secondWindow := allMessages[extraction.DefaultHistoricalWindowSize:]
	for _, m := range secondWindow {
		if _, err := ts.pool.Exec(ctx, `UPDATE messages SET processed = false WHERE id = $1`, m.ID); err != nil {
			t.Fatalf("simulate crash (reset processed=false): %v", err)
		}
	}

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp (resume): %v", err)
	}
	if extractor.calls != 3 {
		t.Errorf("expected exactly 1 ADDITIONAL extraction call (only the unprocessed window), got %d total calls", extractor.calls)
	}

	for _, m := range secondWindow {
		reloaded, err := ts.messages.GetByID(ctx, m.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if !reloaded.Processed {
			t.Errorf("expected message %s to be processed=true after resuming", m.ID)
		}
	}
}

// sortMessagesByTimestamp sorts msgs in place, ascending by Timestamp —
// a small test-only helper since MessageStore.GetByConversationSince
// already returns them ordered by timestamp ASC, but this is asserted
// explicitly here rather than silently relied on.
func sortMessagesByTimestamp(msgs []*memory.Message) {
	for i := 1; i < len(msgs); i++ {
		for j := i; j > 0 && msgs[j].Timestamp.Before(msgs[j-1].Timestamp); j-- {
			msgs[j], msgs[j-1] = msgs[j-1], msgs[j]
		}
	}
}

// TestProcessLiveMessage_DuplicateRedeliveryYieldsOneRowOneWindowSlot
// covers requirement 16: delivering the IDENTICAL RawMessage (same
// native ExternalID — a Whatsmeow redelivery) twice through the live
// path must yield exactly one stored row and leave the in-memory live
// window with exactly one pending slot for it, not two.
func TestProcessLiveMessage_DuplicateRedeliveryYieldsOneRowOneWindowSlot(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	conversationExternalID := "live-dup-" + uuid.New().String()
	raw := ingestion.RawMessage{
		ExternalID: "native-" + uuid.New().String(), Platform: "whatsapp",
		ConversationID: conversationExternalID, SenderExternalID: "John",
		Timestamp: time.Now(), MediaType: ingestion.MediaTypeText, Text: textPtr("hello"),
	}

	if err := a.ProcessLiveMessage(ctx, raw); err != nil {
		t.Fatalf("ProcessLiveMessage (first delivery): %v", err)
	}
	if err := a.ProcessLiveMessage(ctx, raw); err != nil {
		t.Fatalf("ProcessLiveMessage (redelivery): %v", err)
	}

	conversationID := resolveConversationID(raw.Platform, raw.ConversationID)

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id = $2`,
		conversationID, raw.ExternalID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 stored row, got %d", rowCount)
	}

	a.liveWindowsMu.Lock()
	state := a.liveWindows[conversationID]
	pending := 0
	if state != nil {
		pending = len(state.pendingIDs)
	}
	a.liveWindowsMu.Unlock()
	if pending != 1 {
		t.Errorf("expected exactly 1 pending window slot, got %d", pending)
	}
}

// TestProcessLiveMessage_EmptyExternalIDNeverMerges covers requirement
// 18 (live path half): two messages with a genuinely empty ExternalID
// are each stored with external_id=NULL and a warning logged, and are
// NEVER treated as duplicates of each other.
func TestProcessLiveMessage_EmptyExternalIDNeverMerges(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	triager := &fakeTriager{candidate: false}
	extractor := &fakeExtractor{}
	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	conversationExternalID := "empty-extid-" + uuid.New().String()
	base := time.Now()
	for i := 0; i < 2; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "", Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: "John",
			Timestamp: base.Add(time.Duration(i) * time.Second), MediaType: ingestion.MediaTypeText, Text: textPtr("hi"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}

	conversationID := resolveConversationID("whatsapp", conversationExternalID)
	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND external_id IS NULL`,
		conversationID).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("expected exactly 2 separate rows (never merged), got %d", rowCount)
	}

	found := false
	for _, l := range logLines {
		if strings.Contains(l, "empty ExternalID") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a warning log line about the empty ExternalID, got: %v", logLines)
	}
}
