package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Edge timestamps reflect when facts were SAID, not processed ---
//
// Before this fix: insertEdge omitted first_seen/last_reinforced from its
// INSERT (the DB default now() applied to every new edge), and
// storeExtractedTriples drove Reinforce() with the `now` parameter
// (processing time), not any message's own timestamp. A 2019 fact looked
// first-seen today at undecayed confidence. These tests exercise the fix
// through the real pipeline (ImportWhatsApp, ProcessLiveMessage,
// storeExtractedTriples) against real Postgres.

// writeExportFile writes a minimal WhatsApp .txt export with one line per
// message, all under the same sender — matches the exact format
// TestImportWhatsApp_MatchesLivePathOutput already established
// (write_pipeline_test.go): "D/M/YY, H:MM AM - Sender: text". dateStr and
// times are given pre-formatted so callers control the exact parsed
// values (internal/ingestion/whatsapp/export.go's parseExportTimestamp
// is DD/MM/YY(YY), no timezone, time.Local).
func writeExportFile(t *testing.T, path, sender string, dateStr string, lines []string) {
	t.Helper()
	var export strings.Builder
	for i, text := range lines {
		fmt.Fprintf(&export, "%s, 9:%02d AM - %s: %s\n", dateStr, i, sender, text)
	}
	if err := os.WriteFile(path, []byte(export.String()), 0o644); err != nil {
		t.Fatalf("write export file: %v", err)
	}
}

// expectedExportTimestamp mirrors internal/ingestion/whatsapp/export.go's
// parseExportTimestamp exactly (DD/MM/YY(YY), time.Local, no timezone) —
// used to compute the expected edge timestamp independently of the
// parser, so these tests catch a real regression rather than just
// re-asserting whatever the parser happens to produce.
func expectedExportTimestamp(year, month, day, hour, minute int) time.Time {
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.Local)
}

// TestImportWhatsApp_EdgeTimestampReflectsMessageDate covers requirement
// 11: importing a message dated 2019-03-14 must produce an edge whose
// FirstSeen/LastReinforced equal the message timestamps — not today's
// date (the bug), and not merely "within a day of now".
func TestImportWhatsApp_EdgeTimestampReflectsMessageDate(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subjectName := "Edge Timestamp Subject " + uuid.New().String()
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "likes", Object: "vinyl records", ObjectType: "literal", Confidence: 0.8},
	}}
	triager := &fakeTriager{candidate: true}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	dir := t.TempDir()
	exportPath := filepath.Join(dir, "Historical2019.txt")
	lines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range lines {
		lines[i] = fmt.Sprintf("message %d", i)
	}
	// 14/3/19 -> day=14, month=3, year=19->2019 (DD/MM/YY).
	writeExportFile(t, exportPath, "John", "14/3/19", lines)

	if _, err := a.ImportWhatsApp(ctx, exportPath); err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, subjectName)
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d (err=%v)", len(edges), err)
	}
	edge := edges[0]

	wantEarliest := expectedExportTimestamp(2019, 3, 14, 9, 0)
	wantLatest := expectedExportTimestamp(2019, 3, 14, 9, extraction.DefaultHistoricalWindowSize-1)

	if !edge.FirstSeen.Equal(wantEarliest) {
		t.Errorf("expected first_seen = %v (the message's own 2019 timestamp), got %v", wantEarliest, edge.FirstSeen)
	}
	if !edge.LastReinforced.Equal(wantLatest) {
		t.Errorf("expected last_reinforced = %v, got %v", wantLatest, edge.LastReinforced)
	}

	// The actual bug this fix closes: before it, both columns would be
	// today's date via the DB's now() default / the `now` processing-time
	// parameter — nowhere near a day of the real 2019 message date.
	if time.Since(edge.FirstSeen) < 300*24*time.Hour {
		t.Errorf("expected first_seen to be years in the past, got %v (only %v ago)", edge.FirstSeen, time.Since(edge.FirstSeen))
	}
}

// TestGetRelationshipHistory_ChronologicalOrderWithRealDates covers
// requirement 13: two edges for the same contact, imported from two
// different years, come back from GetRelationshipHistory in chronological
// order with their real dates — not import order, not "both look like
// today".
func TestGetRelationshipHistory_ChronologicalOrderWithRealDates(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contactName := "Timeline Contact " + uuid.New().String()
	triager := &fakeTriager{candidate: true}

	// Window 1: 2019.
	extractor2019 := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: contactName, Predicate: "lives_in", Object: "Dubai", ObjectType: "literal", Confidence: 0.8},
	}}
	a2019 := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor2019,
	})
	dir := t.TempDir()
	path2019 := filepath.Join(dir, "Timeline2019.txt")
	lines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range lines {
		lines[i] = fmt.Sprintf("message %d", i)
	}
	writeExportFile(t, path2019, "John", "14/3/19", lines)
	if _, err := a2019.ImportWhatsApp(ctx, path2019); err != nil {
		t.Fatalf("ImportWhatsApp (2019): %v", err)
	}

	// Window 2: 2023, same contact, a different predicate so it lands as
	// a SECOND edge rather than reinforcing the first.
	extractor2023 := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: contactName, Predicate: "works_at", Object: "Acme Corp", ObjectType: "literal", Confidence: 0.8},
	}}
	a2023 := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor2023,
	})
	path2023 := filepath.Join(dir, "Timeline2023.txt")
	writeExportFile(t, path2023, "John", "14/3/23", lines)
	if _, err := a2023.ImportWhatsApp(ctx, path2023); err != nil {
		t.Fatalf("ImportWhatsApp (2023): %v", err)
	}

	contact, err := ts.entities.GetByCanonicalName(ctx, contactName)
	if err != nil || contact == nil {
		t.Fatalf("contact entity not found: err=%v", err)
	}

	history, err := a2023.GetRelationshipHistory(ctx, contact.ID)
	if err != nil {
		t.Fatalf("GetRelationshipHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected exactly 2 edges in history, got %d", len(history))
	}
	if history[0].FirstSeen.Year() != 2019 || history[1].FirstSeen.Year() != 2023 {
		t.Errorf("expected chronological order [2019, 2023], got first_seen years [%d, %d]",
			history[0].FirstSeen.Year(), history[1].FirstSeen.Year())
	}
	if !history[0].FirstSeen.Before(history[1].FirstSeen) {
		t.Errorf("expected history[0] before history[1], got %v then %v", history[0].FirstSeen, history[1].FirstSeen)
	}
}

// TestGetRelevantContext_SinceFilterUsesFirstSeen covers requirement 14:
// GetRelevantContext's Since filter excludes/includes edges based on
// first_seen (memory.SearchFilters.Since -> "e.first_seen >= $3" in
// EdgeStore.SearchRelevant) — confirmed by reading search.go, stated here
// explicitly since the issue asked which field is actually filtered on.
// IncludePruned=true is used deliberately: the 2019 edge's confidence has
// genuinely decayed to near-zero by now with a multi-year gap, which
// would otherwise be excluded by the UNRELATED confidence/importance
// prune gate — this test isolates the Since filter itself, not pruning.
func TestGetRelevantContext_SinceFilterUsesFirstSeen(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	contactName := "Since Filter Contact " + uuid.New().String()
	triager := &fakeTriager{candidate: true}

	extractor2019 := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: contactName, Predicate: "lives_in", Object: "Dubai", ObjectType: "literal", Confidence: 0.8},
	}}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor2019,
	})
	dir := t.TempDir()
	lines := make([]string, extraction.DefaultHistoricalWindowSize)
	for i := range lines {
		lines[i] = fmt.Sprintf("message %d", i)
	}
	path2019 := filepath.Join(dir, "SinceFilter2019.txt")
	writeExportFile(t, path2019, "John", "14/3/19", lines)
	if _, err := a.ImportWhatsApp(ctx, path2019); err != nil {
		t.Fatalf("ImportWhatsApp (2019): %v", err)
	}

	extractorRecent := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: contactName, Predicate: "works_at", Object: "Acme Corp", ObjectType: "literal", Confidence: 0.8},
	}}
	aRecent := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractorRecent,
	})
	recentYear := time.Now().Year() - 1
	pathRecent := filepath.Join(dir, "SinceFilterRecent.txt")
	writeExportFile(t, pathRecent, "John", fmt.Sprintf("14/3/%02d", recentYear%100), lines)
	if _, err := aRecent.ImportWhatsApp(ctx, pathRecent); err != nil {
		t.Fatalf("ImportWhatsApp (recent): %v", err)
	}

	contact, err := ts.entities.GetByCanonicalName(ctx, contactName)
	if err != nil || contact == nil {
		t.Fatalf("contact entity not found: err=%v", err)
	}

	sinceAfter2019 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	afterResults, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &contact.ID, Since: &sinceAfter2019, IncludePruned: true}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext (since after 2019): %v", err)
	}
	assertContainsEdge(t, afterResults, mustGetEdgeID(t, ctx, ts, contact.ID, "lives_in"), false)
	assertContainsEdge(t, afterResults, mustGetEdgeID(t, ctx, ts, contact.ID, "works_at"), true)

	sinceBefore2019 := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	beforeResults, err := a.GetRelevantContext(ctx, "", Filters{SubjectID: &contact.ID, Since: &sinceBefore2019, IncludePruned: true}, 10)
	if err != nil {
		t.Fatalf("GetRelevantContext (since before 2019): %v", err)
	}
	assertContainsEdge(t, beforeResults, mustGetEdgeID(t, ctx, ts, contact.ID, "lives_in"), true)
	assertContainsEdge(t, beforeResults, mustGetEdgeID(t, ctx, ts, contact.ID, "works_at"), true)
}

func mustGetEdgeID(t *testing.T, ctx context.Context, ts *testStores, subjectID uuid.UUID, predicate string) uuid.UUID {
	t.Helper()
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subjectID, predicate)
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge for predicate %q, got %d (err=%v)", predicate, len(edges), err)
	}
	return edges[0].ID
}

// TestStoreExtractedTriples_OutOfOrderReinforcementNeverMovesBackward
// covers requirement 15: a newer message creates the edge, then an OLDER
// message reasserting the same fact reinforces it. The single resulting
// edge ends with first_seen = the older timestamp (pulled EARLIER) and
// last_reinforced = the newer timestamp (left alone, never pushed
// earlier) — EdgeStore.Reinforce's GREATEST/LEAST contract (Part 3).
func TestStoreExtractedTriples_OutOfOrderReinforcementNeverMovesBackward(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Out Of Order Subject "+uuid.New().String())
	predicate := "out_of_order_predicate_" + uuid.New().String()
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil)

	newer := time.Date(2023, 6, 1, 12, 0, 0, 0, time.UTC)
	older := time.Date(2019, 3, 14, 9, 0, 0, 0, time.UTC)

	newerMsg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", newer)
	triple := extraction.ExtractedTriple{Subject: subject.CanonicalName, Predicate: predicate, Object: "chess", ObjectType: "literal", Confidence: 0.8}

	createOutcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{triple}, memory.SourceTypeWhatsAppText, []uuid.UUID{newerMsg.ID}, newer)
	if err != nil {
		t.Fatalf("storeExtractedTriples (newer, create): %v", err)
	}
	if len(createOutcomes) != 1 || createOutcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected Created, got %+v", createOutcomes)
	}
	edgeID := createOutcomes[0].Edge.ID
	if !createOutcomes[0].Edge.FirstSeen.Equal(newer) || !createOutcomes[0].Edge.LastReinforced.Equal(newer) {
		t.Fatalf("expected the new edge to start at the newer timestamp, got first_seen=%v last_reinforced=%v",
			createOutcomes[0].Edge.FirstSeen, createOutcomes[0].Edge.LastReinforced)
	}

	olderMsg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", older)
	reinforceOutcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{triple}, memory.SourceTypeWhatsAppText, []uuid.UUID{olderMsg.ID}, newer)
	if err != nil {
		t.Fatalf("storeExtractedTriples (older, reinforce): %v", err)
	}
	if len(reinforceOutcomes) != 1 || reinforceOutcomes[0].Action != TripleActionReinforced {
		t.Fatalf("expected Reinforced, got %+v", reinforceOutcomes)
	}

	reloaded, err := ts.edges.GetByID(ctx, edgeID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if !reloaded.FirstSeen.Equal(older) {
		t.Errorf("expected first_seen pulled back to the older timestamp %v, got %v", older, reloaded.FirstSeen)
	}
	if !reloaded.LastReinforced.Equal(newer) {
		t.Errorf("expected last_reinforced to stay at the newer timestamp %v (never move backward), got %v", newer, reloaded.LastReinforced)
	}

	var rowCount int
	if err := ts.pool.QueryRow(ctx, `SELECT count(*) FROM edges WHERE subject_id = $1 AND predicate = $2`, subject.ID, predicate).Scan(&rowCount); err != nil {
		t.Fatalf("count edges: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("expected exactly 1 edge (reinforced, not duplicated), got %d", rowCount)
	}
}

// TestProcessLiveMessage_HistorySyncOldTimestampNotNow covers requirement
// 16: a Whatsmeow-style history-sync message with an old timestamp,
// delivered through the LIVE path (ProcessLiveMessage), produces an edge
// timestamped with that old date — not "now". storeExtractedTriples is
// deliberately NOT special-cased per caller (see its own doc comment), so
// this is really confirming the live path gets the same treatment the
// import path does, not a separate code path that happens to agree.
func TestProcessLiveMessage_HistorySyncOldTimestampNotNow(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subjectName := "History Sync Subject " + uuid.New().String()
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: subjectName, Predicate: "likes", Object: "vinyl records", ObjectType: "literal", Confidence: 0.8},
	}}
	triager := &fakeTriager{candidate: true}
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
	})

	conversationExternalID := "history-sync-" + uuid.New().String()
	historical := time.Date(2018, 7, 1, 9, 0, 0, 0, time.UTC)
	for i := 0; i < extraction.MaxLiveWindowMessages+1; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "hsmsg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: subjectName,
			Timestamp: historical.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeText, Text: textPtr("message"),
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}
	if extractor.calls != 1 {
		t.Fatalf("expected extraction called exactly once, got %d", extractor.calls)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, subjectName)
	if err != nil || subject == nil {
		t.Fatalf("subject entity not found: err=%v", err)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil || len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d (err=%v)", len(edges), err)
	}
	edge := edges[0]

	if edge.FirstSeen.Year() != 2018 {
		t.Errorf("expected first_seen in 2018, got %v", edge.FirstSeen)
	}
	if time.Since(edge.FirstSeen) < 300*24*time.Hour {
		t.Errorf("expected first_seen to be years in the past (live path must not use processing time), got %v", edge.FirstSeen)
	}
}

// TestStoreExtractedTriples_FutureDatedMessageIsClampedAndLogged covers
// requirement 17: a message timestamped in the future (the .txt export
// parser's own failure mode — no timezone, a guessed locale) must have
// its timestamp clamped to now when it feeds into an edge's own
// timestamps, with a log line naming it.
func TestStoreExtractedTriples_FutureDatedMessageIsClampedAndLogged(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	subject := createTestEntity(t, ctx, ts, "person", "Future Dated Subject "+uuid.New().String())
	predicate := "future_dated_predicate_" + uuid.New().String()

	now := time.Now()
	future := now.Add(48 * time.Hour)
	msg := createTestMessage(t, ctx, ts, uuid.New(), subject.ID, "text", future)

	var logLines []string
	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Logger: func(format string, args ...any) { logLines = append(logLines, fmt.Sprintf(format, args...)) },
	})

	triple := extraction.ExtractedTriple{Subject: subject.CanonicalName, Predicate: predicate, Object: "chess", ObjectType: "literal", Confidence: 0.8}
	outcomes, err := a.storeExtractedTriples(ctx, []extraction.ExtractedTriple{triple}, memory.SourceTypeWhatsAppText, []uuid.UUID{msg.ID}, now)
	if err != nil {
		t.Fatalf("storeExtractedTriples: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Action != TripleActionCreated {
		t.Fatalf("expected Created, got %+v", outcomes)
	}

	edge := outcomes[0].Edge
	if edge.FirstSeen.After(now.Add(time.Second)) {
		t.Errorf("expected first_seen clamped to ~now, got %v (now=%v)", edge.FirstSeen, now)
	}
	if edge.LastReinforced.After(now.Add(time.Second)) {
		t.Errorf("expected last_reinforced clamped to ~now, got %v (now=%v)", edge.LastReinforced, now)
	}

	found := false
	for _, l := range logLines {
		if strings.Contains(l, "clamping to now") && strings.Contains(l, msg.ID.String()) {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a log line naming the future-dated message being clamped, got: %v", logLines)
	}
}
