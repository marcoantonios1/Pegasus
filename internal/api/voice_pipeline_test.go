package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion"
	"github.com/marcoantonios1/Pegasus/internal/memory"
)

// --- Requirement 8: full voice -> transcript -> triage -> extraction path ---

// TestProcessLiveMessage_VoiceWindowTranscribedThenExtracted covers
// requirement 5's live path: a burst of voice messages must each get
// transcribed (transcript/transcript_confidence persisted) BEFORE
// triage/windowing, then feed the exact same triage -> extraction ->
// storeExtractedTriples path text messages use, ending with an edge whose
// source_type is memory.SourceTypeVoiceTranscript (not whatsapp_text) —
// proving the earlier-stage insertion point, not just that text still
// works.
func TestProcessLiveMessage_VoiceWindowTranscribedThenExtracted(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	transcriber := &fakeTranscriber{}
	// One local, high-confidence English response per message — 11 calls,
	// no escalation.
	for i := 0; i < 11; i++ {
		transcriber.responses = append(transcriber.responses, fakeTranscribeResult{resp: segResp("en", -0.1)})
	}

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Voice Window Contact " + uuid.New().String(), Predicate: "likes", Object: "hiking", ObjectType: "literal", Confidence: 0.8},
	}}
	subjectName := extractor.triples[0].Subject

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
		Transcriber:  transcriber,
		AudioFetcher: fixedAudioFetcher([]byte("fake audio bytes")),
	})

	conversationExternalID := "voice-conv-" + uuid.New().String()
	senderExternalID := "voice-sender-" + uuid.New().String()
	base := time.Now()
	mediaRef := "PTT-voice-note.opus"

	for i := 0; i < 11; i++ {
		raw := ingestion.RawMessage{
			ExternalID: "vmsg-" + uuid.New().String(), Platform: "whatsapp",
			ConversationID: conversationExternalID, SenderExternalID: senderExternalID,
			Timestamp: base.Add(time.Duration(i) * time.Second),
			MediaType: ingestion.MediaTypeVoice, MediaURL: &mediaRef,
		}
		if err := a.ProcessLiveMessage(ctx, raw); err != nil {
			t.Fatalf("ProcessLiveMessage(%d): %v", i, err)
		}
	}

	if extractor.calls != 1 {
		t.Fatalf("expected extraction called exactly once for an 11-message voice burst, got %d", extractor.calls)
	}

	subject, err := ts.entities.GetByCanonicalName(ctx, subjectName)
	if err != nil || subject == nil {
		t.Fatalf("expected subject entity to have been created, err=%v subject=%v", err, subject)
	}
	edges, err := ts.edges.GetBySubjectAndPredicate(ctx, subject.ID, "likes")
	if err != nil {
		t.Fatalf("GetBySubjectAndPredicate: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected exactly 1 edge, got %d", len(edges))
	}
	edge := edges[0]
	if edge.SourceType != memory.SourceTypeVoiceTranscript {
		t.Errorf("expected source_type %q, got %q", memory.SourceTypeVoiceTranscript, edge.SourceType)
	}
	wantConfidence := 0.8 * memory.SourceWeights[memory.SourceTypeVoiceTranscript]
	if edge.Confidence != wantConfidence {
		t.Errorf("expected confidence %v (0.8 * voice_transcript source_weight), got %v", wantConfidence, edge.Confidence)
	}

	// Every stored voice message in the closed window must carry a
	// persisted transcript/transcript_confidence — transcription happened
	// BEFORE triage/windowing, not skipped.
	convID := resolveConversationID("whatsapp", conversationExternalID)
	rows, err := ts.pool.Query(ctx, `SELECT id, transcript, transcript_confidence, processed FROM messages WHERE conversation_id = $1 ORDER BY timestamp ASC`, convID)
	if err != nil {
		t.Fatalf("query messages: %v", err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var id uuid.UUID
		var transcript *string
		var confidence *float64
		var processed bool
		if err := rows.Scan(&id, &transcript, &confidence, &processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
		if transcript == nil || *transcript != "transcribed text" {
			t.Errorf("message %s: expected transcript %q, got %v", id, "transcribed text", transcript)
		}
		if confidence == nil || *confidence != -0.1 {
			t.Errorf("message %s: expected transcript_confidence -0.1, got %v", id, confidence)
		}
	}
	if count != 11 {
		t.Fatalf("expected 11 stored voice messages, got %d", count)
	}
}

// TestProcessLiveMessage_VoiceTranscriptionFailure_MessageStaysUnprocessed
// covers requirement 6's explicit failure behavior at the live path: a
// voice message whose transcription fails entirely (both local and
// escalated) must stay stored with processed=false and transcript=NULL —
// discoverable/re-attemptable, not silently dropped, and must not error
// out ProcessLiveMessage itself (single bad audio file must not abort the
// caller).
func TestProcessLiveMessage_VoiceTranscriptionFailure_MessageStaysUnprocessed(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{err: fmt.Errorf("local speaches unreachable")},
		{err: fmt.Errorf("openai unreachable")},
	}}
	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
		Transcriber:  transcriber,
		AudioFetcher: fixedAudioFetcher([]byte("corrupt audio")),
	})

	mediaRef := "corrupt-note.opus"
	raw := ingestion.RawMessage{
		ExternalID: "vmsg-" + uuid.New().String(), Platform: "whatsapp",
		ConversationID: "voice-fail-conv-" + uuid.New().String(), SenderExternalID: "voice-fail-sender",
		Timestamp: time.Now(), MediaType: ingestion.MediaTypeVoice, MediaURL: &mediaRef,
	}

	if err := a.ProcessLiveMessage(ctx, raw); err != nil {
		t.Fatalf("expected ProcessLiveMessage to return nil (not error) on a single voice transcription failure, got: %v", err)
	}
	if extractor.calls != 0 {
		t.Errorf("expected extraction never called for a message that never got transcribed, got %d calls", extractor.calls)
	}

	convID := resolveConversationID("whatsapp", raw.ConversationID)
	rows, err := ts.pool.Query(ctx, `SELECT transcript, transcript_confidence, processed FROM messages WHERE conversation_id = $1`, convID)
	if err != nil {
		t.Fatalf("query message: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected the voice message to still be stored despite transcription failure")
	}
	var transcript *string
	var confidence *float64
	var processed bool
	if err := rows.Scan(&transcript, &confidence, &processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if transcript != nil {
		t.Errorf("expected transcript to remain NULL after total transcription failure, got %q", *transcript)
	}
	if confidence != nil {
		t.Errorf("expected transcript_confidence to remain NULL after total transcription failure, got %v", *confidence)
	}
	if processed {
		t.Error("expected processed=false after total transcription failure (discoverable/re-attemptable), got true")
	}
}

// TestImportWhatsApp_VoiceMessagesTranscribedAndWindowed covers requirement
// 5's bulk path: a "with media" WhatsApp export referencing sibling voice
// files on disk must have each one transcribed via the AudioFetcher
// ImportWhatsApp builds internally (scoped to the export's own directory),
// before windowing/triage/extraction — same shared path as text.
func TestImportWhatsApp_VoiceMessagesTranscribedAndWindowed(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	windowSize := extraction.DefaultHistoricalWindowSize
	transcriber := &fakeTranscriber{}
	for i := 0; i < windowSize; i++ {
		transcriber.responses = append(transcriber.responses, fakeTranscribeResult{resp: segResp("en", -0.1)})
	}

	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{triples: []extraction.ExtractedTriple{
		{Subject: "Bulk Voice Contact " + uuid.New().String(), Predicate: "likes", Object: "sailing", ObjectType: "literal", Confidence: 0.75},
	}}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor,
		Transcriber: transcriber,
		// Deliberately no AudioFetcher here — ImportWhatsApp must build its
		// own, scoped to exportPath's directory, not rely on one supplied
		// via PipelineDeps (which live-path ProcessLiveMessage uses instead).
	})

	dir := t.TempDir()
	exportPath := filepath.Join(dir, "BulkVoiceConv.txt")

	var export string
	for i := 0; i < windowSize; i++ {
		filename := fmt.Sprintf("PTT-note-%d.opus", i)
		if err := os.WriteFile(filepath.Join(dir, filename), []byte("fake audio bytes"), 0o644); err != nil {
			t.Fatalf("write sibling audio file: %v", err)
		}
		export += fmt.Sprintf("1/1/26, 9:0%d AM - John: %s (file attached)\n", i, filename)
	}
	if err := os.WriteFile(exportPath, []byte(export), 0o644); err != nil {
		t.Fatalf("write export file: %v", err)
	}

	result, err := a.ImportWhatsApp(ctx, exportPath)
	if err != nil {
		t.Fatalf("ImportWhatsApp: %v", err)
	}
	if result.MessagesParsed != windowSize {
		t.Errorf("expected MessagesParsed=%d, got %d", windowSize, result.MessagesParsed)
	}
	if result.MessagesTextExtractable != windowSize {
		t.Errorf("expected all %d voice messages to transcribe successfully and be windowed, got MessagesTextExtractable=%d", windowSize, result.MessagesTextExtractable)
	}
	if extractor.calls != 1 {
		t.Fatalf("expected extraction called exactly once for one full window, got %d", extractor.calls)
	}
	if len(result.Windows) != 1 || !result.Windows[0].IsCandidate {
		t.Fatalf("expected exactly 1 candidate window, got %+v", result.Windows)
	}
	if len(result.Windows[0].Outcomes) != 1 || result.Windows[0].Outcomes[0].Action != TripleActionCreated {
		t.Errorf("expected 1 created-triple outcome, got %+v", result.Windows[0].Outcomes)
	}

	var transcriptCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM messages WHERE media_type = 'voice' AND transcript IS NOT NULL AND processed = true
	`).Scan(&transcriptCount); err != nil {
		t.Fatalf("count transcribed voice messages: %v", err)
	}
	if transcriptCount < windowSize {
		t.Errorf("expected at least %d processed voice messages with a persisted transcript, got %d", windowSize, transcriptCount)
	}
}

// TestImportWhatsApp_VoiceTranscriptionFailure_ContinuesRestOfImport covers
// requirement 6 at the bulk path: one voice message that fails
// transcription entirely must not abort the whole import — it's stored,
// left unprocessed, and skipped from windowing, while the rest of the
// conversation still imports normally.
func TestImportWhatsApp_VoiceTranscriptionFailure_ContinuesRestOfImport(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{err: fmt.Errorf("local speaches unreachable")},
		{err: fmt.Errorf("openai unreachable")},
	}}
	triager := &fakeTriager{candidate: false}
	extractor := &fakeExtractor{}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, Transcriber: transcriber,
	})

	dir := t.TempDir()
	exportPath := filepath.Join(dir, "OneBadVoice.txt")
	// No sibling audio file written on disk at all — the AudioFetcher
	// itself will fail to read it, which is a valid (and simpler) way to
	// force transcribeAndStore's fetch-failure path without needing the
	// fakeTranscriber's error queue to line up with fetch order.
	export := "1/1/26, 9:00 AM - John: missing-note.opus (file attached)\n" +
		"1/1/26, 9:01 AM - John: a perfectly normal text message\n"
	if err := os.WriteFile(exportPath, []byte(export), 0o644); err != nil {
		t.Fatalf("write export file: %v", err)
	}

	result, err := a.ImportWhatsApp(ctx, exportPath)
	if err != nil {
		t.Fatalf("expected ImportWhatsApp to succeed despite one voice message's transcription failure, got: %v", err)
	}
	if result.MessagesParsed != 2 {
		t.Fatalf("expected 2 parsed messages, got %d", result.MessagesParsed)
	}
	// Only the text message should have made it into windowing — the
	// voice message's fetch failed before transcription was even
	// attempted.
	if result.MessagesTextExtractable != 1 {
		t.Errorf("expected 1 windowed message (the text one; voice failed to fetch), got %d", result.MessagesTextExtractable)
	}

	var processed bool
	var transcript *string
	if err := ts.pool.QueryRow(ctx, `SELECT processed, transcript FROM messages WHERE media_type = 'voice'`).Scan(&processed, &transcript); err != nil {
		t.Fatalf("query voice message: %v", err)
	}
	if processed {
		t.Error("expected the failed voice message to remain processed=false")
	}
	if transcript != nil {
		t.Errorf("expected the failed voice message's transcript to remain NULL, got %q", *transcript)
	}
}
