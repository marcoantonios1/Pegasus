package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

// --- Path traversal fix: localExportAudioFetcher ---
//
// These test localExportAudioFetcher directly (bypassing
// whatsapp.ExportParser's own, separate rejection — see
// internal/ingestion/whatsapp/export_test.go's
// TestParseExport_RejectsPathTraversalInAttachedFilename for that layer)
// specifically because the fetcher's own containment must hold
// independently of whatever the parser allows through — defense in
// depth, not "the regex upstream is airtight so this never sees bad
// input."

// writeTestFile is a small helper: write content to path, creating
// parent directories as needed.
func writeTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestLocalExportAudioFetcher_RejectsPathTraversal uses the exact
// reproducer pattern from the path-traversal audit: a mediaRef like
// "../../private/x.m4a" that, before this fix, let
// os.ReadFile(filepath.Join(dir, mediaRef)) read a file anywhere on disk
// reachable by relative traversal from the export directory.
func TestLocalExportAudioFetcher_RejectsPathTraversal(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatalf("mkdir export dir: %v", err)
	}

	secretContent := []byte("this must never be read by the voice import pipeline")
	writeTestFile(t, filepath.Join(root, "private", "x.m4a"), secretContent)

	fetcher := localExportAudioFetcher(exportDir)
	data, err := fetcher(context.Background(), "../private/x.m4a")
	if err == nil {
		t.Fatalf("expected an error for a path-traversal media_ref, got data: %q", data)
	}
	if string(data) == string(secretContent) {
		t.Fatal("the secret file's content was returned — path traversal succeeded")
	}
}

// TestLocalExportAudioFetcher_RejectsAbsolutePath covers requirement 2's
// explicit note: not every traversal attempt contains ".." — an
// already-absolute media_ref must be rejected too, not just one with a
// ".." substring.
func TestLocalExportAudioFetcher_RejectsAbsolutePath(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "export")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatalf("mkdir export dir: %v", err)
	}

	secretContent := []byte("absolute-path secret")
	secretPath := filepath.Join(root, "private", "secret.m4a")
	writeTestFile(t, secretPath, secretContent)

	fetcher := localExportAudioFetcher(exportDir)
	data, err := fetcher(context.Background(), secretPath) // an absolute path, no ".." anywhere in it
	if err == nil {
		t.Fatalf("expected an error for an absolute-path media_ref, got data: %q", data)
	}
	if string(data) == string(secretContent) {
		t.Fatal("the secret file's content was returned via an absolute path — containment failed")
	}
}

// TestLocalExportAudioFetcher_RejectsBackslashPath covers the
// belt-and-suspenders path-separator check's Windows-style variant,
// since a bare filename should never contain either separator.
func TestLocalExportAudioFetcher_RejectsBackslashPath(t *testing.T) {
	exportDir := t.TempDir()
	fetcher := localExportAudioFetcher(exportDir)
	if _, err := fetcher(context.Background(), `..\private\x.m4a`); err == nil {
		t.Fatal("expected an error for a media_ref containing a backslash, got nil")
	}
}

// TestLocalExportAudioFetcher_LegitimateFilenameStillWorks is the
// required regression check: a normal "with media" import — a bare
// filename sitting alongside the export — must still work correctly
// after this fix.
func TestLocalExportAudioFetcher_LegitimateFilenameStillWorks(t *testing.T) {
	exportDir := t.TempDir()
	content := []byte("real audio bytes")
	writeTestFile(t, filepath.Join(exportDir, "PTT-20260702-WA0011.opus"), content)

	fetcher := localExportAudioFetcher(exportDir)
	data, err := fetcher(context.Background(), "PTT-20260702-WA0011.opus")
	if err != nil {
		t.Fatalf("expected a legitimate bare filename to be read successfully, got error: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("expected content %q, got %q", content, data)
	}
}

// TestLocalExportAudioFetcher_NestedTraversalStillRejected covers a
// deeper traversal (the issue's own literal reproducer depth,
// "../../private/x.m4a") against an export dir nested one level deeper,
// so the "../.." actually needs two hops to escape — confirms this isn't
// just catching a single "../" specifically.
func TestLocalExportAudioFetcher_NestedTraversalStillRejected(t *testing.T) {
	root := t.TempDir()
	exportDir := filepath.Join(root, "exports", "2026-07")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatalf("mkdir nested export dir: %v", err)
	}
	secretContent := []byte("two-hops-up secret")
	writeTestFile(t, filepath.Join(root, "private", "x.m4a"), secretContent)

	fetcher := localExportAudioFetcher(exportDir)
	data, err := fetcher(context.Background(), "../../private/x.m4a")
	if err == nil {
		t.Fatalf("expected an error for a nested path-traversal media_ref, got data: %q", data)
	}
	if string(data) == string(secretContent) {
		t.Fatal("the secret file's content was returned — nested path traversal succeeded")
	}
}

// TestImportWhatsApp_PathTraversalAttachmentDoesNotLeakFile is the
// fully end-to-end version of the audit's reproducer: a real export .txt
// file, using the exact literal line pattern from the audit
// ("../../private/x.m4a (file attached)"), run through the real
// ImportWhatsApp call — not just its two components in isolation. A
// legitimate voice attachment in the SAME import must still work
// (requirement 5's regression check), proving the fix doesn't break
// normal "with media" imports while closing the hole.
func TestImportWhatsApp_PathTraversalAttachmentDoesNotLeakFile(t *testing.T) {
	ts := newTestStores(t)
	ctx := context.Background()

	root := t.TempDir()
	exportDir := filepath.Join(root, "WhatsApp Chat with Mallory")
	if err := os.MkdirAll(exportDir, 0o755); err != nil {
		t.Fatalf("mkdir export dir: %v", err)
	}
	secretContent := []byte("private data that must never reach the transcription pipeline")
	writeTestFile(t, filepath.Join(root, "private", "x.m4a"), secretContent)

	// uuid-suffixed: this suite runs against a real, persistent, shared
	// dev database with no per-test reset (see api_test.go's
	// randomBasisIndex doc comment for the established reasoning) — a
	// fixed filename collides with leftover rows media_ref = the same
	// string from earlier runs of this same test.
	legitFilename := "PTT-legit-voice-note-" + uuid.New().String() + ".opus"
	writeTestFile(t, filepath.Join(exportDir, legitFilename), []byte("legit audio bytes"))

	exportPath := filepath.Join(exportDir, "export.txt")
	export := "1/1/26, 9:00 AM - Mallory: ../../private/x.m4a (file attached)\n" +
		fmt.Sprintf("1/1/26, 9:01 AM - Mallory: %s (file attached)\n", legitFilename)
	writeTestFile(t, exportPath, []byte(export))

	transcriber := &fakeTranscriber{responses: []fakeTranscribeResult{
		{resp: segResp("en", -0.1)}, // for the one legitimate voice attachment
	}}
	triager := &fakeTriager{candidate: true}
	extractor := &fakeExtractor{}

	a := New(ts.edges, ts.messages, ts.entities, ts.embeds, ts.relStats, nil).WithPipeline(PipelineDeps{
		Triager: triager, Extractor: extractor, Transcriber: transcriber,
	})

	result, err := a.ImportWhatsApp(ctx, exportPath)
	if err != nil {
		t.Fatalf("expected ImportWhatsApp to complete despite the path-traversal attachment, got: %v", err)
	}

	// The traversal attempt must have been dropped by the parser before
	// ever reaching the fetcher — confirm it didn't become a stored
	// message carrying the secret file's path or content anywhere.
	var transcriptCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM messages
		WHERE transcript = $1 OR media_ref = '../../private/x.m4a'
	`, string(secretContent)).Scan(&transcriptCount); err != nil {
		t.Fatalf("query messages: %v", err)
	}
	if transcriptCount != 0 {
		t.Fatal("found a stored message referencing the path-traversal attachment or the secret content — leak occurred")
	}

	// The legitimate attachment in the same import must still have been
	// transcribed successfully.
	var legitTranscriptCount int
	if err := ts.pool.QueryRow(ctx, `
		SELECT count(*) FROM messages WHERE media_ref = $1 AND transcript IS NOT NULL
	`, legitFilename).Scan(&legitTranscriptCount); err != nil {
		t.Fatalf("query legit message: %v", err)
	}
	if legitTranscriptCount != 1 {
		t.Errorf("expected the legitimate voice attachment to still transcribe successfully, got count=%d", legitTranscriptCount)
	}

	// The traversal line should never have become a RawMessage at all —
	// classifyExportBody's own path-separator rejection (requirement 4's
	// second layer) drops it at parse time, before ImportWhatsApp's loop
	// (and thus localExportAudioFetcher) ever sees it.
	if result.MessagesParsed != 1 {
		t.Errorf("expected only the legitimate attachment to have been parsed into a message, got MessagesParsed=%d", result.MessagesParsed)
	}
}
