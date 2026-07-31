package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// audioExtensions is deliberately permissive — voice notes arrive in
// whatever format WhatsApp/the phone produced (commonly .ogg/.opus/.m4a).
var audioExtensions = map[string]bool{
	".ogg": true, ".opus": true, ".m4a": true, ".mp3": true, ".mp4": true,
	".wav": true, ".aac": true, ".flac": true, ".aiff": true, ".caf": true,
}

// segment mirrors the OpenAI-compatible verbose_json segment shape, which
// Speaches (the local faster-whisper-large-v3 server behind Costguard's
// AUDIO_TRANSCRIPTION_PROVIDER=local) confirmed it returns byte-for-byte
// compatible with real OpenAI Whisper output: avg_logprob, no_speech_prob,
// and compression_ratio are all present — this directly answers
// requirement 1's open question ("check the response shape... if Speaches
// doesn't surface segment-level confidence, that's a real gap to flag") —
// it does surface it, confirmed by hitting the live endpoint directly
// before writing this struct.
type segment struct {
	ID               int     `json:"id"`
	Start            float64 `json:"start"`
	End              float64 `json:"end"`
	Text             string  `json:"text"`
	AvgLogprob       float64 `json:"avg_logprob"`
	NoSpeechProb     float64 `json:"no_speech_prob"`
	CompressionRatio float64 `json:"compression_ratio"`
}

// word mirrors the per-word entries returned when the request includes
// timestamp_granularities[]=word. Speaches includes a "probability" field
// per word (confirmed live) — real OpenAI Whisper's API does NOT document
// a per-word probability field the same way; the report step notes this
// asymmetry explicitly rather than assuming both sources give the same
// shape.
type word struct {
	Start       float64 `json:"start"`
	End         float64 `json:"end"`
	Word        string  `json:"word"`
	Probability float64 `json:"probability"`
}

type transcriptionResponse struct {
	Task     string    `json:"task"`
	Language string    `json:"language"`
	Duration float64   `json:"duration"`
	Text     string    `json:"text"`
	Segments []segment `json:"segments"`
	Words    []word    `json:"words"`
}

// fileResult is one audio file's outcome for one leg (local or openai),
// serialized to the intermediate JSON that `report` later merges.
type fileResult struct {
	Filename  string                 `json:"filename"`
	Label     string                 `json:"label"`
	Response  *transcriptionResponse `json:"response,omitempty"`
	Error     string                 `json:"error,omitempty"`
	RequestMS int64                  `json:"request_ms"`
	Metrics   *transcriptionMetrics  `json:"metrics,omitempty"`
}

// transcriptionMetrics summarizes the raw response into single numbers for
// the review table. Derived here, not invented in the report step, so
// `report` only ever formats numbers that already exist in the JSON.
type transcriptionMetrics struct {
	MeanSegmentAvgLogprob float64 `json:"mean_segment_avg_logprob"`
	MeanNoSpeechProb      float64 `json:"mean_no_speech_prob"`
	MeanWordProbability   float64 `json:"mean_word_probability,omitempty"`
	MinWordProbability    float64 `json:"min_word_probability,omitempty"`
	HasWordProbabilities  bool    `json:"has_word_probabilities"`
}

func computeMetrics(r *transcriptionResponse) *transcriptionMetrics {
	if r == nil {
		return nil
	}
	m := &transcriptionMetrics{}

	if len(r.Segments) > 0 {
		var sumLogprob, sumNoSpeech float64
		for _, s := range r.Segments {
			sumLogprob += s.AvgLogprob
			sumNoSpeech += s.NoSpeechProb
		}
		m.MeanSegmentAvgLogprob = sumLogprob / float64(len(r.Segments))
		m.MeanNoSpeechProb = sumNoSpeech / float64(len(r.Segments))
	}

	if len(r.Words) > 0 {
		m.HasWordProbabilities = true
		sum := 0.0
		min := r.Words[0].Probability
		for _, w := range r.Words {
			sum += w.Probability
			if w.Probability < min {
				min = w.Probability
			}
		}
		m.MeanWordProbability = sum / float64(len(r.Words))
		m.MinWordProbability = min
	}

	return m
}

func runTranscribe(args []string) error {
	fs := flag.NewFlagSet("transcribe", flag.ExitOnError)
	dir := fs.String("dir", "", "directory of audio files")
	baseURL := fs.String("base-url", "http://localhost:8080", "Costguard base URL")
	label := fs.String("label", "local", "leg label recorded in the output (e.g. local, openai)")
	model := fs.String("model", "whisper-1", "model field sent in the request")
	out := fs.String("out", "", "output JSON path")
	agent := fs.String("agent", "pegasus-whisper-spotcheck", "X-Costguard-Agent attribution header")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *out == "" {
		return fmt.Errorf("--dir and --out are required")
	}

	entries, err := os.ReadDir(*dir)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if audioExtensions[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	if len(files) == 0 {
		return fmt.Errorf("no audio files found in %s (looked for extensions: ogg/opus/m4a/mp3/wav/aac/flac/aiff/caf)", *dir)
	}

	fmt.Printf("found %d audio files, transcribing via %s (label=%q)\n", len(files), *baseURL, *label)

	client := &http.Client{Timeout: 5 * time.Minute}
	var results []fileResult

	for i, name := range files {
		fmt.Printf("[%d/%d] %s ... ", i+1, len(files), name)
		start := time.Now()

		resp, err := transcribeFile(client, *baseURL, filepath.Join(*dir, name), *model, *agent)
		elapsed := time.Since(start).Milliseconds()

		fr := fileResult{Filename: name, Label: *label, RequestMS: elapsed}
		if err != nil {
			fr.Error = err.Error()
			fmt.Printf("ERROR: %v\n", err)
		} else {
			fr.Response = resp
			fr.Metrics = computeMetrics(resp)
			fmt.Printf("ok (%dms, %d chars)\n", elapsed, len(resp.Text))
		}
		results = append(results, fr)
	}

	outFile, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	enc := json.NewEncoder(outFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	fmt.Printf("wrote %s\n", *out)
	return nil
}

func transcribeFile(client *http.Client, baseURL, path, model, agent string) (*transcriptionResponse, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	part, err := mw.CreateFormFile("file", filepath.Base(path))
	if err != nil {
		return nil, fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, f); err != nil {
		return nil, fmt.Errorf("copy audio into request: %w", err)
	}
	_ = mw.WriteField("model", model)
	_ = mw.WriteField("response_format", "verbose_json")
	// Deliberately NOT requesting timestamp_granularities[]=word: verified
	// live against the real OpenAI API that doing so suppresses its
	// segment-level confidence entirely (segments comes back null) while
	// only returning placeholder word probabilities (always 0, not real
	// data) — see whisper_spotcheck_review.md. Segment-level avg_logprob/
	// no_speech_prob is what §5's escalation trigger is actually about
	// ("low per-segment transcription confidence"), and it's the one
	// signal both Speaches and real OpenAI reliably populate with
	// meaningful values, so that's what this harness relies on.
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/audio/transcriptions", &buf)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if agent != "" {
		req.Header.Set("X-Costguard-Agent", agent)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call costguard: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("costguard returned status %d: %s", resp.StatusCode, truncate(string(body), 500))
	}

	var result transcriptionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w (body: %s)", err, truncate(string(body), 500))
	}

	return &result, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
