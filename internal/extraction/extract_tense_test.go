package extraction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// These tests exercise the full pipeline (BuildPrompt → Costguard call →
// ParseModelOutput → ValidateTriples) against a stub server returning a
// canned response for each tense scenario, using the realistic message
// phrasing from requirement 5. They prove the pipeline correctly carries a
// given tense classification through end to end and that
// event_past/event_present/event_future are all valid, accepted
// predicates — they do NOT prove the live model actually classifies real
// messages correctly, since the response here is canned, not generated.
// That's validated separately against a live qwen3-coder:30b run; see
// extraction_review.md for those results.
func stubCostguardServer(t *testing.T, content string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Content: content}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestExtractWindow_EventPastTense(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Marco","predicate":"event_past","object":"Rome","object_type":"literal","confidence":0.92}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		{Speaker: "Marco", Timestamp: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), Text: "went to Rome last month, it was amazing"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_past" {
		t.Fatalf("expected event_past to survive validation, got %+v", triples)
	}
}

func TestExtractWindow_EventPresentTense(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Sara","predicate":"event_present","object":"Beirut","object_type":"literal","confidence":0.9}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		{Speaker: "Sara", Timestamp: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), Text: "I live in Beirut now"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_present" {
		t.Fatalf("expected event_present to survive validation, got %+v", triples)
	}
}

func TestExtractWindow_EventPresentTense_OngoingJob(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Marco","predicate":"event_present","object":"still working at Costguard","object_type":"literal","confidence":0.85}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		{Speaker: "Marco", Timestamp: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), Text: "still working at Costguard"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_present" {
		t.Fatalf("expected event_present for an ongoing-status message, got %+v", triples)
	}
}

func TestExtractWindow_EventFutureTense(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Marco","predicate":"event_future","object":"Italy","object_type":"literal","confidence":0.88}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		{Speaker: "Marco", Timestamp: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), Text: "flying to Italy next week"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_future" {
		t.Fatalf("expected event_future to survive validation, got %+v", triples)
	}
}

func TestExtractWindow_EventFutureTense_NewJob(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Sara","predicate":"event_future","object":"starting the new job in March","object_type":"literal","confidence":0.9}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		{Speaker: "Sara", Timestamp: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC), Text: "starting the new job in March"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_future" {
		t.Fatalf("expected event_future for a stated future start date, got %+v", triples)
	}
}

// TestExtractWindow_AmbiguousTenseFallsBackToPresent exercises requirement
// 2/7's fallback rule: on genuinely ambiguous phrasing, the prompt
// instructs the model to prefer event_present over guessing past or
// future. This test simulates a model that correctly followed that
// instruction (the canned response is event_present) and asserts the
// pipeline accepts it — it does not itself prove a live model reliably
// follows the instruction on real ambiguous input, only that
// event_present is what's expected here and that the pipeline doesn't
// reject or mangle it.
func TestExtractWindow_AmbiguousTenseFallsBackToPresent(t *testing.T) {
	server := stubCostguardServer(t, `[{"subject":"Marco","predicate":"event_present","object":"thinking about Dubai","object_type":"literal","confidence":0.5}]`)
	extractor := NewExtractor(NewCostguardClient(server.URL))

	window := Window{Messages: []WindowMessage{
		// deliberately ambiguous: could be a settled plan (event_future),
		// something already decided and acted on (event_present), or just
		// idle talk with no clear tense at all
		{Speaker: "Marco", Timestamp: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), Text: "been talking about Dubai with the team"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 1 || triples[0].Predicate != "event_present" {
		t.Fatalf("expected the ambiguous-tense fallback (event_present) to survive validation, got %+v", triples)
	}
}
