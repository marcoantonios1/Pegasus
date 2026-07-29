package extraction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExtractor_ExtractWindow_EndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Content: `[
					{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98},
					{"subject":"Sara","predicate":"hates","object":"cilantro","object_type":"literal","confidence":0.9}
				]`}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	var rejections []string
	extractor := &Extractor{
		Client: NewCostguardClient(server.URL),
		Logger: func(format string, args ...any) { rejections = append(rejections, format) },
	}

	window := Window{Messages: []WindowMessage{
		{Speaker: "Sara", Timestamp: time.Now(), Text: "I absolutely hate olives"},
	}}

	triples, err := extractor.ExtractWindow(context.Background(), window)
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}

	if len(triples) != 1 || triples[0].Predicate != "dislikes" {
		t.Fatalf("expected only the closed-vocabulary triple to survive, got %+v", triples)
	}
	if len(rejections) != 1 {
		t.Errorf("expected the \"hates\" triple to be rejected and logged, got %d rejections", len(rejections))
	}
}

func TestExtractor_ExtractWindow_EmptyResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Content: `[]`}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	extractor := NewExtractor(NewCostguardClient(server.URL))
	triples, err := extractor.ExtractWindow(context.Background(), Window{})
	if err != nil {
		t.Fatalf("ExtractWindow: %v", err)
	}
	if len(triples) != 0 {
		t.Errorf("expected no triples, got %+v", triples)
	}
}
