package extraction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCostguardClient_Complete(t *testing.T) {
	var gotReq chatCompletionRequest
	var gotAgentHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		gotAgentHeader = r.Header.Get("X-Costguard-Agent")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: `[{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98}]`}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	out, err := client.Complete(context.Background(), "extract from this conversation")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if gotReq.Model != Model {
		t.Errorf("expected model %q, got %q", Model, gotReq.Model)
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" || gotReq.Messages[0].Content != "extract from this conversation" {
		t.Errorf("unexpected request messages: %+v", gotReq.Messages)
	}
	if gotAgentHeader != "pegasus" {
		t.Errorf("expected X-Costguard-Agent: pegasus, got %q", gotAgentHeader)
	}
	if out != `[{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98}]` {
		t.Errorf("unexpected output: %q", out)
	}
}

func TestCostguardClient_Complete_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("upstream provider unavailable"))
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected an error for a non-200 response, got nil")
	}
}

func TestCostguardClient_Complete_NoChoices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(chatCompletionResponse{})
	}))
	defer server.Close()

	client := NewCostguardClient(server.URL)
	_, err := client.Complete(context.Background(), "test")
	if err == nil {
		t.Fatal("expected an error when the response has no choices, got nil")
	}
}
