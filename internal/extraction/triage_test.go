package extraction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTriageTestServer(t *testing.T, responseContent string) (*httptest.Server, *chatCompletionRequest) {
	t.Helper()
	var gotReq chatCompletionRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		resp := chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: responseContent}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	return server, &gotReq
}

func TestTriager_IsCandidate_UsesTriageModelNotExtractionModel(t *testing.T) {
	server, gotReq := newTriageTestServer(t, "CANDIDATE")
	defer server.Close()

	triager := NewTriager(NewCostguardClient(server.URL))
	got, err := triager.IsCandidate(context.Background(), "I'm training for a marathon in March")
	if err != nil {
		t.Fatalf("IsCandidate: %v", err)
	}
	if !got {
		t.Error("expected true for CANDIDATE response")
	}
	if gotReq.Model != TriageModel {
		t.Errorf("expected model %q, got %q", TriageModel, gotReq.Model)
	}
	if gotReq.Model == Model {
		t.Errorf("triage must never use the extraction model %q", Model)
	}
}

func TestTriager_IsCandidate_Noise(t *testing.T) {
	server, _ := newTriageTestServer(t, "NOISE")
	defer server.Close()

	triager := NewTriager(NewCostguardClient(server.URL))
	got, err := triager.IsCandidate(context.Background(), "lol ok")
	if err != nil {
		t.Fatalf("IsCandidate: %v", err)
	}
	if got {
		t.Error("expected false for NOISE response")
	}
}

func TestTriager_IsCandidate_UnparseableDefaultsFalse(t *testing.T) {
	server, _ := newTriageTestServer(t, "uh, maybe? hard to say")
	defer server.Close()

	triager := NewTriager(NewCostguardClient(server.URL))
	got, err := triager.IsCandidate(context.Background(), "something ambiguous")
	if err != nil {
		t.Fatalf("IsCandidate: %v", err)
	}
	if got {
		t.Error("expected an unparseable triage response to default to false (not a candidate), per Triager's documented fail-cheap behavior")
	}
}

func TestTriager_IsCandidate_CaseInsensitiveAndWhitespaceTolerant(t *testing.T) {
	server, _ := newTriageTestServer(t, "  candidate\n")
	defer server.Close()

	triager := NewTriager(NewCostguardClient(server.URL))
	got, err := triager.IsCandidate(context.Background(), "text")
	if err != nil {
		t.Fatalf("IsCandidate: %v", err)
	}
	if !got {
		t.Error("expected lowercase/whitespace-padded \"candidate\" to still parse as true")
	}
}
