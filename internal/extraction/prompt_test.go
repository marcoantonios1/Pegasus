package extraction

import (
	"strings"
	"testing"
	"time"
)

func TestBuildPrompt_ContainsAllPredicatesAndMessages(t *testing.T) {
	vocab := []Predicate{
		{Name: "likes", Category: "preferences", Description: "subject has a positive preference for object"},
		{Name: "works_at", Category: "work_education", Description: "subject is employed at object"},
	}
	w := Window{Messages: []WindowMessage{
		{Speaker: "Sara", Timestamp: time.Date(2026, 1, 5, 9, 1, 15, 0, time.UTC), Text: "I absolutely hate olives"},
		{Speaker: "Marco", Timestamp: time.Date(2026, 1, 5, 9, 1, 40, 0, time.UTC), Text: "noted"},
	}}

	prompt, err := BuildPrompt(w, vocab)
	if err != nil {
		t.Fatalf("BuildPrompt: %v", err)
	}

	for _, p := range vocab {
		if !strings.Contains(prompt, p.Name) {
			t.Errorf("expected prompt to contain predicate name %q", p.Name)
		}
		if !strings.Contains(prompt, p.Description) {
			t.Errorf("expected prompt to contain predicate description %q", p.Description)
		}
	}

	if !strings.Contains(prompt, "Sara") || !strings.Contains(prompt, "I absolutely hate olives") {
		t.Error("expected prompt to contain the first message's speaker and text")
	}
	if !strings.Contains(prompt, "Marco") || !strings.Contains(prompt, "noted") {
		t.Error("expected prompt to contain the second message's speaker and text")
	}
	if !strings.Contains(prompt, "2026-01-05 09:01:15") {
		t.Error("expected prompt to contain a formatted timestamp")
	}

	if !strings.Contains(prompt, "Output ONLY a JSON array") {
		t.Error("expected prompt to carry the §8.2 JSON-only instruction")
	}
	if !strings.Contains(prompt, `"object_type": "entity" | "literal"`) {
		t.Error("expected prompt to specify the exact output shape from §8.2")
	}
}

func TestBuildPrompt_EmptyWindowStillProducesValidPrompt(t *testing.T) {
	prompt, err := BuildPrompt(Window{}, Predicates)
	if err != nil {
		t.Fatalf("BuildPrompt: %v", err)
	}
	if !strings.Contains(prompt, "output exactly: []") {
		t.Error("expected prompt to instruct [] for nothing extractable")
	}
}
