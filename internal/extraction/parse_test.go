package extraction

import (
	"testing"
)

func TestParseModelOutput_PlainJSON(t *testing.T) {
	raw := `[{"subject":"Sara","predicate":"dislikes","object":"olives","object_type":"literal","confidence":0.98}]`

	triples, err := ParseModelOutput(raw)
	if err != nil {
		t.Fatalf("ParseModelOutput: %v", err)
	}
	if len(triples) != 1 {
		t.Fatalf("expected 1 triple, got %d", len(triples))
	}
	want := ExtractedTriple{Subject: "Sara", Predicate: "dislikes", Object: "olives", ObjectType: "literal", Confidence: 0.98}
	if triples[0] != want {
		t.Errorf("got %+v, want %+v", triples[0], want)
	}
}

func TestParseModelOutput_EmptyArray(t *testing.T) {
	triples, err := ParseModelOutput("[]")
	if err != nil {
		t.Fatalf("ParseModelOutput: %v", err)
	}
	if len(triples) != 0 {
		t.Errorf("expected 0 triples, got %d", len(triples))
	}
}

func TestParseModelOutput_StripsMarkdownCodeFence(t *testing.T) {
	raw := "```json\n[{\"subject\":\"Marco\",\"predicate\":\"likes\",\"object\":\"Go\",\"object_type\":\"literal\",\"confidence\":0.9}]\n```"

	triples, err := ParseModelOutput(raw)
	if err != nil {
		t.Fatalf("ParseModelOutput: %v", err)
	}
	if len(triples) != 1 || triples[0].Subject != "Marco" {
		t.Fatalf("expected code fence to be stripped and parsed, got %+v", triples)
	}
}

func TestParseModelOutput_InvalidJSONReturnsError(t *testing.T) {
	_, err := ParseModelOutput("not json at all, the model ignored the instruction")
	if err == nil {
		t.Fatal("expected an error for malformed output, got nil")
	}
}

func TestValidateTriples_RejectsUnknownPredicate(t *testing.T) {
	var rejected []string
	logf := func(format string, args ...any) { rejected = append(rejected, format) }

	in := []ExtractedTriple{
		{Subject: "Sara", Predicate: "hates", Object: "olives", ObjectType: "literal", Confidence: 0.9}, // not in vocab
		{Subject: "Sara", Predicate: "dislikes", Object: "olives", ObjectType: "literal", Confidence: 0.9},
	}

	out := ValidateTriples(in, logf)
	if len(out) != 1 || out[0].Predicate != "dislikes" {
		t.Fatalf("expected only the valid-predicate triple to survive, got %+v", out)
	}
	if len(rejected) != 1 {
		t.Errorf("expected exactly 1 rejection logged, got %d", len(rejected))
	}
}

func TestValidateTriples_RejectsBadObjectType(t *testing.T) {
	in := []ExtractedTriple{
		{Subject: "Marco", Predicate: "likes", Object: "Go", ObjectType: "concept", Confidence: 0.9},
	}
	out := ValidateTriples(in, func(string, ...any) {})
	if len(out) != 0 {
		t.Fatalf("expected the triple with an invalid object_type to be rejected, got %+v", out)
	}
}

func TestValidateTriples_RejectsEmptySubjectOrObject(t *testing.T) {
	in := []ExtractedTriple{
		{Subject: "", Predicate: "likes", Object: "Go", ObjectType: "literal", Confidence: 0.9},
		{Subject: "Marco", Predicate: "likes", Object: "", ObjectType: "literal", Confidence: 0.9},
	}
	out := ValidateTriples(in, func(string, ...any) {})
	if len(out) != 0 {
		t.Fatalf("expected both triples to be rejected for empty subject/object, got %+v", out)
	}
}

func TestValidateTriples_RejectsOutOfRangeConfidence(t *testing.T) {
	in := []ExtractedTriple{
		{Subject: "Marco", Predicate: "likes", Object: "Go", ObjectType: "literal", Confidence: 1.5},
		{Subject: "Marco", Predicate: "likes", Object: "Rust", ObjectType: "literal", Confidence: -0.1},
	}
	out := ValidateTriples(in, func(string, ...any) {})
	if len(out) != 0 {
		t.Fatalf("expected both out-of-range-confidence triples to be rejected, got %+v", out)
	}
}

func TestValidateTriples_DefaultLoggerDoesNotPanic(t *testing.T) {
	in := []ExtractedTriple{{Subject: "Sara", Predicate: "hates", Object: "olives", ObjectType: "literal", Confidence: 0.9}}
	out := ValidateTriples(in, nil) // nil logger should fall back, not panic
	if len(out) != 0 {
		t.Fatalf("expected the invalid triple to be rejected, got %+v", out)
	}
}
