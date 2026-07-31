package memory

import (
	"math"
	"testing"
)

func TestApplySourceWeight_AllFiveStartingSourceTypes(t *testing.T) {
	const extracted = 0.9

	cases := []struct {
		sourceType string
		want       float64
	}{
		{SourceTypeWhatsAppText, 0.9},
		{SourceTypeVoiceTranscript, 0.855},
		{SourceTypeInstagramDM, 0.9},
		{SourceTypeInstagramCaption, 0.36},
		{SourceTypeInstagramStory, 0.18},
	}

	for _, c := range cases {
		t.Run(c.sourceType, func(t *testing.T) {
			got, err := ApplySourceWeight(extracted, c.sourceType)
			if err != nil {
				t.Fatalf("ApplySourceWeight(%v, %q): unexpected error: %v", extracted, c.sourceType, err)
			}
			if math.Abs(got-c.want) > floatTolerance {
				t.Errorf("ApplySourceWeight(%v, %q) = %v, want %v", extracted, c.sourceType, got, c.want)
			}
		})
	}
}

func TestApplySourceWeight_UnmappedSourceTypeErrors(t *testing.T) {
	_, err := ApplySourceWeight(0.9, "telegram_text")
	if err == nil {
		t.Fatal("expected an error for an unmapped source_type, got nil")
	}
}

func TestApplySourceWeight_UnmappedSourceTypeDoesNotSilentlyDefault(t *testing.T) {
	// Explicitly guard against the two wrong fallbacks named in the
	// requirement: silently returning 1.0 (overstates trust) or 0
	// (zeroes the edge) instead of erroring.
	got, err := ApplySourceWeight(0.9, "some_unlisted_source")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if got != 0 {
		t.Errorf("expected the returned float to be the zero value on error, got %v", got)
	}
}

func TestSourceWeights_HasExactlyFiveKeys(t *testing.T) {
	// Proves SourceWeights is a real, inspectable lookup structure — not
	// just a claim in a comment that the weights are "configurable."
	want := map[string]float64{
		SourceTypeWhatsAppText:     1.0,
		SourceTypeVoiceTranscript:  0.95,
		SourceTypeInstagramDM:      1.0,
		SourceTypeInstagramCaption: 0.4,
		SourceTypeInstagramStory:   0.2,
	}

	if len(SourceWeights) != len(want) {
		t.Fatalf("expected exactly %d entries in SourceWeights, got %d: %+v", len(want), len(SourceWeights), SourceWeights)
	}
	for sourceType, weight := range want {
		got, ok := SourceWeights[sourceType]
		if !ok {
			t.Errorf("expected SourceWeights to contain %q", sourceType)
			continue
		}
		if got != weight {
			t.Errorf("SourceWeights[%q] = %v, want %v", sourceType, got, weight)
		}
	}
}
