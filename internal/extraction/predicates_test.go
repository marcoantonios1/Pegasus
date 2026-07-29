package extraction

import "testing"

func TestIsValidPredicate(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"likes", true},
		{"dislikes", true},
		{"event_past", true},
		{"event_present", true},
		{"event_future", true},
		{"works_at", true},
		{"studied_at", true},
		{"role_is", true},
		{"routine_is", true},
		{"goal_is", true},
		{"building", true},
		{"family_member_of", true},
		{"pet_of", true},
		{"nickname_is", true},
		{"inside_joke_ref", true},
		{"hates", false},     // synonym of dislikes — exactly what closing the vocab prevents
		{"has_event", false}, // the pre-split generic predicate §8.5 replaced
		{"", false},
		{"LIKES", false}, // case-sensitive: not a normalization layer
	}

	for _, c := range cases {
		if got := IsValidPredicate(c.name); got != c.want {
			t.Errorf("IsValidPredicate(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPredicates_NoDuplicateNames(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range Predicates {
		if seen[p.Name] {
			t.Errorf("duplicate predicate name: %q", p.Name)
		}
		seen[p.Name] = true
	}
}

func TestPredicates_AllFieldsPopulated(t *testing.T) {
	for _, p := range Predicates {
		if p.Name == "" || p.Category == "" || p.Description == "" {
			t.Errorf("predicate %+v has an empty field", p)
		}
	}
}
