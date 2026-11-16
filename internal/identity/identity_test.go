package identity

import (
	"strings"
	"testing"
)

func mustNew(t *testing.T, cfg Config) *Registry {
	t.Helper()
	r, problems := New(cfg)
	if len(problems) > 0 {
		t.Fatalf("New: %v", problems)
	}
	return r
}

func TestNormalizeWhatsApp(t *testing.T) {
	for in, want := range map[string]string{
		"96170123456@s.whatsapp.net":    "96170123456@s.whatsapp.net",
		"96170123456:12@s.whatsapp.net": "96170123456@s.whatsapp.net", // device suffix
		"96170123456@c.us":              "96170123456@s.whatsapp.net",
		"+961 70 123 456":               "96170123456@s.whatsapp.net",
		"+1 (555) 123-4567":             "15551234567@s.whatsapp.net",
		"88887777:3@lid":                "88887777@lid",
		"  Marco  ":                     "Marco",
		"Marco Antonios":                "Marco Antonios",
	} {
		got, err := NormalizeWhatsApp(in)
		if err != nil || got != want {
			t.Errorf("NormalizeWhatsApp(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "  ", "Al", "123", "12036302@g.us", "status@broadcast", "@s.whatsapp.net"} {
		if got, err := NormalizeWhatsApp(bad); err == nil {
			t.Errorf("NormalizeWhatsApp(%q) = %q, want an error", bad, got)
		}
	}
}

func TestNew_ReportsAllProblems(t *testing.T) {
	_, problems := New(Config{
		SelfName:  "",
		Aliases:   []string{"Mo", ""},
		WhatsApp:  []string{"12036302@g.us", "x"},
		Instagram: []string{" "},
	})
	if len(problems) != 6 {
		t.Fatalf("want 6 problems, got %d: %v", len(problems), problems)
	}

	_, problems = New(Config{SelfName: "Marco"})
	if len(problems) != 1 || !strings.Contains(problems[0], "no self identity") {
		t.Fatalf("zero identities: %v", problems)
	}
}

func TestIsSelf_PlatformScoped(t *testing.T) {
	r := mustNew(t, Config{
		SelfName:  "Marco",
		WhatsApp:  []string{"+961 70 123 456", "Shared Name"},
		Instagram: []string{"Marco Antonios", "Shared Name"},
	})
	cases := []struct {
		platform, id string
		want         bool
	}{
		{WhatsApp, "96170123456@s.whatsapp.net", true},
		{WhatsApp, "96170123456:5@s.whatsapp.net", true},
		{WhatsApp, "Marco Antonios", false}, // an Instagram identity
		{Instagram, "Marco Antonios", true},
		{Instagram, "marco antonios", false}, // exact after NFC+trim
		{Instagram, "96170123456@s.whatsapp.net", false},
		{Instagram, "+961 70 123 456", false},
		{WhatsApp, "Shared Name", true},
		{Instagram, "Shared Name", true},
		{"telegram", "Shared Name", false},
	}
	for _, c := range cases {
		if got := r.IsSelf(c.platform, c.id); got != c.want {
			t.Errorf("IsSelf(%s, %q) = %v, want %v", c.platform, c.id, got, c.want)
		}
	}
}

func TestIsSelf_NFC(t *testing.T) {
	// "é" composed vs decomposed.
	r := mustNew(t, Config{SelfName: "Marco", Instagram: []string{"André M"}})
	if !r.IsSelf(Instagram, "André M ") {
		t.Error("decomposed + trailing space should match after NFC + trim")
	}
}

func TestIsSelfName(t *testing.T) {
	r := mustNew(t, Config{
		SelfName: "Marco", Aliases: []string{"Marco Antonios"},
		WhatsApp: []string{"+961 70 123 456"}, Instagram: []string{"marco.antonios"},
	})
	for _, n := range []string{"Marco", "marco", " MARCO ", "Marco Antonios", "96170123456@s.whatsapp.net", "+961 70 123 456", "marco.antonios"} {
		if !r.IsSelfName(n) {
			t.Errorf("IsSelfName(%q) = false", n)
		}
	}
	for _, n := range []string{"Marc", "Sara", "", "Marco A"} {
		if r.IsSelfName(n) {
			t.Errorf("IsSelfName(%q) = true", n)
		}
	}
}
