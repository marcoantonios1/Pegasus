// Package identity holds Marco's configured self identity: the self
// entity's canonical name, the aliases the extraction model may use for
// him, and his sender identities per platform. It answers two questions
// for the write pipeline:
//
//   - IsSelf(platform, senderID): is this message sender Marco? Scoped per
//     platform: a WhatsApp identity never matches an Instagram sender, even
//     with the same string.
//   - IsSelfName(name): does this extracted subject/object name refer to
//     Marco (the self name, an alias, or any configured identity string)?
//
// It is pure: no database. api.API.BootstrapSelf ties it to the single
// is_self entity.
package identity

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"go.mau.fi/whatsmeow/types"
	"golang.org/x/text/unicode/norm"
)

// Platforms, matching ingestion.RawMessage.Platform values.
const (
	WhatsApp  = "whatsapp"
	Instagram = "instagram"
)

// MinNameLength is the shortest accepted self name, alias or display-name
// identity, in characters: shorter strings match other people by accident.
const MinNameLength = 3

// Config is the raw configuration, before normalization.
type Config struct {
	SelfName  string
	Aliases   []string
	WhatsApp  []string // JIDs, phone numbers, or export sender labels
	Instagram []string // display names (Instagram exports carry no stable ID)
}

// Registry is a validated, normalized self identity. Safe for concurrent
// reads; never mutated after New.
type Registry struct {
	selfName string
	aliases  []string
	ids      map[string]map[string]bool // platform -> normalized identity
	ordered  map[string][]string        // platform -> normalized, config order
	names    map[string]bool            // folded names that mean "Marco"
}

// New normalizes and validates cfg, returning every problem at once.
func New(cfg Config) (*Registry, []string) {
	var problems []string
	r := &Registry{
		ids:     map[string]map[string]bool{WhatsApp: {}, Instagram: {}},
		ordered: map[string][]string{},
		names:   map[string]bool{},
	}

	r.selfName = nfcTrim(cfg.SelfName)
	switch {
	case r.selfName == "":
		problems = append(problems, "self name is required (it becomes the self entity's canonical_name, e.g. \"Marco\")")
	case utf8.RuneCountInString(r.selfName) < MinNameLength:
		problems = append(problems, fmt.Sprintf("self name %q is shorter than %d characters", r.selfName, MinNameLength))
	default:
		r.addName(r.selfName)
	}

	for _, a := range cfg.Aliases {
		n, err := checkName(a, "alias")
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		r.aliases = append(r.aliases, n)
		r.addName(n)
	}

	for _, raw := range cfg.WhatsApp {
		n, err := NormalizeWhatsApp(raw)
		if err != nil {
			problems = append(problems, "whatsapp identity: "+err.Error())
			continue
		}
		r.add(WhatsApp, n, raw)
	}
	for _, raw := range cfg.Instagram {
		n, err := checkName(raw, "instagram identity")
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		r.add(Instagram, n, raw)
	}

	if len(r.ordered[WhatsApp])+len(r.ordered[Instagram]) == 0 && len(problems) == 0 {
		problems = append(problems, "no self identity is configured for any platform (set the WhatsApp and/or Instagram identities); without one, Marco's own messages would be attributed to a contact")
	}
	return r, problems
}

func (r *Registry) add(platform, normalized, raw string) {
	if !r.ids[platform][normalized] {
		r.ids[platform][normalized] = true
		r.ordered[platform] = append(r.ordered[platform], normalized)
	}
	r.addName(normalized)
	r.addName(nfcTrim(raw))
}

func (r *Registry) addName(n string) {
	if n != "" {
		r.names[fold(n)] = true
	}
}

// SelfName is the self entity's canonical name.
func (r *Registry) SelfName() string { return r.selfName }

// Aliases returns the configured aliases (normalized).
func (r *Registry) Aliases() []string { return append([]string(nil), r.aliases...) }

// Identities returns the normalized identities for platform, in config order.
func (r *Registry) Identities(platform string) []string {
	return append([]string(nil), r.ordered[platform]...)
}

// AllIdentities returns every configured identity string across platforms.
func (r *Registry) AllIdentities() []string {
	out := append([]string(nil), r.ordered[WhatsApp]...)
	return append(out, r.ordered[Instagram]...)
}

// IsSelf reports whether senderID on platform is one of Marco's identities.
// senderID is normalized the same way the configuration was (JID device
// suffixes stripped, phone numbers expanded, NFC + trim).
func (r *Registry) IsSelf(platform, senderID string) bool {
	set := r.ids[platform]
	if len(set) == 0 {
		return false
	}
	switch platform {
	case WhatsApp:
		n, err := NormalizeWhatsApp(senderID)
		if err != nil {
			n = nfcTrim(senderID)
		}
		return set[n]
	default:
		return set[nfcTrim(senderID)]
	}
}

// IsSelfName reports whether name (an extracted subject or object) refers
// to Marco: his self name, an alias, or any configured identity string,
// compared case-insensitively after NFC + trim.
func (r *Registry) IsSelfName(name string) bool {
	n := nfcTrim(name)
	return n != "" && r.names[fold(n)]
}

// --- normalization ---

var (
	phoneRe     = regexp.MustCompile(`^\+?[0-9][0-9 ().\-]*$`)
	nonDigitsRe = regexp.MustCompile(`[^0-9]`)
)

// NormalizeWhatsApp canonicalizes a WhatsApp identity the way the live
// source canonicalizes JIDs:
//
//   - a JID: device suffix stripped; c.us becomes s.whatsapp.net; only
//     person JIDs (s.whatsapp.net, lid) are accepted;
//   - a bare phone number ("+961 70 123 456"): digits only, expanded to
//     "<digits>@s.whatsapp.net";
//   - anything else is an export sender label (WhatsApp .txt exports label
//     senders by name, e.g. "Marco"): NFC + trim, at least 3 characters.
func NormalizeWhatsApp(s string) (string, error) {
	s = nfcTrim(s)
	switch {
	case s == "":
		return "", fmt.Errorf("empty entry")
	case strings.Contains(s, "@"):
		j, err := types.ParseJID(s)
		if err != nil {
			return "", fmt.Errorf("%q is not a valid JID: %v", s, err)
		}
		j = j.ToNonAD()
		if j.Server == types.LegacyUserServer {
			j.Server = types.DefaultUserServer
		}
		if j.User == "" || (j.Server != types.DefaultUserServer && j.Server != types.HiddenUserServer) {
			return "", fmt.Errorf("%q is not a person JID (want …@s.whatsapp.net or …@lid)", s)
		}
		return j.String(), nil
	case phoneRe.MatchString(s):
		digits := nonDigitsRe.ReplaceAllString(s, "")
		if len(digits) < 7 || len(digits) > 15 {
			return "", fmt.Errorf("%q does not look like a phone number with country code (7-15 digits)", s)
		}
		return digits + "@" + types.DefaultUserServer, nil
	default:
		return checkName(s, "export label")
	}
}

func checkName(s, what string) (string, error) {
	n := nfcTrim(s)
	switch {
	case n == "":
		return "", fmt.Errorf("%s: empty entry", what)
	case utf8.RuneCountInString(n) < MinNameLength:
		return "", fmt.Errorf("%s %q is shorter than %d characters (accidental-match risk)", what, n, MinNameLength)
	}
	return n, nil
}

// NFC normalizes and trims s (Instagram display names after the adapter's
// mojibake repair, export labels).
func NFC(s string) string { return nfcTrim(s) }

func nfcTrim(s string) string { return strings.TrimSpace(norm.NFC.String(s)) }

func fold(s string) string { return strings.ToLower(s) }
