package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
)

const testSecret = "s3cr3t-do-not-print-7f9a"

// envMap is the environment m plus a valid self identity unless m sets
// one itself (most tests are about other settings).
func envMap(m map[string]string) func(string) (string, bool) {
	full := map[string]string{"PEGASUS_SELF_NAME": "Marco", "PEGASUS_SELF_WHATSAPP_IDS": "+961 70 123 456"}
	for k, v := range m {
		full[k] = v
	}
	return rawEnvMap(full)
}

// rawEnvMap is exactly m.
func rawEnvMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "pegasus.json")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfig_DefaultsApply(t *testing.T) {
	cfg, err := LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), "")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"DBHost", cfg.DBHost, "localhost"},
		{"DBPort", cfg.DBPort, 5432},
		{"DBName", cfg.DBName, "pegasus"},
		{"DBUser", cfg.DBUser, "pegasus"},
		{"DBSSLMode", cfg.DBSSLMode, "disable"},
		{"CostguardURL", cfg.CostguardURL, "http://localhost:8080"},
		{"CostguardAgent", cfg.CostguardAgent, "pegasus"},
		{"CostguardTimeout", cfg.CostguardTimeout, 60 * time.Second},
		{"RetryBackoff", cfg.CostguardTranscriptionRetryBackoff, extraction.DefaultTranscriptionRetryBackoff},
		{"Threshold", cfg.TranscriptionConfidenceThreshold, api.DefaultTranscriptionConfidenceThreshold},
		{"ProviderHint", cfg.TranscriptionOpenAIProviderHint, api.DefaultOpenAIProviderHint},
		{"FlushAfter", cfg.LiveWindowFlushAfter, api.DefaultLiveWindowFlushAfter},
		{"PollInterval", cfg.LiveWindowFlushPollInterval, api.DefaultLiveWindowFlushPollInterval},
		{"RecoverOnStart", cfg.RecoverOnStart, true},
		{"HTTPAddr", cfg.HTTPAddr, DefaultHTTPAddr},
		{"ShutdownTimeout", cfg.ShutdownTimeout, DefaultShutdownTimeout},
		{"SelfName", cfg.SelfName, "Marco"},
		{"SelfWhatsApp", strings.Join(cfg.SelfWhatsApp, ";"), "96170123456@s.whatsapp.net"},
		{"SelfAliases", len(cfg.SelfAliases), 0},
		{"SelfInstagram", len(cfg.SelfInstagram), 0},
		{"Identity", cfg.Identity != nil, true},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
	if cfg.ShutdownTimeout <= api.ShutdownFlushTimeout {
		t.Errorf("default shutdown timeout %s must exceed the final flush timeout %s", cfg.ShutdownTimeout, api.ShutdownFlushTimeout)
	}
}

func TestLoadConfig_ReportsAllProblemsTogether(t *testing.T) {
	_, err := LoadConfig(envMap(map[string]string{
		"PEGASUS_DB_PORT":                            "not-a-port",
		"PEGASUS_COSTGUARD_URL":                      "localhost:8080", // no scheme
		"PEGASUS_LIVE_WINDOW_FLUSH_AFTER":            "5 minutes",
		"PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD": "0",
		"PEGASUS_RECOVER_ON_START":                   "maybe",
		// POSTGRES_PASSWORD missing
	}), "")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !IsConfigError(err) {
		t.Fatalf("expected a *ConfigError, got %T", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"POSTGRES_PASSWORD is required",
		"PEGASUS_DB_PORT",
		"PEGASUS_COSTGUARD_URL",
		"PEGASUS_LIVE_WINDOW_FLUSH_AFTER",
		"PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD",
		"PEGASUS_RECOVER_ON_START",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not mention %s:\n%s", want, msg)
		}
	}
	if n := len(err.(*ConfigError).Problems); n != 6 {
		t.Errorf("expected 6 problems, got %d:\n%s", n, msg)
	}
}

func TestLoadConfig_SecretNeverInErrorsOrDump(t *testing.T) {
	// A config error elsewhere must not echo the password.
	_, err := LoadConfig(envMap(map[string]string{
		"POSTGRES_PASSWORD": testSecret,
		"PEGASUS_DB_PORT":   "nope",
	}), "")
	if err == nil || strings.Contains(err.Error(), testSecret) {
		t.Fatalf("error leaked the secret or was nil: %v", err)
	}

	// The password in the file is rejected without echoing the value.
	p := writeFile(t, `{"postgres_password": "`+testSecret+`"}`)
	_, err = LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": "x"}), p)
	if err == nil {
		t.Fatal("expected a password in the config file to be rejected")
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("error leaked the file's secret: %v", err)
	}
	if !strings.Contains(err.Error(), "POSTGRES_PASSWORD") {
		t.Errorf("error should point at the env var: %v", err)
	}

	cfg, err := LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), "")
	if err != nil {
		t.Fatal(err)
	}
	dump := strings.Join(cfg.Redacted(), "\n")
	if strings.Contains(dump, testSecret) {
		t.Fatalf("redacted dump leaked the secret:\n%s", dump)
	}
	for _, want := range []string{
		"POSTGRES_PASSWORD=(redacted)",
		"PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD=-0.7",
		extraction.Model, extraction.TriageModel, extraction.EmbeddingModel, extraction.TranscriptionModel,
	} {
		if !strings.Contains(dump, want) {
			t.Errorf("dump missing %q:\n%s", want, dump)
		}
	}
	if !strings.Contains(cfg.DSN(), testSecret) {
		t.Error("DSN should carry the password (it is only ever handed to pgx)")
	}
}

func TestLoadConfig_EnvOverridesFile(t *testing.T) {
	p := writeFile(t, `{"db_host": "from-file", "db_port": 6543, "recover_on_start": false, "self_whatsapp_ids": ["+961 70 123 456", "Marco, from export"]}`)
	cfg, err := LoadConfig(rawEnvMap(map[string]string{
		"POSTGRES_PASSWORD": testSecret,
		"PEGASUS_DB_HOST":   "from-env",
		"PEGASUS_SELF_NAME": "Marco",
	}), p)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.DBHost != "from-env" {
		t.Errorf("env should win: DBHost=%q", cfg.DBHost)
	}
	if cfg.DBPort != 6543 || cfg.RecoverOnStart {
		t.Errorf("file values should apply where env is unset: port=%d recover=%v", cfg.DBPort, cfg.RecoverOnStart)
	}
	if got := strings.Join(cfg.SelfWhatsApp, ";"); got != "96170123456@s.whatsapp.net;Marco, from export" {
		t.Errorf("self whatsapp ids from the file (array, commas kept): %q", got)
	}

	// PEGASUS_CONFIG_FILE is honored when no --config path is given.
	cfg, err = LoadConfig(rawEnvMap(map[string]string{"POSTGRES_PASSWORD": testSecret, "PEGASUS_SELF_NAME": "Marco", ConfigFileEnv: p}), "")
	if err != nil || cfg.DBHost != "from-file" {
		t.Fatalf("PEGASUS_CONFIG_FILE not honored: %v %+v", err, cfg)
	}
}

func TestLoadConfig_UnknownFileKeysRejected(t *testing.T) {
	p := writeFile(t, `{"db_host": "x", "db_hots": "typo", "costguard_url": "http://h:1"}`)
	_, err := LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), p)
	if err == nil || !strings.Contains(err.Error(), `unknown key "db_hots"`) {
		t.Fatalf("expected unknown key error, got %v", err)
	}

	p = writeFile(t, `{"config_file": "other.json"}`)
	if _, err := LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), p); err == nil {
		t.Fatal("config_file must not be settable from the file")
	}

	p = writeFile(t, `[1,2]`)
	if _, err := LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), p); err == nil {
		t.Fatal("non-object file must be rejected")
	}
}

func TestResolveHTTPAddr_IgnoresOtherSettings(t *testing.T) {
	// No password, and a broken unrelated setting: healthcheck must still work.
	addr, err := ResolveHTTPAddr(envMap(map[string]string{"PEGASUS_DB_PORT": "bad", "PEGASUS_HTTP_ADDR": "0.0.0.0:9999"}), "")
	if err != nil || addr != "0.0.0.0:9999" {
		t.Fatalf("got %q, %v", addr, err)
	}
	if got := dialableAddr(addr); got != "127.0.0.1:9999" {
		t.Errorf("dialableAddr: %q", got)
	}
	if got := dialableAddr(":8095"); got != "127.0.0.1:8095" {
		t.Errorf("dialableAddr: %q", got)
	}
}

func TestLoadConfig_SelfIdentity(t *testing.T) {
	t.Run("semicolon lists, normalization", func(t *testing.T) {
		cfg, err := LoadConfig(rawEnvMap(map[string]string{
			"POSTGRES_PASSWORD":            testSecret,
			"PEGASUS_SELF_NAME":            " Marco ",
			"PEGASUS_SELF_ALIASES":         "Marco Antonios; M. Antonios",
			"PEGASUS_SELF_WHATSAPP_IDS":    "+961 70 123 456; 96170123456:7@s.whatsapp.net ;Marco",
			"PEGASUS_SELF_INSTAGRAM_NAMES": "Antonios, Marco;André M",
		}), "")
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SelfName != "Marco" || strings.Join(cfg.SelfAliases, "|") != "Marco Antonios|M. Antonios" {
			t.Errorf("name/aliases: %q %q", cfg.SelfName, cfg.SelfAliases)
		}
		// Phone number and device-suffixed JID collapse to one identity.
		if got := strings.Join(cfg.SelfWhatsApp, "|"); got != "96170123456@s.whatsapp.net|Marco" {
			t.Errorf("whatsapp: %q", got)
		}
		// Commas survive (they appear in display names); NFC applied.
		if got := strings.Join(cfg.SelfInstagram, "|"); got != "Antonios, Marco|André M" {
			t.Errorf("instagram: %q", got)
		}
		if !cfg.Identity.IsSelf("whatsapp", "96170123456:2@s.whatsapp.net") || cfg.Identity.IsSelf("instagram", "96170123456@s.whatsapp.net") {
			t.Error("registry not platform-scoped as configured")
		}
	})

	t.Run("all problems together", func(t *testing.T) {
		_, err := LoadConfig(rawEnvMap(map[string]string{
			"POSTGRES_PASSWORD":            testSecret,
			"PEGASUS_SELF_ALIASES":         "Mo",
			"PEGASUS_SELF_WHATSAPP_IDS":    "12036302@g.us;;",
			"PEGASUS_SELF_INSTAGRAM_NAMES": "ab",
			"PEGASUS_DB_PORT":              "nope",
		}), "")
		if err == nil || !IsConfigError(err) {
			t.Fatalf("want a ConfigError, got %v", err)
		}
		for _, want := range []string{"PEGASUS_DB_PORT", "self name is required", `alias "Mo" is shorter`, "not a person JID", "empty entry", `instagram identity "ab" is shorter`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("missing %q in:\n%v", want, err)
			}
		}
	})

	t.Run("no identity on any platform", func(t *testing.T) {
		_, err := LoadConfig(rawEnvMap(map[string]string{"POSTGRES_PASSWORD": testSecret, "PEGASUS_SELF_NAME": "Marco"}), "")
		if err == nil || !strings.Contains(err.Error(), "no self identity is configured") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("file arrays, env wins", func(t *testing.T) {
		p := writeFile(t, `{"self_name": "Marco", "self_whatsapp_ids": ["Marco; with a semicolon", "+961 70 123 456"], "self_instagram_names": ["From File"]}`)
		cfg, err := LoadConfig(rawEnvMap(map[string]string{
			"POSTGRES_PASSWORD":            testSecret,
			"PEGASUS_SELF_INSTAGRAM_NAMES": "From Env",
		}), p)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(cfg.SelfWhatsApp, "|"); got != "Marco; with a semicolon|96170123456@s.whatsapp.net" {
			t.Errorf("file array (semicolon kept in an entry): %q", got)
		}
		if got := strings.Join(cfg.SelfInstagram, "|"); got != "From Env" {
			t.Errorf("env should override the file: %q", got)
		}

		bad := writeFile(t, `{"self_name": "Marco", "self_whatsapp_ids": "Marco"}`)
		if _, err := LoadConfig(rawEnvMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), bad); err == nil || !strings.Contains(err.Error(), "JSON array") {
			t.Errorf("a string where an array is required: %v", err)
		}
	})

	t.Run("pair skips it", func(t *testing.T) {
		if _, err := LoadConfig(rawEnvMap(map[string]string{"POSTGRES_PASSWORD": testSecret}), "", WithoutSelfIdentity()); err != nil {
			t.Errorf("WithoutSelfIdentity: %v", err)
		}
	})

	t.Run("LoadSelfIdentity ignores unrelated settings", func(t *testing.T) {
		reg, err := LoadSelfIdentity(rawEnvMap(map[string]string{
			"PEGASUS_DB_PORT": "nope", "PEGASUS_SELF_NAME": "Marco", "PEGASUS_SELF_INSTAGRAM_NAMES": "Marco Antonios",
		}), "")
		if err != nil || reg.SelfName() != "Marco" {
			t.Fatalf("got %v, %v", reg, err)
		}
		if _, err := LoadSelfIdentity(rawEnvMap(nil), ""); err == nil {
			t.Error("no self identity must fail")
		}
	})
}
