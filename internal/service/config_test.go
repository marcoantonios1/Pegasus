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

func envMap(m map[string]string) func(string) (string, bool) {
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
		{"SelfExternalIDs", len(cfg.SelfExternalIDs), 0},
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
	p := writeFile(t, `{"db_host": "from-file", "db_port": 6543, "recover_on_start": false, "self_external_ids": "a@s.whatsapp.net, b@s.whatsapp.net"}`)
	cfg, err := LoadConfig(envMap(map[string]string{
		"POSTGRES_PASSWORD": testSecret,
		"PEGASUS_DB_HOST":   "from-env",
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
	if len(cfg.SelfExternalIDs) != 2 || cfg.SelfExternalIDs[1] != "b@s.whatsapp.net" {
		t.Errorf("SelfExternalIDs: %v", cfg.SelfExternalIDs)
	}

	// PEGASUS_CONFIG_FILE is honored when no --config path is given.
	cfg, err = LoadConfig(envMap(map[string]string{"POSTGRES_PASSWORD": testSecret, ConfigFileEnv: p}), "")
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
