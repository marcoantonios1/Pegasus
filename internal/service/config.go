package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcoantonios1/Pegasus/internal/api"
	"github.com/marcoantonios1/Pegasus/internal/extraction"
	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp/wasource"
)

// Config is the fully-resolved, validated service configuration. Build it
// with LoadConfig; every field is populated (defaults applied).
//
// Deliberately absent: api.PipelineDeps.HonorCorrectionsOnImport. This
// process never runs imports (ImportWhatsApp is driven by the one-shot
// cmd/ tools), so the knob would have no effect here.
type Config struct {
	DBHost           string
	DBPort           int
	DBName           string
	DBUser           string
	DBPassword       string // secret: environment only, never logged
	DBSSLMode        string
	DBConnectTimeout time.Duration

	CostguardURL                       string
	CostguardAgent                     string
	CostguardTimeout                   time.Duration
	CostguardTranscriptionRetryBackoff time.Duration

	TranscriptionConfidenceThreshold float64
	TranscriptionOpenAIProviderHint  string

	SelfExternalIDs []string

	LiveWindowFlushAfter        time.Duration
	LiveWindowFlushPollInterval time.Duration

	RecoverOnStart bool

	HTTPAddr        string
	ShutdownTimeout time.Duration

	LiveSinkTimeout time.Duration

	WhatsAppEnabled              bool
	WhatsAppSessionPath          string
	WhatsAppReconnectBackoffBase time.Duration
	WhatsAppReconnectBackoffMax  time.Duration
	WhatsAppLogLevel             string
}

// ConfigFileEnv names the environment variable holding the optional
// config file path (the --config flag overrides it).
const ConfigFileEnv = "PEGASUS_CONFIG_FILE"

// setting is one row of the configuration table. The same table drives
// loading (env > file > default), validation, the redacted startup dump,
// and the docs test (settings_docs_test.go) that keeps README.md and
// .env.example in sync with it.
type setting struct {
	Env      string
	Default  string
	Desc     string
	Secret   bool // env only; value never printed
	Required bool // must be non-empty after resolution
	EnvOnly  bool // not accepted in the config file (implied by Secret)
	apply    func(c *Config, v string) error
}

// FileKey is the setting's key in the JSON config file: the env var name
// lower-cased with the PEGASUS_ prefix dropped (PEGASUS_DB_HOST -> db_host).
func (s setting) FileKey() string {
	return strings.ToLower(strings.TrimPrefix(s.Env, "PEGASUS_"))
}

func (s setting) envOnly() bool { return s.Secret || s.EnvOnly }

// Defaults chosen by this package (the rest come from existing package
// constants so they can't drift).
const (
	DefaultHTTPAddr         = "127.0.0.1:8095"
	DefaultShutdownTimeout  = 60 * time.Second
	DefaultDBConnectTimeout = 5 * time.Second
	DefaultCostguardTimeout = 60 * time.Second // extraction.NewCostguardClient's HTTPClient.Timeout
	DefaultCostguardURL     = "http://localhost:8080"
	DefaultLiveSinkTimeout  = 5 * time.Minute
	// DefaultWhatsAppSessionPath is for host runs; docker-compose.yml sets
	// /var/lib/pegasus/whatsmeow/session.db on a dedicated named volume.
	DefaultWhatsAppSessionPath = "~/.pegasus/whatsmeow/session.db"
)

// settings is THE configuration table. Order is the order of the
// startup dump and of the docs.
var settings = []setting{
	{Env: ConfigFileEnv, Default: "", EnvOnly: true,
		Desc:  "Optional path to a JSON config file (keys are the setting names lower-cased without the PEGASUS_ prefix, e.g. `db_host`). The --config flag overrides this. Unknown keys are an error.",
		apply: func(*Config, string) error { return nil }},

	{Env: "PEGASUS_DB_HOST", Default: "localhost", Desc: "Postgres host.",
		apply: func(c *Config, v string) error { c.DBHost = v; return requireNonEmpty(v) }},
	{Env: "PEGASUS_DB_PORT", Default: "5432", Desc: "Postgres port.",
		apply: func(c *Config, v string) (err error) { c.DBPort, err = parsePort(v); return }},
	{Env: "PEGASUS_DB_NAME", Default: "pegasus", Desc: "Postgres database name.",
		apply: func(c *Config, v string) error { c.DBName = v; return requireNonEmpty(v) }},
	{Env: "PEGASUS_DB_USER", Default: "pegasus", Desc: "Postgres user.",
		apply: func(c *Config, v string) error { c.DBUser = v; return requireNonEmpty(v) }},
	{Env: "POSTGRES_PASSWORD", Secret: true, Required: true,
		Desc:  "Postgres password. Secret: read only from the environment (the same variable docker-compose.yml's postgres service uses), never from the config file, never logged.",
		apply: func(c *Config, v string) error { c.DBPassword = v; return nil }},
	{Env: "PEGASUS_DB_SSLMODE", Default: "disable", Desc: "libpq sslmode (disable, allow, prefer, require, verify-ca, verify-full).",
		apply: func(c *Config, v string) error {
			switch v {
			case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
				c.DBSSLMode = v
				return nil
			}
			return fmt.Errorf("must be one of disable, allow, prefer, require, verify-ca, verify-full")
		}},
	{Env: "PEGASUS_DB_CONNECT_TIMEOUT", Default: DefaultDBConnectTimeout.String(), Desc: "Timeout for the startup database connect+ping.",
		apply: func(c *Config, v string) (err error) { c.DBConnectTimeout, err = parsePositiveDuration(v); return }},

	{Env: "PEGASUS_COSTGUARD_URL", Default: DefaultCostguardURL,
		Desc:  "Costguard gateway base URL (same default as cmd/phase0_dryrun's --costguard-url). docker-compose.yml overrides it to http://host.docker.internal:8080 so the container reaches a Costguard running on the host.",
		apply: func(c *Config, v string) error { c.CostguardURL = v; return validateHTTPURL(v) }},
	{Env: "PEGASUS_COSTGUARD_AGENT", Default: "pegasus", Desc: "X-Costguard-Agent attribution tag sent on every model call.",
		apply: func(c *Config, v string) error { c.CostguardAgent = v; return requireNonEmpty(v) }},
	{Env: "PEGASUS_COSTGUARD_TIMEOUT", Default: DefaultCostguardTimeout.String(), Desc: "Per-request HTTP timeout for Costguard calls (triage, extraction, embeddings, transcription).",
		apply: func(c *Config, v string) (err error) { c.CostguardTimeout, err = parsePositiveDuration(v); return }},
	{Env: "PEGASUS_COSTGUARD_TRANSCRIPTION_RETRY_BACKOFF", Default: extraction.DefaultTranscriptionRetryBackoff.String(), Desc: "Wait before Transcribe's single retry after a Costguard 502.",
		apply: func(c *Config, v string) (err error) {
			c.CostguardTranscriptionRetryBackoff, err = parsePositiveDuration(v)
			return
		}},

	{Env: "PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD", Default: strconv.FormatFloat(api.DefaultTranscriptionConfidenceThreshold, 'g', -1, 64),
		Desc: "Escalate a local transcription to OpenAI when its mean segment avg_logprob is below this (Arabic always escalates regardless). Must be negative: 0 means \"unset\" to api.TranscriptionConfig.",
		apply: func(c *Config, v string) error {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Errorf("must be a number")
			}
			if f >= 0 {
				return fmt.Errorf("must be negative (avg_logprob is <= 0, and 0 is treated as unset)")
			}
			c.TranscriptionConfidenceThreshold = f
			return nil
		}},
	{Env: "PEGASUS_TRANSCRIPTION_OPENAI_PROVIDER_HINT", Default: api.DefaultOpenAIProviderHint, Desc: "Costguard provider name sent as X-Costguard-Provider on escalated transcription calls.",
		apply: func(c *Config, v string) error { c.TranscriptionOpenAIProviderHint = v; return requireNonEmpty(v) }},

	{Env: "PEGASUS_SELF_EXTERNAL_IDS", Default: "",
		Desc:  "Comma-separated platform sender IDs (e.g. WhatsApp JIDs) that are Marco himself. Empty is allowed but logged loudly: once a live source exists, his own messages would be attributed to a contact entity.",
		apply: func(c *Config, v string) error { c.SelfExternalIDs = splitList(v); return nil }},

	{Env: "PEGASUS_LIVE_WINDOW_FLUSH_AFTER", Default: api.DefaultLiveWindowFlushAfter.String(), Desc: "Flush an open live window after this long with no new message arriving.",
		apply: func(c *Config, v string) (err error) { c.LiveWindowFlushAfter, err = parsePositiveDuration(v); return }},
	{Env: "PEGASUS_LIVE_WINDOW_FLUSH_POLL_INTERVAL", Default: api.DefaultLiveWindowFlushPollInterval.String(), Desc: "How often the idle-window flusher checks.",
		apply: func(c *Config, v string) (err error) {
			c.LiveWindowFlushPollInterval, err = parsePositiveDuration(v)
			return
		}},

	{Env: "PEGASUS_RECOVER_ON_START", Default: "true",
		Desc:  "Re-extract every processed=false message with text in the background at startup. Treats ALL such rows as backlog, so do not start the service while a bulk import is mid-flight.",
		apply: func(c *Config, v string) (err error) { c.RecoverOnStart, err = strconv.ParseBool(v); return }},

	{Env: "PEGASUS_HTTP_ADDR", Default: DefaultHTTPAddr, Desc: "Listen address for /healthz and /readyz. docker-compose.yml sets 0.0.0.0:8095 inside the container and publishes it on 127.0.0.1 only.",
		apply: func(c *Config, v string) error {
			if _, port, err := net.SplitHostPort(v); err != nil {
				return fmt.Errorf("must be host:port")
			} else if _, err := parsePort(port); err != nil {
				return err
			}
			c.HTTPAddr = v
			return nil
		}},
	{Env: "PEGASUS_SHUTDOWN_TIMEOUT", Default: DefaultShutdownTimeout.String(),
		Desc: "Overall bound on graceful shutdown. Must exceed the live-window final flush timeout (30s); docker-compose.yml's stop_grace_period must exceed this.",
		apply: func(c *Config, v string) error {
			d, err := parsePositiveDuration(v)
			if err != nil {
				return err
			}
			if d <= api.ShutdownFlushTimeout {
				return fmt.Errorf("must be greater than the final live-window flush timeout (%s)", api.ShutdownFlushTimeout)
			}
			c.ShutdownTimeout = d
			return nil
		}},

	{Env: "PEGASUS_LIVE_SINK_TIMEOUT", Default: DefaultLiveSinkTimeout.String(),
		Desc: "Per-message bound on api.ProcessLiveMessage when a live source delivers a message (it may run a window's triage+extraction+embedding inline). A message that times out after being stored stays processed=false and is recovered on next start.",
		apply: func(c *Config, v string) (err error) { c.LiveSinkTimeout, err = parsePositiveDuration(v); return }},

	{Env: "PEGASUS_WHATSAPP_ENABLED", Default: "false",
		Desc: "Run the live WhatsApp source (whatsmeow). Requires a paired session: with no paired device the service exits 2 telling you to run `docker compose run --rm pegasus pair`.",
		apply: func(c *Config, v string) (err error) { c.WhatsAppEnabled, err = strconv.ParseBool(v); return }},
	{Env: "PEGASUS_WHATSAPP_SESSION_PATH", Default: DefaultWhatsAppSessionPath,
		Desc: "whatsmeow session database (SQLite). A credential: must resolve outside the git repo; created 0600 in a 0700 directory. docker-compose.yml uses /var/lib/pegasus/whatsmeow/session.db on the whatsmeow_session volume.",
		apply: func(c *Config, v string) error { c.WhatsAppSessionPath = v; return requireNonEmpty(v) }},
	{Env: "PEGASUS_WHATSAPP_RECONNECT_BACKOFF_BASE", Default: "2s",
		Desc: "First wait between supervised reconnect attempts (after whatsmeow's own auto-reconnect has had 30s); doubles per attempt, ±20% jitter.",
		apply: func(c *Config, v string) (err error) {
			c.WhatsAppReconnectBackoffBase, err = parsePositiveDuration(v)
			return
		}},
	{Env: "PEGASUS_WHATSAPP_RECONNECT_BACKOFF_MAX", Default: "5m0s",
		Desc: "Cap on the reconnect backoff. Reconnecting continues indefinitely at this interval.",
		apply: func(c *Config, v string) error {
			d, err := parsePositiveDuration(v)
			if err != nil {
				return err
			}
			if c.WhatsAppReconnectBackoffBase > 0 && d < c.WhatsAppReconnectBackoffBase {
				return fmt.Errorf("must be >= PEGASUS_WHATSAPP_RECONNECT_BACKOFF_BASE")
			}
			c.WhatsAppReconnectBackoffMax = d
			return nil
		}},
	{Env: "PEGASUS_WHATSAPP_LOG_LEVEL", Default: "INFO",
		Desc: "whatsmeow's own log level: DEBUG, INFO, WARN or ERROR. Avoid DEBUG: it can print message payloads.",
		apply: func(c *Config, v string) error {
			switch u := strings.ToUpper(v); u {
			case "DEBUG", "INFO", "WARN", "ERROR":
				c.WhatsAppLogLevel = u
				return nil
			}
			return fmt.Errorf("must be DEBUG, INFO, WARN or ERROR")
		}},
}

// FixedModels are the model names the pipeline uses. They are package
// constants in internal/extraction, not configurable — listed here so
// the startup dump and README can show the current values.
var FixedModels = []struct{ Role, Name, Const string }{
	{"triage (pass 1)", extraction.TriageModel, "extraction.TriageModel"},
	{"extraction (pass 2)", extraction.Model, "extraction.Model"},
	{"embeddings", extraction.EmbeddingModel, "extraction.EmbeddingModel"},
	{"transcription", extraction.TranscriptionModel, "extraction.TranscriptionModel"},
}

// ConfigError reports every configuration problem found, in one error.
// main maps it to exit code 2. Messages never contain secret values.
type ConfigError struct{ Problems []string }

func (e *ConfigError) Error() string {
	return "invalid configuration:\n  - " + strings.Join(e.Problems, "\n  - ")
}

// IsConfigError reports whether err is (or wraps) a *ConfigError.
func IsConfigError(err error) bool {
	var ce *ConfigError
	return errors.As(err, &ce)
}

// LoadConfig resolves every setting with precedence environment > config
// file > default, then validates all of them, returning a *ConfigError
// listing every problem at once. lookupEnv is os.LookupEnv in production;
// configPath is the --config flag value (empty to fall back to
// PEGASUS_CONFIG_FILE, then to no file).
func LoadConfig(lookupEnv func(string) (string, bool), configPath string) (*Config, error) {
	var problems []string

	if configPath == "" {
		configPath, _ = lookupEnv(ConfigFileEnv)
	}
	fileVals := map[string]string{}
	if configPath != "" {
		var err error
		fileVals, err = readConfigFile(configPath)
		if err != nil {
			// Unknown keys/secrets-in-file are reported alongside every
			// other problem; an unreadable file means we can't know what
			// it would have set, so report it and keep validating env.
			var ce *ConfigError
			if errors.As(err, &ce) {
				problems = append(problems, ce.Problems...)
			} else {
				problems = append(problems, err.Error())
			}
		}
	}

	cfg := &Config{}
	for _, s := range settings {
		v, ok := lookupEnv(s.Env)
		if !ok && !s.envOnly() {
			v, ok = fileVals[s.FileKey()]
		}
		if !ok {
			v = s.Default
		}
		v = strings.TrimSpace(v)
		if v == "" && s.Required {
			hint := ""
			if s.Secret {
				hint = " (secret: set it in the environment, e.g. .env; it is never read from the config file)"
			}
			problems = append(problems, fmt.Sprintf("%s is required but not set%s", s.Env, hint))
			continue
		}
		if err := s.apply(cfg, v); err != nil {
			if s.Secret {
				problems = append(problems, fmt.Sprintf("%s is invalid: %v", s.Env, err))
			} else {
				problems = append(problems, fmt.Sprintf("%s=%q is invalid: %v", s.Env, v, err))
			}
		}
	}

	if len(problems) > 0 {
		return nil, &ConfigError{Problems: problems}
	}
	return cfg, nil
}

// readConfigFile decodes the JSON config file strictly: unknown keys and
// env-only (secret) keys are errors. Values may be JSON strings, numbers
// or booleans; they are passed through the same parsers as env values.
func readConfigFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("config file %s: not a JSON object: %w", path, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("config file %s: trailing data after the JSON object", path)
	}

	known := map[string]setting{}
	for _, s := range settings {
		known[s.FileKey()] = s
	}

	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var problems []string
	out := map[string]string{}
	for _, k := range keys {
		s, ok := known[k]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("config file %s: unknown key %q", path, k))
			continue
		case s.envOnly():
			problems = append(problems, fmt.Sprintf("config file %s: key %q is not allowed in the file; set %s in the environment", path, k, s.Env))
			continue
		}
		v, err := jsonScalar(raw[k])
		if err != nil {
			problems = append(problems, fmt.Sprintf("config file %s: key %q: %v", path, k, err))
			continue
		}
		out[k] = v
	}
	if len(problems) > 0 {
		return nil, &ConfigError{Problems: problems}
	}
	return out, nil
}

func jsonScalar(m json.RawMessage) (string, error) {
	var v any
	if err := json.Unmarshal(m, &v); err != nil {
		return "", err
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case float64:
		return string(m), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "", fmt.Errorf("must be a string, number or boolean")
	}
}

// Redacted returns the effective configuration as display lines with
// secrets masked, plus the fixed model names — logged once at startup.
func (c *Config) Redacted() []string {
	vals := c.displayValues()
	lines := make([]string, 0, len(settings)+len(FixedModels))
	for _, s := range settings {
		if s.Env == ConfigFileEnv {
			continue
		}
		v := vals[s.Env]
		if s.Secret {
			if v == "" {
				v = "(unset)"
			} else {
				v = "(redacted)"
			}
		}
		lines = append(lines, fmt.Sprintf("%s=%s", s.Env, v))
	}
	for _, m := range FixedModels {
		lines = append(lines, fmt.Sprintf("model[%s]=%s (fixed: %s)", m.Role, m.Name, m.Const))
	}
	return lines
}

func (c *Config) displayValues() map[string]string {
	return map[string]string{
		"PEGASUS_DB_HOST":            c.DBHost,
		"PEGASUS_DB_PORT":            strconv.Itoa(c.DBPort),
		"PEGASUS_DB_NAME":            c.DBName,
		"PEGASUS_DB_USER":            c.DBUser,
		"POSTGRES_PASSWORD":          c.DBPassword,
		"PEGASUS_DB_SSLMODE":         c.DBSSLMode,
		"PEGASUS_DB_CONNECT_TIMEOUT": c.DBConnectTimeout.String(),

		"PEGASUS_COSTGUARD_URL":                         c.CostguardURL,
		"PEGASUS_COSTGUARD_AGENT":                       c.CostguardAgent,
		"PEGASUS_COSTGUARD_TIMEOUT":                     c.CostguardTimeout.String(),
		"PEGASUS_COSTGUARD_TRANSCRIPTION_RETRY_BACKOFF": c.CostguardTranscriptionRetryBackoff.String(),

		"PEGASUS_TRANSCRIPTION_CONFIDENCE_THRESHOLD": strconv.FormatFloat(c.TranscriptionConfidenceThreshold, 'g', -1, 64),
		"PEGASUS_TRANSCRIPTION_OPENAI_PROVIDER_HINT": c.TranscriptionOpenAIProviderHint,

		"PEGASUS_SELF_EXTERNAL_IDS": strings.Join(c.SelfExternalIDs, ","),

		"PEGASUS_LIVE_WINDOW_FLUSH_AFTER":         c.LiveWindowFlushAfter.String(),
		"PEGASUS_LIVE_WINDOW_FLUSH_POLL_INTERVAL": c.LiveWindowFlushPollInterval.String(),

		"PEGASUS_RECOVER_ON_START": strconv.FormatBool(c.RecoverOnStart),

		"PEGASUS_HTTP_ADDR":        c.HTTPAddr,
		"PEGASUS_SHUTDOWN_TIMEOUT": c.ShutdownTimeout.String(),

		"PEGASUS_LIVE_SINK_TIMEOUT": c.LiveSinkTimeout.String(),

		"PEGASUS_WHATSAPP_ENABLED":                strconv.FormatBool(c.WhatsAppEnabled),
		"PEGASUS_WHATSAPP_SESSION_PATH":           c.WhatsAppSessionPath,
		"PEGASUS_WHATSAPP_RECONNECT_BACKOFF_BASE": c.WhatsAppReconnectBackoffBase.String(),
		"PEGASUS_WHATSAPP_RECONNECT_BACKOFF_MAX":  c.WhatsAppReconnectBackoffMax.String(),
		"PEGASUS_WHATSAPP_LOG_LEVEL":              c.WhatsAppLogLevel,
	}
}

// WhatsAppConfig is the wasource configuration derived from c.
func (c *Config) WhatsAppConfig(logf func(string, ...any)) wasource.Config {
	return wasource.Config{
		SessionPath:          c.WhatsAppSessionPath,
		LogLevel:             c.WhatsAppLogLevel,
		ReconnectBackoffBase: c.WhatsAppReconnectBackoffBase,
		ReconnectBackoffMax:  c.WhatsAppReconnectBackoffMax,
		Logf:                 logf,
	}
}

// DSN builds the pgx connection string. Contains the password: never log it.
func (c *Config) DSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.DBUser, c.DBPassword),
		Host:   net.JoinHostPort(c.DBHost, strconv.Itoa(c.DBPort)),
		Path:   "/" + c.DBName,
	}
	q := url.Values{}
	q.Set("sslmode", c.DBSSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

func requireNonEmpty(v string) error {
	if v == "" {
		return fmt.Errorf("must not be empty")
	}
	return nil
}

func parsePort(v string) (int, error) {
	p, err := strconv.Atoi(v)
	if err != nil || p < 1 || p > 65535 {
		return 0, fmt.Errorf("must be a port number 1-65535")
	}
	return p, nil
}

func parsePositiveDuration(v string) (time.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("must be a Go duration like 30s or 5m")
	}
	if d <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return d, nil
}

func validateHTTPURL(v string) error {
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("must be an absolute http:// or https:// URL")
	}
	return nil
}

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
