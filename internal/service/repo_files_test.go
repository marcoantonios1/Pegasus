package service

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const repoRoot = "../.."

func readRepoFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestEverySettingIsDocumented keeps README.md's Configuration table and
// .env.example in sync with the settings table: adding a setting without
// documenting it fails here.
func TestEverySettingIsDocumented(t *testing.T) {
	readme := readRepoFile(t, "README.md")
	envExample := readRepoFile(t, ".env.example")
	for _, s := range settings {
		if !strings.Contains(readme, "`"+s.Env+"`") {
			t.Errorf("README.md does not document %s (expected `%s` in the Configuration table)", s.Env, s.Env)
		}
		if !regexp.MustCompile(`(?m)^#?\s*` + regexp.QuoteMeta(s.Env) + `=`).MatchString(envExample) {
			t.Errorf(".env.example has no %s= line", s.Env)
		}
		if !s.Secret && s.Default != "" && !strings.Contains(readme, s.Default) {
			t.Errorf("README.md does not show %s's default %q", s.Env, s.Default)
		}
	}
	for _, m := range FixedModels {
		if !strings.Contains(readme, m.Name) {
			t.Errorf("README.md does not show the fixed %s model %q", m.Role, m.Name)
		}
	}
}

// pegasusBlock returns the raw text of the pegasus service in the compose
// file (from "  pegasus:" to the next top-level or service-level key).
func pegasusBlock(t *testing.T, compose string) string {
	t.Helper()
	lines := strings.Split(compose, "\n")
	start := -1
	for i, l := range lines {
		if l == "  pegasus:" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatal("docker-compose.yml has no pegasus service")
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		l := lines[i]
		if l != "" && !strings.HasPrefix(l, "   ") && !strings.HasPrefix(strings.TrimSpace(l), "#") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

// TestComposeFile validates docker-compose.yml with a throwaway env file
// (skips without docker) and checks the pegasus service's invariants: no
// password literal, loopback-only port, grace period above the shutdown
// timeout, healthcheck via the binary.
func TestComposeFile(t *testing.T) {
	compose := readRepoFile(t, "docker-compose.yml")

	block := pegasusBlock(t, compose)
	if regexp.MustCompile(`(?i)password`).MatchString(block) {
		t.Errorf("pegasus service mentions a password; secrets must come only from env_file:\n%s", block)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not installed; skipping `docker compose config`")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
	const throwaway = "throwaway-test-password-1234"
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("POSTGRES_PASSWORD="+throwaway+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("docker", "compose", "config", "--format", "json")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "COMPOSE_PROJECT_NAME=pegasus-config-test")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok && strings.Contains(string(ee.Stderr), "Cannot connect") {
			t.Skipf("docker unavailable: %s", ee.Stderr)
		}
		t.Fatalf("docker compose config failed: %v\n%s", err, out)
	}

	var cfg struct {
		Services map[string]struct {
			Ports []struct {
				HostIP    string `json:"host_ip"`
				Target    int    `json:"target"`
				Published string `json:"published"`
			} `json:"ports"`
			StopGracePeriod string            `json:"stop_grace_period"`
			Environment     map[string]string `json:"environment"`
			DependsOn       map[string]struct {
				Condition string `json:"condition"`
			} `json:"depends_on"`
			Healthcheck struct {
				Test []string `json:"test"`
			} `json:"healthcheck"`
			ExtraHosts any `json:"extra_hosts"`
			Volumes    []struct {
				Type   string `json:"type"`
				Source string `json:"source"`
				Target string `json:"target"`
			} `json:"volumes"`
		} `json:"services"`
	}
	if err := json.Unmarshal(out, &cfg); err != nil {
		t.Fatalf("parse compose config: %v", err)
	}
	svc, ok := cfg.Services["pegasus"]
	if !ok {
		t.Fatal("resolved config has no pegasus service")
	}
	if len(svc.Ports) == 0 {
		t.Error("pegasus publishes no health port")
	}
	for _, p := range svc.Ports {
		if p.HostIP != "127.0.0.1" {
			t.Errorf("port %s:%d published on %q, want 127.0.0.1 only", p.Published, p.Target, p.HostIP)
		}
	}
	grace, err := time.ParseDuration(svc.StopGracePeriod)
	if err != nil || grace <= DefaultShutdownTimeout {
		t.Errorf("stop_grace_period %q must exceed PEGASUS_SHUTDOWN_TIMEOUT default %s", svc.StopGracePeriod, DefaultShutdownTimeout)
	}
	if strings.Join(svc.Healthcheck.Test, " ") != "CMD /pegasus healthcheck" {
		t.Errorf("healthcheck test = %v", svc.Healthcheck.Test)
	}
	if svc.DependsOn["postgres"].Condition != "service_healthy" || svc.DependsOn["migrate"].Condition != "service_completed_successfully" {
		t.Errorf("depends_on = %+v", svc.DependsOn)
	}
	if svc.Environment["PEGASUS_DB_HOST"] != "postgres" {
		t.Errorf("PEGASUS_DB_HOST = %q, want postgres", svc.Environment["PEGASUS_DB_HOST"])
	}
	if !strings.Contains(svc.Environment["PEGASUS_COSTGUARD_URL"], "host.docker.internal") {
		t.Errorf("PEGASUS_COSTGUARD_URL = %q, want host.docker.internal default", svc.Environment["PEGASUS_COSTGUARD_URL"])
	}
	// The WhatsApp session lives on its own named volume, at the path the
	// service is told to use, and the backup service never mounts it.
	foundSession := false
	for _, v := range svc.Volumes {
		if v.Target == "/var/lib/pegasus/whatsmeow" {
			foundSession = v.Type == "volume" && v.Source == "whatsmeow_session"
		}
	}
	if !foundSession {
		t.Errorf("pegasus must mount the whatsmeow_session named volume at /var/lib/pegasus/whatsmeow: %+v", svc.Volumes)
	}
	if got := svc.Environment["PEGASUS_WHATSAPP_SESSION_PATH"]; !strings.HasPrefix(got, "/var/lib/pegasus/whatsmeow/") {
		t.Errorf("PEGASUS_WHATSAPP_SESSION_PATH = %q, want a path on the session volume", got)
	}
	for _, v := range cfg.Services["backup"].Volumes {
		if v.Source == "whatsmeow_session" || strings.Contains(v.Target, "whatsmeow") {
			t.Errorf("backup service mounts the WhatsApp session (%+v); it is a credential and must not be backed up", v)
		}
	}

	// The password reaches the container only through env_file.
	if svc.Environment["POSTGRES_PASSWORD"] != throwaway {
		t.Errorf("POSTGRES_PASSWORD should come from the env file")
	}
}
