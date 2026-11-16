package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/marcoantonios1/Pegasus/internal/service"
)

// helperEnv switches the test binary into a stand-in for the real
// process (see TestMain): realMain with a run function that ignores
// cancellation, so a second signal is the only way out.
const helperEnv = "PEGASUS_MAIN_TEST_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "hang" {
		env := func(k string) (string, bool) {
			if k == "POSTGRES_PASSWORD" {
				return "unused", true
			}
			return "", false
		}
		hang := func(ctx context.Context, _ *service.Config) error {
			fmt.Println("running")
			<-ctx.Done()
			fmt.Println("cancelled, still busy")
			time.Sleep(time.Hour) // a shutdown that never finishes
			return nil
		}
		os.Exit(realMain(nil, os.Stderr, env, hang, os.Exit))
	}
	os.Exit(m.Run())
}

func TestRealMain_ConfigErrorExits2(t *testing.T) {
	env := func(string) (string, bool) { return "", false } // no POSTGRES_PASSWORD
	called := false
	run := func(context.Context, *service.Config) error { called = true; return nil }
	var stderr strings.Builder
	if code := realMain(nil, &stderr, env, run, func(int) {}); code != exitConfig {
		t.Fatalf("exit code %d, want %d", code, exitConfig)
	}
	if called {
		t.Error("run must not be called with an invalid config")
	}
	if !strings.Contains(stderr.String(), "POSTGRES_PASSWORD") {
		t.Errorf("stderr should name the missing setting: %s", stderr.String())
	}
	if code := realMain([]string{"bogus"}, io.Discard, env, run, func(int) {}); code != exitConfig {
		t.Errorf("unknown argument: exit %d, want %d", code, exitConfig)
	}
}

func TestRealMain_RuntimeErrorExits1(t *testing.T) {
	env := func(k string) (string, bool) {
		if k == "POSTGRES_PASSWORD" {
			return "x", true
		}
		return "", false
	}
	run := func(context.Context, *service.Config) error { return errors.New("boom") }
	if code := realMain(nil, io.Discard, env, run, func(int) {}); code != exitFail {
		t.Fatalf("exit code %d, want %d", code, exitFail)
	}
}

// TestHealthcheckSubcommand: exit 0 against a ready server, 1 against a
// failing one (and against nothing listening).
func TestHealthcheckSubcommand(t *testing.T) {
	noEnv := func(string) (string, bool) { return "", false }
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(`{"status":"ready"}`))
	}))
	defer ready.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	host := func(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }
	if code := realMain([]string{"healthcheck", "--addr", host(ready)}, io.Discard, noEnv, nil, nil); code != 0 {
		t.Errorf("ready server: exit %d, want 0", code)
	}
	if code := realMain([]string{"healthcheck", "--addr", host(failing)}, io.Discard, noEnv, nil, nil); code != 1 {
		t.Errorf("failing server: exit %d, want 1", code)
	}

	// Address taken from PEGASUS_HTTP_ADDR when --addr is absent; no
	// password needed.
	envAddr := func(k string) (string, bool) {
		if k == "PEGASUS_HTTP_ADDR" {
			return host(ready), true
		}
		return "", false
	}
	if code := realMain([]string{"healthcheck"}, io.Discard, envAddr, nil, nil); code != 0 {
		t.Errorf("env-configured address: exit %d, want 0", code)
	}
}

// TestForcedExitOnSecondSignal: first SIGTERM starts a graceful shutdown
// (here one that never finishes); the second forces exit code 1.
func TestForcedExitOnSecondSignal(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), helperEnv+"=hang")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	lines := bufio.NewScanner(stdout)
	waitLine := func(want string) {
		t.Helper()
		got := make(chan bool, 1)
		go func() { got <- lines.Scan() && lines.Text() == want }()
		select {
		case ok := <-got:
			if !ok {
				t.Fatalf("helper did not print %q", want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
	waitLine("running")

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitLine("cancelled, still busy")

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case err := <-exited:
		t.Fatalf("process exited after the first signal (%v); it should still be shutting down", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != exitFail {
			t.Fatalf("second signal: got %v, want exit code %d", err, exitFail)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second signal did not force exit")
	}
}

// TestBinary_SIGTERMExitsCleanly builds the real binary, runs it against
// the test database with no live source, waits until `pegasus
// healthcheck` passes, sends SIGTERM and expects exit code 0 within the
// shutdown timeout. Skips without a reachable test database (same
// convention as the other DB-backed tests; auth failures fail).
func TestBinary_SIGTERMExitsCleanly(t *testing.T) {
	dsn := os.Getenv("PEGASUS_TEST_DATABASE_URL")
	if dsn == "" {
		if pw := os.Getenv("POSTGRES_PASSWORD"); pw != "" {
			dsn = "postgres://pegasus:" + pw + "@localhost:5432/pegasus?sslmode=disable"
		}
	}
	if dsn == "" {
		t.Skip("no test database configured — set PEGASUS_TEST_DATABASE_URL, or POSTGRES_PASSWORD (`set -a; source .env; set +a`), to enable this test")
	}
	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := pgx.ConnectConfig(ctx, pc)
	cancel()
	if err != nil {
		if strings.Contains(err.Error(), "password authentication failed") || strings.Contains(err.Error(), "28P01") {
			t.Fatalf("authentication failed connecting to the test database (%v) — re-export POSTGRES_PASSWORD or fix PEGASUS_TEST_DATABASE_URL", err)
		}
		t.Skipf("no test database available (%v) — run `docker compose up -d postgres migrate` to enable this test", err)
	}
	conn.Close(context.Background())
	sslmode := "disable"
	if u, err := url.Parse(dsn); err == nil && u.Query().Get("sslmode") != "" {
		sslmode = u.Query().Get("sslmode")
	}

	bin := filepath.Join(t.TempDir(), "pegasus")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	addr := freeAddr(t)
	cmd := exec.Command(bin)
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"POSTGRES_PASSWORD=" + pc.Password,
		"PEGASUS_DB_HOST=" + pc.Host,
		"PEGASUS_DB_PORT=" + strconv.Itoa(int(pc.Port)),
		"PEGASUS_DB_NAME=" + pc.Database,
		"PEGASUS_DB_USER=" + pc.User,
		"PEGASUS_DB_SSLMODE=" + sslmode,
		"PEGASUS_HTTP_ADDR=" + addr,
		"PEGASUS_RECOVER_ON_START=false", // never touch other tests' unprocessed rows
		"PEGASUS_COSTGUARD_URL=http://127.0.0.1:1",
	}
	var logs strings.Builder
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	deadline := time.Now().Add(20 * time.Second)
	for {
		hc := exec.Command(bin, "healthcheck", "--addr", addr)
		if hc.Run() == nil {
			break
		}
		select {
		case err := <-exited:
			t.Fatalf("binary exited during startup: %v\n%s", err, logs.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("binary never became ready\n%s", logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("exit after SIGTERM: %v, want 0\n%s", err, logs.String())
		}
	case <-time.After(service.DefaultShutdownTimeout):
		t.Fatalf("binary did not exit within %s of SIGTERM\n%s", service.DefaultShutdownTimeout, logs.String())
	}
	if !strings.Contains(logs.String(), "shutdown complete") {
		t.Errorf("expected the shutdown sequence to complete:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), pc.Password) {
		t.Error("logs contain the database password")
	}

	// Nothing listening anymore: healthcheck exits 1.
	if err := exec.Command(bin, "healthcheck", "--addr", addr).Run(); err == nil {
		t.Error("healthcheck should fail once the service has stopped")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}
