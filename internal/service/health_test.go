package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func readyServer(st *healthState) *httptest.Server {
	return httptest.NewServer(newHealthHandler(st))
}

func okFn(context.Context) error   { return nil }
func failFn(context.Context) error { return errors.New("connection refused to secret-host:5432") }

func getReady(t *testing.T, url string) (int, readyResponse, string) {
	t.Helper()
	resp, err := http.Get(url + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body readyResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body, string(raw)
}

func TestReadyz(t *testing.T) {
	t.Run("ready; costguard down is informational only", func(t *testing.T) {
		srv := readyServer(&healthState{ping: okFn, probeCostguard: failFn})
		defer srv.Close()
		code, body, _ := getReady(t, srv.URL)
		if code != 200 || body.Status != "ready" {
			t.Fatalf("got %d %+v", code, body)
		}
		if c := body.Checks["costguard"]; c.Status != "unreachable" || !c.Informational {
			t.Errorf("costguard check: %+v", c)
		}
	})
	t.Run("db down -> 503, no upstream error text", func(t *testing.T) {
		srv := readyServer(&healthState{ping: failFn, probeCostguard: okFn})
		defer srv.Close()
		code, body, raw := getReady(t, srv.URL)
		if code != 503 || body.Checks["database"].Status != "fail" {
			t.Fatalf("got %d %+v", code, body)
		}
		if strings.Contains(raw, "secret-host") {
			t.Errorf("readyz leaked upstream error text: %s", raw)
		}
	})
	t.Run("migration failure -> 503", func(t *testing.T) {
		srv := readyServer(&healthState{ping: okFn, probeCostguard: okFn, migrationErr: &MigrationError{"schema is behind"}})
		defer srv.Close()
		code, body, _ := getReady(t, srv.URL)
		if code != 503 || body.Checks["migrations"].Status != "fail" {
			t.Fatalf("got %d %+v", code, body)
		}
	})
}

func TestHealthcheck(t *testing.T) {
	ready := readyServer(&healthState{ping: okFn, probeCostguard: okFn})
	defer ready.Close()
	if err := Healthcheck(context.Background(), strings.TrimPrefix(ready.URL, "http://")); err != nil {
		t.Errorf("expected ready server to pass: %v", err)
	}

	failing := readyServer(&healthState{ping: failFn, probeCostguard: okFn})
	defer failing.Close()
	if err := Healthcheck(context.Background(), strings.TrimPrefix(failing.URL, "http://")); err == nil {
		t.Error("expected failing server to fail the healthcheck")
	}

	if err := Healthcheck(context.Background(), "127.0.0.1:1"); err == nil {
		t.Error("expected nothing-listening to fail the healthcheck")
	}
}
