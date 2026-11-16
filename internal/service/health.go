package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	readyDBTimeout        = 2 * time.Second
	costguardProbeTimeout = 2 * time.Second
)

// checkResult is one entry in /readyz's JSON body. Detail is a fixed,
// non-private description (never an upstream error string, which could
// carry hostnames, query text or response bodies).
type checkResult struct {
	Status        string `json:"status"`
	Detail        string `json:"detail,omitempty"`
	Informational bool   `json:"informational,omitempty"`
}

type readyResponse struct {
	Status string                 `json:"status"`
	Checks map[string]checkResult `json:"checks"`
}

// healthState is what /readyz reports on. migrationErr is fixed at
// startup; ping and probeCostguard run per request.
type healthState struct {
	ping           func(ctx context.Context) error
	migrationErr   error
	probeCostguard func(ctx context.Context) error
}

func newHealthHandler(st *healthState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		resp := st.ready(r.Context())
		code := http.StatusOK
		if resp.Status != "ready" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, resp)
	})
	return mux
}

func (st *healthState) ready(ctx context.Context) readyResponse {
	var (
		wg       sync.WaitGroup
		dbErr    error
		probeErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		c, cancel := context.WithTimeout(ctx, readyDBTimeout)
		defer cancel()
		dbErr = st.ping(c)
	}()
	go func() {
		defer wg.Done()
		c, cancel := context.WithTimeout(ctx, costguardProbeTimeout)
		defer cancel()
		probeErr = st.probeCostguard(c)
	}()
	wg.Wait()

	resp := readyResponse{Status: "ready", Checks: map[string]checkResult{}}

	if dbErr != nil {
		resp.Status = "not_ready"
		resp.Checks["database"] = checkResult{Status: "fail", Detail: fmt.Sprintf("ping did not succeed within %s", readyDBTimeout)}
	} else {
		resp.Checks["database"] = checkResult{Status: "ok"}
	}

	if st.migrationErr != nil {
		resp.Status = "not_ready"
		resp.Checks["migrations"] = checkResult{Status: "fail", Detail: migrationDetail(st.migrationErr)}
	} else {
		resp.Checks["migrations"] = checkResult{Status: "ok"}
	}

	// Informational only: Costguard down never makes the service unready
	// (messages are still stored; extraction is recoverable).
	if probeErr != nil {
		resp.Checks["costguard"] = checkResult{Status: "unreachable", Informational: true}
	} else {
		resp.Checks["costguard"] = checkResult{Status: "reachable", Informational: true}
	}
	return resp
}

func migrationDetail(err error) string {
	if me, ok := err.(*MigrationError); ok {
		return me.Msg
	}
	return "migration check failed"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

// costguardProber returns a reachability probe: GET {base}/healthz. Any
// 2xx counts as reachable. The response body is never read or logged.
func costguardProber(baseURL string) func(ctx context.Context) error {
	client := &http.Client{Timeout: costguardProbeTimeout}
	url := strings.TrimRight(baseURL, "/") + "/healthz"
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	}
}
