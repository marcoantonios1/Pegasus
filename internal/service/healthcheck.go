package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// ResolveHTTPAddr resolves only PEGASUS_HTTP_ADDR (env > file > default)
// through the same settings table LoadConfig uses. The healthcheck
// subcommand uses this so it doesn't need, or validate, the rest of the
// configuration (notably the database password).
func ResolveHTTPAddr(lookupEnv func(string) (string, bool), configPath string) (string, error) {
	if configPath == "" {
		configPath, _ = lookupEnv(ConfigFileEnv)
	}
	var fileVals map[string]string
	if configPath != "" {
		var err error
		if fileVals, err = readConfigFile(configPath); err != nil {
			return "", err
		}
	}
	for _, s := range settings {
		if s.Env != "PEGASUS_HTTP_ADDR" {
			continue
		}
		v, ok := lookupEnv(s.Env)
		if !ok {
			v, ok = fileVals[s.FileKey()]
		}
		if !ok {
			v = s.Default
		}
		var c Config
		if err := s.apply(&c, strings.TrimSpace(v)); err != nil {
			return "", &ConfigError{Problems: []string{fmt.Sprintf("%s=%q is invalid: %v", s.Env, v, err)}}
		}
		return c.HTTPAddr, nil
	}
	return "", errors.New("PEGASUS_HTTP_ADDR missing from the settings table")
}

// dialableAddr turns a listen address into one a client can connect to:
// an empty or unspecified host (":8095", "0.0.0.0:8095", "[::]:8095")
// becomes loopback.
func dialableAddr(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// Healthcheck GETs /readyz on listenAddr and returns nil only for 200.
func Healthcheck(ctx context.Context, listenAddr string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := "http://" + dialableAddr(listenAddr) + "/readyz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %d %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
