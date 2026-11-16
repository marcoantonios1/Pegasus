// pegasus is the production Pegasus service: it keeps the memory
// pipeline running (live-window flushing, restart recovery, live intake
// once a source exists) and serves /healthz and /readyz.
//
//	pegasus [--config file.json]              run the service
//	pegasus healthcheck [--config f] [--addr host:port]
//	                                          GET /readyz; exit 0 if ready, 1 if not
//	pegasus pair [--config f]                 link WhatsApp (QR in the terminal);
//	                                          stop the service first
//
// Exit codes: 0 clean shutdown, 1 runtime failure (including a forced
// exit on a second signal), 2 configuration/usage error.
//
// All logic lives in internal/service; this file only parses flags,
// installs signal handling and maps errors to exit codes. See README.md
// "Configuration" for every setting.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/marcoantonios1/Pegasus/internal/ingestion/whatsapp/wasource"
	"github.com/marcoantonios1/Pegasus/internal/service"
)

const (
	exitOK     = 0
	exitFail   = 1
	exitConfig = 2
)

func main() {
	os.Exit(realMain(os.Args[1:], os.Stderr, os.LookupEnv, runService, os.Exit))
}

// runFunc is service.Run's shape after config loading; swapped in tests.
type runFunc func(ctx context.Context, cfg *service.Config) error

func runService(ctx context.Context, cfg *service.Config) error {
	return service.Run(ctx, cfg, service.Options{})
}

func realMain(args []string, stderr io.Writer, lookupEnv func(string) (string, bool), run runFunc, exit func(int)) int {
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetOutput(stderr)

	if len(args) > 0 && args[0] == "healthcheck" {
		return healthcheck(args[1:], stderr, lookupEnv)
	}
	if len(args) > 0 && args[0] == "pair" {
		return pair(args[1:], os.Stdout, stderr, lookupEnv)
	}

	fs := flag.NewFlagSet("pegasus", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "optional JSON config file (overrides "+service.ConfigFileEnv+"); environment variables take precedence over it")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "pegasus: unexpected argument %q (subcommands: healthcheck, pair)\n", fs.Arg(0))
		return exitConfig
	}

	cfg, err := service.LoadConfig(lookupEnv, *configPath)
	if err != nil {
		fmt.Fprintf(stderr, "pegasus: %v\n", err)
		return exitConfig
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		sig, ok := <-sigs
		if !ok {
			return
		}
		log.Printf("pegasus: received %s, shutting down gracefully (send again to force exit)", sig)
		cancel()
		sig, ok = <-sigs
		if !ok {
			return
		}
		log.Printf("pegasus: received second %s, forcing exit; unfinished messages stay processed=false and are recovered on next start", sig)
		exit(exitFail)
	}()

	if err := run(ctx, cfg); err != nil {
		if service.IsConfigError(err) {
			fmt.Fprintf(stderr, "pegasus: %v\n", err)
			return exitConfig
		}
		log.Printf("pegasus: %v", err)
		return exitFail
	}
	return exitOK
}

func healthcheck(args []string, stderr io.Writer, lookupEnv func(string) (string, bool)) int {
	fs := flag.NewFlagSet("pegasus healthcheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "optional JSON config file, as for the service")
	addr := fs.String("addr", "", "health server address (default: the configured PEGASUS_HTTP_ADDR)")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	if *addr == "" {
		a, err := service.ResolveHTTPAddr(lookupEnv, *configPath)
		if err != nil {
			fmt.Fprintf(stderr, "pegasus healthcheck: %v\n", err)
			return exitConfig
		}
		*addr = a
	}
	if err := service.Healthcheck(context.Background(), *addr); err != nil {
		fmt.Fprintf(stderr, "pegasus healthcheck: not ready: %v\n", err)
		return exitFail
	}
	return exitOK
}

// pair links the WhatsApp account interactively. The long-running service
// never pairs; it fails fast (exit 2) pointing here instead.
func pair(args []string, stdout, stderr io.Writer, lookupEnv func(string) (string, bool)) int {
	fs := flag.NewFlagSet("pegasus pair", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "optional JSON config file, as for the service")
	if err := fs.Parse(args); err != nil {
		return exitConfig
	}
	cfg, err := service.LoadConfig(lookupEnv, *configPath)
	if err != nil {
		fmt.Fprintf(stderr, "pegasus pair: %v\n", err)
		return exitConfig
	}
	fmt.Fprintln(stdout, "Make sure the pegasus service is STOPPED (`docker compose stop pegasus`): two clients on one session replace each other.")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := wasource.Pair(ctx, cfg.WhatsAppConfig(log.Printf), stdout); err != nil {
		fmt.Fprintf(stderr, "pegasus pair: %v\n", err)
		if strings.Contains(err.Error(), "inside the git repository") {
			return exitConfig
		}
		return exitFail
	}
	return exitOK
}
