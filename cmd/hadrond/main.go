package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"encoding/json"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/telemetry"
)

const version = "0.4.0"

func main() {
	args := os.Args[1:]
	subcommand := "serve"
	if len(args) > 0 {
		subcommand = args[0]
		args = args[1:]
	}

	switch subcommand {
	case "serve":
		if err := runServe(args); err != nil {
			log.Fatalf("hadrond serve: %v", err)
		}
	case "mcp":
		if err := runMCP(args); err != nil {
			log.Fatalf("hadrond mcp: %v", err)
		}
	case "--help", "-h", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "hadrond: unknown subcommand %q\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: hadrond <subcommand> [flags]")
	fmt.Println()
	fmt.Println("Subcommands:")
	fmt.Println("  serve   Start HTTP REST API server (default)")
	fmt.Println("  mcp     Start MCP stdio adapter")
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addrFlag := fs.String("addr", "", "listen address (default: "+config.DefaultAddr+")")
	dbFlag := fs.String("db", "", "SQLite database path")
	logsFlag := fs.String("logs", "", "run logs directory")
	dataFlag := fs.String("data", "", "data directory")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	if *addrFlag != "" {
		cfg.Addr = *addrFlag
	}
	if *dbFlag != "" {
		cfg.DBPath = *dbFlag
	}
	if *logsFlag != "" {
		cfg.LogsDir = *logsFlag
	}
	if *dataFlag != "" {
		cfg.DataDir = *dataFlag
	}

	if err := cfg.Ensure(); err != nil {
		return fmt.Errorf("ensure dirs: %w", err)
	}

	store, err := persistence.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	sett, err := settings.Load(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	workers := sett.Execution.Workers
	if workers <= 0 {
		workers = 3
	}

	tel := telemetry.New(cfg.LogsDir, sett.Telemetry.Enabled)

	mgr := execution.NewManager(store, sett, workers, cfg.LogsDir, tel)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	sched.Start()
	defer sched.Stop()

	pipelineRunner := pipeline.NewRunner(store, mgr)

	srv := api.NewServer(cfg.Addr, api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	})

	startMsg, _ := json.Marshal(map[string]string{
		"level":   "info",
		"msg":     "hadron daemon starting",
		"addr":    cfg.Addr,
		"db":      cfg.DBPath,
		"version": version,
	})
	log.Println(string(startMsg))

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Println("shutting down...")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
		return nil
	}
}

func runMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	dbFlag := fs.String("db", "", "SQLite database path")
	logsFlag := fs.String("logs", "", "run logs directory")
	dataFlag := fs.String("data", "", "data directory")
	tokenFlag := fs.String("token", "", "bearer token for mutating tools")
	scopesFlag := fs.String("token-scopes", "", "comma-separated scopes (e.g. run.write,pipeline.write)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := config.Default()
	if *dbFlag != "" {
		cfg.DBPath = *dbFlag
	}
	if *logsFlag != "" {
		cfg.LogsDir = *logsFlag
	}
	if *dataFlag != "" {
		cfg.DataDir = *dataFlag
	}

	if err := cfg.Ensure(); err != nil {
		return fmt.Errorf("ensure dirs: %w", err)
	}

	store, err := persistence.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	sett, err := settings.Load(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	workers := sett.Execution.Workers
	if workers <= 0 {
		workers = 3
	}

	tel := telemetry.New(cfg.LogsDir, sett.Telemetry.Enabled)

	mgr := execution.NewManager(store, sett, workers, cfg.LogsDir, tel)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	sched.Start()
	defer sched.Stop()

	pipelineRunner := pipeline.NewRunner(store, mgr)

	var scopes []string
	if *scopesFlag != "" {
		for _, s := range strings.Split(*scopesFlag, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				scopes = append(scopes, s)
			}
		}
	}

	adapter := mcpadapter.New(store, mgr, sched, pipelineRunner, *tokenFlag, scopes)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return adapter.Run(ctx)
}
