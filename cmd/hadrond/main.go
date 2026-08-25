package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"encoding/json"

	feotel "github.com/hollis-labs/go-otel"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/webui"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

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
	case "version", "--version", "-v":
		printVersion()
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
	fmt.Println("  version Print version information")
}

func printVersion() {
	fmt.Printf("hadrond %s\n", version)
	fmt.Printf("commit: %s\n", commit)
	fmt.Printf("built: %s\n", buildDate)
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

	// Initialize OpenTelemetry tracing.
	otelCtx := context.Background()
	otelShutdown, otelErr := feotel.Init(
		otelCtx,
		feotel.WithServiceName("hadron"),
		feotel.WithServiceVersion(version),
		feotel.WithServiceNamespace("hollis"),
		feotel.WithEnvironment(hadronEnvironment()),
	)
	if otelErr != nil {
		log.Printf("warning: OTel init failed: %v", otelErr)
	} else {
		defer func() { _ = otelShutdown(otelCtx) }()
	}

	// Install a trace-correlated slog handler as the process default. Any code
	// that emits via slog with a traced context (current code uses stdlib log,
	// but future migrations + imported libraries that use slog) automatically
	// includes trace_id / span_id fields. NewLogHandler wraps an inner handler
	// (text on stderr here) and is a no-op for untraced contexts, so this is
	// safe to install unconditionally — even when OTel init failed above.
	slog.SetDefault(slog.New(feotel.NewLogHandler(slog.NewTextHandler(os.Stderr, nil))))

	store, err := persistence.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = store.Close() }()

	sett, err := settings.Load(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	workers := sett.Execution.Workers
	if workers <= 0 {
		workers = 3
	}

	workflowRuntime, err := newProductionWorkflowRuntime(store, cfg, workers)
	if err != nil {
		return fmt.Errorf("compose graph workflow runtime: %w", err)
	}
	if err := workflowRuntime.Start(context.Background()); err != nil {
		return fmt.Errorf("start graph workflow runtime: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if shutdownErr := workflowRuntime.Shutdown(shutdownCtx); shutdownErr != nil {
			log.Printf("workflow runtime shutdown error: %v", shutdownErr)
		}
	}()

	srv := api.NewServer(cfg.Addr, api.Dependencies{
		Workspaces: store, Workflows: workflowRuntime.operations, WorkflowReads: workflowRuntime.operations,
		WorkflowLifecycle: workflowRuntime.lifecycle, WorkflowAuth: workflowRuntime.auth,
		WorkflowActivations: workflowRuntime.externalActivations,
		A2ATasks:            workflowRuntime.a2a, AgentCard: workflowRuntime.card,
		WorkflowHealth: workflowRuntime.host, BuildVersion: version, WebUI: webui.Handler(),
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
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	tokenFlag := fs.String("token", "", "durable workflow principal bearer token")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tokenFlag == "" {
		return errors.New("mcp requires --token for a durable workflow principal")
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
	defer func() { _ = store.Close() }()

	sett, err := settings.Load(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}

	workers := sett.Execution.Workers
	if workers <= 0 {
		workers = 3
	}

	workflowRuntime, err := newProductionWorkflowRuntime(store, cfg, workers)
	if err != nil {
		return fmt.Errorf("compose graph workflow runtime: %w", err)
	}
	if err := workflowRuntime.Start(context.Background()); err != nil {
		return fmt.Errorf("start graph workflow runtime: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if shutdownErr := workflowRuntime.Shutdown(shutdownCtx); shutdownErr != nil {
			log.Printf("workflow runtime shutdown error: %v", shutdownErr)
		}
	}()
	if err := workflowRuntime.BootstrapMCP(context.Background(), *tokenFlag); err != nil {
		return fmt.Errorf("bootstrap MCP workflow principal: %w", err)
	}
	adapter := mcpadapter.New(nil, nil, nil, nil, *tokenFlag, nil,
		mcpadapter.WithServerVersion(version),
		mcpadapter.WithWorkflowOnly(),
		mcpadapter.WithWorkflowServices(workflowRuntime.exposure, workflowRuntime.operations, workflowRuntime.operations, workflowRuntime.operations),
		mcpadapter.WithWorkflowLifecycle(workflowRuntime.lifecycle))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return adapter.Run(ctx)
}

func workflowSourceRoot(cfg *config.Config) string {
	return filepath.Join(cfg.DataDir, "workflows")
}

func hadronEnvironment() string {
	for _, key := range []string{"HOLLIS_ENV", "APP_ENV", "ENV"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return "development"
}
