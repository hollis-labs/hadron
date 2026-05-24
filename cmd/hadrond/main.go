package main

import (
	"context"
	"errors"
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

	feotel "github.com/hollis-labs/go-otel"

	"github.com/hollis-labs/hadron/internal/agentsubstrate"
	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/telemetry"
	"github.com/hollis-labs/hadron/internal/trigger"
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
	otelShutdown, otelErr := feotel.Init(otelCtx, feotel.WithServiceName("hadron"))
	if otelErr != nil {
		log.Printf("warning: OTel init failed: %v", otelErr)
	} else {
		defer func() { _ = otelShutdown(otelCtx) }()
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

	tel := telemetry.New(cfg.LogsDir, sett.Telemetry.Enabled)

	mgr := execution.NewManager(store, sett, workers, cfg.LogsDir, tel)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	sched.Start()
	defer sched.Stop()

	pipelineRunner := pipeline.NewRunner(store, mgr)
	serveReg := registry.New(store)
	pipelineRunner.SetBlueprintResolver(serveReg.Resolve)
	internalMCP := mcpadapter.New(store, mgr, sched, pipelineRunner, "internal", mcpadapter.AllScopes(),
		mcpadapter.WithServerVersion(version),
		mcpadapter.WithBlueprintDir(sett.BlueprintDir),
		mcpadapter.WithRegistry(serveReg))
	internalCaller := mcpadapter.NewInternalCaller(internalMCP, mcpadapter.WithExternalServers(externalMCPServers(sett)))
	defer func() { _ = internalCaller.Close() }()
	mgr.SetMCPCaller(internalCaller)
	agentLauncher := agentsubstrate.NewLauncher(cfg.DataDir, sett.AgentSubstrates)
	defer func() { _ = agentLauncher.Close() }()
	mgr.SetAgentLauncher(agentLauncher)
	messageService := messagesubstrate.New(store, sett.MessageSubstrates)
	mgr.SetMessageSource(messageService)

	srv := api.NewServer(cfg.Addr, api.Dependencies{
		Runs:         store,
		Schedules:    store,
		Pipelines:    store,
		Workspaces:   store,
		Triggers:     store,
		HumanGates:   store,
		Messages:     messageService,
		Runner:       mgr,
		Scheduler:    sched,
		Pipeline:     pipelineRunner,
		BlueprintDir: sett.BlueprintDir,
	})

	// Start trigger file watchers and TTL cleanup.
	trigMgr := trigger.New(store, mgr)
	trigMgr.StartFileWatchers()
	trigMgr.StartTTLCleanup(60 * time.Second)
	defer trigMgr.StopFileWatchers()
	defer trigMgr.StopTTLCleanup()

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
	defer func() { _ = store.Close() }()

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
	reg := registry.New(store)
	pipelineRunner.SetBlueprintResolver(reg.Resolve)
	internalMCP := mcpadapter.New(store, mgr, sched, pipelineRunner, "internal", mcpadapter.AllScopes(),
		mcpadapter.WithServerVersion(version),
		mcpadapter.WithBlueprintDir(sett.BlueprintDir),
		mcpadapter.WithRegistry(reg))
	internalCaller := mcpadapter.NewInternalCaller(internalMCP, mcpadapter.WithExternalServers(externalMCPServers(sett)))
	defer func() { _ = internalCaller.Close() }()
	mgr.SetMCPCaller(internalCaller)
	agentLauncher := agentsubstrate.NewLauncher(cfg.DataDir, sett.AgentSubstrates)
	defer func() { _ = agentLauncher.Close() }()
	mgr.SetAgentLauncher(agentLauncher)
	messageService := messagesubstrate.New(store, sett.MessageSubstrates)
	mgr.SetMessageSource(messageService)

	var scopes []string
	if *scopesFlag != "" {
		for _, s := range strings.Split(*scopesFlag, ",") {
			s = strings.TrimSpace(s)
			if s != "" {
				scopes = append(scopes, s)
			}
		}
	}

	adapter := mcpadapter.New(store, mgr, sched, pipelineRunner, *tokenFlag, scopes,
		mcpadapter.WithServerVersion(version),
		mcpadapter.WithBlueprintDir(sett.BlueprintDir),
		mcpadapter.WithRegistry(reg))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return adapter.Run(ctx)
}

func externalMCPServers(sett *settings.Settings) map[string]mcpadapter.ExternalServerConfig {
	if sett == nil || len(sett.MCPServers) == 0 {
		return nil
	}
	out := make(map[string]mcpadapter.ExternalServerConfig, len(sett.MCPServers))
	for name, server := range sett.MCPServers {
		out[name] = mcpadapter.ExternalServerConfig{
			Transport:      server.Transport,
			Command:        server.Command,
			Args:           append([]string(nil), server.Args...),
			Env:            cloneStringMap(server.Env),
			URL:            server.URL,
			Headers:        cloneStringMap(server.Headers),
			TimeoutSeconds: server.TimeoutSeconds,
		}
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
