// Command hadron is the Hadron daemon and CLI, built on Substrate. This file is
// the app's own code; the shell (config, logging, health, HTTP, auth, MCP,
// database, queue, scheduler, events) comes from the manifest.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/hollis-labs/substrate/boot"
	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/auth"
	"github.com/hollis-labs/substrate/modules/cli"
	"github.com/hollis-labs/substrate/modules/database"
	"github.com/hollis-labs/substrate/modules/httphost"
	"github.com/hollis-labs/substrate/modules/mcp"
	"github.com/hollis-labs/substrate/modules/policy"
	"github.com/hollis-labs/substrate/modules/scheduler"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/mcptools"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Route access is decided by the scopes each route declares; what a workflow
	// run may do is decided inside the engine host (internal/runtime), so the
	// framework decision point has nothing further to add.
	policy.UsePDP(policy.AllowAll{})

	// The scheduler module's runner is the workflow activation service, which
	// exists only once the Runtime is built; Fires is bound to it in serve.
	fires := runtime.RegisterScheduler()

	migFS, migRoot := persistence.Migrations()
	database.UseMigrationFS(migFS, migRoot)

	run, err := boot.Start(ctx, substrateApp, substrateModules, substrateConfigJSON)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hadron:", err)
		os.Exit(1)
	}
	defer func() { _ = run.Stop(context.Background()) }()

	root := cli.Root(run.App)
	root.AddCommand(serveCmd(ctx, run.App, fires), mcpCmd(ctx, run.App, fires), workflowCmd(ctx, run.App), buildCmd())
	if err := root.ExecuteContext(ctx); err != nil {
		_ = run.Stop(context.Background())
		os.Exit(1)
	}
}

// startRuntime builds the workflow runtime over the framework's database,
// registers the MCP workflow tools and starts the runtime (recovery, workers,
// the scheduler engine). The caller shuts it down.
func startRuntime(ctx context.Context, app *module.App, fires *runtime.Fires) (*runtime.Runtime, error) {
	cfg, err := runtime.ConfigFromApp(app)
	if err != nil {
		return nil, err
	}
	db := database.From(app)
	if db == nil {
		return nil, errors.New("hadron: the database module is not started")
	}
	rt, err := runtime.New(persistence.NewStore(db.DB()), cfg,
		runtime.WithFires(fires), runtime.WithSourceEngine(scheduler.Engine(app)))
	if err != nil {
		return nil, err
	}
	if err := mcptools.Register(app, mcptools.Dependencies{
		Operations: rt.Operations(), Reads: rt.Operations(), Signals: rt.Operations(), Lifecycle: rt.Lifecycle(),
	}); err != nil {
		return nil, err
	}
	// The runtime starts the scheduler engine with the recovered host.
	if err := rt.Start(ctx); err != nil {
		return nil, err
	}
	return rt, nil
}

func stopRuntime(rt *runtime.Runtime) error {
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return rt.Shutdown(shutdown)
}

// serveCmd is the daemon: the workflow runtime, the /api/workflows routes and
// the /mcp endpoint on the framework's HTTP host.
func serveCmd(ctx context.Context, app *module.App, fires *runtime.Fires) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the Hadron daemon",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			defer startTelemetry(version)()
			rt, err := startRuntime(ctx, app, fires)
			if err != nil {
				return err
			}
			srv := api.NewServer(api.Dependencies{
				Workflows: rt.Operations(), WorkflowReads: rt.Operations(), WorkflowSignals: rt.Operations(), WorkflowLifecycle: rt.Lifecycle(),
				WorkflowAuth:        runtime.RequestAuthenticator{Source: "http", Mode: auth.Guard(app).Mode},
				WorkflowActivations: rt.Activations(), A2ATasks: rt.A2A(), AgentCard: rt.AgentCard(),
			})
			api.RegisterScopes(auth.ScopeTable(app))
			mux := http.NewServeMux()
			mux.Handle("/api/workflows/", srv.Handler())
			h, err := httphost.Handler(app, httphost.Options{Routes: mux, Mounts: srv.Mounts()})
			if err != nil {
				_ = stopRuntime(rt)
				return err
			}
			serveErr := httphost.Serve(ctx, app, withTracing(h), httphost.ServeOptions{})
			return errors.Join(serveErr, stopRuntime(rt))
		},
	}
}

// mcpCmd serves the MCP tools over stdio for an agent that launches Hadron as a
// subprocess. The process runs the workflow runtime in-process, over the same
// database as the daemon; claims and leases keep two processes from running the
// same work twice.
func mcpCmd(ctx context.Context, app *module.App, fires *runtime.Fires) *cobra.Command {
	var tokenFile string
	c := &cobra.Command{
		Use:   "mcp",
		Short: "Serve MCP over stdio",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			defer startTelemetry(version)()
			rt, err := startRuntime(ctx, app, fires)
			if err != nil {
				return err
			}
			serveErr := mcp.ServeStdio(ctx, app, mcp.StdioOptions{TokenFile: tokenFile})
			return errors.Join(serveErr, stopRuntime(rt))
		},
	}
	c.Flags().StringVar(&tokenFile, "token-file", "", "file holding the caller's service token (required when auth is enforcing)")
	return c
}
