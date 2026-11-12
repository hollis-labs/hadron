// Command hadron is the Hadron daemon and CLI, built on Substrate. This file is
// the app's own code; the shell (config, logging, health, HTTP, auth, MCP,
// database, queue, scheduler, events) comes from the manifest.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/hollis-labs/substrate/boot"
	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/cli"
	"github.com/hollis-labs/substrate/modules/database"
	"github.com/hollis-labs/substrate/modules/httphost"
	"github.com/hollis-labs/substrate/modules/mcp"
	"github.com/hollis-labs/substrate/modules/policy"
	"github.com/hollis-labs/substrate/modules/scheduler"

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
	root.AddCommand(serveCmd(ctx, run.App, fires), mcpCmd(ctx, run.App))
	if err := root.ExecuteContext(ctx); err != nil {
		_ = run.Stop(context.Background())
		os.Exit(1)
	}
}

// serveCmd is the daemon: it builds the workflow runtime over the framework's
// database, starts its workers and the scheduler engine, then serves HTTP.
func serveCmd(ctx context.Context, app *module.App, fires *runtime.Fires) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the Hadron daemon",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			cfg, err := runtime.ConfigFromApp(app)
			if err != nil {
				return err
			}
			db := database.From(app)
			if db == nil {
				return errors.New("hadron: the database module is not started")
			}
			rt, err := runtime.New(persistence.NewStore(db.DB()), cfg,
				runtime.WithFires(fires), runtime.WithSourceEngine(scheduler.Engine(app)))
			if err != nil {
				return err
			}
			// The runtime starts the scheduler engine with the recovered host.
			if err := rt.Start(ctx); err != nil {
				return err
			}
			h, err := httphost.Handler(app, httphost.Options{})
			if err != nil {
				_ = rt.Shutdown(context.Background())
				return err
			}
			serveErr := httphost.Serve(ctx, app, h, httphost.ServeOptions{})
			shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return errors.Join(serveErr, rt.Shutdown(shutdown))
		},
	}
}

// mcpCmd serves the MCP tools over stdio for an agent that launches Hadron as a subprocess.
func mcpCmd(ctx context.Context, app *module.App) *cobra.Command {
	var tokenFile string
	c := &cobra.Command{
		Use:   "mcp",
		Short: "Serve MCP over stdio",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return mcp.ServeStdio(ctx, app, mcp.StdioOptions{TokenFile: tokenFile})
		},
	}
	c.Flags().StringVar(&tokenFile, "token-file", "", "file holding the caller's service token (required when auth is enforcing)")
	return c
}
