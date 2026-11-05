// Command hadron is the Hadron daemon and CLI, built on Substrate. This file is
// the app's own code; the shell (config, logging, health, HTTP, auth, MCP,
// database, queue, scheduler, events) comes from the manifest.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/hollis-labs/substrate/boot"
	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/cli"
	"github.com/hollis-labs/substrate/modules/database"
	"github.com/hollis-labs/substrate/modules/httphost"
	"github.com/hollis-labs/substrate/modules/mcp"
	"github.com/hollis-labs/substrate/modules/policy"
	"github.com/hollis-labs/substrate/modules/queue"
	"github.com/hollis-labs/substrate/modules/scheduler"

	"github.com/hollis-labs/hadron/internal/persistence"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// TODO(hadron): replace AllowAll with Hadron's policy decision point when the
	// engine host is ported (phase 2). It exists so the skeleton starts.
	policy.UsePDP(policy.AllowAll{})

	migFS, migRoot := persistence.Migrations()
	database.UseMigrationFS(migFS, migRoot)

	run, err := boot.Start(ctx, substrateApp, substrateModules, substrateConfigJSON)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hadron:", err)
		os.Exit(1)
	}
	defer func() { _ = run.Stop(context.Background()) }()

	root := cli.Root(run.App)
	root.AddCommand(serveCmd(ctx, run.App), mcpCmd(ctx, run.App))
	if err := root.ExecuteContext(ctx); err != nil {
		_ = run.Stop(context.Background())
		os.Exit(1)
	}
}

// serveCmd is the daemon: it starts the worker and the scheduler, then serves HTTP.
func serveCmd(ctx context.Context, app *module.App) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the Hadron daemon",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			if err := queue.Run(app); err != nil {
				return err
			}
			if err := scheduler.Run(app); err != nil {
				return err
			}
			h, err := httphost.Handler(app, httphost.Options{})
			if err != nil {
				return err
			}
			return httphost.Serve(ctx, app, h, httphost.ServeOptions{})
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
