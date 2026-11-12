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

	libpolicy "github.com/hollis-labs/libs/security/policy"
	"github.com/hollis-labs/libs/workflow/engine/graph"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/mcptools"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Which workflows a caller may use is a policy decision over its exposure
	// profile (runtime.AccessPDP), made through the policy module so its mode
	// (shadow or enforce) and audit apply. Route and tool access is the scopes
	// each declares.
	h := &hadron{pdp: runtime.NewAccessPDP(), tools: mcptools.New()}
	policy.UsePDP(h.pdp)

	// The scheduler module's runner is the workflow activation service, which
	// exists only once the Runtime is built; Fires is bound to it in serve.
	h.fires = runtime.RegisterScheduler()

	migFS, migRoot := persistence.Migrations()
	database.UseMigrationFS(migFS, migRoot)

	run, err := boot.Start(ctx, substrateApp, substrateModules, substrateConfigJSON)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hadron:", err)
		os.Exit(1)
	}
	defer func() { _ = run.Stop(context.Background()) }()

	h.app = run.App
	root := cli.Root(run.App)
	root.AddCommand(h.serveCmd(ctx), h.mcpCmd(ctx), workflowCmd(ctx, run.App), buildCmd())
	if err := root.ExecuteContext(ctx); err != nil {
		_ = run.Stop(context.Background())
		os.Exit(1)
	}
}

// hadron is what the commands share: the framework app, the late-bound pieces
// created before boot, and the workflow tool surface.
type hadron struct {
	app   *module.App
	fires *runtime.Fires
	pdp   *runtime.AccessPDP
	tools *mcptools.Surface
}

// policyAccess routes the definition access decision through the policy module,
// so its mode and audit apply. A request the module cannot evaluate is refused.
func policyAccess(app *module.App) runtime.AccessChecker {
	return func(ctx context.Context, s runtime.Subject, ref graph.DefinitionRef) error {
		az := policy.From(app)
		if az == nil {
			return errors.New("hadron: the policy module is not started")
		}
		res, err := az.Evaluate(ctx, runtime.PolicyRequest(s, ref))
		if err != nil {
			return err
		}
		if res.Admission != libpolicy.Allow {
			return appworkflow.ErrWorkflowHidden
		}
		return nil
	}
}

// startRuntime builds the workflow runtime over the framework's database,
// registers the MCP workflow tools and starts the runtime (recovery, workers,
// the scheduler engine). The caller shuts it down. The typed tools follow the
// catalog until ctx ends.
func (h *hadron) startRuntime(ctx context.Context) (*runtime.Runtime, error) {
	cfg, err := runtime.ConfigFromApp(h.app)
	if err != nil {
		return nil, err
	}
	db := database.From(h.app)
	if db == nil {
		return nil, errors.New("hadron: the database module is not started")
	}
	rt, err := runtime.New(persistence.NewStore(db.DB()), cfg,
		runtime.WithFires(h.fires), runtime.WithSourceEngine(scheduler.Engine(h.app)),
		runtime.WithAccessPDP(h.pdp), runtime.WithAccess(policyAccess(h.app)))
	if err != nil {
		return nil, err
	}
	// The runtime starts the scheduler engine with the recovered host.
	if err := rt.Start(ctx); err != nil {
		return nil, err
	}
	if err := h.tools.Register(ctx, h.app, mcptools.Dependencies{
		Operations: rt.Operations(), Reads: rt.Operations(), Signals: rt.Operations(), Lifecycle: rt.Lifecycle(), Typed: rt,
	}); err != nil {
		_ = stopRuntime(rt)
		return nil, err
	}
	go h.tools.Run(ctx, time.Minute)
	return rt, nil
}

func stopRuntime(rt *runtime.Runtime) error {
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return rt.Shutdown(shutdown)
}

// serveCmd is the daemon: the workflow runtime, the /api/workflows routes and
// the /mcp endpoint on the framework's HTTP host.
func (h *hadron) serveCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the Hadron daemon",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			defer startTelemetry(version)()
			rt, err := h.startRuntime(ctx)
			if err != nil {
				return err
			}
			srv := api.NewServer(api.Dependencies{
				Workflows: rt.Operations(), WorkflowReads: rt.Operations(), WorkflowSignals: rt.Operations(), WorkflowLifecycle: h.tools.Lifecycle(),
				WorkflowAuth:        runtime.RequestAuthenticator{Source: "http", Mode: auth.Guard(h.app).Mode},
				WorkflowActivations: rt.Activations(), A2ATasks: rt.A2A(), AgentCard: rt.AgentCard(),
			})
			api.RegisterScopes(auth.ScopeTable(h.app))
			mux := http.NewServeMux()
			mux.Handle("/api/workflows/", srv.Handler())
			handler, err := httphost.Handler(h.app, httphost.Options{Routes: mux, Mounts: srv.Mounts()})
			if err != nil {
				_ = stopRuntime(rt)
				return err
			}
			serveErr := httphost.Serve(ctx, h.app, withTracing(handler), httphost.ServeOptions{})
			return errors.Join(serveErr, stopRuntime(rt))
		},
	}
}

// mcpCmd serves the MCP tools over stdio for an agent that launches Hadron as a
// subprocess. The process runs the workflow runtime in-process, over the same
// database as the daemon; claims and leases keep two processes from running the
// same work twice.
func (h *hadron) mcpCmd(ctx context.Context) *cobra.Command {
	var tokenFile string
	c := &cobra.Command{
		Use:   "mcp",
		Short: "Serve MCP over stdio",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			defer startTelemetry(version)()
			rt, err := h.startRuntime(ctx)
			if err != nil {
				return err
			}
			serveErr := mcp.ServeStdio(ctx, h.app, mcp.StdioOptions{TokenFile: tokenFile})
			return errors.Join(serveErr, stopRuntime(rt))
		},
	}
	c.Flags().StringVar(&tokenFile, "token-file", "", "file holding the caller's service token (required when auth is enforcing)")
	return c
}
