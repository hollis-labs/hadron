// Package mcptools serves Hadron's workflow tools on Substrate's MCP server,
// over both transports (HTTP /mcp and stdio). The tool set is static: each tool
// is one workflow operation, named hadron_workflow_*. Identity comes from the
// framework's verified principal (see runtime.Authenticate); a tool's scope is
// checked by the auth module before the handler runs.
package mcptools

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	gmcp "github.com/hollis-labs/libs/mcp/plugin-mcp/go-mcp/server"
	libauth "github.com/hollis-labs/libs/security/auth"
	"github.com/hollis-labs/libs/workflow/engine/graph"
	"github.com/hollis-labs/libs/workflow/engine/values"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/runtime"
	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/auth"
	"github.com/hollis-labs/substrate/modules/mcp"
)

// Dependencies are the runtime services the tools call.
type Dependencies struct {
	Operations appworkflow.WorkflowOperations
	Reads      appworkflow.WorkflowRunReadOperations
	Signals    appworkflow.WorkflowSignalOperations
	Lifecycle  appworkflow.WorkflowLifecycleOperations
	// Typed supplies the workflows exposed as one typed tool each. Nil exposes
	// only the static tools.
	Typed TypedSource
}

type tool struct {
	name, description string
	schema            map[string]any
	scope             string
	behavior          gmcp.Behavior
	handler           func(context.Context, map[string]any) (any, error)
}

// Surface is the workflow tool surface of one app: the static tools, and a typed
// tool per exposed workflow kept in step with the catalog.
type Surface struct {
	app   *module.App
	deps  Dependencies
	srv   *gmcp.Server
	mode  func() libauth.Mode
	nonce string
	seq   atomic.Uint64

	mu      sync.Mutex
	tools   map[string]tool   // static tools by name
	typed   map[string]string // registered typed tools: name to definition and schema key
	syncNow chan struct{}
}

// New creates the surface and installs its per-caller tools/list filter. Call
// it before boot.Start (the MCP module reads the filter when it starts), then
// Register after.
func New() *Surface {
	s := &Surface{typed: map[string]string{}, tools: map[string]tool{}, syncNow: make(chan struct{}, 1)}
	mcp.UseToolFilter(s.visibleTools)
	return s
}

// Register adds the static workflow tools to the app's MCP server and their
// scopes to the auth scope table, and syncs the typed tools. Call it after
// boot.Start and before serving.
func (s *Surface) Register(ctx context.Context, app *module.App, deps Dependencies) error {
	srv, table, guard := mcp.Server(app), auth.ScopeTable(app), auth.Guard(app)
	if srv == nil || table == nil || guard == nil {
		return errors.New("mcptools: the mcp and auth modules must be started")
	}
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	s.app, s.srv, s.mode, s.nonce = app, srv, guard.Mode, hex.EncodeToString(b[:])
	s.deps = deps
	if deps.Lifecycle != nil {
		s.deps.Lifecycle = syncingLifecycle{WorkflowLifecycleOperations: deps.Lifecycle, changed: s.changed}
	}
	for _, t := range s.staticTools() {
		t := t
		s.tools[t.name] = t
		table.Tool(t.name, t.scope)
		srv.RegisterChecked(gmcp.Tool{
			Name: t.name, Description: t.description, InputSchema: t.schema,
			Handler: func(ctx context.Context, args map[string]any) (any, error) { return s.call(ctx, t, args) },
		}, t.behavior)
	}
	return s.Sync(ctx)
}

// Lifecycle returns the lifecycle operations to hand to other transports: they
// resync the typed tools after a change to the catalog or to a profile.
func (s *Surface) Lifecycle() appworkflow.WorkflowLifecycleOperations { return s.deps.Lifecycle }

// call checks the tool's scope, binds the caller's identity and runs the handler.
func (s *Surface) call(ctx context.Context, t tool, args map[string]any) (any, error) {
	if err := mcp.Authorize(ctx, s.app, t.name); err != nil {
		return nil, workflowFailure(appworkflow.ErrPolicyDenied)
	}
	if _, err := boundedWorkflowArguments(args); err != nil {
		return nil, workflowFailure(err)
	}
	bound, err := runtime.Authenticate(ctx, s.mode(), "mcp")
	if err != nil {
		return nil, workflowFailure(err)
	}
	return t.handler(bound, args)
}

func identity() appworkflow.IdentityRequest {
	return appworkflow.IdentityRequest{SourceAuthority: "mcp"}
}

func refFromArgs(args map[string]any) graph.DefinitionRef {
	return graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: argString(args, "name", ""), Version: argString(args, "version", ""), Digest: argString(args, "digest", "")}
}

func runID(args map[string]any) appworkflow.RunID {
	return appworkflow.RunID(argString(args, "run_id", ""))
}

func display(args map[string]any) values.DisplayPolicy {
	if argBool(args, "reveal_private", false) {
		return values.DisplayPolicy{Private: values.PrivateDisplayReveal}
	}
	return values.DisplayPolicy{}
}

func (s *Surface) generated(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, s.nonce, s.seq.Add(1))
}

func unavailable[T any](v T) error {
	if nilInterfaceValue(v) {
		return workflowFailure(appworkflow.ErrHostNotReady)
	}
	return nil
}
