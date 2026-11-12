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
}

type tool struct {
	name, description string
	schema            map[string]any
	scope             string
	behavior          gmcp.Behavior
	handler           func(context.Context, map[string]any) (any, error)
}

type surface struct {
	app   *module.App
	deps  Dependencies
	mode  func() libauth.Mode
	nonce string
	seq   atomic.Uint64
}

// Register adds the workflow tools to the app's MCP server and their scopes to
// the auth scope table. Call it after boot.Start and before serving.
func Register(app *module.App, deps Dependencies) error {
	srv, table, guard := mcp.Server(app), auth.ScopeTable(app), auth.Guard(app)
	if srv == nil || table == nil || guard == nil {
		return errors.New("mcptools: the mcp and auth modules must be started")
	}
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	s := &surface{app: app, deps: deps, mode: guard.Mode, nonce: hex.EncodeToString(b[:])}
	for _, t := range s.tools() {
		t := t
		table.Tool(t.name, t.scope)
		srv.RegisterChecked(gmcp.Tool{
			Name: t.name, Description: t.description, InputSchema: t.schema,
			Handler: func(ctx context.Context, args map[string]any) (any, error) { return s.call(ctx, t, args) },
		}, t.behavior)
	}
	return nil
}

// call checks the tool's scope, binds the caller's identity and runs the handler.
func (s *surface) call(ctx context.Context, t tool, args map[string]any) (any, error) {
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

func (s *surface) generated(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, s.nonce, s.seq.Add(1))
}

func unavailable[T any](v T) error {
	if nilInterfaceValue(v) {
		return workflowFailure(appworkflow.ErrHostNotReady)
	}
	return nil
}
