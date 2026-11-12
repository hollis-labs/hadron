package mcptools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	gmcp "github.com/hollis-labs/libs/mcp/plugin-mcp/go-mcp/server"
	libauth "github.com/hollis-labs/libs/security/auth"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/runtime"
	"github.com/hollis-labs/substrate/modules/mcp"
)

// reservedPrefix names the static tools; a workflow cannot take one of their names.
const reservedPrefix = "hadron_workflow"

// TypedSource supplies the workflows shown as typed tools.
type TypedSource interface {
	// ExposedWorkflows lists every workflow some caller may be shown, one
	// descriptor per tool name. These are the tools that exist.
	ExposedWorkflows(ctx context.Context) ([]appworkflow.WorkflowExposureDescriptor, error)
	// VisibleWorkflows lists the ones the caller in ctx (authenticated, with an
	// access subject) may see and call.
	VisibleWorkflows(ctx context.Context) ([]appworkflow.WorkflowExposureDescriptor, error)
}

// Sync makes the registered typed tools match the exposed workflows: a tool is
// added for a new workflow, replaced when its definition or input schema
// changed, and removed when the workflow is no longer exposed. The tool list a
// caller sees is a further per-caller filter over these (visibleTools).
func (s *Surface) Sync(ctx context.Context) error {
	if s.deps.Typed == nil {
		return nil
	}
	exposed, err := s.deps.Typed.ExposedWorkflows(ctx)
	if err != nil {
		return fmt.Errorf("mcptools: list exposed workflows: %w", err)
	}
	desired := map[string]appworkflow.WorkflowExposureDescriptor{}
	for _, d := range exposed {
		if d.ToolName == "" || strings.HasPrefix(d.ToolName, reservedPrefix) {
			slog.Warn("workflow cannot be a typed tool", "tool", d.ToolName, "workflow", d.Name)
			continue
		}
		desired[d.ToolName] = d
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.typed {
		if _, keep := desired[name]; !keep {
			s.srv.RemoveTools(name)
			delete(s.typed, name)
		}
	}
	for name, d := range desired {
		schema := inputSchema(d)
		key := d.Name + "@" + d.Version + "#" + d.Digest + "/" + schemaHash(schema)
		if s.typed[name] == key {
			continue
		}
		if _, replacing := s.typed[name]; replacing {
			s.srv.RemoveTools(name)
		}
		name := name
		description := d.Description
		if description == "" {
			description = "Start the workflow " + d.Name + "."
		}
		s.srv.RegisterChecked(gmcp.Tool{
			Name: name, Description: description + " (workflow " + d.Name + "@" + d.Version + ")", InputSchema: schema,
			Handler: func(ctx context.Context, args map[string]any) (any, error) { return s.callTyped(ctx, name, args) },
		}, gmcp.Writes())
		s.typed[name] = key
	}
	return nil
}

// Run keeps the typed tools in step with the catalog until ctx ends: on every
// change made through this process (see changed) and, to catch changes made by
// another process on the same database, every interval.
func (s *Surface) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-s.syncNow:
		}
		if err := s.Sync(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("typed workflow tools not synced", "error", err)
		}
	}
}

// changed asks Run to resync soon.
func (s *Surface) changed() {
	select {
	case s.syncNow <- struct{}{}:
	default:
	}
}

func inputSchema(d appworkflow.WorkflowExposureDescriptor) map[string]any {
	schema := map[string]any{}
	maps.Copy(schema, d.InputSchema)
	if _, ok := schema["type"]; !ok {
		schema["type"] = "object"
	}
	return schema
}

func schemaHash(schema map[string]any) string {
	b, _ := json.Marshal(schema)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// callTyped starts the caller's visible workflow behind a typed tool; the tool's
// arguments are the workflow's inputs. The tool shares the run scope, and the
// definition authorizer decides again when the run resolves.
func (s *Surface) callTyped(ctx context.Context, name string, args map[string]any) (any, error) {
	if err := mcp.Authorize(ctx, s.app, "hadron_workflow_run"); err != nil {
		return nil, workflowFailure(appworkflow.ErrPolicyDenied)
	}
	inputs, err := boundedWorkflowArguments(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	bound, err := runtime.Authenticate(ctx, s.mode(), "mcp")
	if err != nil {
		return nil, workflowFailure(err)
	}
	if err := unavailable(s.deps.Operations); err != nil {
		return nil, err
	}
	visible, err := s.deps.Typed.VisibleWorkflows(bound)
	if err != nil {
		return nil, workflowFailure(err)
	}
	i := slices.IndexFunc(visible, func(d appworkflow.WorkflowExposureDescriptor) bool { return d.ToolName == name })
	if i < 0 {
		return nil, workflowFailure(appworkflow.ErrWorkflowHidden)
	}
	id := s.generated("mcp-run")
	started, runErr := s.deps.Operations.RunWorkflow(bound, appworkflow.RunWorkflowRequest{
		RunID: appworkflow.RunID(id), Definition: visible[i].Definition, Inputs: inputs,
		IdempotencyKey: s.generated("mcp-start"), Identity: identity(),
	})
	return workflowStartResult(appworkflow.RunID(id), started, runErr)
}

// visibleTools is the tools/list filter. A static tool is listed to a caller
// whose token carries its scope; a typed tool to a caller with the run scope
// whose profile (or administrator role) shows it. With auth off every caller is
// the local operator and sees everything. Listing is visibility only: calls
// authorize again.
func (s *Surface) visibleTools(ctx context.Context, names []string) []string {
	p, ok := libauth.FromContext(ctx)
	verified := ok && p.Verified
	var shown map[string]bool
	s.mu.Lock()
	hasTyped := len(s.typed) > 0
	s.mu.Unlock()
	if hasTyped && s.deps.Typed != nil && s.mode != nil {
		if bound, err := runtime.Authenticate(ctx, s.mode(), "mcp"); err == nil {
			if visible, err := s.deps.Typed.VisibleWorkflows(bound); err == nil {
				shown = map[string]bool{}
				for _, d := range visible {
					shown[d.ToolName] = true
				}
			}
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		s.mu.Lock()
		t, static := s.tools[n]
		_, typed := s.typed[n]
		s.mu.Unlock()
		switch {
		case static:
			if verified && !p.HasScope(t.scope) {
				continue
			}
		case typed:
			if verified && !p.HasScope(runtime.ScopeRun) || !shown[n] {
				continue
			}
		}
		out = append(out, n)
	}
	return out
}

// syncingLifecycle resyncs the typed tools after anything that can change which
// workflows are exposed: registering, publishing or pinning a version, clearing
// the current alias, and changing a profile.
type syncingLifecycle struct {
	appworkflow.WorkflowLifecycleOperations
	changed func()
}

func (l syncingLifecycle) RegisterWorkflowDraft(ctx context.Context, r appworkflow.RegisterWorkflowDraftRequest) (appworkflow.WorkflowRegistrationResult, error) {
	res, err := l.WorkflowLifecycleOperations.RegisterWorkflowDraft(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) PublishWorkflowVersion(ctx context.Context, r appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	res, err := l.WorkflowLifecycleOperations.PublishWorkflowVersion(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) PinRegistryVersion(ctx context.Context, r appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	res, err := l.WorkflowLifecycleOperations.PinRegistryVersion(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) UnpinRegistryVersion(ctx context.Context, r appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	res, err := l.WorkflowLifecycleOperations.UnpinRegistryVersion(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) ClearWorkflowCurrentExact(ctx context.Context, r appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	res, err := l.WorkflowLifecycleOperations.ClearWorkflowCurrentExact(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) PinWorkflowExposure(ctx context.Context, r appworkflow.MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	res, err := l.WorkflowLifecycleOperations.PinWorkflowExposure(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) UnpinWorkflowExposure(ctx context.Context, r appworkflow.MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	res, err := l.WorkflowLifecycleOperations.UnpinWorkflowExposure(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) PutWorkflowExposureProfile(ctx context.Context, r appworkflow.PutWorkflowExposureProfileRequest) (hoststate.ExposureProfileSnapshot, error) {
	res, err := l.WorkflowLifecycleOperations.PutWorkflowExposureProfile(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}

func (l syncingLifecycle) DeleteWorkflowExposureProfile(ctx context.Context, r appworkflow.DeleteWorkflowExposureProfileRequest) (appworkflow.WorkflowExposureProfileDeleted, error) {
	res, err := l.WorkflowLifecycleOperations.DeleteWorkflowExposureProfile(ctx, r)
	if err == nil {
		l.changed()
	}
	return res, err
}
