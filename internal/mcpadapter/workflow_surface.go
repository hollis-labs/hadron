package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const maximumWorkflowMCPArguments = 1 << 20

type WorkflowExposureOperations interface {
	ResolveSession(context.Context, string, string) (context.Context, appworkflow.WorkflowExposureSession, error)
	DirectWorkflows(context.Context, appworkflow.WorkflowExposureSession) ([]appworkflow.WorkflowExposureDescriptor, error)
	Search(context.Context, appworkflow.WorkflowExposureSession, string, int) ([]appworkflow.WorkflowExposureSummary, error)
	Load(context.Context, appworkflow.WorkflowExposureSession, []graph.DefinitionRef) ([]appworkflow.WorkflowExposureDescriptor, error)
	Describe(context.Context, appworkflow.WorkflowExposureSession, graph.DefinitionRef, string) (appworkflow.WorkflowExposureDescriptor, error)
	NamespaceCatalog(context.Context, appworkflow.WorkflowExposureSession) (map[string]int, error)
	DisplayPolicy(context.Context, appworkflow.WorkflowExposureSession, values.DisplayPolicy) (values.DisplayPolicy, error)
}

type WorkflowOperations interface {
	appworkflow.WorkflowOperations
}

type WorkflowReadOperations interface {
	appworkflow.WorkflowRunReadOperations
}

type WorkflowSignalOperations interface {
	appworkflow.WorkflowSignalOperations
}

type WorkflowLifecycleOperations interface {
	appworkflow.WorkflowLifecycleOperations
}

type workflowSurface struct {
	adapter    *Adapter
	exposure   WorkflowExposureOperations
	operations WorkflowOperations
	reads      WorkflowReadOperations
	signals    WorkflowSignalOperations
	lifecycle  WorkflowLifecycleOperations
	sequence   atomic.Uint64

	mu       sync.Mutex
	server   *server.MCPServer
	base     map[string]server.ServerTool
	sessions map[string]workflowMount
}

type workflowMount struct {
	session appworkflow.WorkflowExposureSession
	direct  map[string]appworkflow.WorkflowExposureDescriptor
	lazy    map[string]appworkflow.WorkflowExposureDescriptor
}

func newWorkflowSurface(adapter *Adapter, exposure WorkflowExposureOperations, operations WorkflowOperations, reads WorkflowReadOperations, signals WorkflowSignalOperations, lifecycle WorkflowLifecycleOperations) *workflowSurface {
	return &workflowSurface{adapter: adapter, exposure: exposure, operations: operations, reads: reads, signals: signals, lifecycle: lifecycle, sessions: make(map[string]workflowMount)}
}

func (w *workflowSurface) bindServer(s *server.MCPServer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.server = s
	w.base = cloneListedServerTools(s.ListTools())
}

func (w *workflowSurface) onRegisterSession(ctx context.Context, session server.ClientSession) {
	if session == nil {
		return
	}
	if w.server == nil {
		return
	}
	ctx = w.server.WithContext(ctx, session)
	_, _, _ = w.current(ctx, session.SessionID(), w.adapter.token)
}

func (w *workflowSurface) onUnregisterSession(_ context.Context, session server.ClientSession) {
	if session == nil {
		return
	}
	w.mu.Lock()
	if session.SessionID() == "stdio" && w.server != nil {
		w.server.SetTools(serverToolsSlice(cloneServerTools(w.base))...)
	}
	delete(w.sessions, session.SessionID())
	w.mu.Unlock()
}

func (w *workflowSurface) registerTools(s *server.MCPServer) {
	handlers := w.handlerMap()
	names := make([]string, 0, len(handlers))
	for name := range handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		handler := handlers[name]
		s.AddTool(workflowMetaTool(name), handler)
	}
}

func workflowMetaTool(name string) mcp.Tool {
	var tool mcp.Tool
	switch name {
	case "hadron_workflows_search":
		tool = mcp.NewTool(name, mcp.WithDescription("Search workflows visible to this MCP principal."), mcp.WithString("query"), mcp.WithNumber("limit"))
	case "hadron_workflow_catalog_search":
		tool = mcp.NewTool(name, mcp.WithDescription("Return bounded ranked workflow recommendations and the next authoring step."), mcp.WithString("query"), mcp.WithString("namespace"), mcp.WithNumber("limit"))
	case "hadron_workflows_load":
		tool = mcp.NewTool(name, mcp.WithDescription("Mount exact discoverable workflow schemas for this MCP session."), mcp.WithArray("definitions", mcp.WithStringItems()))
	case "hadron_workflow_describe", "hadron_workflow_validate":
		tool = mcp.NewTool(name, mcp.WithString("name", mcp.Required()), mcp.WithString("version", mcp.Required()), mcp.WithString("digest", mcp.Required()))
	case "hadron_workflow_run":
		tool = mcp.NewTool(name, mcp.WithString("name", mcp.Required()), mcp.WithString("version", mcp.Required()), mcp.WithString("digest", mcp.Required()), mcp.WithObject("inputs"), mcp.WithString("run_id"), mcp.WithString("idempotency_key"), mcp.WithBoolean("confirmed"))
	case "hadron_workflow_run_inspect":
		tool = mcp.NewTool(name, mcp.WithString("run_id", mcp.Required()), mcp.WithBoolean("reveal_private"))
	case "hadron_workflow_run_cancel":
		tool = mcp.NewTool(name, mcp.WithString("run_id", mcp.Required()), mcp.WithString("idempotency_key", mcp.Required()), mcp.WithString("reason"))
	case "hadron_workflow_run_events", "hadron_workflow_run_subscribe":
		tool = mcp.NewTool(name, mcp.WithString("run_id", mcp.Required()), mcp.WithNumber("limit"), mcp.WithBoolean("reveal_private"))
	case "hadron_workflow_run_resume", "hadron_workflow_gate_submit", "hadron_workflow_message_submit":
		tool = mcp.NewTool(name, mcp.WithString("run_id", mcp.Required()), mcp.WithString("wait_id", mcp.Required()), mcp.WithString("correlation", mcp.Required()), mcp.WithString("token"), mcp.WithObject("payload", mcp.Required()), mcp.WithString("idempotency_key"))
	case "hadron_workflow_signal":
		tool = mcp.NewTool(name, mcp.WithString("run_id", mcp.Required()), mcp.WithString("name", mcp.Required()), mcp.WithString("correlation", mcp.Required()), mcp.WithObject("payload", mcp.Required()), mcp.WithString("idempotency_key", mcp.Required()), mcp.WithBoolean("confirmed"))
	case "hadron_workflow_catalog_inspect", "hadron_workflow_registry_pin_version", "hadron_workflow_registry_unpin_version", "hadron_workflow_registry_publish", "hadron_workflow_registry_clear_current":
		tool = mcp.NewTool(name, mcp.WithString("name", mcp.Required()), mcp.WithString("version", mcp.Required()), mcp.WithString("digest", mcp.Required()))
	case "hadron_workflow_author_validate":
		tool = mcp.NewTool(name, mcp.WithDescription("Validate one bounded graph-native draft without mutating the workflow catalog."), workflowDraftToolOption())
	case "hadron_workflow_author_scaffold":
		tool = mcp.NewTool(name, mcp.WithDescription("Validate one bounded graph-native draft and generate an editable deterministic contract-test scaffold without catalog mutation."), workflowDraftToolOption())
	case "hadron_workflow_author_test":
		tool = mcp.NewTool(name, mcp.WithDescription("Validate a draft and execute its deterministic contract suite without registering it."), workflowDraftToolOption(), workflowContractSuiteToolOption())
	case "hadron_workflow_author_register":
		tool = mcp.NewTool(name, mcp.WithDescription("Validate and test a draft, then register its exact immutable version in an authorized namespace."), workflowDraftToolOption(), workflowContractSuiteToolOption(), mcp.WithBoolean("make_current", mcp.Description("Also move the registry current alias to this qualified exact version.")))
	case "hadron_workflow_registry_package":
		tool = mcp.NewTool(name, mcp.WithString("name", mcp.Required()), mcp.WithString("version", mcp.Required()), mcp.WithString("digest", mcp.Required()), workflowContractSuiteToolOption())
	case "hadron_workflow_exposure_inspect":
		tool = mcp.NewTool(name, mcp.WithString("profile_id", mcp.Required()))
	case "hadron_workflow_exposure_pin_definition", "hadron_workflow_exposure_unpin_definition":
		tool = mcp.NewTool(name, mcp.WithString("profile_id", mcp.Required()), mcp.WithString("name", mcp.Required()), mcp.WithString("version", mcp.Required()), mcp.WithString("digest", mcp.Required()), mcp.WithNumber("expected_generation", mcp.Required()))
	default:
		tool = mcp.NewTool(name)
	}
	if name == "hadron_workflows_search" || name == "hadron_workflow_catalog_search" || name == "hadron_workflow_run_events" || name == "hadron_workflow_run_subscribe" {
		tool.InputSchema.Properties["limit"] = map[string]any{"type": "integer"}
	}
	if name == "hadron_workflow_exposure_pin_definition" || name == "hadron_workflow_exposure_unpin_definition" {
		tool.InputSchema.Properties["expected_generation"] = map[string]any{"type": "integer", "minimum": 1}
	}
	return applyToolBehavior(tool, workflowMetaBehavior(name))
}

func workflowMetaBehavior(name string) toolBehavior {
	switch name {
	case "hadron_workflows_search", "hadron_workflow_catalog_search", "hadron_workflow_describe", "hadron_workflow_validate", "hadron_workflow_run_inspect", "hadron_workflow_run_events", "hadron_workflow_run_subscribe", "hadron_workflow_catalog_inspect", "hadron_workflow_author_validate", "hadron_workflow_author_scaffold", "hadron_workflow_author_test", "hadron_workflow_registry_package", "hadron_workflow_exposure_inspect":
		return toolBehavior{readOnly: true, idempotent: true}
	case "hadron_workflow_run_cancel":
		return toolBehavior{destructive: true, idempotent: true}
	case "hadron_workflows_load", "hadron_workflow_run_resume", "hadron_workflow_gate_submit", "hadron_workflow_message_submit", "hadron_workflow_signal", "hadron_workflow_author_register", "hadron_workflow_registry_pin_version", "hadron_workflow_registry_unpin_version", "hadron_workflow_registry_publish", "hadron_workflow_registry_clear_current", "hadron_workflow_exposure_pin_definition", "hadron_workflow_exposure_unpin_definition":
		return toolBehavior{idempotent: true}
	default:
		return toolBehavior{}
	}
}

func (w *workflowSurface) handlerMap() map[string]server.ToolHandlerFunc {
	return map[string]server.ToolHandlerFunc{
		"hadron_workflows_search":                   w.handleSearch,
		"hadron_workflows_load":                     w.handleLoad,
		"hadron_workflow_describe":                  w.handleDescribe,
		"hadron_workflow_validate":                  w.handleValidate,
		"hadron_workflow_run":                       w.handleRun,
		"hadron_workflow_run_inspect":               w.handleInspectRun,
		"hadron_workflow_run_cancel":                w.handleCancelRun,
		"hadron_workflow_run_events":                w.handleEvents,
		"hadron_workflow_run_subscribe":             w.handleEvents,
		"hadron_workflow_run_resume":                w.handleResume,
		"hadron_workflow_gate_submit":               w.handleGate,
		"hadron_workflow_message_submit":            w.handleMessage,
		"hadron_workflow_signal":                    w.handleSignal,
		"hadron_workflow_catalog_search":            w.handleLifecycleCatalogSearch,
		"hadron_workflow_catalog_inspect":           w.handleLifecycleCatalogInspect,
		"hadron_workflow_author_validate":           w.handleLifecycleAuthorValidate,
		"hadron_workflow_author_scaffold":           w.handleLifecycleAuthorScaffold,
		"hadron_workflow_author_test":               w.handleLifecycleAuthorTest,
		"hadron_workflow_author_register":           w.handleLifecycleAuthorRegister,
		"hadron_workflow_registry_package":          w.handleLifecyclePackage,
		"hadron_workflow_registry_pin_version":      w.handleLifecycleRegistryPin,
		"hadron_workflow_registry_unpin_version":    w.handleLifecycleRegistryUnpin,
		"hadron_workflow_registry_publish":          w.handleLifecycleRegistryPublish,
		"hadron_workflow_registry_clear_current":    w.handleLifecycleClearCurrent,
		"hadron_workflow_exposure_inspect":          w.handleLifecycleExposureInspect,
		"hadron_workflow_exposure_pin_definition":   w.handleLifecycleExposurePin,
		"hadron_workflow_exposure_unpin_definition": w.handleLifecycleExposureUnpin,
	}
}

func (w *workflowSurface) registerResources(s *server.MCPServer) {
	const uri = "hadron://workflows/namespaces"
	s.AddResource(mcp.NewResource(uri, "Hadron Workflow Namespaces", mcp.WithMIMEType("application/json"), mcp.WithResourceDescription("Compact namespace counts visible to this MCP principal.")), func(ctx context.Context, _ mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		sessionID := workflowSessionID(ctx, w.adapter.sessionID)
		bound, session, err := w.current(ctx, sessionID, w.adapter.token)
		if err != nil {
			return nil, errors.New("workflow namespace catalog is unavailable")
		}
		catalog, err := w.exposure.NamespaceCatalog(bound, session)
		if err != nil {
			return nil, errors.New("workflow namespace catalog is unavailable")
		}
		encoded, err := json.Marshal(map[string]any{"namespaces": catalog})
		if err != nil {
			return nil, errors.New("workflow namespace catalog is unavailable")
		}
		return []mcp.ResourceContents{mcp.TextResourceContents{URI: uri, MIMEType: "application/json", Text: string(encoded)}}, nil
	})
}

func (w *workflowSurface) current(ctx context.Context, sessionID, token string) (context.Context, appworkflow.WorkflowExposureSession, error) {
	if nilInterfaceValue(w.exposure) {
		w.clear(ctx, sessionID)
		return ctx, appworkflow.WorkflowExposureSession{}, appworkflow.ErrHostNotReady
	}
	bound, session, err := w.exposure.ResolveSession(ctx, sessionID, token)
	if err != nil {
		w.clear(ctx, sessionID)
		return ctx, appworkflow.WorkflowExposureSession{}, err
	}
	direct, err := w.exposure.DirectWorkflows(bound, session)
	if err != nil {
		w.clear(ctx, sessionID)
		return bound, session, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	prior := w.sessions[sessionID]
	changed := !sameWorkflowSession(prior.session, session)
	next := workflowMount{session: session.Clone(), direct: descriptorMap(direct), lazy: make(map[string]appworkflow.WorkflowExposureDescriptor)}
	if !changed {
		for name, descriptor := range prior.lazy {
			refreshed, refreshErr := w.exposure.Describe(bound, session, descriptor.Definition, "mounted")
			if refreshErr == nil {
				next.lazy[name] = refreshed
			}
		}
	}
	if len(next.direct)+len(next.lazy) > session.Profile.MaxDirectTools {
		_ = w.applyLocked(ctx, sessionID, workflowMount{session: session.Clone(), direct: map[string]appworkflow.WorkflowExposureDescriptor{}, lazy: map[string]appworkflow.WorkflowExposureDescriptor{}})
		return bound, session, appworkflow.ErrPolicyDenied
	}
	if err := w.applyLocked(ctx, sessionID, next); err != nil {
		return bound, session, err
	}
	return bound, session.Clone(), nil
}

func (w *workflowSurface) clear(ctx context.Context, sessionID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.applyLocked(ctx, sessionID, workflowMount{direct: map[string]appworkflow.WorkflowExposureDescriptor{}, lazy: map[string]appworkflow.WorkflowExposureDescriptor{}})
}

func (w *workflowSurface) applyLocked(ctx context.Context, sessionID string, next workflowMount) error {
	prior := w.sessions[sessionID]
	if reflect.DeepEqual(prior.direct, next.direct) && reflect.DeepEqual(prior.lazy, next.lazy) && sameWorkflowSession(prior.session, next.session) {
		return nil
	}
	tools, err := w.toolsForMount(next)
	if err != nil {
		return err
	}
	if client := server.ClientSessionFromContext(ctx); client != nil && client.SessionID() == sessionID {
		if sessionTools, ok := client.(server.SessionWithTools); ok {
			sessionTools.SetSessionTools(tools)
			w.sessions[sessionID] = next
			if client.Initialized() && w.server != nil {
				_ = w.server.SendNotificationToSpecificClient(sessionID, mcp.MethodNotificationToolsListChanged, nil)
			}
			return nil
		}
		// mcp-go's stdio session is deliberately single-client and does not
		// implement SessionWithTools. Its exact reserved session ID is the only
		// safe place to publish a mount through the server-global tool set.
		if sessionID == "stdio" && w.server != nil {
			combined := cloneServerTools(w.base)
			for name, tool := range tools {
				combined[name] = tool
			}
			w.server.SetTools(serverToolsSlice(combined)...)
			w.sessions[sessionID] = next
			return nil
		}
	}
	if sessionID == w.adapter.sessionID && w.server != nil {
		combined := cloneServerTools(w.base)
		for name, tool := range tools {
			combined[name] = tool
		}
		ordered := serverToolsSlice(combined)
		w.server.SetTools(ordered...)
	}
	w.sessions[sessionID] = next
	return nil
}

func (w *workflowSurface) toolsForMount(mount workflowMount) (map[string]server.ServerTool, error) {
	result := make(map[string]server.ServerTool, len(mount.direct)+len(mount.lazy))
	for _, descriptors := range []map[string]appworkflow.WorkflowExposureDescriptor{mount.direct, mount.lazy} {
		for name, descriptor := range descriptors {
			tool, err := workflowDescriptorTool(descriptor)
			if err != nil {
				return nil, err
			}
			captured := descriptor
			result[name] = server.ServerTool{Tool: tool, Handler: func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return w.handleDirect(ctx, request, captured)
			}}
		}
	}
	return result, nil
}

func workflowDescriptorTool(descriptor appworkflow.WorkflowExposureDescriptor) (mcp.Tool, error) {
	input, err := json.Marshal(descriptor.InputSchema)
	if err != nil {
		return mcp.Tool{}, err
	}
	output, err := workflowInvocationOutputSchema(descriptor)
	if err != nil {
		return mcp.Tool{}, err
	}
	description := "Start an asynchronous durable run of " + descriptor.Name + "@" + descriptor.Version + " (" + descriptor.Digest + "). The result is a run handle; outputs is optional and only valid for a terminal result. Follow with hadron_workflow_run_inspect, hadron_workflow_run_events, or hadron_workflow_run_subscribe."
	tool := mcp.NewToolWithRawSchema(descriptor.ToolName, description, input)
	tool.RawOutputSchema = output
	readOnly, destructive := true, false
	for _, effect := range descriptor.Effects {
		if effect == graph.EffectMaterialize || effect == graph.EffectMutate || effect == graph.EffectDestructive {
			readOnly = false
		}
		if effect == graph.EffectDestructive {
			destructive = true
		}
	}
	return applyToolBehavior(tool, toolBehavior{readOnly: readOnly, destructive: destructive, idempotent: false}), nil
}

func workflowInvocationOutputSchema(descriptor appworkflow.WorkflowExposureDescriptor) ([]byte, error) {
	result := graph.Schema{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"run_id":      map[string]any{"type": "string"},
			"status":      map[string]any{"type": "string", "enum": []string{"bound", "not_admitted", "pending", "running", "waiting", "succeeded", "failed", "canceled", "timed_out", "crashed"}},
			"outcome":     map[string]any{"type": "string", "enum": []string{"applied", "replayed"}},
			"diagnostics": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"outputs":     descriptor.OutputSchema,
		},
		"required": []string{"run_id", "status"},
	}
	if err := values.ValidateSchema(result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

// workflowInvocationResult is the immediate asynchronous contract of every
// generated workflow tool. Outputs are intentionally absent at start time;
// the nested output schema reserves the canonical terminal workflow contract
// for an await-capable path without misdescribing the run handle itself.
type workflowInvocationResult struct {
	RunID       appworkflow.RunID       `json:"run_id"`
	Status      string                  `json:"status"`
	Outcome     string                  `json:"outcome,omitempty"`
	Diagnostics []diagnostic.Diagnostic `json:"diagnostics,omitempty"`
	Outputs     map[string]any          `json:"outputs,omitempty"`
}

func (w *workflowSurface) handleSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	limit, err := exactWorkflowLimitArgument(request, 20)
	if err != nil {
		return workflowFailure(err), nil
	}
	items, err := w.exposure.Search(bound, session, request.GetString("query", ""), limit)
	return workflowResult(items, err)
}

func (w *workflowSurface) handleLoad(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	var input struct {
		Definitions []string `json:"definitions"`
	}
	if err := decodeWorkflowArguments(request, &input); err != nil || len(input.Definitions) == 0 {
		return workflowFailure(errors.New("invalid workflow load request")), nil
	}
	refs := make([]graph.DefinitionRef, 0, len(input.Definitions))
	for _, raw := range input.Definitions {
		ref, err := parseExactWorkflowRef(raw)
		if err != nil {
			return workflowFailure(err), nil
		}
		refs = append(refs, ref)
	}
	descriptors, err := w.exposure.Load(bound, session, refs)
	if err != nil {
		return workflowFailure(err), nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	mount := w.sessions[session.SessionID]
	if !sameWorkflowSession(mount.session, session) {
		return workflowFailure(appworkflow.ErrPolicyDenied), nil
	}
	next := workflowMount{session: mount.session.Clone(), direct: cloneDescriptorMap(mount.direct), lazy: cloneDescriptorMap(mount.lazy)}
	for _, descriptor := range descriptors {
		if prior, exists := next.direct[descriptor.ToolName]; exists {
			if prior.Definition != descriptor.Definition {
				return workflowFailure(hostConflict()), nil
			}
			continue
		}
		if prior, exists := next.lazy[descriptor.ToolName]; exists && prior.Definition != descriptor.Definition {
			return workflowFailure(hostConflict()), nil
		}
		next.lazy[descriptor.ToolName] = descriptor
	}
	if len(next.direct)+len(next.lazy) > session.Profile.MaxDirectTools {
		return workflowFailure(appworkflow.ErrPolicyDenied), nil
	}
	if err := w.applyLocked(ctx, session.SessionID, next); err != nil {
		return workflowFailure(err), nil
	}
	return workflowSuccess(map[string]any{"mounted": descriptorNames(descriptors), "count": len(descriptors)}), nil
}

func (w *workflowSurface) handleDescribe(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	descriptor, err := w.exposure.Describe(bound, session, workflowRefFromRequest(request), "inspect")
	return workflowResult(descriptor, err)
}

func (w *workflowSurface) handleValidate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	ref := workflowRefFromRequest(request)
	if _, err := w.exposure.Describe(bound, session, ref, "validate"); err != nil {
		return workflowFailure(err), nil
	}
	if nilInterfaceValue(w.operations) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	validated, err := w.operations.ValidateWorkflow(bound, appworkflow.ValidateWorkflowRequest{Definition: ref, Identity: workflowIdentityRequest()})
	return workflowResult(validated, err)
}

func (w *workflowSurface) handleRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	ref := workflowRefFromRequest(request)
	if _, err := w.exposure.Describe(bound, session, ref, "run"); err != nil {
		return workflowFailure(err), nil
	}
	return w.run(bound, session.SessionID, request, ref, false)
}

func (w *workflowSurface) handleDirect(ctx context.Context, request mcp.CallToolRequest, descriptor appworkflow.WorkflowExposureDescriptor) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if !w.mounted(session.SessionID, descriptor) {
		return workflowFailure(appworkflow.ErrWorkflowHidden), nil
	}
	if _, err := w.exposure.Describe(bound, session, descriptor.Definition, "run"); err != nil {
		return workflowFailure(err), nil
	}
	return w.run(bound, session.SessionID, request, descriptor.Definition, true)
}

func (w *workflowSurface) run(ctx context.Context, sessionID string, request mcp.CallToolRequest, ref graph.DefinitionRef, direct bool) (*mcp.CallToolResult, error) {
	if nilInterfaceValue(w.operations) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	args, err := boundedWorkflowArguments(request)
	if err != nil {
		return workflowFailure(err), nil
	}
	inputs := args
	if !direct {
		inputs = map[string]any{}
		if nested, exists := args["inputs"]; exists {
			var ok bool
			inputs, ok = nested.(map[string]any)
			if !ok {
				return workflowFailure(errors.New("workflow inputs must be an object")), nil
			}
		}
	}
	sequence := w.sequence.Add(1)
	if w.adapter.workflowNonce == "" {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	nonce := w.adapter.workflowNonce
	runID := ""
	if !direct {
		runID = request.GetString("run_id", "")
	}
	if runID == "" {
		runID = fmt.Sprintf("mcp-%s-%s-%d", safeSessionFragment(sessionID), nonce, sequence)
	}
	key := ""
	if !direct {
		key = request.GetString("idempotency_key", "")
	}
	if key == "" {
		key = fmt.Sprintf("mcp-start-%s-%s-%d", safeSessionFragment(sessionID), nonce, sequence)
	}
	confirmed := false
	if !direct {
		confirmed = request.GetBool("confirmed", false)
	}
	started, runErr := w.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{RunID: appworkflow.RunID(runID), Definition: ref, Inputs: inputs, IdempotencyKey: key, Identity: workflowIdentityRequest(), Confirmed: confirmed})
	return workflowStartResult(appworkflow.RunID(runID), started, runErr)
}

func (w *workflowSurface) handleInspectRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if nilInterfaceValue(w.operations) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	display, err := w.display(bound, session, request.GetBool("reveal_private", false))
	if err != nil {
		return workflowFailure(err), nil
	}
	inspected, err := w.operations.InspectWorkflowRun(bound, appworkflow.InspectWorkflowRunRequest{RunID: appworkflow.RunID(request.GetString("run_id", "")), Identity: workflowIdentityRequest(), Display: display, NodeLimit: 100, AttemptLimit: 100, EventLimit: 100, ValueLimit: 100, ResourceLimit: 100, ActivationLimit: 100})
	return workflowResult(inspected, err)
}

func (w *workflowSurface) handleCancelRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if nilInterfaceValue(w.operations) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	canceled, err := w.operations.CancelWorkflowRun(bound, appworkflow.CancelWorkflowRunRequest{RunID: appworkflow.RunID(request.GetString("run_id", "")), Identity: workflowIdentityRequest(), IdempotencyKey: request.GetString("idempotency_key", ""), Reason: request.GetString("reason", "")})
	return workflowResult(canceled, err)
}

func (w *workflowSurface) handleEvents(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, session, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if nilInterfaceValue(w.reads) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	display, err := w.display(bound, session, request.GetBool("reveal_private", false))
	if err != nil {
		return workflowFailure(err), nil
	}
	limit, err := exactWorkflowLimitArgument(request, 100)
	if err != nil {
		return workflowFailure(err), nil
	}
	events, err := w.reads.FetchWorkflowEvents(bound, appworkflow.WorkflowRunReadRequest{RunID: appworkflow.RunID(request.GetString("run_id", "")), Identity: workflowIdentityRequest(), Display: display, EventLimit: boundedWorkflowLimit(limit)})
	return workflowResult(events, err)
}

func (w *workflowSurface) handleResume(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.resume(ctx, request, appworkflow.WorkflowWakeCallback)
}

func (w *workflowSurface) handleGate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.resume(ctx, request, appworkflow.WorkflowWakeGate)
}

func (w *workflowSurface) handleMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return w.resume(ctx, request, appworkflow.WorkflowWakeMessage)
}

func (w *workflowSurface) resume(ctx context.Context, request mcp.CallToolRequest, source appworkflow.WorkflowWakeSource) (*mcp.CallToolResult, error) {
	bound, _, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if nilInterfaceValue(w.operations) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	payload, err := workflowValueArgument(request, "payload")
	if err != nil {
		return workflowFailure(err), nil
	}
	resumed, err := w.operations.ResumeWorkflowRun(bound, appworkflow.ResumeWorkflowRunRequest{RunID: appworkflow.RunID(request.GetString("run_id", "")), Identity: workflowIdentityRequest(), WaitID: appworkflow.WaitID(request.GetString("wait_id", "")), Correlation: request.GetString("correlation", ""), Token: request.GetString("token", ""), WakeSource: source, Payload: payload, IdempotencyKey: request.GetString("idempotency_key", "")})
	return workflowResult(resumed, err)
}

func (w *workflowSurface) handleSignal(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bound, _, result := w.requestSession(ctx, request)
	if result != nil {
		return result, nil
	}
	if nilInterfaceValue(w.signals) {
		return workflowFailure(appworkflow.ErrHostNotReady), nil
	}
	payload, err := workflowValueArgument(request, "payload")
	if err != nil {
		return workflowFailure(err), nil
	}
	signaled, err := w.signals.SignalWorkflowRun(bound, appworkflow.SignalWorkflowRunRequest{RunID: appworkflow.RunID(request.GetString("run_id", "")), Name: request.GetString("name", ""), Correlation: request.GetString("correlation", ""), Payload: payload, IdempotencyKey: request.GetString("idempotency_key", ""), Identity: workflowIdentityRequest(), Confirmed: request.GetBool("confirmed", false)})
	return workflowResult(signaled, err)
}

func (w *workflowSurface) requestSession(ctx context.Context, request mcp.CallToolRequest) (context.Context, appworkflow.WorkflowExposureSession, *mcp.CallToolResult) {
	if _, err := boundedWorkflowArguments(request); err != nil {
		return ctx, appworkflow.WorkflowExposureSession{}, workflowFailure(err)
	}
	bound, session, err := w.current(ctx, workflowSessionID(ctx, w.adapter.sessionID), workflowToken(request.Header, w.adapter.token))
	if err != nil {
		return ctx, appworkflow.WorkflowExposureSession{}, workflowFailure(err)
	}
	return bound, session, nil
}

func (w *workflowSurface) display(ctx context.Context, session appworkflow.WorkflowExposureSession, reveal bool) (values.DisplayPolicy, error) {
	requested := values.DisplayPolicy{}
	if reveal {
		requested.Private = values.PrivateDisplayReveal
	}
	return w.exposure.DisplayPolicy(ctx, session, requested)
}

func (w *workflowSurface) mounted(sessionID string, descriptor appworkflow.WorkflowExposureDescriptor) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	mount := w.sessions[sessionID]
	current, ok := mount.direct[descriptor.ToolName]
	if !ok {
		current, ok = mount.lazy[descriptor.ToolName]
	}
	return ok && current.Definition == descriptor.Definition
}

func workflowRefFromRequest(request mcp.CallToolRequest) graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "registry", ID: request.GetString("name", ""), Version: request.GetString("version", ""), Digest: request.GetString("digest", "")}
}

func parseExactWorkflowRef(raw string) (graph.DefinitionRef, error) {
	raw = strings.TrimSpace(raw)
	at := strings.LastIndex(raw, "@")
	if at < 1 {
		return graph.DefinitionRef{}, errors.New("workflow definition must use name@version@digest")
	}
	digest := raw[at+1:]
	prefix := raw[:at]
	at = strings.LastIndex(prefix, "@")
	if at < 1 || prefix[at+1:] == "" || digest == "" {
		return graph.DefinitionRef{}, errors.New("workflow definition must use name@version@digest")
	}
	return graph.DefinitionRef{Kind: "registry", ID: prefix[:at], Version: prefix[at+1:], Digest: digest}, nil
}

func workflowIdentityRequest() appworkflow.IdentityRequest {
	return appworkflow.IdentityRequest{SourceAuthority: "mcp"}
}

func workflowValueArgument(request mcp.CallToolRequest, key string) (values.Value, error) {
	arguments, err := boundedWorkflowArguments(request)
	if err != nil {
		return values.Value{}, err
	}
	raw, ok := arguments[key]
	if !ok {
		return values.Value{}, errors.New("typed workflow payload is required")
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var payload values.Value
	if err := decoder.Decode(&payload); err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	if err := payload.Validate(); err != nil {
		return values.Value{}, errors.New("typed workflow payload is invalid")
	}
	return payload, nil
}

func boundedWorkflowArguments(request mcp.CallToolRequest) (map[string]any, error) {
	arguments := request.GetArguments()
	if arguments == nil {
		arguments = map[string]any{}
	}
	if err := validateWorkflowArgumentNumbers(reflect.ValueOf(arguments), 0); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(arguments)
	if err != nil || len(encoded) > maximumWorkflowMCPArguments {
		return nil, errors.New("workflow tool arguments are invalid or exceed the supported bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, errors.New("workflow tool arguments are invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("workflow tool arguments contain trailing data")
	}
	return result, nil
}

func validateWorkflowArgumentNumbers(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	if depth > 100 {
		return errors.New("workflow tool arguments exceed the supported nesting bound")
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateWorkflowArgumentNumbers(value.Elem(), depth+1)
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		maximumExactInteger := float64(1<<53 - 1)
		if value.Kind() == reflect.Float32 {
			maximumExactInteger = float64(1<<24 - 1)
		}
		if math.Trunc(number) == number && math.Abs(number) > maximumExactInteger {
			return errors.New("workflow tool arguments contain an integer that cannot be represented losslessly")
		}
	case reflect.Array, reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if err := validateWorkflowArgumentNumbers(value.Index(index), depth+1); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateWorkflowArgumentNumbers(iterator.Value(), depth+1); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	return nil
}

func exactWorkflowLimitArgument(request mcp.CallToolRequest, defaultValue int) (int, error) {
	value, exists := request.GetArguments()["limit"]
	if !exists {
		return defaultValue, nil
	}
	if number, ok := value.(json.Number); ok {
		parsed, err := number.Int64()
		if err != nil {
			return 0, errors.New("workflow argument \"limit\" must be an exact integer")
		}
		return checkedWorkflowInteger(parsed)
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return 0, errors.New("workflow argument \"limit\" must be an exact integer")
	}
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return checkedWorkflowInteger(reflected.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		maximum := uint64(^uint(0) >> 1)
		if reflected.Uint() > maximum {
			return 0, errors.New("workflow argument \"limit\" exceeds the supported integer range")
		}
		return int(reflected.Uint()), nil // #nosec G115 -- range checked against the platform int maximum above.
	case reflect.Float32, reflect.Float64:
		number := reflected.Float()
		maximumExactInteger := float64(1<<53 - 1)
		if reflected.Kind() == reflect.Float32 {
			maximumExactInteger = float64(1<<24 - 1)
		}
		maximumInteger := float64(^uint(0) >> 1)
		minimumInteger := -maximumInteger - 1
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || math.Abs(number) > maximumExactInteger || number > maximumInteger || number < minimumInteger {
			return 0, errors.New("workflow argument \"limit\" must be an exact integer")
		}
		return int(number), nil
	default:
		return 0, errors.New("workflow argument \"limit\" must be an exact integer")
	}
}

func checkedWorkflowInteger(value int64) (int, error) {
	maximum := int64(^uint(0) >> 1)
	minimum := -maximum - 1
	if value > maximum || value < minimum {
		return 0, errors.New("workflow argument \"limit\" exceeds the supported integer range")
	}
	return int(value), nil
}

func decodeWorkflowArguments(request mcp.CallToolRequest, target any) error {
	arguments, err := boundedWorkflowArguments(request)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(arguments)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("workflow tool arguments contain trailing data")
	}
	return nil
}

func workflowResult(value any, err error) (*mcp.CallToolResult, error) {
	if err != nil {
		return workflowFailure(err), nil
	}
	return workflowSuccess(value), nil
}

func workflowStartResult(runID appworkflow.RunID, value appworkflow.StartRunResult, err error) (*mcp.CallToolResult, error) {
	if err != nil {
		safe := appworkflow.SafeWorkflowOperationError(err, &value)
		return workflowFailureEnvelope(safe), nil
	}
	status := "not_admitted"
	if value.Run != nil {
		status = string(value.Run.Status)
	} else if value.Bound != nil {
		status = "bound"
	}
	return workflowSuccess(workflowInvocationResult{RunID: runID, Status: status, Outcome: string(value.Outcome), Diagnostics: append([]diagnostic.Diagnostic(nil), value.Diagnostics...)}), nil
}

func workflowSuccess(value any) *mcp.CallToolResult {
	return mcp.NewToolResultStructuredOnly(value)
}

func workflowFailure(err error) *mcp.CallToolResult {
	if errors.Is(err, appworkflow.ErrHostNotReady) || errors.Is(err, appworkflow.ErrInvalidHost) {
		return workflowFailureEnvelope(appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
	}
	return workflowFailureEnvelope(appworkflow.SafeWorkflowOperationError(err, nil))
}

func workflowFailureEnvelope(safe appworkflow.WorkflowOperationError) *mcp.CallToolResult {
	result := mcp.NewToolResultStructuredOnly(map[string]any{"error": safe})
	result.IsError = true
	return result
}

func workflowToken(header http.Header, fallback string) string {
	value := header.Get("Authorization")
	if value == "" {
		return canonicalWorkflowToken(fallback)
	}
	if len(value) > 16<<10 {
		return ""
	}
	scheme, token, found := strings.Cut(value, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return canonicalWorkflowToken(token)
}

func workflowSessionID(ctx context.Context, fallback string) string {
	if session := server.ClientSessionFromContext(ctx); session != nil && strings.TrimSpace(session.SessionID()) != "" {
		return session.SessionID()
	}
	return fallback
}

func sameWorkflowSession(left, right appworkflow.WorkflowExposureSession) bool {
	return left.SessionID == right.SessionID && left.Authenticated == right.Authenticated && left.Principal.Record.ID == right.Principal.Record.ID && left.Principal.Generation == right.Principal.Generation && reflect.DeepEqual(left.Profile, right.Profile) && left.ProfileGeneration == right.ProfileGeneration && left.AgentNamespace == right.AgentNamespace
}

func descriptorMap(input []appworkflow.WorkflowExposureDescriptor) map[string]appworkflow.WorkflowExposureDescriptor {
	result := make(map[string]appworkflow.WorkflowExposureDescriptor, len(input))
	for _, descriptor := range input {
		result[descriptor.ToolName] = descriptor
	}
	return result
}

func cloneDescriptorMap(input map[string]appworkflow.WorkflowExposureDescriptor) map[string]appworkflow.WorkflowExposureDescriptor {
	result := make(map[string]appworkflow.WorkflowExposureDescriptor, len(input))
	for name, descriptor := range input {
		result[name] = descriptor
	}
	return result
}

func descriptorNames(input []appworkflow.WorkflowExposureDescriptor) []string {
	result := make([]string, 0, len(input))
	for _, descriptor := range input {
		result = append(result, descriptor.ToolName)
	}
	sort.Strings(result)
	return result
}

func cloneServerTools(input map[string]server.ServerTool) map[string]server.ServerTool {
	result := make(map[string]server.ServerTool, len(input))
	for name, tool := range input {
		result[name] = tool
	}
	return result
}

func cloneListedServerTools(input map[string]*server.ServerTool) map[string]server.ServerTool {
	result := make(map[string]server.ServerTool, len(input))
	for name, tool := range input {
		if tool != nil {
			result[name] = *tool
		}
	}
	return result
}

func serverToolsSlice(input map[string]server.ServerTool) []server.ServerTool {
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]server.ServerTool, 0, len(names))
	for _, name := range names {
		result = append(result, input[name])
	}
	return result
}

func boundedWorkflowLimit(value int) int {
	if value < 1 {
		return 100
	}
	if value > 1000 {
		return 1000
	}
	return value
}

func safeSessionFragment(input string) string {
	var result strings.Builder
	for _, current := range input {
		if current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9' || current == '-' || current == '_' {
			result.WriteRune(current)
		}
		if result.Len() >= 48 {
			break
		}
	}
	if result.Len() == 0 {
		return "session"
	}
	return result.String()
}

func nilInterfaceValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func hostConflict() error { return errors.New("workflow tool name collision") }

var _ WorkflowOperations = (*appworkflow.WorkflowOperator)(nil)
var _ WorkflowReadOperations = (*appworkflow.WorkflowOperator)(nil)
var _ WorkflowSignalOperations = (*appworkflow.WorkflowOperator)(nil)
