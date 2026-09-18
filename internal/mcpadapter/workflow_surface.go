package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hollis-labs/go-mcp/budget"
	gomcp "github.com/hollis-labs/go-mcp/server"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
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

// workflowSurface mounts graph-native workflows as dynamically generated MCP
// tools for the single MCP principal Hadron serves.
//
// This is deliberately simpler than the mark3labs-era design it replaces,
// which tracked a workflowMount per client session (keyed by session ID) to
// support mark3labs' SessionWithTools interface for stateful multi-client
// HTTP sessions. Hadron only ever serves one transport, stdio
// (cmd/hadrond/main.go's only call site is Adapter.Run), which is
// single-session by construction -- and the go-mcp/official-SDK direction is
// stateless-first at the protocol layer besides (apps needing cross-call
// continuity use Tether's agent-session layer, not protocol sessions; see
// project/atlas/knowledge/mcp/mcp-go-sdk-migration-playbook). So there is
// exactly one mount, updated in place via the official SDK's global
// RegisterTool/RemoveTools, computed synchronously in Adapter.newServer
// (not deferred to whatever later serves the *gomcp.Server it returns --
// Run's stdio, an HTTP handler wrapping it directly in a test, or anything
// else) and refreshed in place by handlers that need to (a token's profile
// generation changing, an explicit hadron_workflows_load).
type workflowSurface struct {
	adapter    *Adapter
	exposure   WorkflowExposureOperations
	operations WorkflowOperations
	reads      WorkflowReadOperations
	signals    WorkflowSignalOperations
	lifecycle  WorkflowLifecycleOperations
	sequence   atomic.Uint64

	mu     sync.Mutex
	server *gomcp.Server
	mount  workflowMount
}

type workflowMount struct {
	session appworkflow.WorkflowExposureSession
	direct  map[string]appworkflow.WorkflowExposureDescriptor
	lazy    map[string]appworkflow.WorkflowExposureDescriptor
}

func newWorkflowSurface(adapter *Adapter, exposure WorkflowExposureOperations, operations WorkflowOperations, reads WorkflowReadOperations, signals WorkflowSignalOperations, lifecycle WorkflowLifecycleOperations) *workflowSurface {
	return &workflowSurface{adapter: adapter, exposure: exposure, operations: operations, reads: reads, signals: signals, lifecycle: lifecycle}
}

func (w *workflowSurface) bindServer(s *gomcp.Server) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.server = s
}

func (w *workflowSurface) registerTools(s *gomcp.Server) {
	handlers := w.handlerMap()
	names := make([]string, 0, len(handlers))
	for name := range handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		description, schema := workflowMetaToolSchema(name)
		b := workflowMetaBehavior(name)
		s.RegisterTool(gomcp.Tool{
			Name:            name,
			Description:     description,
			InputSchema:     schema,
			Handler:         handlers[name],
			ReadOnlyHint:    b.readOnly,
			DestructiveHint: b.destructive,
			IdempotentHint:  b.idempotent,
			OpenWorldHint:   b.openWorld,
		})
	}
}

// workflowMetaToolSchema returns the description and input schema for one of
// the static, profile-authorized meta-tools (as opposed to the dynamically
// generated per-workflow tools built by workflowDescriptorGomcpTool).
func workflowMetaToolSchema(name string) (string, map[string]any) {
	props := map[string]any{}
	var required []any
	description := ""
	switch name {
	case "hadron_workflows_search":
		description = "Search workflows visible to this MCP principal."
		props["query"] = map[string]any{"type": "string"}
		props["limit"] = map[string]any{"type": "integer"}
	case "hadron_workflow_catalog_search":
		description = "Return bounded ranked workflow recommendations and the next authoring step."
		props["query"] = map[string]any{"type": "string"}
		props["namespace"] = map[string]any{"type": "string"}
		props["limit"] = map[string]any{"type": "integer"}
	case "hadron_workflows_load":
		description = "Mount exact discoverable workflow schemas for this MCP session."
		props["definitions"] = map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
	case "hadron_workflow_describe", "hadron_workflow_validate":
		props["name"] = map[string]any{"type": "string"}
		props["version"] = map[string]any{"type": "string"}
		props["digest"] = map[string]any{"type": "string"}
		required = []any{"name", "version", "digest"}
	case "hadron_workflow_run":
		props["name"] = map[string]any{"type": "string"}
		props["version"] = map[string]any{"type": "string"}
		props["digest"] = map[string]any{"type": "string"}
		props["inputs"] = map[string]any{"type": "object"}
		props["run_id"] = map[string]any{"type": "string"}
		props["idempotency_key"] = map[string]any{"type": "string"}
		props["confirmed"] = map[string]any{"type": "boolean"}
		required = []any{"name", "version", "digest"}
	case "hadron_workflow_run_inspect":
		props["run_id"] = map[string]any{"type": "string"}
		props["reveal_private"] = map[string]any{"type": "boolean"}
		required = []any{"run_id"}
	case "hadron_workflow_run_cancel":
		props["run_id"] = map[string]any{"type": "string"}
		props["idempotency_key"] = map[string]any{"type": "string"}
		props["reason"] = map[string]any{"type": "string"}
		required = []any{"run_id", "idempotency_key"}
	case "hadron_workflow_run_events", "hadron_workflow_run_subscribe":
		props["run_id"] = map[string]any{"type": "string"}
		props["limit"] = map[string]any{"type": "integer"}
		props["reveal_private"] = map[string]any{"type": "boolean"}
		required = []any{"run_id"}
	case "hadron_workflow_run_resume", "hadron_workflow_gate_submit", "hadron_workflow_message_submit":
		props["run_id"] = map[string]any{"type": "string"}
		props["wait_id"] = map[string]any{"type": "string"}
		props["correlation"] = map[string]any{"type": "string"}
		props["token"] = map[string]any{"type": "string"}
		props["payload"] = map[string]any{"type": "object"}
		props["idempotency_key"] = map[string]any{"type": "string"}
		required = []any{"run_id", "wait_id", "correlation", "payload"}
	case "hadron_workflow_signal":
		props["run_id"] = map[string]any{"type": "string"}
		props["name"] = map[string]any{"type": "string"}
		props["correlation"] = map[string]any{"type": "string"}
		props["payload"] = map[string]any{"type": "object"}
		props["idempotency_key"] = map[string]any{"type": "string"}
		props["confirmed"] = map[string]any{"type": "boolean"}
		required = []any{"run_id", "name", "correlation", "payload", "idempotency_key"}
	case "hadron_workflow_catalog_inspect", "hadron_workflow_registry_pin_version", "hadron_workflow_registry_unpin_version", "hadron_workflow_registry_publish", "hadron_workflow_registry_clear_current":
		props["name"] = map[string]any{"type": "string"}
		props["version"] = map[string]any{"type": "string"}
		props["digest"] = map[string]any{"type": "string"}
		required = []any{"name", "version", "digest"}
	case "hadron_workflow_author_validate":
		description = "Validate one bounded graph-native draft without mutating the workflow catalog."
		props["draft"] = workflowDraftProperty()
		required = []any{"draft"}
	case "hadron_workflow_author_scaffold":
		description = "Validate one bounded graph-native draft and generate an editable deterministic contract-test scaffold without catalog mutation."
		props["draft"] = workflowDraftProperty()
		required = []any{"draft"}
	case "hadron_workflow_author_test":
		description = "Validate a draft and execute its deterministic contract suite without registering it."
		props["draft"] = workflowDraftProperty()
		props["suite"] = workflowContractSuiteProperty()
		required = []any{"draft", "suite"}
	case "hadron_workflow_author_register":
		description = "Validate and test a draft, then register its exact immutable version in an authorized namespace."
		props["draft"] = workflowDraftProperty()
		props["suite"] = workflowContractSuiteProperty()
		props["make_current"] = map[string]any{"type": "boolean", "description": "Also move the registry current alias to this qualified exact version."}
		required = []any{"draft", "suite"}
	case "hadron_workflow_registry_package":
		props["name"] = map[string]any{"type": "string"}
		props["version"] = map[string]any{"type": "string"}
		props["digest"] = map[string]any{"type": "string"}
		props["suite"] = workflowContractSuiteProperty()
		required = []any{"name", "version", "digest", "suite"}
	case "hadron_workflow_exposure_inspect":
		props["profile_id"] = map[string]any{"type": "string"}
		required = []any{"profile_id"}
	case "hadron_workflow_exposure_pin_definition", "hadron_workflow_exposure_unpin_definition":
		props["profile_id"] = map[string]any{"type": "string"}
		props["name"] = map[string]any{"type": "string"}
		props["version"] = map[string]any{"type": "string"}
		props["digest"] = map[string]any{"type": "string"}
		props["expected_generation"] = map[string]any{"type": "integer", "minimum": 1}
		required = []any{"profile_id", "name", "version", "digest", "expected_generation"}
	}
	schema := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		schema["required"] = required
	}
	return description, schema
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

func (w *workflowSurface) handlerMap() map[string]gomcp.ToolHandler {
	return map[string]gomcp.ToolHandler{
		"hadron_workflows_search":                   w.handleSearch,
		"hadron_workflows_load":                     w.handleLoad,
		"hadron_workflow_describe":                  w.handleDescribe,
		"hadron_workflow_validate":                  w.handleValidate,
		"hadron_workflow_run":                       w.handleRun,
		"hadron_workflow_run_inspect":                w.handleInspectRun,
		"hadron_workflow_run_cancel":                w.handleCancelRun,
		"hadron_workflow_run_events":                w.handleEvents,
		"hadron_workflow_run_subscribe":              w.handleEvents,
		"hadron_workflow_run_resume":                w.handleResume,
		"hadron_workflow_gate_submit":                w.handleGate,
		"hadron_workflow_message_submit":             w.handleMessage,
		"hadron_workflow_signal":                     w.handleSignal,
		"hadron_workflow_catalog_search":             w.handleLifecycleCatalogSearch,
		"hadron_workflow_catalog_inspect":            w.handleLifecycleCatalogInspect,
		"hadron_workflow_author_validate":            w.handleLifecycleAuthorValidate,
		"hadron_workflow_author_scaffold":            w.handleLifecycleAuthorScaffold,
		"hadron_workflow_author_test":                w.handleLifecycleAuthorTest,
		"hadron_workflow_author_register":            w.handleLifecycleAuthorRegister,
		"hadron_workflow_registry_package":           w.handleLifecyclePackage,
		"hadron_workflow_registry_pin_version":       w.handleLifecycleRegistryPin,
		"hadron_workflow_registry_unpin_version":     w.handleLifecycleRegistryUnpin,
		"hadron_workflow_registry_publish":           w.handleLifecycleRegistryPublish,
		"hadron_workflow_registry_clear_current":     w.handleLifecycleClearCurrent,
		"hadron_workflow_exposure_inspect":           w.handleLifecycleExposureInspect,
		"hadron_workflow_exposure_pin_definition":    w.handleLifecycleExposurePin,
		"hadron_workflow_exposure_unpin_definition":  w.handleLifecycleExposureUnpin,
	}
}

func (w *workflowSurface) registerResources(s *gomcp.Server) {
	const uri = "hadron://workflows/namespaces"
	s.SDKServer().AddResource(&mcpsdk.Resource{
		URI:         uri,
		Name:        "Hadron Workflow Namespaces",
		Description: "Compact namespace counts visible to this MCP principal.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, _ *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		bound, session, err := w.current(ctx, w.adapter.token)
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
		return &mcpsdk.ReadResourceResult{
			Contents: []*mcpsdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(encoded)}},
		}, nil
	})
}

func (w *workflowSurface) current(ctx context.Context, token string) (context.Context, appworkflow.WorkflowExposureSession, error) {
	if nilInterfaceValue(w.exposure) {
		w.clear()
		return ctx, appworkflow.WorkflowExposureSession{}, appworkflow.ErrHostNotReady
	}
	bound, session, err := w.exposure.ResolveSession(ctx, w.adapter.sessionID, token)
	if err != nil {
		w.clear()
		return ctx, appworkflow.WorkflowExposureSession{}, err
	}
	direct, err := w.exposure.DirectWorkflows(bound, session)
	if err != nil {
		w.clear()
		return bound, session, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	prior := w.mount
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
		_ = w.applyLocked(workflowMount{session: session.Clone(), direct: map[string]appworkflow.WorkflowExposureDescriptor{}, lazy: map[string]appworkflow.WorkflowExposureDescriptor{}})
		return bound, session, appworkflow.ErrPolicyDenied
	}
	if err := w.applyLocked(next); err != nil {
		return bound, session, err
	}
	return bound, session.Clone(), nil
}

func (w *workflowSurface) clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.applyLocked(workflowMount{direct: map[string]appworkflow.WorkflowExposureDescriptor{}, lazy: map[string]appworkflow.WorkflowExposureDescriptor{}})
}

// applyLocked reconciles the dynamically registered tool set with next,
// directly against the official SDK's global tool registry via
// RegisterTool/RemoveTools -- see workflowSurface's doc comment for why a
// single global mount (not a per-session one) is correct here. Caller must
// hold w.mu.
func (w *workflowSurface) applyLocked(next workflowMount) error {
	prior := w.mount
	if reflect.DeepEqual(prior.direct, next.direct) && reflect.DeepEqual(prior.lazy, next.lazy) && sameWorkflowSession(prior.session, next.session) {
		return nil
	}
	if w.server != nil {
		merged := mergeDescriptorMaps(next.direct, next.lazy)
		for _, descriptor := range merged {
			tool, err := w.workflowDescriptorGomcpTool(descriptor)
			if err != nil {
				return err
			}
			w.server.RegisterTool(tool)
		}
		var toRemove []string
		for name := range mergeDescriptorMaps(prior.direct, prior.lazy) {
			if _, keep := merged[name]; !keep {
				toRemove = append(toRemove, name)
			}
		}
		if len(toRemove) > 0 {
			w.server.RemoveTools(toRemove...)
		}
	}
	w.mount = next
	return nil
}

func mergeDescriptorMaps(a, b map[string]appworkflow.WorkflowExposureDescriptor) map[string]appworkflow.WorkflowExposureDescriptor {
	result := make(map[string]appworkflow.WorkflowExposureDescriptor, len(a)+len(b))
	for name, descriptor := range a {
		result[name] = descriptor
	}
	for name, descriptor := range b {
		result[name] = descriptor
	}
	return result
}

func (w *workflowSurface) workflowDescriptorGomcpTool(descriptor appworkflow.WorkflowExposureDescriptor) (gomcp.Tool, error) {
	output, err := workflowInvocationOutputSchema(descriptor)
	if err != nil {
		return gomcp.Tool{}, err
	}
	description := "Start an asynchronous durable run of " + descriptor.Name + "@" + descriptor.Version + " (" + descriptor.Digest + "). The result is a run handle; outputs is optional and only valid for a terminal result. Follow with hadron_workflow_run_inspect, hadron_workflow_run_events, or hadron_workflow_run_subscribe."
	readOnly, destructive := true, false
	for _, effect := range descriptor.Effects {
		if effect == graph.EffectMaterialize || effect == graph.EffectMutate || effect == graph.EffectDestructive {
			readOnly = false
		}
		if effect == graph.EffectDestructive {
			destructive = true
		}
	}
	captured := descriptor
	return gomcp.Tool{
		Name:            descriptor.ToolName,
		Description:     description,
		InputSchema:     descriptor.InputSchema,
		OutputSchema:    json.RawMessage(output),
		ReadOnlyHint:    readOnly,
		DestructiveHint: destructive,
		IdempotentHint:  false,
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return w.handleDirect(ctx, args, captured)
		},
	}, nil
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

func (w *workflowSurface) handleSearch(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	limit, err := exactWorkflowLimitArgument(args, 20)
	if err != nil {
		return nil, workflowFailure(err)
	}
	items, err := w.exposure.Search(bound, session, argString(args, "query", ""), limit)
	return workflowResult(items, err)
}

func (w *workflowSurface) handleLoad(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	var input struct {
		Definitions []string `json:"definitions"`
	}
	if decodeErr := decodeWorkflowArguments(args, &input); decodeErr != nil || len(input.Definitions) == 0 {
		return nil, workflowFailure(errors.New("invalid workflow load request"))
	}
	refs := make([]graph.DefinitionRef, 0, len(input.Definitions))
	for _, raw := range input.Definitions {
		ref, parseErr := parseExactWorkflowRef(raw)
		if parseErr != nil {
			return nil, workflowFailure(parseErr)
		}
		refs = append(refs, ref)
	}
	descriptors, err := w.exposure.Load(bound, session, refs)
	if err != nil {
		return nil, workflowFailure(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	mount := w.mount
	if !sameWorkflowSession(mount.session, session) {
		return nil, workflowFailure(appworkflow.ErrPolicyDenied)
	}
	next := workflowMount{session: mount.session.Clone(), direct: cloneDescriptorMap(mount.direct), lazy: cloneDescriptorMap(mount.lazy)}
	for _, descriptor := range descriptors {
		if prior, exists := next.direct[descriptor.ToolName]; exists {
			if prior.Definition != descriptor.Definition {
				return nil, workflowFailure(hostConflict())
			}
			continue
		}
		if prior, exists := next.lazy[descriptor.ToolName]; exists && prior.Definition != descriptor.Definition {
			return nil, workflowFailure(hostConflict())
		}
		next.lazy[descriptor.ToolName] = descriptor
	}
	if len(next.direct)+len(next.lazy) > session.Profile.MaxDirectTools {
		return nil, workflowFailure(appworkflow.ErrPolicyDenied)
	}
	if err := w.applyLocked(next); err != nil {
		return nil, workflowFailure(err)
	}
	return workflowSuccess(map[string]any{"mounted": descriptorNames(descriptors), "count": len(descriptors)}), nil
}

func (w *workflowSurface) handleDescribe(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	descriptor, err := w.exposure.Describe(bound, session, workflowRefFromArgs(args), "inspect")
	return workflowResult(descriptor, err)
}

func (w *workflowSurface) handleValidate(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	ref := workflowRefFromArgs(args)
	if _, err = w.exposure.Describe(bound, session, ref, "validate"); err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.operations) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	validated, err := w.operations.ValidateWorkflow(bound, appworkflow.ValidateWorkflowRequest{Definition: ref, Identity: workflowIdentityRequest()})
	return workflowResult(validated, err)
}

func (w *workflowSurface) handleRun(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	ref := workflowRefFromArgs(args)
	if _, err := w.exposure.Describe(bound, session, ref, "run"); err != nil {
		return nil, workflowFailure(err)
	}
	return w.run(bound, args, ref, false)
}

func (w *workflowSurface) handleDirect(ctx context.Context, args map[string]any, descriptor appworkflow.WorkflowExposureDescriptor) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if !w.mounted(descriptor) {
		return nil, workflowFailure(appworkflow.ErrWorkflowHidden)
	}
	if _, err := w.exposure.Describe(bound, session, descriptor.Definition, "run"); err != nil {
		return nil, workflowFailure(err)
	}
	return w.run(bound, args, descriptor.Definition, true)
}

func (w *workflowSurface) run(ctx context.Context, args map[string]any, ref graph.DefinitionRef, direct bool) (any, error) {
	if nilInterfaceValue(w.operations) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	boundedArgs, err := boundedWorkflowArguments(args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	inputs := boundedArgs
	if !direct {
		inputs = map[string]any{}
		if nested, exists := boundedArgs["inputs"]; exists {
			var ok bool
			inputs, ok = nested.(map[string]any)
			if !ok {
				return nil, workflowFailure(errors.New("workflow inputs must be an object"))
			}
		}
	}
	sequence := w.sequence.Add(1)
	if w.adapter.workflowNonce == "" {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	nonce := w.adapter.workflowNonce
	runID := ""
	if !direct {
		runID = argString(args, "run_id", "")
	}
	if runID == "" {
		runID = fmt.Sprintf("mcp-%s-%s-%d", safeSessionFragment(w.adapter.sessionID), nonce, sequence)
	}
	key := ""
	if !direct {
		key = argString(args, "idempotency_key", "")
	}
	if key == "" {
		key = fmt.Sprintf("mcp-start-%s-%s-%d", safeSessionFragment(w.adapter.sessionID), nonce, sequence)
	}
	confirmed := false
	if !direct {
		confirmed = argBool(args, "confirmed", false)
	}
	started, runErr := w.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{RunID: appworkflow.RunID(runID), Definition: ref, Inputs: inputs, IdempotencyKey: key, Identity: workflowIdentityRequest(), Confirmed: confirmed})
	return workflowStartResult(appworkflow.RunID(runID), started, runErr)
}

func (w *workflowSurface) handleInspectRun(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.operations) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	display, err := w.display(bound, session, argBool(args, "reveal_private", false))
	if err != nil {
		return nil, workflowFailure(err)
	}
	inspected, err := w.operations.InspectWorkflowRun(bound, appworkflow.InspectWorkflowRunRequest{RunID: appworkflow.RunID(argString(args, "run_id", "")), Identity: workflowIdentityRequest(), Display: display, NodeLimit: 100, AttemptLimit: 100, EventLimit: 100, ValueLimit: 100, ResourceLimit: 100, ActivationLimit: 100})
	return workflowResult(inspected, err)
}

func (w *workflowSurface) handleCancelRun(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.operations) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	canceled, err := w.operations.CancelWorkflowRun(bound, appworkflow.CancelWorkflowRunRequest{RunID: appworkflow.RunID(argString(args, "run_id", "")), Identity: workflowIdentityRequest(), IdempotencyKey: argString(args, "idempotency_key", ""), Reason: argString(args, "reason", "")})
	return workflowResult(canceled, err)
}

func (w *workflowSurface) handleEvents(ctx context.Context, args map[string]any) (any, error) {
	bound, session, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.reads) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	display, err := w.display(bound, session, argBool(args, "reveal_private", false))
	if err != nil {
		return nil, workflowFailure(err)
	}
	limit, err := exactWorkflowLimitArgument(args, 100)
	if err != nil {
		return nil, workflowFailure(err)
	}
	events, err := w.reads.FetchWorkflowEvents(bound, appworkflow.WorkflowRunReadRequest{RunID: appworkflow.RunID(argString(args, "run_id", "")), Identity: workflowIdentityRequest(), Display: display, EventLimit: boundedWorkflowLimit(limit)})
	return workflowResult(events, err)
}

func (w *workflowSurface) handleResume(ctx context.Context, args map[string]any) (any, error) {
	return w.resume(ctx, args, appworkflow.WorkflowWakeCallback)
}

func (w *workflowSurface) handleGate(ctx context.Context, args map[string]any) (any, error) {
	return w.resume(ctx, args, appworkflow.WorkflowWakeGate)
}

func (w *workflowSurface) handleMessage(ctx context.Context, args map[string]any) (any, error) {
	return w.resume(ctx, args, appworkflow.WorkflowWakeMessage)
}

func (w *workflowSurface) resume(ctx context.Context, args map[string]any, source appworkflow.WorkflowWakeSource) (any, error) {
	bound, _, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.operations) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	payload, err := workflowValueArgument(args, "payload")
	if err != nil {
		return nil, workflowFailure(err)
	}
	resumed, err := w.operations.ResumeWorkflowRun(bound, appworkflow.ResumeWorkflowRunRequest{RunID: appworkflow.RunID(argString(args, "run_id", "")), Identity: workflowIdentityRequest(), WaitID: appworkflow.WaitID(argString(args, "wait_id", "")), Correlation: argString(args, "correlation", ""), Token: argString(args, "token", ""), WakeSource: source, Payload: payload, IdempotencyKey: argString(args, "idempotency_key", "")})
	return workflowResult(resumed, err)
}

func (w *workflowSurface) handleSignal(ctx context.Context, args map[string]any) (any, error) {
	bound, _, err := w.requestSession(ctx, args)
	if err != nil {
		return nil, workflowFailure(err)
	}
	if nilInterfaceValue(w.signals) {
		return nil, workflowFailure(appworkflow.ErrHostNotReady)
	}
	payload, err := workflowValueArgument(args, "payload")
	if err != nil {
		return nil, workflowFailure(err)
	}
	signaled, err := w.signals.SignalWorkflowRun(bound, appworkflow.SignalWorkflowRunRequest{RunID: appworkflow.RunID(argString(args, "run_id", "")), Name: argString(args, "name", ""), Correlation: argString(args, "correlation", ""), Payload: payload, IdempotencyKey: argString(args, "idempotency_key", ""), Identity: workflowIdentityRequest(), Confirmed: argBool(args, "confirmed", false)})
	return workflowResult(signaled, err)
}

func (w *workflowSurface) requestSession(ctx context.Context, args map[string]any) (context.Context, appworkflow.WorkflowExposureSession, error) {
	if _, err := boundedWorkflowArguments(args); err != nil {
		return ctx, appworkflow.WorkflowExposureSession{}, err
	}
	bound, session, err := w.current(ctx, w.adapter.token)
	if err != nil {
		return ctx, appworkflow.WorkflowExposureSession{}, err
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

func (w *workflowSurface) mounted(descriptor appworkflow.WorkflowExposureDescriptor) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	current, ok := w.mount.direct[descriptor.ToolName]
	if !ok {
		current, ok = w.mount.lazy[descriptor.ToolName]
	}
	return ok && current.Definition == descriptor.Definition
}

func workflowRefFromArgs(args map[string]any) graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "registry", ID: argString(args, "name", ""), Version: argString(args, "version", ""), Digest: argString(args, "digest", "")}
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

func workflowValueArgument(args map[string]any, key string) (values.Value, error) {
	arguments, err := boundedWorkflowArguments(args)
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

func boundedWorkflowArguments(args map[string]any) (map[string]any, error) {
	if args == nil {
		args = map[string]any{}
	}
	if err := validateWorkflowArgumentNumbers(reflect.ValueOf(args), 0); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(args)
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

func exactWorkflowLimitArgument(args map[string]any, defaultValue int) (int, error) {
	value, exists := args["limit"]
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

func decodeWorkflowArguments(args map[string]any, target any) error {
	arguments, err := boundedWorkflowArguments(args)
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

func workflowResult(value any, err error) (any, error) {
	if err != nil {
		return nil, workflowFailure(err)
	}
	return workflowSuccess(value), nil
}

func workflowStartResult(runID appworkflow.RunID, value appworkflow.StartRunResult, err error) (any, error) {
	if err != nil {
		safe := appworkflow.SafeWorkflowOperationError(err, &value)
		return nil, workflowOperationErr{safe: safe}
	}
	status := "not_admitted"
	if value.Run != nil {
		status = string(value.Run.Status)
	} else if value.Bound != nil {
		status = "bound"
	}
	return workflowSuccess(workflowInvocationResult{RunID: runID, Status: status, Outcome: string(value.Outcome), Diagnostics: append([]diagnostic.Diagnostic(nil), value.Diagnostics...)}), nil
}

func workflowSuccess(value any) any { return value }

// workflowOperationErr adapts appworkflow.WorkflowOperationError to
// budget.StructuredError: the type deliberately carries no human-readable
// message ("Message text is intentionally not transported"), so it can't
// satisfy budget.ToolError's required Message field, but still needs
// StructuredContent+IsError treatment for its own safe, bounded shape.
type workflowOperationErr struct {
	safe appworkflow.WorkflowOperationError
}

func (e workflowOperationErr) Error() string        { return "workflow error: " + e.safe.Code }
func (e workflowOperationErr) ToolErrorContent() any { return map[string]any{"error": e.safe} }

var _ budget.StructuredError = workflowOperationErr{}

func workflowFailure(err error) error {
	if errors.Is(err, appworkflow.ErrHostNotReady) || errors.Is(err, appworkflow.ErrInvalidHost) {
		return workflowOperationErr{safe: appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable}}
	}
	return workflowOperationErr{safe: appworkflow.SafeWorkflowOperationError(err, nil)}
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
