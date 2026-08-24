package call_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/adapters/call/calltest"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestNestedInlineAndRunCallsDriveRealRuntimeDispatch(t *testing.T) {
	for _, test := range []struct {
		name       string
		nestedMode graph.CallMode
		wantEvents int
	}{
		{name: "nested_inline", nestedMode: graph.CallInline, wantEvents: 2},
		{name: "nested_run", nestedMode: graph.CallRun, wantEvents: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			host := newRuntimeCallHost(t, test.nestedMode)
			result := host.dispatchRoot(t)
			if result.Node.Status != workflowruntime.NodeSucceeded || result.Attempt.Status != workflowruntime.NodeSucceeded {
				t.Fatalf("root dispatch = %#v", result)
			}
			if got := len(host.journal.Events()); got != test.wantEvents {
				t.Fatalf("resolution events = %d, want %d", got, test.wantEvents)
			}
			for _, event := range host.journal.Events() {
				if event.Record.Resolved.Digest == "" || event.Record.Resolved.Provenance == nil ||
					event.Record.Resolved.Provenance.Digest != event.Record.Resolved.Digest {
					t.Fatalf("durable resolved provenance = %#v", event)
				}
			}
			for _, request := range host.inline.requestsSnapshot() {
				if request.Parent.RunID != string(host.runID) {
					t.Fatalf("inline child escaped parent run identity: %#v", request.Parent)
				}
			}

			switch test.nestedMode {
			case graph.CallInline:
				if result.Result == nil || result.Result.Outputs["result"].Inline != "node-local" || host.runs.createdCount() != 0 {
					t.Fatalf("inline nested result = %#v, child runs = %d", result, host.runs.createdCount())
				}
			case graph.CallRun:
				if result.Result == nil || result.Result.Outputs[calladapter.OutputStatus].Inline != string(workflowruntime.RunRunning) ||
					host.runs.createdCount() != 1 {
					t.Fatalf("run nested result = %#v, child runs = %d", result, host.runs.createdCount())
				}
				runID, ok := result.Result.Outputs[calladapter.OutputRunID].Inline.(string)
				if !ok || runID == "" {
					t.Fatalf("run handle is not wait_for-compatible: %#v", result.Result.Outputs)
				}
				record := host.runs.onlyRecord(t)
				if string(record.result.Run.ID) != runID || record.result.Link.Policy != graph.ParentCloseCancel ||
					record.result.Cancellation.RunID != record.result.Run.ID || len(record.events) != 1 {
					t.Fatalf("durable child operation = %#v", record)
				}
			}
		})
	}
}

func TestRuntimeDispatchRejectsMissingCallLineageBeforeStartingAttempt(t *testing.T) {
	host := newRuntimeCallHost(t, graph.CallInline)
	node := graph.Node{
		ID: "missing-lineage", Kind: calladapter.KindName, KindVersion: calladapter.KindVersion, Config: graph.Config{},
		Call: &graph.CallSpec{
			Definition: graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "middle", Version: "stable"},
			Mode:       graph.CallInline, OnParentClose: graph.ParentCloseCancel,
		},
		Outputs: outputDeclarations(host.resolver.definitions["middle"].Graph.Outputs),
	}
	_, claim, err := host.materializeReady(t.Context(), node, values.ValueSet{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := host.dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: "missing-lineage"})
	if !errors.Is(err, workflowruntime.ErrInvalidDispatch) || result.Node.Status != workflowruntime.NodeReady ||
		result.Node.LatestAttempt != 0 || result.Node.Lease != nil {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
}

type runtimeCallHost struct {
	store      *runtimetest.Store
	registry   stepkind.Registry
	dispatcher *workflowruntime.StepDispatcher
	resolver   *definitionMap
	contexts   *contextTable
	journal    *calltest.Journal
	inline     *inlineRuntime
	runs       *atomicRunHost
	runID      workflowruntime.RunID
	now        time.Time
	clockMu    sync.Mutex
	tick       int
}

func newRuntimeCallHost(t *testing.T, nestedMode graph.CallMode) *runtimeCallHost {
	t.Helper()
	host := &runtimeCallHost{
		store: runtimetest.NewStore(), resolver: &definitionMap{definitions: make(map[string]workflowcompile.ResolvedDefinition)},
		contexts: newContextTable(), journal: calltest.NewJournal(), runs: newAtomicRunHost(),
		runID: "nested-call-run", now: time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC),
	}
	host.resolver.definitions["leaf"] = leafDefinition()
	host.resolver.definitions["middle"] = middleDefinition(nestedMode)
	host.inline = &inlineRuntime{host: host}
	callExecutor := mustExecutor(t, calladapter.Options{
		Resolver: host.resolver, State: host.journal, Context: host.contexts,
		Inline: host.inline, Runs: host.runs,
	})
	host.registry = stepkind.NewRegistry()
	if err := host.registry.Register(callExecutor); err != nil {
		t.Fatal(err)
	}
	if err := host.registry.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: host.store, Registry: host.registry, Now: host.nextTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	host.dispatcher = dispatcher
	host.createRun(t)
	return host
}

func (h *runtimeCallHost) createRun(t *testing.T) {
	t.Helper()
	plan := workflowruntime.PlanRef{ID: "root", Version: "v1", Digest: rootDefinition().Digest, SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion}
	if _, _, err := h.store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
		ID: h.runID, Plan: plan, Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-nested-call", CreatedAt: h.nextTime(),
	}); err != nil {
		t.Fatal(err)
	}
	run, err := h.store.LoadRun(t.Context(), h.runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{
		RunID: h.runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: h.nextTime(),
	}); err != nil {
		t.Fatal(err)
	}
}

func (h *runtimeCallHost) dispatchRoot(t *testing.T) workflowruntime.DispatchResult {
	t.Helper()
	local := inlineValue(t, "node-local", "root-binding", values.RedactionPrivate, values.RetentionRun)
	contextDefault := inlineValue(t, "resolver-default", "root-context", values.RedactionPrivate, values.RetentionRun)
	node := graph.Node{
		ID: "root-call", Kind: calladapter.KindName, KindVersion: calladapter.KindVersion, Config: graph.Config{},
		Call: &graph.CallSpec{
			Definition: graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "middle", Version: "stable"},
			Mode:       graph.CallInline, OnParentClose: graph.ParentCloseCancel,
		},
		Outputs: outputDeclarations(h.resolver.definitions["middle"].Graph.Outputs),
	}
	identity, claim, err := h.materializeReady(t.Context(), node, values.ValueSet{"message": local})
	if err != nil {
		t.Fatal(err)
	}
	h.contexts.put(calladapter.CallSiteIdentity{RunID: string(identity.RunID), NodeID: identity.NodeID, Iteration: identity.Iteration}, values.ExpressionContext{
		Inputs: values.ValueSet{"default_message": contextDefault},
	})
	result, err := h.dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{
		Claim: claim, Node: node, CallLineage: []graph.DefinitionRef{rootDefinition()}, IdempotencyKey: "root-call-key",
	})
	if err != nil {
		t.Fatalf("root Dispatch() = %#v, %v", result, err)
	}
	return result
}

func (h *runtimeCallHost) materializeReady(ctx context.Context, node graph.Node, inputs values.ValueSet) (workflowruntime.NodeInvocationID, workflowruntime.ReadyClaim, error) {
	if ctx == nil {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, errors.New("context is required")
	}
	inputRef, err := h.store.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "call-fixture-inputs", RunID: h.runID}, Values: inputs,
	})
	if err != nil {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, err
	}
	id := workflowruntime.NodeInvocationID{RunID: h.runID, NodeID: node.ID}
	createdAt := h.nextTime()
	created, err := h.store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, Inputs: &inputRef, CreatedAt: createdAt, UpdatedAt: createdAt,
	}})
	if err != nil {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, err
	}
	ready, err := h.store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: created.Generation, To: workflowruntime.NodeReady, At: h.nextTime(),
	})
	if err != nil {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, err
	}
	now := h.nextTime()
	queue := workflowruntime.NewReadyQueueCoordinator(h.store, workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
		return []workflowruntime.NodeInvocationID{id}, nil
	}))
	claim, acquired, err := queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{
		Owner: "fixture-worker", Token: "token-" + node.ID,
		IdempotencyKey: "claim-" + node.ID, Now: now, LeaseUntil: now.Add(time.Hour),
	})
	if err != nil {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, fmt.Errorf("ClaimNext(%s): %w", node.ID, err)
	}
	if !acquired || claim.Candidate.InvocationID != id || ready.Snapshot.Status != workflowruntime.NodeReady {
		return workflowruntime.NodeInvocationID{}, workflowruntime.ReadyClaim{}, fmt.Errorf("ClaimNext(%s) = %#v, acquired=%v", node.ID, claim, acquired)
	}
	return id, claim, nil
}

func (h *runtimeCallHost) nextTime() time.Time {
	h.clockMu.Lock()
	defer h.clockMu.Unlock()
	h.tick++
	return h.now.Add(time.Duration(h.tick) * time.Second)
}

type inlineRuntime struct {
	host     *runtimeCallHost
	mu       sync.Mutex
	requests []calladapter.InlineRequest
}

func (r *inlineRuntime) ExecuteInline(ctx context.Context, request calladapter.InlineRequest) (calladapter.InlineResult, error) {
	if ctx == nil || r == nil || r.host == nil {
		return calladapter.InlineResult{}, errors.New("inline runtime is unavailable")
	}
	r.mu.Lock()
	r.requests = append(r.requests, cloneInlineRequest(request))
	r.mu.Unlock()
	if request.Parent.RunID != string(r.host.runID) || len(request.Definition.Graph.Nodes) != 1 {
		return calladapter.InlineResult{}, errors.New("fixture inline request has invalid run or graph shape")
	}
	original := request.Definition.Graph.Nodes[0]
	node := original
	node.ID = nestedNodeID(request.Parent.NodeID, original.ID)
	inputs, err := evaluateNodeInputs(original.InputBindings, request.Inputs, request.Parent)
	if err != nil {
		return calladapter.InlineResult{}, err
	}
	identity, claim, err := r.host.materializeReady(ctx, node, inputs)
	if err != nil {
		return calladapter.InlineResult{}, err
	}
	r.host.contexts.put(calladapter.CallSiteIdentity{RunID: string(identity.RunID), NodeID: identity.NodeID, Iteration: identity.Iteration}, values.ExpressionContext{Inputs: cloneValues(request.Inputs)})
	dispatchRequest := workflowruntime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: request.IdempotencyKey}
	if node.Call != nil {
		dispatchRequest.CallLineage = request.Lineage
	}
	result, err := r.host.dispatcher.Dispatch(ctx, dispatchRequest)
	if err != nil {
		return calladapter.InlineResult{}, fmt.Errorf("nested dispatch: %w (failure=%#v)", err, result.Attempt.Failure)
	}
	if result.Result == nil || result.Result.Outcome != stepkind.StepCompleted {
		return calladapter.InlineResult{}, errors.New("nested runtime did not complete")
	}
	return calladapter.InlineResult{Outputs: cloneValues(result.Result.Outputs)}, nil
}

func (r *inlineRuntime) requestsSnapshot() []calladapter.InlineRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]calladapter.InlineRequest, len(r.requests))
	for index, request := range r.requests {
		result[index] = cloneInlineRequest(request)
	}
	return result
}

type definitionMap struct {
	mu          sync.RWMutex
	definitions map[string]workflowcompile.ResolvedDefinition
}

func (r *definitionMap) ResolveDefinition(_ context.Context, ref graph.DefinitionRef) (workflowcompile.ResolvedDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	resolved, ok := r.definitions[ref.ID]
	if !ok {
		return workflowcompile.ResolvedDefinition{}, fmt.Errorf("definition %q was not found", ref.ID)
	}
	return resolved, nil
}

type contextTable struct {
	mu       sync.RWMutex
	contexts map[calladapter.CallSiteIdentity]values.ExpressionContext
}

func newContextTable() *contextTable {
	return &contextTable{contexts: make(map[calladapter.CallSiteIdentity]values.ExpressionContext)}
}

func (t *contextTable) put(id calladapter.CallSiteIdentity, value values.ExpressionContext) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.contexts[id] = value
}

func (t *contextTable) ExpressionContext(_ context.Context, invocation stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error) {
	id := calladapter.CallSiteIdentity{RunID: invocation.Identity.RunID, NodeID: invocation.Identity.NodeID, Iteration: invocation.Identity.Iteration}
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, ok := t.contexts[id]
	if !ok {
		return values.ExpressionContext{}, values.ExpressionOptions{}, fmt.Errorf("context for %#v was not found", id)
	}
	return value, values.ExpressionOptions{}, nil
}

type atomicRunRecord struct {
	request calladapter.ChildRunRequest
	result  calladapter.ChildRunResult
	events  []string
}

type atomicRunHost struct {
	mu      sync.Mutex
	records map[string]atomicRunRecord
}

func newAtomicRunHost() *atomicRunHost {
	return &atomicRunHost{records: make(map[string]atomicRunRecord)}
}

func (h *atomicRunHost) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	if ctx == nil {
		return calladapter.ChildRunResult{}, errors.New("context is required")
	}
	if ctx.Err() != nil {
		return calladapter.ChildRunResult{}, context.Cause(ctx)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if prior, ok := h.records[request.IdempotencyKey]; ok {
		if !reflect.DeepEqual(prior.request, request) {
			return calladapter.ChildRunResult{}, workflowruntime.ErrIdempotencyConflict
		}
		return prior.result, nil
	}
	result, err := validChildRun(ctx, request)
	if err != nil {
		return calladapter.ChildRunResult{}, err
	}
	// The reference host commits request, Run, ChildRunLink, creation event,
	// and cancellation handle together under this lock.
	h.records[request.IdempotencyKey] = atomicRunRecord{
		request: request, result: result,
		events: []string{"child_run.created"},
	}
	return result, nil
}

func (h *atomicRunHost) createdCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *atomicRunHost) onlyRecord(t *testing.T) atomicRunRecord {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("child records = %#v", h.records)
	}
	for _, record := range h.records {
		return record
	}
	return atomicRunRecord{}
}

func leafDefinition() workflowcompile.ResolvedDefinition {
	inputs := []graph.InputSpec{{Name: "message", Required: true, Schema: graph.Schema{"type": "string"}}}
	outputs := []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}}
	resolved := resolvedDefinition("leaf", inputs, outputs)
	resolved.InputBindings = map[string]graph.Binding{"message": {Kind: graph.BindingLiteral, Literal: "leaf-default"}}
	resolved.Graph.Nodes = []graph.Node{{
		ID: "echo", Kind: transform.Name, KindVersion: transform.Version,
		Config:        graph.Config{"result": "inputs.message"},
		InputBindings: map[string]graph.Binding{"message": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}},
		Outputs:       outputDeclarations(outputs),
	}}
	return resolved
}

func middleDefinition(nestedMode graph.CallMode) workflowcompile.ResolvedDefinition {
	inputs := []graph.InputSpec{{Name: "message", Required: true, Schema: graph.Schema{"type": "string"}}}
	var outputs []graph.OutputSpec
	if nestedMode == graph.CallInline {
		outputs = []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}}
	} else {
		outputs = childHandleDeclarations()
	}
	resolved := resolvedDefinition("middle", inputs, outputs)
	resolved.InputBindings = map[string]graph.Binding{
		"message": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.default_message"}},
	}
	resolved.Graph.Nodes = []graph.Node{{
		ID: "nested", Kind: calladapter.KindName, KindVersion: calladapter.KindVersion, Config: graph.Config{},
		Call: &graph.CallSpec{
			Definition: graph.DefinitionRef{Authority: "package", Kind: "package", ID: "leaf", Locator: "pkg://fixture/leaf", Version: "v1"},
			Mode:       nestedMode, OnParentClose: graph.ParentCloseCancel,
		},
		InputBindings: map[string]graph.Binding{"message": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}},
		Outputs:       outputDeclarations(outputs),
	}}
	return resolved
}

func childHandleDeclarations() []graph.OutputSpec {
	return []graph.OutputSpec{
		{Name: calladapter.OutputRunID, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputStatus, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputEventsRef, Schema: graph.Schema{"type": "string"}},
		{Name: calladapter.OutputCancellation, Schema: graph.Schema{"type": "object"}},
		{Name: calladapter.OutputOutputsRef, Schema: graph.Schema{"type": []any{"object", "null"}}},
	}
}

func outputDeclarations(input []graph.OutputSpec) []graph.OutputSpec {
	result := make([]graph.OutputSpec, len(input))
	for index, output := range input {
		result[index] = graph.OutputSpec{Name: output.Name, Schema: output.Schema}
	}
	return result
}

func evaluateNodeInputs(bindings map[string]graph.Binding, parent values.ValueSet, identity calladapter.CallSiteIdentity) (values.ValueSet, error) {
	engine := values.NewExpressionEngine()
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(values.ValueSet, len(names))
	for _, name := range names {
		value, err := engine.EvaluateBinding(bindings[name], values.ExpressionContext{Inputs: parent}, values.ExpressionOptions{}, values.Metadata{
			Producer:  values.Producer{Kind: "fixture_node_binding", Reference: identity.RunID + "/" + identity.NodeID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return nil, err
		}
		result[name] = value
	}
	return result, nil
}

func nestedNodeID(parent, child string) string {
	value := strings.Trim(strings.Join([]string{parent, child}, "-"), "-")
	if len(value) <= graph.MaxIDLength {
		return value
	}
	return value[:graph.MaxIDLength]
}

func cloneInlineRequest(request calladapter.InlineRequest) calladapter.InlineRequest {
	return calladapter.InlineRequest{
		Parent: request.Parent, Definition: request.Definition, Inputs: cloneValues(request.Inputs),
		Lineage: append([]graph.DefinitionRef(nil), request.Lineage...), IdempotencyKey: request.IdempotencyKey,
	}
}

func cloneValues(input values.ValueSet) values.ValueSet {
	result := make(values.ValueSet, len(input))
	for name, value := range input {
		result[name] = value
	}
	return result
}

var _ workflowcompile.DefinitionResolver = (*definitionMap)(nil)
var _ calladapter.ContextProvider = (*contextTable)(nil)
var _ calladapter.InlineExecutor = (*inlineRuntime)(nil)
var _ calladapter.ChildRunExecutor = (*atomicRunHost)(nil)
