package agent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/adapters/agent"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestCompileAgentLaunchSourceBundlesRestartSafeTypedComposition(t *testing.T) {
	first := compileAgentSource(t, "first/agent.workflow.yaml", waitAgentSource)
	second := compileAgentSource(t, "relocated/agent.workflow.yaml", waitAgentSource)
	if first.Digest != second.Digest || first.Graph.Digest != second.Graph.Digest {
		t.Fatalf("relocation changed semantic digests: %s/%s vs %s/%s", first.Digest, first.Graph.Digest, second.Digest, second.Graph.Digest)
	}
	if len(first.BundledDefinitions) != 1 {
		t.Fatalf("bundled definitions = %#v", first.BundledDefinitions)
	}
	launch := nodeByID(t, first.Graph, "review-launch")
	result := nodeByID(t, first.Graph, "review")
	if launch.Kind != "call" || launch.Call == nil || launch.Call.Mode != graph.CallRun || launch.Call.OnParentClose != graph.ParentCloseRequestCancel {
		t.Fatalf("launch = %#v", launch)
	}
	if result.Kind != waitadapter.WaitForName || !slices.Equal(outputNames(result.Outputs), []string{"payload", "resume", "timed_out"}) {
		t.Fatalf("authored result node = %#v", result)
	}
	if !slices.Equal(outputNames(launch.Outputs), []string{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}) {
		t.Fatalf("launch outputs = %#v", launch.Outputs)
	}
	if launch.If == nil || launch.If.Text != "inputs.enabled" || launch.ReadyWhen != graph.ReadyAllSuccess || len(launch.Needs) != 1 || launch.Needs[0].Node != "prepare" {
		t.Fatalf("launch control fields = %#v", launch)
	}
	if launch.Timeout != nil || result.Timeout == nil || result.Timeout.Wait != "30m" || first.BundledDefinitions[0].Graph.Nodes[0].Timeout.Heartbeat != "2m" {
		t.Fatalf("distributed timeout = launch=%#v wait=%#v child=%#v", launch.Timeout, result.Timeout, first.BundledDefinitions[0].Graph.Nodes[0].Timeout)
	}
	if binding := launch.InputBindings["request-id"]; binding.Expression == nil || binding.Expression.Text != "inputs.request-id" {
		t.Fatalf("parent input binding = %#v", binding)
	}
	child := first.BundledDefinitions[0].Graph
	if len(child.Inputs) != 2 || child.Inputs[1].Name != "request-id" || child.Nodes[0].InputBindings["request-id"].Expression.Text != `inputs["request-id"]` {
		t.Fatalf("generated typed child inputs = %#v / %#v", child.Inputs, child.Nodes[0].InputBindings)
	}
	if mapped, ok := first.SourceMap.Nodes["review-launch"]; !ok || !slices.Equal(mapped.Path, []string{"steps", "1"}) || mapped.StartLine != 15 {
		t.Fatalf("generated launch source = %#v", mapped)
	}
	if mapped := first.SourceMap.Nodes["review"]; !slices.Equal(mapped.Path, []string{"steps", "1"}) || mapped.StartLine != 15 {
		t.Fatalf("authored result source = %#v", mapped)
	}

	inferred := workflowcompile.InferValueDependencies(first, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 || !hasEdge(inferred.Plan.Graph.Edges, "review", "summarize", graph.EdgeData) {
		t.Fatalf("dependency inference = %#v", inferred)
	}

	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var restarted workflowcompile.ExecutionPlan
	if decodeErr := decoder.Decode(&restarted); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	resolver, err := workflowcompile.NewBundledDefinitionResolver(&restarted)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.ResolveDefinition(t.Context(), launch.Call.Definition)
	if err != nil || resolved.Graph.Digest != launch.Call.Definition.Digest || resolved.Graph.Nodes[0].Kind != agent.KindName {
		t.Fatalf("restart resolution = %#v, %v", resolved, err)
	}
	// Construction owns a complete copy; mutating the deserialized plan cannot
	// alter subsequent resolution.
	restarted.BundledDefinitions[0].Graph.Nodes[0].Config["substrate"] = "mutated"
	again, err := resolver.ResolveDefinition(t.Context(), launch.Call.Definition)
	if err != nil || again.Graph.Nodes[0].Config["substrate"] != "remote" {
		t.Fatalf("resolver retained caller-owned plan material: %#v, %v", again, err)
	}

	registry := stepkind.NewRegistry()
	for _, kind := range []stepkind.StepKind{
		stepkindtest.NewNoopKind("transform", "v1"),
		stepkindtest.NewNoopKind("call", "v1"),
		stepkindtest.NewNoopKind(waitadapter.WaitForName, waitadapter.Version),
		stepkindtest.NewNoopKind(agent.KindName, agent.KindVersion),
	} {
		if err := registry.Register(kind); err != nil {
			t.Fatal(err)
		}
	}
	if findings := workflowcompile.ValidatePlan(t.Context(), first, workflowcompile.ValidationOptions{StepKinds: registry, Definitions: resolver}); len(findings) != 0 {
		t.Fatalf("ValidatePlan = %#v", findings)
	}
}

func TestCompileAgentLaunchFireAndForgetKeepsAuthoredCallContract(t *testing.T) {
	plan := compileAgentSource(t, "fire.workflow.yaml", fireAgentSource)
	if len(plan.Graph.Nodes) != 1 || len(plan.BundledDefinitions) != 1 {
		t.Fatalf("fire plan = %#v", plan)
	}
	launch := plan.Graph.Nodes[0]
	if launch.ID != "review" || launch.Kind != "call" || launch.Call == nil || launch.Call.OnParentClose != graph.ParentCloseAbandon ||
		!slices.Equal(outputNames(launch.Outputs), []string{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}) {
		t.Fatalf("fire authored contract = %#v", launch)
	}
	if _, waitOutput := findOutput(launch.Outputs, "payload"); waitOutput {
		t.Fatal("fire-and-forget authored node exposed wait payload")
	}
}

func TestCompiledFireAndForgetDispatchReturnsOrdinaryTypedCallHandle(t *testing.T) {
	plan := compileAgentSource(t, "fire-dispatch.workflow.yaml", fireAgentSource)
	launch := plan.Graph.Nodes[0]
	registry := stepkind.NewRegistry()
	callKind := stepkindtest.NewNoopKind("call", "v1")
	callKind.SpecValue.InputSchema = graph.Schema{"type": "object", "required": []any{agent.ParentCorrelationInput}, "properties": map[string]any{
		agent.ParentCorrelationInput: map[string]any{"type": "string"},
	}, "additionalProperties": false}
	callKind.SpecValue.OutputSchema = graph.Schema{"type": "object", "required": []any{"run-id", "status", "events-ref", "cancellation", "outputs-ref"}, "properties": map[string]any{
		"run-id": map[string]any{"type": "string"}, "status": map[string]any{"type": "string"}, "events-ref": map[string]any{"type": "string"},
		"cancellation": map[string]any{"type": "object"}, "outputs-ref": map[string]any{"type": []any{"object", "null"}},
	}, "additionalProperties": false}
	callKind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if prepared.Invocation.Call == nil || prepared.Invocation.Call.Spec.OnParentClose != graph.ParentCloseAbandon ||
			prepared.Invocation.Inputs[agent.ParentCorrelationInput].Inline != "agent:parent-run:review" {
			t.Fatalf("compiled fire invocation = %#v", prepared.Invocation)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{
			"run-id":       inline(t, "child-run-fire", values.RedactionPrivate, values.RetentionRun),
			"status":       inline(t, "started", values.RedactionPrivate, values.RetentionRun),
			"events-ref":   inline(t, "events://child-run-fire", values.RedactionPrivate, values.RetentionRun),
			"cancellation": inline(t, map[string]any{"policy": "abandon"}, values.RedactionPrivate, values.RetentionRun),
			"outputs-ref":  inline(t, nil, values.RedactionPrivate, values.RetentionRun),
		}}, nil
	}
	if err := registry.Register(callKind); err != nil {
		t.Fatal(err)
	}
	store := runtimetest.NewStore()
	now := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
	if _, _, err := store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
		ID: "parent-run", Plan: testPlanRef(), Status: workflowruntime.RunPending, StartIdempotencyKey: "fire-start", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	metadata := values.Metadata{Producer: values.Producer{Kind: "binding", Reference: "parent-run/review"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	correlation, err := values.NewExpressionEngine().EvaluateBinding(launch.InputBindings[agent.ParentCorrelationInput], values.ExpressionContext{Run: map[string]any{"id": "parent-run"}}, values.ExpressionOptions{}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	claim := createReadyClaim(t, store, "parent-run", launch.ID, values.ValueSet{agent.ParentCorrelationInput: correlation}, now, "fire-call")
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now.Add(2 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{
		Claim: claim, Node: launch, IdempotencyKey: "fire-call-key",
		CallLineage: []graph.DefinitionRef{{Kind: "workflow", ID: "parent", Version: "v1", Digest: values.SHA256Digest([]byte("parent"))}},
	})
	if err != nil || result.Node.Status != workflowruntime.NodeSucceeded || result.Outputs == nil {
		t.Fatalf("Dispatch = %#v, %v", result, err)
	}
	outputs, err := store.LoadValues(t.Context(), *result.Outputs)
	if err != nil || outputs["run-id"].Inline != "child-run-fire" || len(outputs) != 5 {
		t.Fatalf("typed fire outputs = %#v, %v", outputs, err)
	}
}

func TestCompileAgentLaunchFailsClosedWithSourceMappedDiagnostics(t *testing.T) {
	tests := []struct {
		name, step string
		message    string
	}{
		{"bare wait", "timeout: {execution: 1h}\n    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: true}", "requires timeout.wait"},
		{"conflicting timeout", "timeout: {wait: 20m}\n    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: {timeout: 30m}}", "conflicts with timeout.wait"},
		{"fanout", "for_each: inputs.items\n    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: false}", "does not support for_each"},
		{"owned output", "outputs: [{name: handle, type: object}]\n    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: false}", "owns its closed output"},
		{"reserved control", "agent_launch: {substrate: remote, logical_agent_id: reviewer, correlation: authored, wait: false}", "correlation is unsupported"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := "workflow: {id: invalid, version: v1}\nsteps:\n  - id: review\n    " + test.step + "\n"
			loaded := workflowcompile.LoadBytes("invalid.workflow.yaml", []byte(source))
			if len(loaded.Diagnostics) != 0 {
				t.Fatal(loaded.Diagnostics)
			}
			result := agent.Compile(loaded.Source)
			if result.Plan != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != workflowcompile.CodeNodeExpansion ||
				result.Diagnostics[0].Source == nil || !slices.Equal(result.Diagnostics[0].Source.Path, []string{"steps", "0"}) ||
				!strings.Contains(result.Diagnostics[0].Message, test.message) {
				t.Fatalf("Compile = %#v", result)
			}
		})
	}
}

func TestCompileAgentLaunchRejectsGeneratedIdentityCollisionAndAmbiguousControlTarget(t *testing.T) {
	for name, source := range map[string]string{
		"collision": `workflow: {id: invalid, version: v1}
steps:
  - id: review-launch
    transform: {value: 1}
  - id: review
    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: {timeout: 30m}}
`,
		"control target": `workflow: {id: invalid, version: v1}
steps:
  - id: route
    transform: {value: 1}
    switch:
      arms:
        - when: inputs.enabled
          targets: [review]
  - id: review
    agent_launch: {substrate: remote, logical_agent_id: reviewer, wait: {timeout: 30m}}
`,
	} {
		t.Run(name, func(t *testing.T) {
			loaded := workflowcompile.LoadBytes("invalid.workflow.yaml", []byte(source))
			result := agent.Compile(loaded.Source)
			if result.Plan != nil || len(result.Diagnostics) == 0 || result.Diagnostics[0].Code != workflowcompile.CodeNodeExpansion {
				t.Fatalf("Compile = %#v", result)
			}
		})
	}
}

func TestBundledResolverRequiresExactImmutableTupleAndContext(t *testing.T) {
	plan := compileAgentSource(t, "exact.workflow.yaml", fireAgentSource)
	exactDuplicate := *plan
	exactDuplicate.BundledDefinitions = append(append([]workflowcompile.ResolvedDefinition(nil), plan.BundledDefinitions...), plan.BundledDefinitions[0])
	if _, err := workflowcompile.NewBundledDefinitionResolver(&exactDuplicate); err != nil {
		t.Fatalf("exact duplicate should deduplicate: %v", err)
	}
	conflicting := exactDuplicate
	conflicting.BundledDefinitions = append([]workflowcompile.ResolvedDefinition(nil), exactDuplicate.BundledDefinitions...)
	conflicting.BundledDefinitions[1].InputBindings = map[string]graph.Binding{
		"override": {Kind: graph.BindingLiteral, Literal: "changed"},
	}
	if _, err := workflowcompile.NewBundledDefinitionResolver(&conflicting); !errors.Is(err, workflowcompile.ErrBundledDefinitionConflict) {
		t.Fatalf("conflicting duplicate = %v", err)
	}
	resolver, err := workflowcompile.NewBundledDefinitionResolver(plan)
	if err != nil {
		t.Fatal(err)
	}
	requested := plan.Graph.Nodes[0].Call.Definition
	for name, mutate := range map[string]func(*graph.DefinitionRef){
		"digest":  func(ref *graph.DefinitionRef) { ref.Digest = "" },
		"locator": func(ref *graph.DefinitionRef) { ref.Locator += "-other" },
		"version": func(ref *graph.DefinitionRef) { ref.Version += "-other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := requested
			mutate(&changed)
			if _, err := resolver.ResolveDefinition(t.Context(), changed); !errors.Is(err, workflowcompile.ErrBundledDefinitionNotFound) {
				t.Fatalf("ResolveDefinition = %v", err)
			}
		})
	}
	changedProvenance := requested
	provenance := *requested.Provenance
	provenance.Origin = "changed"
	changedProvenance.Provenance = &provenance
	if _, err := resolver.ResolveDefinition(t.Context(), changedProvenance); !errors.Is(err, workflowcompile.ErrBundledDefinitionNotFound) {
		t.Fatalf("changed provenance = %v", err)
	}
	var nilContext context.Context
	if _, err := resolver.ResolveDefinition(nilContext, requested); !errors.Is(err, workflowcompile.ErrBundledDefinitionNotFound) {
		t.Fatalf("nil context = %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := resolver.ResolveDefinition(canceled, requested); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context = %v", err)
	}
}

func compileAgentSource(t *testing.T, locator, source string) *workflowcompile.ExecutionPlan {
	t.Helper()
	loaded := workflowcompile.LoadBytes(locator, []byte(source))
	if len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes = %#v", loaded.Diagnostics)
	}
	first := agent.Compile(loaded.Source)
	second := agent.Compile(loaded.Source)
	if first.Plan == nil || len(first.Diagnostics) != 0 || second.Plan == nil || !reflect.DeepEqual(first.Plan, second.Plan) {
		t.Fatalf("Compile = %#v / %#v", first, second)
	}
	return first.Plan
}

func nodeByID(t *testing.T, value graph.Graph, id string) graph.Node {
	t.Helper()
	for _, node := range value.Nodes {
		if node.ID == id {
			return node
		}
	}
	t.Fatalf("node %q missing from %#v", id, value.Nodes)
	return graph.Node{}
}

func hasEdge(edges []graph.Edge, from, to string, kind graph.EdgeKind) bool {
	for _, edge := range edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return true
		}
	}
	return false
}

func findOutput(outputs []graph.OutputSpec, name string) (graph.OutputSpec, bool) {
	for _, output := range outputs {
		if output.Name == name {
			return output, true
		}
	}
	return graph.OutputSpec{}, false
}

const waitAgentSource = `workflow:
  id: agent-sugar
  version: v1
inputs:
  - name: request-id
    type: string
    required: true
  - name: enabled
    type: boolean
    required: true
steps:
  - id: prepare
    transform:
      ready: true
  - id: review
    name: Review candidate
    needs: [prepare]
    ready_when: all_success
    if: inputs.enabled
    timeout:
      execution: 2h
      heartbeat: 2m
    with:
      request-id: inputs.request-id
    agent_launch:
      substrate: remote
      logical_agent_id: reviewer
      launch_id: review-session
      prompt_append: Review the candidate.
      parent_close: request_cancel
      wait: {timeout: 30m}
  - id: summarize
    transform:
      session: steps.review.outputs.payload.handle.session_id
outputs:
  status:
    type: string
    value: steps.review.outputs.payload.status
`

const fireAgentSource = `workflow: {id: agent-fire, version: v1}
steps:
  - id: review
    agent_launch:
      substrate: remote
      logical_agent_id: reviewer
      parent_close: abandon
      wait: false
outputs:
  child-run:
    type: string
    value: steps.review.outputs.run-id
`
