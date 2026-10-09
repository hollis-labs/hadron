package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/libs/workflow/adapters/call"
	workflowcompile "github.com/hollis-labs/libs/workflow/compile"
	"github.com/hollis-labs/libs/workflow/diagnostic"
	"github.com/hollis-labs/libs/workflow/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	"github.com/hollis-labs/libs/workflow/stepkind"
	"github.com/hollis-labs/libs/workflow/values"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
)

const productionCallParentSource = `workflow:
  id: call-parent
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: launch
    kind: call
    kind_version: v1
    call:
      definition:
        kind: file
        id: production-transform
        locator: production-transform.workflow.yaml
        version: v1
      mode: run
    with:
      message: inputs.message
    idempotency:
      mode: keyed
      scope: workflow
    outputs:
      run-id:
        type: string
      status:
        type: string
      events-ref:
        type: string
      cancellation:
        type: object
      outputs-ref:
        schema:
          type: [object, "null"]
  - id: collect
    kind_version: v1
    needs: [launch]
    wait_for:
      child_run:
        input: child
        fail_on_unsuccessful: true
      timeout: 1h
      payload_schema:
        type: object
    with:
      child: steps.launch.outputs["run-id"]
    outputs:
      payload:
        type: object
outputs:
  result:
    type: string
    value: steps.collect.outputs.payload.result
`

func productionLocalContext(t *testing.T) context.Context {
	t.Helper()
	ctx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestProductionCallRunDeliversChildOutputsToParent(t *testing.T) {
	runtime, cfg, store := newTestProductionWorkflowRuntime(t)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-parent.workflow.yaml"), []byte(productionCallParentSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-parent", Locator: "call-parent.workflow.yaml", Version: "v1"}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "call-parent-one", IdempotencyKey: "call-parent-one", Definition: definition,
		Inputs: map[string]any{"message": "from-parent"}, Confirmed: true, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		validated, validationErr := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
		t.Fatalf("RunWorkflow = %#v, %v; validation=%#v, %v", started, err, validated, validationErr)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var run workflowruntime.RunSnapshot
	for {
		run, err = state.LoadRun(t.Context(), started.Run.ID)
		if err == nil && run.Status.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			candidates, candidateErr := state.RecoverChildTerminalWaits(t.Context(), 10)
			t.Fatalf("parent run did not finish: %#v, %v; %s\nhealth=%#v\ncandidates=%#v %v", run, err, describeProductionRun(t, state, started.Run.ID), runtime.host.Health(), candidates, candidateErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != workflowruntime.RunSucceeded || run.Outputs == nil {
		inspected, inspectErr := runtime.operations.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{RunID: started.Run.ID})
		t.Fatalf("parent run = %#v; inspect=%#v, %v", run, inspected, inspectErr)
	}
	outputs, err := state.LoadValues(t.Context(), *run.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	if outputs["result"].Inline != "from-parent" {
		t.Fatalf("parent outputs = %#v", outputs)
	}
	children, err := state.ListChildRuns(t.Context(), started.Run.ID)
	if err != nil || len(children) != 1 {
		t.Fatalf("child runs = %#v, %v", children, err)
	}
	child, err := state.LoadRun(t.Context(), children[0].ChildRunID)
	if err != nil || child.Status != workflowruntime.RunSucceeded {
		t.Fatalf("child run = %#v, %v", child, err)
	}
}

func TestProductionCallPolicyRefusesInlineBeforeRun(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	inline := strings.Replace(productionCallParentSource, "id: call-parent", "id: call-inline", 1)
	inline = strings.Replace(inline, "mode: run", "mode: inline", 1)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-inline.workflow.yaml"), []byte(inline), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-inline", Locator: "call-inline.workflow.yaml", Version: "v1"}
	validated, err := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
	if err != nil || validated.Plan != nil || !diagnosticsMention(validated.Diagnostics, inlineCallRefusalMessage) {
		t.Fatalf("inline validation = %#v, %v", validated, err)
	}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "call-inline-one", IdempotencyKey: "call-inline-one", Definition: definition,
		Inputs: map[string]any{"message": "x"}, Confirmed: true, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err == nil || started.Run != nil || !errorMentions(err, inlineCallRefusalMessage) {
		t.Fatalf("inline RunWorkflow = %#v, %v", started, err)
	}
}

func TestRefusingInlineCallsIsPermanentStepFailure(t *testing.T) {
	_, err := refusingInlineCalls{}.ExecuteInline(t.Context(), calladapter.InlineRequest{})
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Classification != stepkind.RetryPermanent ||
		execution.Code != codeInlineCallUnsupported || !strings.Contains(execution.Message, "inline calls are not supported by hadrond") {
		t.Fatalf("inline refusal = %#v", err)
	}
	if stepkind.ClassifyError(err) != stepkind.RetryPermanent {
		t.Fatalf("inline refusal classification = %v", stepkind.ClassifyError(err))
	}
}

func TestProductionAgentLaunchIsRefusedWithGateMessage(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	sources := map[string]string{
		"agent-sugar": `workflow: {id: agent-sugar, version: v1}
steps:
  - id: review
    agent_launch:
      substrate: local
      logical_agent_id: reviewer
      wait: false
`,
		"agent-direct": `workflow: {id: agent-direct, version: v1}
steps:
  - id: session
    kind: agent_session
    kind_version: v1
    config:
      substrate: local
      launch_id: review
      logical_agent_id: reviewer
`,
	}
	for name, source := range sources {
		if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), name+".workflow.yaml"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	for name := range sources {
		t.Run(name, func(t *testing.T) {
			definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: name, Locator: name + ".workflow.yaml", Version: "v1"}
			validated, err := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
			if err != nil || validated.Plan != nil || !diagnosticsMention(validated.Diagnostics, agentSessionGateMessage) {
				t.Fatalf("validation = %#v, %v", validated, err)
			}
			for _, finding := range validated.Diagnostics {
				if finding.Code == "HADR-SOURCE-022" {
					t.Fatalf("gate surfaced as a generic unknown-kind finding: %#v", finding)
				}
			}
			started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
				RunID: workflowruntime.RunID(name + "-run"), IdempotencyKey: name + "-run", Definition: definition, Confirmed: true, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
			})
			if err == nil || started.Run != nil || !errorMentions(err, agentSessionGateMessage) {
				t.Fatalf("RunWorkflow = %#v, %v", started, err)
			}
		})
	}
	if _, ok := runtime.host.Registry().Lookup("agent_session", "v1"); ok {
		t.Fatal("agent_session@v1 is registered with the production Host while gated")
	}
}

func TestLocalWorkflowIdentityGrantsCallAndAgentSessionCapabilities(t *testing.T) {
	local := localWorkflowIdentity().ExecutionTarget.Capabilities
	have := make(map[string]bool, len(local))
	for _, capability := range local {
		have[capability] = true
	}
	for _, want := range []string{"workflow.call", "agent.session.launch", "agent.session.observe", "agent.session.cancel"} {
		if !have[want] {
			t.Fatalf("local execution target capabilities %v lack %q", local, want)
		}
	}
	// The MCP principal keeps its pre-CW-0227 set exactly, so tokens
	// bootstrapped before call@v1 still match their stored principal and no
	// stored grant is widened silently.
	if mcp := workflowMCPPrincipal().Identity.ExecutionTarget.Capabilities; !slices.Equal(mcp, []string{"gate.respond", "message.receive", "wait.resume"}) {
		t.Fatalf("mcp execution target capabilities = %v; want the pre-CW-0227 set", mcp)
	}
	if local := localWorkflowIdentity().ExecutionTarget.Capabilities; slices.Contains(workflowMCPPrincipal().Identity.ExecutionTarget.Capabilities, "workflow.call") || len(local) == 3 {
		t.Fatal("the MCP freeze leaked into the local identity or the MCP principal gained workflow.call")
	}
	call, err := calladapter.New(calladapter.Options{
		Resolver: &productionCallResolver{}, State: callStoreStub{}, Context: &workflowCallContext{}, Inline: refusingInlineCalls{}, Runs: callStoreStub{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := call.Spec().RequiredCapabilities; len(got) != 1 || got[0] != capabilityWorkflowCall {
		t.Fatalf("call@v1 capabilities = %v, want [%s]", got, capabilityWorkflowCall)
	}
	gated, err := newGatedAgentSessionKind()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{capabilityAgentSessionLaunch: true, capabilityAgentSessionObserve: true, capabilityAgentSessionCancel: true}
	for _, capability := range gated.Spec().RequiredCapabilities {
		if !want[capability] {
			t.Fatalf("agent_session@v1 requires unexpected capability %q", capability)
		}
		delete(want, capability)
	}
	if len(want) != 0 {
		t.Fatalf("agent_session@v1 capability constants drifted: missing %v", want)
	}
}

func TestProductionRuntimeRetainsSubstrateSettingsCopy(t *testing.T) {
	sett := testWorkflowSettings()
	sett.AgentSubstrates["local"] = settingsAgentSubstrate([]string{"--flag"})
	runtime, _, _ := newTestProductionWorkflowRuntimeWithSettings(t, sett)
	sett.AgentSubstrates["local"].Args[0] = "--mutated"
	if got := runtime.substrates.AgentSubstrates["local"].Args; len(got) != 1 || got[0] != "--flag" {
		t.Fatalf("retained agent substrate args = %v", got)
	}
	if runtime.substrates.MCPServers == nil || runtime.substrates.MessageSubstrates == nil {
		t.Fatalf("retained substrate maps = %#v", runtime.substrates)
	}
}

func diagnosticsMention(findings []diagnostic.Diagnostic, text string) bool {
	for _, finding := range findings {
		if strings.Contains(finding.Message, text) {
			return true
		}
	}
	return false
}

func errorMentions(err error, text string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), text) {
		return true
	}
	var definitionErr *appworkflow.DefinitionDiagnosticError
	return errors.As(err, &definitionErr) && diagnosticsMention(definitionErr.Findings, text)
}

type callStoreStub struct{}

func (callStoreStub) RecordCallResolution(context.Context, calladapter.RecordResolutionRequest) (calladapter.ResolutionRecord, calladapter.ResolutionOutcome, error) {
	return calladapter.ResolutionRecord{}, "", errors.New("unused")
}

func (callStoreStub) StartChildRun(context.Context, calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	return calladapter.ChildRunResult{}, errors.New("unused")
}

func settingsAgentSubstrate(args []string) settings.AgentSubstrateSettings {
	return settings.AgentSubstrateSettings{Kind: "local", Args: args}
}

func describeProductionRun(t *testing.T, state *persistence.WorkflowStateStore, runID workflowruntime.RunID) string {
	t.Helper()
	var report strings.Builder
	for _, nodeID := range []string{"launch", "collect", "echo", "first", "second", "caller"} {
		node, err := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID})
		if err != nil {
			continue
		}
		fmt.Fprintf(&report, "\n%s/%s status=%s latest=%d", runID, nodeID, node.Status, node.LatestAttempt)
		if node.LatestAttempt > 0 {
			attempt, attemptErr := state.LoadAttempt(t.Context(), workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
			if attemptErr == nil && attempt.Failure != nil {
				fmt.Fprintf(&report, " failure=%#v", *attempt.Failure)
			}
		}
	}
	children, _ := state.ListChildRuns(t.Context(), runID)
	for _, child := range children {
		run, _ := state.LoadRun(t.Context(), child.ChildRunID)
		fmt.Fprintf(&report, "\nchild %s status=%s", child.ChildRunID, run.Status)
		report.WriteString(describeProductionRun(t, state, child.ChildRunID))
	}
	return report.String()
}

// productionPolicyRefusedChildSource needs message.receive (message_wait),
// which the restricted test identity's execution target does not grant.
const productionPolicyRefusedChildSource = `workflow:
  id: policy-refused-child
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: listen
    kind_version: v1
    message_wait:
      substrate: tether
      to: mailbox://agent/replies
      correlation: corr-1
      timeout: 1h
`

func TestProductionCallChildIsGatedByStartPolicy(t *testing.T) {
	runtime, cfg, store := newTestProductionWorkflowRuntime(t)
	root := workflowSourceRoot(cfg)
	parent := strings.Replace(productionCallParentSource, "id: call-parent", "id: call-refused-parent", 1)
	parent = strings.Replace(parent, "id: production-transform\n        locator: production-transform.workflow.yaml", "id: policy-refused-child\n        locator: policy-refused-child.workflow.yaml", 1)
	for name, source := range map[string]string{"policy-refused-child": productionPolicyRefusedChildSource, "call-refused-parent": parent} {
		if err := os.WriteFile(filepath.Join(root, name+".workflow.yaml"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	restricted := localWorkflowIdentity()
	var kept []string
	for _, capability := range restricted.ExecutionTarget.Capabilities {
		if capability != "message.receive" {
			kept = append(kept, capability)
		}
	}
	restricted.ExecutionTarget.Capabilities = kept
	ctx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), restricted)
	if err != nil {
		t.Fatal(err)
	}
	identity := appworkflow.IdentityRequest{SourceAuthority: "http"}

	// The same definition is refused when started directly.
	childDefinition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "policy-refused-child", Locator: "policy-refused-child.workflow.yaml", Version: "v1"}
	direct, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "policy-refused-direct", IdempotencyKey: "policy-refused-direct", Definition: childDefinition,
		Inputs: map[string]any{"message": "x"}, Confirmed: true, Identity: identity,
	})
	if !errors.Is(err, appworkflow.ErrExecutionTarget) || direct.Run != nil {
		validated, validationErr := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: childDefinition})
		t.Fatalf("direct start of policy-refused child = %v; validation=%#v, %v", err, validated.Diagnostics, validationErr)
	}

	parentDefinition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-refused-parent", Locator: "call-refused-parent.workflow.yaml", Version: "v1"}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "call-refused-parent-one", IdempotencyKey: "call-refused-parent-one", Definition: parentDefinition,
		Inputs: map[string]any{"message": "x"}, Confirmed: true, Identity: identity,
	})
	if err != nil || started.Run == nil {
		t.Fatalf("allowed parent start = %#v, %v", started, err)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	var run workflowruntime.RunSnapshot
	for {
		run, err = state.LoadRun(t.Context(), started.Run.ID)
		if err == nil && run.Status.Terminal() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent run did not finish: %#v, %v; %s", run, err, describeProductionRun(t, state, started.Run.ID))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if run.Status != workflowruntime.RunFailed {
		t.Fatalf("parent run status = %s; %s", run.Status, describeProductionRun(t, state, started.Run.ID))
	}
	launch, err := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: started.Run.ID, NodeID: "launch"})
	if err != nil || launch.Status != workflowruntime.NodeFailed || launch.LatestAttempt < 1 {
		t.Fatalf("launch node = %#v, %v", launch, err)
	}
	attempt, err := state.LoadAttempt(t.Context(), workflowruntime.AttemptID{Invocation: launch.ID, Number: launch.LatestAttempt})
	if err != nil || attempt.Failure == nil || attempt.Failure.Code != calladapter.CodeChildRunFailed || attempt.Failure.Retryable {
		t.Fatalf("launch failure = %#v, %v", attempt.Failure, err)
	}
	if children, listErr := state.ListChildRuns(t.Context(), started.Run.ID); listErr != nil || len(children) != 0 {
		t.Fatalf("policy-refused call created child runs: %#v, %v", children, listErr)
	}

	// call@v1 records only its generic child-start failure; the Host gate is
	// the cause. Drive the gate directly with the same pinned child.
	loaded := workflowcompile.LoadBytes("policy-refused-child.workflow.yaml", []byte(productionPolicyRefusedChildSource))
	compiled := workflowcompile.CompileWithOptions(loaded.Source, workflowcompile.CompileOptions{})
	if len(loaded.Diagnostics) != 0 || compiled.Plan == nil {
		t.Fatalf("compile child = %#v / %#v", loaded.Diagnostics, compiled.Diagnostics)
	}
	childDefinitionRef := compiled.Plan.Definition
	childDefinitionRef.Digest = compiled.Plan.Graph.Digest
	_, gateErr := runtime.host.StartChildRun(t.Context(), calladapter.ChildRunRequest{
		Parent:     calladapter.CallSiteIdentity{RunID: string(started.Run.ID), NodeID: "launch"},
		ChildRunID: "policy-refused-direct-child", Inputs: values.ValueSet{},
		Definition: workflowcompile.ResolvedDefinition{Definition: childDefinitionRef, Graph: compiled.Plan.Graph},
		Plan: workflowruntime.PlanRef{
			ID: compiled.Plan.ID, Version: compiled.Plan.Graph.Version, Digest: compiled.Plan.Digest, SchemaVersion: compiled.Plan.SchemaVersion,
		},
	})
	var execution *stepkind.ExecutionError
	if !errors.As(gateErr, &execution) || execution.Code != appworkflow.CodeChildRunPolicyDenied ||
		execution.Classification != stepkind.RetryPermanent || !errors.Is(gateErr, appworkflow.ErrExecutionTarget) {
		t.Fatalf("host child policy gate = %v", gateErr)
	}
}

const productionVisibilitySource = `workflow:
  id: call-visibility
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: first
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    outputs:
      result:
        type: string
    effects: [compute]
  - id: second
    kind_version: v1
    transform:
      result: '"hidden"'
    outputs:
      result:
        type: string
    effects: [compute]
  - id: caller
    kind_version: v1
    transform:
      result: inputs.value
    with:
      value: steps.first.outputs.result
    outputs:
      result:
        type: string
    effects: [compute]
outputs:
  result:
    type: string
    value: steps.caller.outputs.result
`

func TestCallExpressionContextProviderScopesToNodeVisibility(t *testing.T) {
	runtime, cfg, store := newTestProductionWorkflowRuntime(t)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-visibility.workflow.yaml"), []byte(productionVisibilitySource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "call-visibility-one", IdempotencyKey: "call-visibility-one",
		Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-visibility", Locator: "call-visibility.workflow.yaml", Version: "v1"},
		Inputs:     map[string]any{"message": "visible"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		t.Fatalf("RunWorkflow = %#v, %v", started, err)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := persistence.NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		run, loadErr := state.LoadRun(t.Context(), started.Run.ID)
		if loadErr == nil && run.Status == workflowruntime.RunSucceeded {
			break
		}
		if time.Now().After(deadline) || (loadErr == nil && run.Status.Terminal()) {
			t.Fatalf("visibility run = %#v, %v; %s", run, loadErr, describeProductionRun(t, state, started.Run.ID))
		}
		time.Sleep(25 * time.Millisecond)
	}
	provider, err := appworkflow.NewCallExpressionContextProvider(state, state, appworkflow.PinnedRecoveryPlanSource{
		Roots: journal, Children: journal, State: state, Replays: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	invocation := stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: string(started.Run.ID), NodeID: "caller", Attempt: 1}}
	scoped, options, err := provider.ExpressionContext(t.Context(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	if scoped.Inputs["message"].Inline != "visible" {
		t.Fatalf("scoped inputs = %#v", scoped.Inputs)
	}
	if _, ok := scoped.Steps["first"]; !ok || len(scoped.Steps) != 1 || scoped.Steps["first"].Outputs["result"].Inline != "visible" {
		t.Fatalf("scoped steps = %#v", scoped.Steps)
	}
	if options.AllowEnv || len(options.VisibleSteps) != 1 || options.VisibleSteps[0] != "first" {
		t.Fatalf("expression options = %#v", options)
	}
	if scoped.Item != nil || scoped.Index != nil {
		t.Fatalf("non-fan-out invocation received item roots: %#v", scoped)
	}

	invocation.Identity.Iteration = "0"
	if _, _, err := provider.ExpressionContext(t.Context(), invocation); err == nil {
		t.Fatal("fan-out iteration without durable expansion was accepted")
	}
	invocation.Identity = stepkind.InvocationIdentity{RunID: "missing-run", NodeID: "caller", Attempt: 1}
	if _, _, err := provider.ExpressionContext(t.Context(), invocation); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("missing run error = %v", err)
	}
	invocation.Identity = stepkind.InvocationIdentity{RunID: string(started.Run.ID), NodeID: "absent", Attempt: 1}
	if _, _, err := provider.ExpressionContext(t.Context(), invocation); !errors.Is(err, appworkflow.ErrCallContextUnavailable) {
		t.Fatalf("absent node error = %v", err)
	}
	if _, err := appworkflow.NewCallExpressionContextProvider(nil, state, appworkflow.PinnedRecoveryPlanSource{}); !errors.Is(err, appworkflow.ErrCallContextUnavailable) {
		t.Fatalf("nil state error = %v", err)
	}
}
