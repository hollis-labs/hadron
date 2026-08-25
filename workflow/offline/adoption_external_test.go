package offline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/offline"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

const adoptionSource = `workflow:
  name: Public Host Example
  version: 1.0.0
inputs:
  - name: value
    type: integer
    required: true
steps:
  - id: first
    kind: public_copy
    kind_version: v1
    with:
      value:
        expression: inputs.value
    outputs:
      value:
        type: integer
  - id: second
    kind: public_copy
    kind_version: v1
    with:
      value:
        expression: steps.first.outputs.value
    outputs:
      value:
        type: integer
outputs:
  result:
    type: integer
    value:
      expression: steps.second.outputs.value
`

// TestMinimalExternalHostAdoption is compiled as an external package and uses
// only public module paths. It is also the executable companion to the
// downstream adoption guide.
func TestMinimalExternalHostAdoption(t *testing.T) {
	loaded := compile.LoadBytes("public-host.workflow.yaml", []byte(adoptionSource))
	if len(loaded.Diagnostics) != 0 || loaded.Source == nil {
		t.Fatalf("load source: %#v", loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 || compiled.Plan == nil {
		t.Fatalf("compile source: %#v", compiled.Diagnostics)
	}

	inferred := compile.InferValueDependencies(compiled.Plan, compile.DependencyOptions{})
	if len(inferred.Diagnostics) != 0 || inferred.Plan == nil {
		t.Fatalf("infer dependencies: %#v", inferred.Diagnostics)
	}
	if !hasDataEdge(inferred.Plan.Graph.Edges, "first", "second") {
		t.Fatalf("inferred edges = %#v", inferred.Plan.Graph.Edges)
	}

	registry := stepkind.NewRegistry()
	copyKind := stepkindtest.NewNoopKind("public_copy", "v1")
	copyKind.SpecValue.InputSchema = graph.Schema{"type": "object"}
	copyKind.SpecValue.OutputSchema = graph.Schema{"type": "object"}
	copyKind.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		input, ok := invocation.Invocation.Inputs["value"]
		if !ok {
			return stepkind.StepResult{}, errors.New("value input is missing")
		}
		output, err := values.NewInline(input.Inline, values.Metadata{
			Producer:  values.Producer{Kind: "example", Reference: invocation.Invocation.Identity.NodeID, Output: "value"},
			MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun,
		})
		if err != nil {
			return stepkind.StepResult{}, err
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"value": output}}, nil
	}
	if err := registry.Register(copyKind); err != nil {
		t.Fatal(err)
	}
	if findings := compile.ValidatePlan(t.Context(), inferred.Plan, compile.ValidationOptions{StepKinds: registry}); hasErrors(findings) {
		t.Fatalf("validate plan: %#v", findings)
	}

	built, err := offline.Build(t.Context(), inferred.Plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(built.Diagnostics) != 0 || built.Manifest == nil {
		t.Fatalf("build manifest: diagnostics=%#v err=%v", built.Diagnostics, err)
	}
	executed, err := offline.Execute(t.Context(), *built.Manifest, offline.ExecuteOptions{Registry: registry, Inputs: map[string]any{"value": 7}})
	if err != nil {
		t.Fatal(err)
	}
	if got := executed.Outputs["result"].Inline; got != json.Number("7") {
		t.Fatalf("result = %#v, want json.Number(7)", got)
	}
}

type offlineReversibleKind struct{ *stepkindtest.Kind }

func (*offlineReversibleKind) DescribeReversibility(context.Context, stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	return stepkind.ReversibilityEvidence{Operation: "fixture.offline.effect", ReceiptSchema: graph.Schema{}}, nil
}

func TestExternalOfflineExecutionDrivesCompensationBeforeReturningOriginalFailure(t *testing.T) {
	plan := customExternalPlan(t, "offline-effect")
	plan.Graph.Nodes = []graph.Node{
		{ID: "a-effect", Kind: "offline-effect", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}},
		{ID: "z-fail", Kind: "offline-fail", KindVersion: "v1", Needs: []graph.Need{{Node: "a-effect", Kind: graph.EdgeControl}}},
		{ID: "undo", Kind: "offline-undo", KindVersion: "v1"},
	}
	plan.Graph.Edges = []graph.Edge{{From: "a-effect", To: "z-fail", Kind: graph.EdgeControl}}
	plan.Graph.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}, Mode: graph.CompensationBestEffort}
	var err error
	plan.Graph.Digest, err = compile.GraphDigest(plan.Graph)
	if err != nil {
		t.Fatal(err)
	}
	plan.Digest, err = compile.PlanDigest(*plan)
	if err != nil {
		t.Fatal(err)
	}
	effect := &offlineReversibleKind{Kind: stepkindtest.NewNoopKind("offline-effect", "v1")}
	effect.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize}
	effect.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	effect.SpecValue.RetrySafety = stepkind.RetrySafe
	effect.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	effect.SpecValue.EmbeddedModeSupported = true
	effect.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Compensation: &stepkind.CompensationReceipt{Operation: "fixture.offline.effect", Values: values.ValueSet{}}}, nil
	}
	failing := stepkindtest.NewNoopKind("offline-fail", "v1")
	failing.SpecValue.Effects = graph.EffectSet{graph.EffectCompute}
	failing.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	failing.SpecValue.RetrySafety = stepkind.RetrySafe
	failing.SpecValue.EmbeddedModeSupported = true
	failing.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "fixture_failed", Message: "forward failure", Classification: stepkind.RetryPermanent}
	}
	undoCalls := 0
	undo := stepkindtest.NewNoopKind("offline-undo", "v1")
	undo.SpecValue.Effects = graph.EffectSet{graph.EffectMaterialize}
	undo.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	undo.SpecValue.RetrySafety = stepkind.RetrySafe
	undo.SpecValue.EmbeddedModeSupported = true
	undo.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		undoCalls++
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
	}
	registry := stepkind.NewRegistry()
	for _, kind := range []stepkind.StepKind{effect, failing, undo} {
		if err := registry.Register(kind); err != nil {
			t.Fatal(err)
		}
	}
	built, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(built.Diagnostics) != 0 || built.Manifest == nil {
		t.Fatalf("Build = %#v, %v", built.Diagnostics, err)
	}
	store := inmemory.NewStore()
	_, executeErr := offline.ExecuteWithStore(t.Context(), *built.Manifest, offline.ExecuteOptions{Registry: registry, RunID: "offline-compensation-run", IdempotencyKey: "offline-compensation-start"}, store)
	var runFailure *offline.RunFailureError
	if !errors.As(executeErr, &runFailure) || runFailure.Run.Status != workflowruntime.RunFailed || undoCalls != 1 {
		t.Fatalf("ExecuteWithStore failure=%#v undo_calls=%d", executeErr, undoCalls)
	}
	ledger, err := store.LoadCompensationLedger(t.Context(), runFailure.Run.ID)
	if err != nil || ledger.Status != workflowruntime.CompensationTerminal || ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || ledger.OriginalStatus != workflowruntime.RunFailed {
		t.Fatalf("ledger = %#v, %v", ledger, err)
	}
	if _, err := store.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: runFailure.Run.ID, NodeID: "undo"}); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("forward phase materialized dormant handler: %v", err)
	}
	entries, err := store.ListCompensationEntries(t.Context(), runFailure.Run.ID)
	if err != nil || len(entries) != 1 || entries[0].Handler.Iteration == "" || entries[0].Status != workflowruntime.CompensationSucceeded {
		t.Fatalf("entries = %#v, %v", entries, err)
	}
}

func customExternalPlan(t *testing.T, kind string) *compile.ExecutionPlan {
	t.Helper()
	loaded := compile.LoadBytes("offline-compensation.workflow.yaml", []byte("workflow: {name: Offline Compensation, version: 1.0.0}\nsteps:\n  - id: work\n    kind: "+kind+"\n    kind_version: v1\n    config: {}\n"))
	if len(loaded.Diagnostics) != 0 || loaded.Source == nil {
		t.Fatal(loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 || compiled.Plan == nil {
		t.Fatal(compiled.Diagnostics)
	}
	return compiled.Plan
}

// TestCompleteConformanceEntryPointIsCallableFromExternalPackage demonstrates
// downstream wiring. The expectation runner is intentionally a harness fake;
// a real host replaces each factory with an adapter that exercises its own
// implementation against the opaque fixture input.
func TestCompleteConformanceEntryPointIsCallableFromExternalPackage(t *testing.T) {
	conformance.RunComplete(t, conformance.EmbeddedFixtures(), adoptionConformanceHost{})
}

type adoptionConformanceHost struct{}

func (adoptionConformanceHost) CompilerFactory() conformance.Factory   { return expectationFactory }
func (adoptionConformanceHost) StateStoreFactory() conformance.Factory { return expectationFactory }
func (adoptionConformanceHost) SchedulerFactory() conformance.Factory  { return expectationFactory }
func (adoptionConformanceHost) WaitFactory() conformance.Factory       { return expectationFactory }
func (adoptionConformanceHost) StepKindRegistryFactory() conformance.Factory {
	return expectationFactory
}
func (adoptionConformanceHost) VerificationFactory() conformance.Factory { return expectationFactory }
func (adoptionConformanceHost) MemoizationFactory() conformance.Factory  { return expectationFactory }

func expectationFactory() (conformance.Runner, error) {
	return conformance.RunnerFunc(func(_ context.Context, fixture conformance.Fixture) error {
		if fixture.Expectation == conformance.ExpectFail {
			return errors.New("expected fixture rejection")
		}
		return nil
	}), nil
}

func hasDataEdge(edges []graph.Edge, from, to string) bool {
	for _, edge := range edges {
		if edge.From == from && edge.To == to && edge.Kind == graph.EdgeData {
			return true
		}
	}
	return false
}

func hasErrors(findings []diagnostic.Diagnostic) bool {
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			return true
		}
	}
	return false
}
