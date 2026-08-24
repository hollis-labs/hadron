package compile_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestAdvancedMatrixLowersDeterministicallyToForEach(t *testing.T) {
	const source = `workflow: {id: matrix-demo, version: v1}
steps:
  - id: build
    kind: test
    matrix:
      dimensions:
        version: [1, 2]
        os: [linux, darwin]
      exclude:
        - {os: darwin, version: 1}
      include:
        - {os: windows, version: 2}
      fail_fast: true
      max_parallel: 2
`
	first := compileAdvanced(t, source)
	second := compileAdvanced(t, source)
	if first.Digest != second.Digest || first.Graph.Digest != second.Graph.Digest {
		t.Fatalf("matrix digests are unstable: %s/%s vs %s/%s", first.Digest, first.Graph.Digest, second.Digest, second.Graph.Digest)
	}
	got := first.Graph.Nodes[0].ForEach
	if got == nil || !got.FailFast || got.MaxConcurrency != 2 || got.ItemName != "matrix" || got.IndexName != "matrix-index" {
		t.Fatalf("matrix for_each = %#v", got)
	}
	want := `[{"os":"linux","version":1},{"os":"linux","version":2},{"os":"darwin","version":2},{"os":"windows","version":2}]`
	if got.Items.Text != want {
		t.Fatalf("matrix items = %s, want %s", got.Items.Text, want)
	}
	if got.Items.Source == nil || !reflect.DeepEqual(got.Items.Source.Path, []string{"steps", "0", "matrix"}) {
		t.Fatalf("matrix source = %#v", got.Items.Source)
	}
	registry := validationRegistry(t, stepkindtest.NewNoopKind("test", "v1"))
	if findings := workflowcompile.ValidateGraph(context.Background(), first.Graph, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		t.Fatalf("lowered matrix graph is invalid: %#v", findings)
	}
}

func TestAdvancedJoinAndSequentialGroupsLowerToControlEdges(t *testing.T) {
	const source = `workflow: {id: dependency-demo, version: v1}
steps:
  - {id: prepare, kind: test}
  - {id: build, kind: test}
  - {id: lint, kind: test}
  - id: publish
    kind: test
    join: [build, lint]
sequential_groups:
  - name: build-order
    nodes: [prepare, build]
`
	plan := compileAdvanced(t, source)
	want := []graph.Edge{
		{From: "build", To: "publish", Kind: graph.EdgeControl},
		{From: "lint", To: "publish", Kind: graph.EdgeControl},
		{From: "prepare", To: "build", Kind: graph.EdgeControl},
	}
	if len(plan.Graph.Edges) != len(want) {
		t.Fatalf("edges = %#v", plan.Graph.Edges)
	}
	for index, edge := range plan.Graph.Edges {
		if edge.From != want[index].From || edge.To != want[index].To || edge.Kind != want[index].Kind || edge.Source == nil {
			t.Fatalf("edge[%d] = %#v, want %#v with source", index, edge, want[index])
		}
		if _, ok := plan.SourceMap.Edges[workflowcompile.EdgeSourceKey(edge.From, edge.To, edge.Kind)]; !ok {
			t.Fatalf("edge source map missing for %#v", edge)
		}
	}
}

func TestOptionalNonBlockingCheckpointLowersToOrdinaryBindingsAndReadiness(t *testing.T) {
	const proceed = `workflow: {id: optional-gate, version: v1}
inputs:
  - {name: require-approval, type: boolean, required: true}
steps:
  - id: approval
    checkpoint:
      prompt: Release?
      options:
        - {id: approve, label: Approve}
        - {id: reject, label: Reject}
      decision_schema:
        type: object
        properties: {decision: {type: string, enum: [approve, reject]}}
        required: [decision]
        additionalProperties: false
      environment: production
      policy_subject: {kind: deployment, reference: production}
      correlation: release-approval
      timeout: 30m
      optional: true
      blocking: false
      trigger: inputs["require-approval"]
      not_triggered: proceed
      default_decision: approve
`
	plan := compileAdvanced(t, proceed)
	approval := plan.Graph.Nodes[0]
	binding, exists := approval.InputBindings["gate-trigger"]
	if !exists || binding.Kind != graph.BindingExpression || binding.Expression == nil || binding.Expression.Text != `inputs["require-approval"]` || binding.Source == nil || approval.If != nil {
		t.Fatalf("optional proceed lowering = %#v", approval)
	}
	if approval.Config["trigger_input"] != "gate-trigger" || approval.Config["trigger"] != nil || approval.Config["not_triggered"] != "proceed" || approval.Config["default_decision"] != "approve" {
		t.Fatalf("optional proceed config = %#v", approval.Config)
	}
	if got := approval.InputBindings["gate-trigger"].Source.Path; !reflect.DeepEqual(got, []string{"steps", "0", "checkpoint", "trigger"}) {
		t.Fatalf("trigger source = %#v", got)
	}

	const skip = `workflow: {id: optional-gate-skip, version: v1}
inputs:
  - {name: require-approval, type: boolean, required: true}
steps:
  - id: approval
    checkpoint:
      prompt: Release?
      options: [{id: approve, label: Approve}]
      decision_schema: {type: object}
      environment: production
      policy_subject: {kind: deployment, reference: production}
      correlation: release-approval
      timeout: 30m
      optional: true
      blocking: false
      trigger: inputs["require-approval"]
      not_triggered: skip
  - id: consumer
    kind: test
    with:
      approval: steps.approval.outputs.decision
`
	skipPlan := compileAdvanced(t, skip)
	if skipPlan.Graph.Nodes[0].If == nil || skipPlan.Graph.Nodes[0].If.Text != `inputs["require-approval"]` {
		t.Fatalf("optional skip readiness = %#v", skipPlan.Graph.Nodes[0].If)
	}
	inferred := workflowcompile.InferValueDependencies(skipPlan, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("optional output dependency inference = %#v", inferred.Diagnostics)
	}
	foundDeferred := false
	for _, dependency := range inferred.Deferred {
		if dependency.ProducerID == "approval" && dependency.Reason == workflowcompile.DeferredOptionalProducer {
			foundDeferred = true
		}
	}
	if !foundDeferred {
		t.Fatalf("optional producer was not deferred: %#v", inferred.Deferred)
	}
}

func TestOptionalNonBlockingCheckpointRejectsReservedBindingCollision(t *testing.T) {
	const source = `workflow: {id: optional-gate-invalid, version: v1}
steps:
  - id: approval
    with: {gate-trigger: {literal: true}}
    checkpoint:
      prompt: Release?
      options: [{id: approve, label: Approve}]
      decision_schema: {type: object}
      environment: production
      policy_subject: {kind: deployment, reference: production}
      timeout: 30m
      optional: true
      blocking: false
      trigger: inputs["require-approval"]
      not_triggered: proceed
      default_decision: approve
`
	loaded := workflowcompile.LoadBytes("optional-invalid.workflow.yaml", []byte(source))
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan != nil || len(result.Diagnostics) == 0 || !strings.Contains(result.Diagnostics[0].Message, "reserved binding gate-trigger") {
		t.Fatalf("reserved binding collision = %#v", result.Diagnostics)
	}
}

func TestServiceSourceLowersToDurableStartAndGlobalFinallyTeardown(t *testing.T) {
	const source = `workflow: {id: service-demo, version: v1}
steps:
  - id: database
    service:
      provider: fixture
      config: {image: postgres, port: 5432}
      heartbeat_timeout: 30s
      ready_check:
        - {type: output_schema}
  - id: consumer
    kind: test
    needs: [database]
`
	plan := compileAdvanced(t, source)
	if len(plan.Graph.Nodes) != 3 {
		t.Fatalf("service lowering nodes = %#v", plan.Graph.Nodes)
	}
	start, teardown := plan.Graph.Nodes[0], plan.Graph.Nodes[1]
	if start.ID != "database" || start.Kind != "service" || start.Service == nil || start.Service.HeartbeatTimeout != "30s" || len(start.Service.TeardownNodes) != 1 || start.Service.TeardownNodes[0] != "database-teardown" || start.Idempotency == nil || start.Idempotency.Mode != graph.IdempotencyKeyed {
		t.Fatalf("service start = %#v", start)
	}
	if teardown.ID != "database-teardown" || teardown.Finally == nil || teardown.Service == nil || teardown.Service.TeardownOf != "database" || teardown.Idempotency == nil || teardown.Idempotency.Mode != graph.IdempotencyKeyed {
		t.Fatalf("service teardown = %#v", teardown)
	}
	if sourceRef, ok := plan.SourceMap.Nodes[teardown.ID]; !ok || !reflect.DeepEqual(sourceRef.Path, []string{"steps", "0", "service"}) {
		t.Fatalf("service teardown source = %#v", sourceRef)
	}
	beforeMutation, err := json.Marshal(teardown.Config)
	if err != nil {
		t.Fatal(err)
	}
	start.Config["config"].(graph.Config)["port"] = 9999
	afterMutation, err := json.Marshal(teardown.Config)
	if err != nil || string(afterMutation) != string(beforeMutation) {
		t.Fatalf("generated teardown config aliases start config: before=%s after=%s err=%v", beforeMutation, afterMutation, err)
	}
	registry := validationRegistry(t, compileServiceKind{}, stepkindtest.NewNoopKind("test", "v1"))
	if findings := workflowcompile.ValidateGraph(context.Background(), plan.Graph, workflowcompile.ValidationOptions{StepKinds: registry, Verifiers: verification.NewDefaultRegistry()}); len(findings) != 0 {
		t.Fatalf("lowered service graph is invalid: %#v", findings)
	}
}

func TestServiceGeneratedTeardownIdentityCollisionIsSourceLocated(t *testing.T) {
	const source = `workflow: {id: service-collision, version: v1}
steps:
  - id: database
    service: {provider: fixture, config: {}}
  - id: database-teardown
    kind: test
`
	loaded := workflowcompile.LoadBytes("service-collision.workflow.yaml", []byte(source))
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan != nil || len(compiled.Diagnostics) == 0 {
		t.Fatalf("service teardown collision = %#v", compiled)
	}
	found := false
	for _, finding := range compiled.Diagnostics {
		if strings.Contains(finding.Message, "collides with a generated service teardown") && finding.Source != nil && reflect.DeepEqual(finding.Source.Path, []string{"steps", "1"}) {
			found = true
		}
	}
	if !found {
		t.Fatalf("source-located service collision diagnostic = %#v", compiled.Diagnostics)
	}
}

func TestAdvancedAuthoringRejectsAmbiguousAndUnboundedShapes(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "matrix empty", body: `steps: [{id: n, kind: test, matrix: {dimensions: {os: []}}}]`, want: "between 1 and 256"},
		{name: "matrix duplicate", body: `steps: [{id: n, kind: test, matrix: {dimensions: {os: [linux, linux]}}}]`, want: "duplicate value"},
		{name: "matrix incomplete include", body: `steps: [{id: n, kind: test, matrix: {dimensions: {os: [linux], arch: [arm]}, include: [{os: darwin}]}}]`, want: "declare every dimension"},
		{name: "matrix normalized duplicate", body: `steps: [{id: n, kind: test, matrix: {dimensions: {foo_bar: [one]}, exclude: [{foo_bar: one, foo-bar: one}]}}]`, want: "duplicate normalized dimension"},
		{name: "join and needs", body: `steps: [{id: a, kind: test}, {id: b, kind: test, needs: [a], join: [a]}]`, want: "join cannot be combined"},
		{name: "join unknown", body: `steps: [{id: a, kind: test, join: [missing]}]`, want: "unknown source node"},
		{name: "group ownership", body: "steps: [{id: a, kind: test}]\nsequential_groups: [{name: one, nodes: [a]}, {name: two, nodes: [a]}]", want: "already belongs"},
		{name: "empty groups", body: "steps: [{id: a, kind: test}]\nsequential_groups: []", want: "at least one group"},
		{name: "induced cycle", body: "steps: [{id: a, kind: test, join: [b]}, {id: b, kind: test}]\nsequential_groups: [{name: order, nodes: [a, b]}]", want: "introduces a graph cycle"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := "workflow: {id: invalid, version: v1}\n" + test.body + "\n"
			loaded := workflowcompile.LoadBytes("invalid.workflow.yaml", []byte(source))
			if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
				t.Fatalf("LoadBytes() = %#v", loaded.Diagnostics)
			}
			result := workflowcompile.Compile(loaded.Source)
			if result.Plan != nil || len(result.Diagnostics) == 0 {
				t.Fatalf("Compile() = plan %#v diagnostics %#v", result.Plan, result.Diagnostics)
			}
			messages := make([]string, len(result.Diagnostics))
			for index, diagnostic := range result.Diagnostics {
				messages[index] = diagnostic.Message
			}
			if !strings.Contains(strings.Join(messages, "\n"), test.want) {
				t.Fatalf("diagnostics = %q, want %q", messages, test.want)
			}
		})
	}
}

func compileAdvanced(t *testing.T, source string) *workflowcompile.ExecutionPlan {
	t.Helper()
	loaded := workflowcompile.LoadBytes("advanced.workflow.yaml", []byte(source))
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes() = %#v", loaded.Diagnostics)
	}
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile() = plan %#v diagnostics %#v", result.Plan, result.Diagnostics)
	}
	return result.Plan
}

type compileServiceKind struct{}

func (compileServiceKind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: "service", Version: "v1", ConfigSchema: graph.Schema{"type": "object"}, InputSchema: graph.Schema{"type": "object"}, OutputSchema: graph.Schema{"type": "object"},
		Effects: graph.EffectSet{graph.EffectMaterialize}, Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationExplicit}, Observation: stepkind.ObservationSpec{Mode: stepkind.ObservationPoll, Heartbeat: true}, Lifecycle: stepkind.LifecycleSpec{Service: true},
	}
}

func (compileServiceKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic {
	return nil
}
func (compileServiceKind) Execute(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{Kind: "fixture", ID: "service"}}, nil
}
func (compileServiceKind) ObserveService(context.Context, stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error) {
	return stepkind.ServiceObservation{State: stepkind.ServiceObservationStarting}, nil
}
func (compileServiceKind) RequestStop(context.Context, stepkind.ExternalOperationRef, string) error {
	return nil
}
