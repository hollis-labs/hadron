package checkpoint_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	checkpointadapter "github.com/hollis-labs/hadron/workflow/adapters/checkpoint"
	"github.com/hollis-labs/hadron/workflow/adapters/emit"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestCompilerLowersAndValidatesEmitCheckpointSource(t *testing.T) {
	const source = `workflow:
  name: Emit Checkpoint Contract
  version: v1
inputs:
  - name: Payload
    type: object
    required: true
  - name: Correlation
    type: string
    required: true
steps:
  - name: Publish Event
    with:
      payload:
        expression: inputs.payload
    idempotency:
      mode: keyed
      key: inputs.correlation
    emit:
      destination:
        kind: event-bus
        reference: project/releases
        attributes: {tenant: project-a}
      event_type: release.ready
      payload_input: payload
      correlation: release-42
  - name: Release Approval
    needs: [Publish Event]
    checkpoint:
      prompt: Release version?
      options:
        - {id: approve, label: Approve}
        - {id: reject, label: Reject}
      decision_schema:
        type: object
        additionalProperties: false
        required: [decision]
        properties:
          decision: {type: string, enum: [approve, reject]}
      environment: production
      correlation: release-42
      timeout: 24h
      escalations:
        - after: 1h
          subject: {kind: notification, reference: release-team}
`
	loaded := workflowcompile.LoadBytes("emit-checkpoint.workflow.yaml", []byte(source))
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes = %#v", loaded.Diagnostics)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 || len(compiled.Plan.Graph.Nodes) != 2 {
		t.Fatalf("Compile = %#v", compiled)
	}
	publish, approval := compiled.Plan.Graph.Nodes[0], compiled.Plan.Graph.Nodes[1]
	if publish.Kind != emit.KindName || approval.Kind != checkpointadapter.KindName || approval.Config["decision_schema"] == nil ||
		publish.Source == nil || approval.Source == nil || approval.Source.StartLine != 27 {
		t.Fatalf("lowered emit/checkpoint nodes = %#v / %#v", publish, approval)
	}

	registry := stepkind.NewRegistry()
	if _, err := emit.Register(registry, emit.Options{
		Policy: emit.PolicyFunc(func(context.Context, emit.AuthorizationRequest) error { return nil }),
		Publisher: emit.PublisherFunc(func(_ context.Context, envelope emit.Envelope) (emit.PublicationResult, error) {
			return emit.PublicationResult{EnvelopeID: envelope.ID, PublicationID: "compile-fixture", Outcome: emit.PublicationApplied, PublishedAt: checkpointTime}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointadapter.Register(registry, checkpointOptions(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if diagnostics := workflowcompile.ValidatePlan(t.Context(), compiled.Plan, workflowcompile.ValidationOptions{StepKinds: registry}); len(diagnostics) != 0 {
		t.Fatalf("ValidatePlan = %#v", diagnostics)
	}
	ref, err := workflowwait.NewSchemaRef(graph.Schema(approval.Config["decision_schema"].(map[string]any)))
	if err != nil || !reflect.DeepEqual(ref.Schema, graph.Schema(approval.Config["decision_schema"].(map[string]any))) {
		t.Fatalf("checkpoint config schema = %#v, %v", ref, err)
	}
}

func TestEmitCheckpointConformanceFixturesMatchRegisteredContracts(t *testing.T) {
	fixtures, err := conformance.EmbeddedFixtures().Fixtures(conformance.ExecutorMetadataFixtures)
	if err != nil {
		t.Fatal(err)
	}
	var nilEmit *emit.Kind
	checkpointExecutor, err := checkpointadapter.New(checkpointOptions(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]stepkind.StepKindSpec{
		"emit-v1-contract":       nilEmit.Spec(),
		"checkpoint-v1-contract": checkpointExecutor.Spec(),
	}
	found := map[string]bool{}
	for _, fixture := range fixtures {
		expected, ok := want[fixture.Name]
		if !ok {
			continue
		}
		var document struct {
			Spec stepkind.StepKindSpec `json:"spec"`
		}
		if decodeErr := json.Unmarshal(fixture.Input, &document); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if document.Spec.Name != expected.Name || document.Spec.Version != expected.Version ||
			!reflect.DeepEqual(document.Spec.Effects, expected.Effects) || !reflect.DeepEqual(document.Spec.RequiredCapabilities, expected.RequiredCapabilities) ||
			document.Spec.Idempotency != expected.Idempotency || document.Spec.RetrySafety != expected.RetrySafety ||
			document.Spec.CanSuspend != expected.CanSuspend || document.Spec.Memoization != expected.Memoization {
			t.Fatalf("fixture %s drifted from adapter: %#v / %#v", fixture.Name, document.Spec, expected)
		}
		found[fixture.Name] = true
	}
	if len(found) != len(want) {
		t.Fatalf("missing adapter conformance fixtures: %#v", found)
	}

	waits, err := conformance.EmbeddedFixtures().Fixtures(conformance.WaitFixtures)
	if err != nil {
		t.Fatal(err)
	}
	foundCheckpointWait := false
	for _, fixture := range waits {
		foundCheckpointWait = foundCheckpointWait || fixture.Name == "wait-checkpoint-gate"
	}
	if !foundCheckpointWait {
		t.Fatal("checkpoint wait conformance fixture is missing")
	}
}
