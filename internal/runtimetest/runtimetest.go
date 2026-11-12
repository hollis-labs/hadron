// Package runtimetest registers real workflows in a runtime's catalog, for tests
// that need an exposed workflow without driving the authoring flow by hand.
package runtimetest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hollis-labs/libs/workflow/engine/adapters/transform"
	"github.com/hollis-labs/libs/workflow/engine/authoring"
	"github.com/hollis-labs/libs/workflow/engine/graph"
	"github.com/hollis-labs/libs/workflow/engine/values"

	"github.com/hollis-labs/hadron/internal/appworkflow"
)

// Publish registers a one-step echo workflow team/<id>@<version> (input
// "message", output "result"), pins it and publishes it, and returns its exact
// registry reference. ctx must carry an identity that may manage the catalog.
func Publish(ctx context.Context, t *testing.T, lifecycle *appworkflow.WorkflowLifecycleService, id, version string) graph.DefinitionRef {
	t.Helper()
	detail := registerDraft(ctx, t, lifecycle, draft(t, id, version))
	ref := detail.Descriptor.Definition
	if _, err := lifecycle.PinRegistryVersion(ctx, appworkflow.MutateWorkflowVersionRequest{Definition: ref}); err != nil {
		t.Fatalf("pin %s: %v", ref.ID, err)
	}
	if _, err := lifecycle.PublishWorkflowVersion(ctx, appworkflow.MutateWorkflowVersionRequest{Definition: ref}); err != nil {
		t.Fatalf("publish %s: %v", ref.ID, err)
	}
	return ref
}

func draft(t *testing.T, id, version string) appworkflow.WorkflowDraft {
	t.Helper()
	binding := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}
	output := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.echo.outputs.result"}}
	envelope := authoring.New(id, version).
		Input(graph.InputSpec{Name: "message", Schema: graph.Schema{"type": "string"}, Required: true}).
		Node(graph.Node{
			ID: "echo", Kind: transform.Name, KindVersion: "v1", Config: graph.Config{"result": "inputs.message"},
			InputBindings: map[string]graph.Binding{"message": binding},
			Outputs:       []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}},
			Effects:       graph.EffectSet{graph.EffectCompute},
		}).
		Output(graph.OutputSpec{Name: "result", Schema: graph.Schema{"type": "string"}, Value: &output}).Envelope()
	envelope.Graph.Source.Format = graph.SourceAgent
	envelope.Graph.SourceMap.Graph.Format = graph.SourceAgent
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return appworkflow.WorkflowDraft{Envelope: encoded, ID: id, Version: version, Namespace: "team"}
}

func registerDraft(ctx context.Context, t *testing.T, lifecycle *appworkflow.WorkflowLifecycleService, draft appworkflow.WorkflowDraft) appworkflow.WorkflowVersionDetail {
	t.Helper()
	scaffold, err := lifecycle.GenerateWorkflowContract(ctx, appworkflow.GenerateWorkflowContractRequest{Draft: draft})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil || len(scaffold.Scaffold.Cases) == 0 {
		t.Fatalf("activation scaffold = %#v, %v", scaffold, err)
	}
	input, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-input", Reference: scaffold.Validation.Plan.ID, Output: "message"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	output, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-output", Reference: scaffold.Validation.Plan.ID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	caseOne := &scaffold.Scaffold.Cases[0]
	caseOne.Name, caseOne.Editable = "echo", false
	caseOne.Inputs, caseOne.ExpectedOutputs = values.ValueSet{"message": input}, values.ValueSet{"result": output}
	if len(caseOne.Mocks) != 1 {
		t.Fatalf("activation scaffold mocks = %#v", caseOne.Mocks)
	}
	caseOne.Mocks[0].ExpectedInputs = values.ValueSet{"message": input}
	caseOne.Mocks[0].ExpectedInputsEditable = false
	caseOne.Mocks[0].Results = []appworkflow.ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	registered, err := lifecycle.RegisterWorkflowDraft(ctx, appworkflow.RegisterWorkflowDraftRequest{Draft: draft, Suite: *scaffold.Scaffold, MakeCurrent: true})
	if err != nil || registered.Detail == nil {
		t.Fatalf("activation register = %#v, %v", registered, err)
	}
	return *registered.Detail
}
