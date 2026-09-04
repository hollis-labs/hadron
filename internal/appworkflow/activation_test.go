package appworkflow

import (
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
)

func TestMaterializeCanonicalActivationLocalsAndEvaluateBindings(t *testing.T) {
	loaded, loadErr := workflowcompile.LoadFile("testdata/activations.workflow.yaml")
	if loadErr != nil || loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadFile = %#v, %v", loaded, loadErr)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile = %#v", compiled)
	}
	identity := hoststate.IdentityBinding{
		Principal: "service:activation", SourceAuthority: "activation", Trust: "trusted",
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "activation-project"},
	}
	definition := compiled.Plan.Definition
	definition.Authority = "project"
	createdAt := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	registrations := make(map[string]hoststate.ActivationRegistration, len(compiled.Plan.Graph.Activations))
	for _, declaration := range compiled.Plan.Graph.Activations {
		registration, materializeErr := MaterializeActivationRegistration(ActivationMaterializationRequest{
			Declaration: declaration, Definition: definition, Identity: identity, ExposureRef: "exposure-" + declaration.ID,
			Enabled: true, CreatedAt: createdAt,
		})
		if materializeErr != nil {
			t.Fatalf("MaterializeActivationRegistration(%s) = %v", declaration.ID, materializeErr)
		}
		registrations[declaration.ID] = registration
	}

	webhook := registrations["incoming-hook"]
	contextValues, err := privateActivationContext(webhook, map[string]any{
		"body": map[string]any{"tasks": []any{"one", "two"}}, "message": map[string]any{"id": "request-42"},
		"project_id": "project-one", "schedule": map[string]any{"fire_id": "forged", "attempt": 99},
	}, "fire-one", createdAt)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := evaluateActivationInputs(webhook, contextValues, "fire-one")
	if err != nil || inputs["request_id"] != "request-request-42" {
		t.Fatalf("webhook inputs = %#v, %v", inputs, err)
	}
	tasks, ok := inputs["tasks"].([]any)
	if !ok || len(tasks) != 2 || tasks[0] != "one" {
		t.Fatalf("webhook tasks = %#v", inputs["tasks"])
	}
	schedule, ok := contextValues["schedule"].Inline.(map[string]any)
	if !ok || schedule["fire_id"] != "fire-one" || schedule["attempt"] != nil {
		t.Fatalf("host-owned schedule local = %#v", contextValues["schedule"].Inline)
	}

	message := registrations["agent-message"]
	messageContext, err := privateActivationContext(message, map[string]any{"message": map[string]any{"project_id": "project-two"}}, "fire-two", createdAt)
	if err != nil {
		t.Fatal(err)
	}
	messageInputs, err := evaluateActivationInputs(message, messageContext, "fire-two")
	if err != nil || messageInputs["project_id"] != "project-two" {
		t.Fatalf("message inputs = %#v, %v", messageInputs, err)
	}

	for _, forbidden := range []string{"steps.echo.outputs", "env.token", "run.id", "target.id"} {
		invalid, err := webhook.Clone()
		if err != nil {
			t.Fatal(err)
		}
		invalid.InputBindings["forbidden"] = graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: forbidden}}
		if err := invalid.Validate(); err == nil {
			t.Fatalf("forbidden activation expression %q passed", forbidden)
		}
	}
}
