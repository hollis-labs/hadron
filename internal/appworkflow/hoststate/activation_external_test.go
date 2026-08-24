package hoststate_test

import (
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestActivationRegistrationValidationAndDefensiveClone(t *testing.T) {
	registration := activationModelFixture()
	clone, err := registration.Clone()
	if err != nil || clone.Validate() != nil {
		t.Fatalf("Clone = %#v, %v", clone, err)
	}
	registration.InputBindings["payload"].Literal.(map[string]any)["nested"] = "mutated"
	registration.Provenance.Metadata["owner"] = "mutated"
	registration.Principal.Attributes["tenant"] = "mutated"
	if clone.InputBindings["payload"].Literal.(map[string]any)["nested"] != "original" ||
		clone.Provenance.Metadata["owner"] != "workflow" || clone.Principal.Attributes["tenant"] != "project-one" {
		t.Fatalf("clone retained caller aliases: %#v", clone)
	}

	for name, mutate := range map[string]func(*hoststate.ActivationRegistration){
		"secret principal": func(input *hoststate.ActivationRegistration) { input.Principal.ExposureRef = "bearer raw-secret" },
		"unsupported config": func(input *hoststate.ActivationRegistration) {
			input.Source.Config["headers"] = map[string]any{"authorization": "hidden"}
		},
		"ambient expression": func(input *hoststate.ActivationRegistration) {
			input.InputBindings["payload"] = graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.fetch.outputs"}}
		},
		"unsafe provenance": func(input *hoststate.ActivationRegistration) {
			input.Provenance.Metadata["api_key"] = "not-even-persisted"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, err := clone.Clone()
			if err != nil {
				t.Fatal(err)
			}
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("malformed activation registration passed")
			}
		})
	}
}

func activationModelFixture() hoststate.ActivationRegistration {
	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	return hoststate.ActivationRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: "model-fixture",
		Definition:    graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "definition", Version: "v1", Digest: values.SHA256Digest([]byte("definition"))},
		InputBindings: map[string]graph.Binding{"payload": {Kind: graph.BindingLiteral, Literal: map[string]any{"nested": "original"}}},
		Principal: hoststate.ActivationPrincipal{Principal: "service:activation", SourceAuthority: "activation", Trust: "trusted",
			Grants: []string{"workflow.run"}, ExposureRef: "route-one", Attributes: map[string]string{"tenant": "project-one"}},
		RunScope:   hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "project-one"},
		Source:     hoststate.ActivationSource{Kind: hoststate.ActivationSourceWebhook, Reference: "webhook-one", Config: graph.Config{"path": "/hooks/one"}},
		Authority:  hoststate.ActivationAuthorityProject,
		Provenance: graph.Provenance{Authority: "project", Origin: "workflow-source", Digest: values.SHA256Digest([]byte("source")), Metadata: graph.Metadata{"owner": "workflow"}},
		Policy: hoststate.ActivationPolicy{Overlap: graph.OverlapAllow, RunIDReuse: graph.RunIDReuseReject,
			Retry: hoststate.ActivationRetryPolicy{MaxAttempts: 3, Strategy: "constant", Initial: time.Second, Maximum: time.Minute}},
		Enabled: true, CreatedAt: base, UpdatedAt: base, Generation: 1,
	}
}
