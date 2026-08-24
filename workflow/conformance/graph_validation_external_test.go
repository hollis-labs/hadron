package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

type validationFixtureInput struct {
	Graph           graph.Graph `json:"graph"`
	RegisteredKinds []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"registered_kinds"`
}

func TestExternalGraphValidatorRunsConformanceFixtures(t *testing.T) {
	conformance.GraphValidationSuite(t, conformance.EmbeddedFixtures(), func() (conformance.Runner, error) {
		return conformance.RunnerFunc(func(ctx context.Context, fixture conformance.Fixture) error {
			var input validationFixtureInput
			if err := json.Unmarshal(fixture.Input, &input); err != nil {
				return fmt.Errorf("decode graph-validation fixture: %w", err)
			}
			registry := stepkind.NewRegistry()
			for _, registered := range input.RegisteredKinds {
				if err := registry.Register(stepkindtest.NewNoopKind(registered.Name, registered.Version)); err != nil {
					return fmt.Errorf("register fixture kind: %w", err)
				}
			}
			findings := workflowcompile.ValidateGraph(ctx, input.Graph, workflowcompile.ValidationOptions{StepKinds: registry})
			if len(findings) != 0 {
				return errors.New("graph validation produced structured diagnostics")
			}
			return nil
		}), nil
	})
}
