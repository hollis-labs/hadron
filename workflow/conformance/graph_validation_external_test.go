package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

type validationFixtureInput struct {
	Graph               graph.Graph `json:"graph"`
	AnalyzeDependencies bool        `json:"analyze_dependencies"`
	RegisteredKinds     []struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"registered_kinds"`
	ExpectedDataEdges  []string                                   `json:"expected_data_edges"`
	ExpectedDeferred   []workflowcompile.DeferredDependencyReason `json:"expected_deferred"`
	ExpectedVisibility map[string][]string                        `json:"expected_visibility"`
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
			value := input.Graph
			if input.AnalyzeDependencies {
				result := workflowcompile.InferValueDependencies(&workflowcompile.ExecutionPlan{
					SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
					ID:            value.ID,
					Definition:    graph.DefinitionRef{Kind: "workflow", ID: value.ID, Version: value.Version},
					Graph:         value,
					SourceMap:     value.SourceMap,
				}, workflowcompile.DependencyOptions{})
				if len(result.Diagnostics) != 0 {
					return errors.New("dependency inference produced structured diagnostics")
				}
				if result.Plan == nil {
					return errors.New("dependency inference returned no plan")
				}
				value = result.Plan.Graph
				dataEdges := make([]string, 0)
				for _, edge := range value.Edges {
					if edge.Kind == graph.EdgeData {
						dataEdges = append(dataEdges, workflowcompile.EdgeSourceKey(edge.From, edge.To, edge.Kind))
					}
				}
				sort.Strings(dataEdges)
				wantEdges := append([]string(nil), input.ExpectedDataEdges...)
				sort.Strings(wantEdges)
				if !slices.Equal(dataEdges, wantEdges) {
					return fmt.Errorf("data edges = %#v, want %#v", dataEdges, wantEdges)
				}
				deferred := make([]workflowcompile.DeferredDependencyReason, len(result.Deferred))
				for i := range result.Deferred {
					deferred[i] = result.Deferred[i].Reason
				}
				sort.Slice(deferred, func(i, j int) bool { return deferred[i] < deferred[j] })
				wantDeferred := append([]workflowcompile.DeferredDependencyReason(nil), input.ExpectedDeferred...)
				sort.Slice(wantDeferred, func(i, j int) bool { return wantDeferred[i] < wantDeferred[j] })
				if !slices.Equal(deferred, wantDeferred) {
					return fmt.Errorf("deferred reasons = %#v, want %#v", deferred, wantDeferred)
				}
				for nodeID, want := range input.ExpectedVisibility {
					got := result.Visibility.Nodes[nodeID].Producers
					if !reflect.DeepEqual(got, want) {
						return fmt.Errorf("visibility for %s = %#v, want %#v", nodeID, got, want)
					}
				}
			}
			findings := workflowcompile.ValidateGraph(ctx, value, workflowcompile.ValidationOptions{StepKinds: registry})
			if len(findings) != 0 {
				return errors.New("graph validation produced structured diagnostics")
			}
			return nil
		}), nil
	})
}
