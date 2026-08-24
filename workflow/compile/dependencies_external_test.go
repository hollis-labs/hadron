package compile_test

import (
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestExternalValueDependencyAndBinderAPI(t *testing.T) {
	t.Parallel()

	source := &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "external.workflow.yaml", StartLine: 1}
	plan := &workflowcompile.ExecutionPlan{
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
		ID:            "external",
		Definition:    graph.DefinitionRef{Kind: "workflow", ID: "external", Version: "v1"},
		Graph: graph.Graph{
			ID: "external", Version: "v1", Source: source,
			Nodes: []graph.Node{
				{ID: "producer", Kind: "external", Source: source},
				{ID: "consumer", Kind: "external", Source: source, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
					Kind: "external-check", Config: graph.Config{"rule": "steps.producer.status"}, Source: source,
				}}}},
			},
		},
	}
	result := workflowcompile.InferValueDependencies(plan, workflowcompile.DependencyOptions{
		VerificationExtractors: map[string]workflowcompile.VerificationExpressionExtractor{
			"external-check": workflowcompile.VerificationExpressionExtractorFunc(func(check graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic) {
				return []graph.Expression{{Text: check.Config["rule"].(string), Source: check.Source}}, nil
			}),
		},
	})
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("external inference failed: %#v", result.Diagnostics)
	}
	context, options, err := result.Visibility.ScopeNodeContext(
		"consumer",
		values.ExpressionContext{Steps: map[string]values.StepContext{
			"producer":  {Status: "succeeded"},
			"unrelated": {Status: "succeeded"},
		}},
		values.ExpressionOptions{AllowEnv: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !options.AllowEnv || len(options.VisibleSteps) != 1 || options.VisibleSteps[0] != "producer" {
		t.Fatalf("external options = %#v", options)
	}
	if len(context.Steps) != 1 || context.Steps["producer"].Status != "succeeded" {
		t.Fatalf("external context steps = %#v", context.Steps)
	}
}
