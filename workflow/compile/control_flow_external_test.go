package compile_test

import (
	"context"
	"slices"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestCompileLowersContinueOnErrorAndStatusAwareFinally(t *testing.T) {
	plan := compileBytes(t, "control.workflow.yaml", []byte(`workflow:
  id: control
steps:
  - id: risky
    kind: noop
    continue_on_error: true
  - id: handler
    kind: noop
finally:
  - id: cleanup
    kind: noop
    if: run.status == "failed"
`))
	risky, cleanup := plan.Graph.Nodes[0], plan.Graph.Nodes[2]
	if len(risky.Catch) != 1 || !risky.Catch[0].ContinueOnError() || risky.Catch[0].Source == nil || !slices.Equal(risky.Catch[0].Source.Path, []string{"steps", "0", "continue_on_error"}) {
		t.Fatalf("continue_on_error lowering = %#v", risky.Catch)
	}
	if cleanup.Finally == nil || cleanup.If == nil || cleanup.If.Text != `run.status == "failed"` {
		t.Fatalf("status-aware finally lowering = %#v", cleanup)
	}
}

func TestValidateControlFlowAliasesTargetsAndFinallyShape(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	base := graph.Graph{ID: "control", Version: "v1", Nodes: []graph.Node{
		{ID: "origin", Kind: "noop", KindVersion: "v1", Catch: []graph.CatchRule{{Targets: []string{"handler"}, BindAs: "create_error"}}},
		{ID: "handler", Kind: "noop", KindVersion: "v1"},
		{ID: "cleanup", Kind: "noop", KindVersion: "v1", Finally: &graph.FinallySpec{Scope: []string{"origin", "handler"}}, If: &graph.Expression{Text: `run.status == "failed"`}},
	}}
	if diagnostics := workflowcompile.ValidateGraph(context.Background(), base, workflowcompile.ValidationOptions{StepKinds: registry}); len(diagnostics) != 0 {
		t.Fatalf("valid control graph diagnostics = %#v", diagnostics)
	}
	for name, alias := range map[string]string{"reserved": "inputs", "hyphen": "create-error", "upper": "Create_error"} {
		t.Run(name, func(t *testing.T) {
			invalid := base
			invalid.Nodes = append([]graph.Node(nil), base.Nodes...)
			invalid.Nodes[0].Catch = []graph.CatchRule{{Targets: []string{"handler"}, BindAs: alias}}
			if diagnostics := workflowcompile.ValidateGraph(context.Background(), invalid, workflowcompile.ValidationOptions{StepKinds: registry}); !containsControlDiagnostic(diagnostics, workflowcompile.CodeInvalidCatch) {
				t.Fatalf("alias %q diagnostics = %#v", alias, diagnostics)
			}
		})
	}
	invalidFinally := base
	invalidFinally.Nodes = append([]graph.Node(nil), base.Nodes...)
	invalidFinally.Nodes[2].ForEach = &graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}}
	if diagnostics := workflowcompile.ValidateGraph(context.Background(), invalidFinally, workflowcompile.ValidationOptions{StepKinds: registry}); !containsControlDiagnostic(diagnostics, workflowcompile.CodeInvalidFinally) {
		t.Fatalf("fan-out finally diagnostics = %#v", diagnostics)
	}
	duplicateBinding := base
	duplicateBinding.Nodes = append([]graph.Node(nil), base.Nodes...)
	duplicateBinding.Nodes[0].Catch = []graph.CatchRule{
		{Errors: []string{"first"}, Targets: []string{"handler"}, BindAs: "create_error"},
		{Errors: []string{"second"}, Targets: []string{"handler"}, BindAs: "create_error"},
	}
	if diagnostics := workflowcompile.ValidateGraph(context.Background(), duplicateBinding, workflowcompile.ValidationOptions{StepKinds: registry}); !containsControlDiagnostic(diagnostics, workflowcompile.CodeInvalidCatch) {
		t.Fatalf("duplicate binding diagnostics = %#v", diagnostics)
	}
	unreachable := base
	unreachable.Nodes = append([]graph.Node(nil), base.Nodes...)
	unreachable.Nodes[0].Catch = []graph.CatchRule{
		{Targets: []string{"handler"}},
		{Errors: []string{"narrow"}, Targets: []string{"handler"}},
	}
	if diagnostics := workflowcompile.ValidateGraph(context.Background(), unreachable, workflowcompile.ValidationOptions{StepKinds: registry}); !containsControlDiagnostic(diagnostics, workflowcompile.CodeInvalidCatch) {
		t.Fatalf("unreachable catch diagnostics = %#v", diagnostics)
	}
	cyclicFinally := graph.Graph{ID: "cyclic-finally", Version: "v1", Nodes: []graph.Node{
		{ID: "work", Kind: "noop", KindVersion: "v1"},
		{ID: "cleanup-global", Kind: "noop", KindVersion: "v1", Finally: &graph.FinallySpec{}},
		{ID: "cleanup-explicit", Kind: "noop", KindVersion: "v1", Finally: &graph.FinallySpec{Scope: []string{"cleanup-global"}}},
	}}
	if diagnostics := workflowcompile.ValidateGraph(context.Background(), cyclicFinally, workflowcompile.ValidationOptions{StepKinds: registry}); !containsControlDiagnostic(diagnostics, workflowcompile.CodeGraphCycle) {
		t.Fatalf("cyclic finally diagnostics = %#v", diagnostics)
	}
}

func TestValidateRejectsCleanupRoutingAndDoesNotDuplicateSwitchDiagnostics(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	invalid := graph.Graph{ID: "cleanup-routes", Version: "v1", Nodes: []graph.Node{
		{ID: "source", Kind: "noop", KindVersion: "v1", Catch: []graph.CatchRule{{Targets: []string{"cleanup"}}}, Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{}, Targets: []string{"cleanup"}}}, Default: []string{"cleanup"}}},
		{ID: "cleanup", Kind: "noop", KindVersion: "v1", Finally: &graph.FinallySpec{}, Catch: []graph.CatchRule{{Errors: []string{graph.CatchAllErrors}}}},
	}}
	findings := workflowcompile.ValidateGraph(context.Background(), invalid, workflowcompile.ValidationOptions{StepKinds: registry})
	counts := map[diagnostic.Code]int{}
	for _, finding := range findings {
		counts[finding.Code]++
	}
	if counts[workflowcompile.CodeInvalidFinally] != 4 {
		t.Fatalf("cleanup-routing diagnostics = %#v, want four finalizer findings", findings)
	}
	if counts[workflowcompile.CodeInvalidSwitch] != 1 {
		t.Fatalf("switch diagnostics = %#v, want one empty-predicate finding", findings)
	}
}

func containsControlDiagnostic(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
