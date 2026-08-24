package appworkflow

import (
	"testing"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
)

func TestChildRecoveryPlanBindsExactSemanticGraphDigest(t *testing.T) {
	workflow := graph.Graph{ID: "child", Version: "v1", Nodes: []graph.Node{{ID: "work", Kind: "safe", KindVersion: "v1"}}}
	digest, err := compile.GraphDigest(workflow)
	if err != nil {
		t.Fatal(err)
	}
	workflow.Digest = digest
	ref := runtime.PlanRef{ID: workflow.ID, Version: workflow.Version, Digest: digest, SchemaVersion: "workflow.execution-plan/v1"}
	plan := compile.ExecutionPlan{SchemaVersion: ref.SchemaVersion, ID: ref.ID, Digest: ref.Digest, Graph: workflow}
	result, err := recoveryPlan(plan, ref, compile.DependencyOptions{}, false)
	if err != nil || result.Ref != ref || result.Plan.Graph.Digest != digest {
		t.Fatalf("exact child recovery plan = %#v, %v", result, err)
	}

	tampered := plan
	tampered.Graph.Nodes = append([]graph.Node(nil), plan.Graph.Nodes...)
	tampered.Graph.Nodes[0].Effects = graph.EffectSet{graph.EffectMutate}
	if _, err := recoveryPlan(tampered, ref, compile.DependencyOptions{}, false); err == nil {
		t.Fatal("tampered child graph retained old pinned digest")
	}

	notInferred := plan
	notInferred.Graph.Nodes = append(notInferred.Graph.Nodes, graph.Node{ID: "consume", Kind: "safe", KindVersion: "v1", InputBindings: map[string]graph.Binding{"value": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.work.outputs.value"}}}})
	if _, err := recoveryPlan(notInferred, ref, compile.DependencyOptions{}, false); err == nil {
		t.Fatal("child graph changed by dependency inference")
	}
}
