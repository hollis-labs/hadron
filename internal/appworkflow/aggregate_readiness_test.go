package appworkflow

import (
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/libs/workflow/engine/compile"
	"github.com/hollis-labs/libs/workflow/engine/graph"
	"github.com/hollis-labs/libs/workflow/engine/runtime"
)

func TestRootAndChildFanOutStartupPassPlanNodeToReadiness(t *testing.T) {
	for _, child := range []bool{false, true} {
		name := "root"
		if child {
			name = "child"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newChildMaterializerFixture(t)
			fixture.request.Definition.Graph.Nodes[0].ForEach = &graph.ForEachSpec{Items: graph.Expression{Text: `["one", "two"]`}}
			clock := ClockFunc(func() time.Time { return fixture.now.Add(time.Second) })
			if child {
				materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: fixture.store, Clock: clock})
				if err != nil {
					t.Fatal(err)
				}
				if err := materializer.MaterializeChildRun(t.Context(), fixture.request); err != nil {
					t.Fatal(err)
				}
			} else {
				run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
				if err != nil {
					t.Fatal(err)
				}
				record := hoststate.StartRecord{
					Run:  runtime.BoundRun{ID: run.ID, Plan: run.Plan, InputsRef: *run.Inputs, CreatedAt: run.CreatedAt},
					Plan: compile.ExecutionPlan{Graph: fixture.request.Definition.Graph},
				}
				host := &Host{state: fixture.store, clock: clock}
				if err := host.materializeNodes(t.Context(), record); err != nil {
					t.Fatal(err)
				}
				if err := host.readyRootNodes(t.Context(), record); err != nil {
					t.Fatal(err)
				}
			}
			aggregate, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "root"})
			if err != nil || aggregate.Status != runtime.NodeReady || aggregate.Inputs != nil {
				t.Fatalf("aggregate startup = %#v, %v", aggregate, err)
			}
		})
	}
}

func TestChildRootWithoutBindingsPersistsEmptyInputsBeforeReady(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	fixture.request.Definition.Graph.Nodes[0].InputBindings = nil
	materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) })})
	if err != nil {
		t.Fatal(err)
	}
	if err := materializer.MaterializeChildRun(t.Context(), fixture.request); err != nil {
		t.Fatal(err)
	}
	root, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "root"})
	if err != nil || root.Status != runtime.NodeReady || root.Inputs == nil {
		t.Fatalf("empty-input child root = %#v, %v", root, err)
	}
	inputs, err := fixture.store.LoadValues(t.Context(), *root.Inputs)
	if err != nil || len(inputs) != 0 {
		t.Fatalf("empty child inputs = %#v, %v", inputs, err)
	}
}
