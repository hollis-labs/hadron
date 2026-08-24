package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestSwitchOrderedFirstMatchDefaultAndImmutableReplay(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "switch")
	for _, id := range []string{"choose", "first", "second", "fallback"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "choose"), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
	workflow := graph.Node{ID: "choose", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{
		{When: graph.Expression{Text: "inputs.first"}, Targets: []string{"first"}},
		{When: graph.Expression{Text: "inputs.second"}, Targets: []string{"second"}},
	}, Default: []string{"fallback"}}}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	result, decisionErr := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(runID, "choose"), Node: workflow, ExpressionContext: values.ExpressionContext{Inputs: boolInputs(t, true, true)}, At: base.Add(3 * time.Second)})
	if decisionErr != nil || result.Decision.Outcome != workflowruntime.ControlSelected || result.Decision.RuleIndex == nil || *result.Decision.RuleIndex != 0 || !slices.Equal(result.Decision.Targets, []workflowruntime.NodeInvocationID{invocationID(runID, "first")}) || result.Event == nil || result.Event.Type != workflowruntime.EventSwitchDecided {
		t.Fatalf("ordered switch = %#v, %v", result, decisionErr)
	}
	switchGraph := graph.Graph{Nodes: []graph.Node{workflow, {ID: "first"}, {ID: "second"}, {ID: "fallback"}}}
	for _, branch := range []struct {
		id   string
		want workflowruntime.NodeStatus
	}{{id: "first", want: workflowruntime.NodeReady}, {id: "second", want: workflowruntime.NodeSkipped}, {id: "fallback", want: workflowruntime.NodeSkipped}} {
		progress, progressErr := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: switchGraph, InvocationID: invocationID(runID, branch.id), At: base.Add(4 * time.Second)})
		if progressErr != nil || progress.Snapshot.Status != branch.want {
			t.Fatalf("switch branch %s = %#v, %v", branch.id, progress, progressErr)
		}
	}
	replay, replayErr := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(runID, "choose"), Node: workflow, ExpressionContext: values.ExpressionContext{Inputs: boolInputs(t, true, true)}, At: base.Add(30 * time.Second)})
	if replayErr != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.Event != nil {
		t.Fatalf("switch replay = %#v, %v", replay, replayErr)
	}
	conflict := result.Decision
	conflict.Targets[0] = invocationID(runID, "second")
	if _, err := store.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: conflict, ExpectedSourceGeneration: conflict.SourceGeneration, At: conflict.CreatedAt}); !errors.Is(err, workflowruntime.ErrControlFlowConflict) {
		t.Fatalf("changed decision = %v", err)
	}

	ctx2, store2, base2, run2 := controlFixture(t, "default")
	for _, id := range []string{"choose", "first", "second", "fallback"} {
		createNode(t, store2, invocationID(run2, id), workflowruntime.NodePending, 0, base2)
	}
	makeTerminalNode(t, store2, invocationID(run2, "choose"), workflowruntime.NodeSucceeded, base2.Add(2*time.Second))
	defaulted, err := workflowruntime.NewControlFlowCoordinator(store2, store2, nil).DecideSwitch(ctx2, workflowruntime.DecideSwitchRequest{Source: invocationID(run2, "choose"), Node: workflow, ExpressionContext: values.ExpressionContext{Inputs: boolInputs(t, false, false)}, At: base2.Add(3 * time.Second)})
	if err != nil || defaulted.Decision.Outcome != workflowruntime.ControlDefault || !slices.Equal(defaulted.Decision.Targets, []workflowruntime.NodeInvocationID{invocationID(run2, "fallback")}) {
		t.Fatalf("switch default = %#v, %v", defaulted, err)
	}
}

func TestCatchTimeoutTypedErrorBindingAndContinueOnError(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "catch")
	for _, id := range []string{"origin", "handler"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "origin"), workflowruntime.NodeTimedOut, base.Add(2*time.Second))
	origin := graph.Node{ID: "origin", Catch: []graph.CatchRule{
		{Errors: []string{"other"}, Targets: []string{"handler"}},
		{Errors: []string{"timed_out"}, When: &graph.Expression{Text: `steps.origin.error.code == "test_timed_out"`}, Targets: []string{"handler"}, BindAs: "create_error"},
	}}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	decision, decisionErr := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, "origin"), Node: origin, Timeout: workflowruntime.TimeoutExecution, At: base.Add(3 * time.Second)})
	if decisionErr != nil || decision.Decision.Outcome != workflowruntime.ControlSelected || decision.Decision.RuleIndex == nil || *decision.Decision.RuleIndex != 1 || decision.Decision.Error == nil || decision.Decision.BindAs != "create_error" {
		t.Fatalf("timeout catch = %#v, %v", decision, decisionErr)
	}
	expressionContext, contextErr := workflowruntime.BuildExpressionContext(ctx, store, store, graph.Graph{Nodes: []graph.Node{origin, {ID: "handler"}}}, runID)
	if contextErr != nil || expressionContext.Steps["origin"].Error == nil || expressionContext.Steps["origin"].Error.Redaction != values.RedactionPrivate || expressionContext.Steps["origin"].Error.Retention != values.RetentionRun {
		t.Fatalf("durable typed error context = %#v, %v", expressionContext, contextErr)
	}
	if attempt, ok := expressionContext.Steps["origin"].Error.Inline.(map[string]any)["attempt"].(json.Number); !ok || attempt.String() != "1" {
		t.Fatalf("typed error attempt = %#v", expressionContext.Steps["origin"].Error.Inline)
	}
	if timeout := expressionContext.Steps["origin"].Error.Inline.(map[string]any)["timeout_kind"]; timeout != string(workflowruntime.TimeoutExecution) {
		t.Fatalf("typed error timeout = %#v", expressionContext.Steps["origin"].Error.Inline)
	}
	replayed, replayErr := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, "origin"), Node: origin, Timeout: workflowruntime.TimeoutExecution, At: base.Add(3 * time.Second)})
	if replayErr != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Decision.Error == nil || *replayed.Decision.Error != *decision.Decision.Error {
		t.Fatalf("catch replay = %#v, %v", replayed, replayErr)
	}
	if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, "origin"), Node: origin, Timeout: workflowruntime.TimeoutHeartbeat, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrControlFlowConflict) {
		t.Fatalf("conflicting timeout classification = %v", err)
	}
	malformed := decision.Decision
	errorValues, valuesErr := store.LoadValues(ctx, *decision.Decision.Error)
	if valuesErr != nil {
		t.Fatal(valuesErr)
	}
	malformed.Error = nil
	malformed.Outcome = workflowruntime.ControlDefault
	if _, err := store.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: malformed, ErrorValues: errorValues, ExpectedSourceGeneration: malformed.SourceGeneration, At: malformed.CreatedAt}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("catch/default malformed decision = %v", err)
	}
	malformed = decision.Decision
	malformed.Error = nil
	malformed.Outcome, malformed.RuleIndex, malformed.Targets = workflowruntime.ControlUnmatched, nil, nil
	if _, err := store.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: malformed, ErrorValues: errorValues, ExpectedSourceGeneration: malformed.SourceGeneration, At: malformed.CreatedAt}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("unmatched catch binding malformed decision = %v", err)
	}
	handler := graph.Node{ID: "handler", If: &graph.Expression{Text: `create_error.code == "test_timed_out"`}}
	if _, err := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{
		Graph: graph.Graph{Nodes: []graph.Node{origin, handler}}, InvocationID: invocationID(runID, "handler"),
		ExpressionContext: values.ExpressionContext{Locals: values.ValueSet{"create_error": *expressionContext.Steps["origin"].Error}}, At: base.Add(4 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrControlFlowConflict) {
		t.Fatalf("catch local shadow = %v", err)
	}
	progress, err := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: graph.Graph{Nodes: []graph.Node{origin, handler}}, InvocationID: invocationID(runID, "handler"), At: base.Add(4 * time.Second)})
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady || !progress.PredicateResult {
		t.Fatalf("catch handler progression = %#v, %v", progress, err)
	}

	ctx2, store2, base2, run2 := controlFixture(t, "continue")
	createNode(t, store2, invocationID(run2, "origin"), workflowruntime.NodePending, 0, base2)
	makeTerminalNode(t, store2, invocationID(run2, "origin"), workflowruntime.NodeFailed, base2.Add(2*time.Second))
	continued, err := workflowruntime.NewControlFlowCoordinator(store2, store2, nil).DecideCatch(ctx2, workflowruntime.DecideCatchRequest{Source: invocationID(run2, "origin"), Node: graph.Node{ID: "origin", Catch: []graph.CatchRule{{Errors: []string{graph.CatchAllErrors}}}}, At: base2.Add(3 * time.Second)})
	if err != nil || continued.Decision.Outcome != workflowruntime.ControlContinued {
		t.Fatalf("continue_on_error = %#v, %v", continued, err)
	}
}

func TestRouteConvergenceIsAnySelectedAcrossCompleteOwnerSet(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "converge")
	for _, id := range []string{"fail-a", "fail-b", "switch-a", "switch-b", "handler", "shared"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	for _, id := range []string{"fail-a", "fail-b"} {
		makeTerminalNode(t, store, invocationID(runID, id), workflowruntime.NodeFailed, base.Add(2*time.Second))
	}
	for _, id := range []string{"switch-a", "switch-b"} {
		makeTerminalNode(t, store, invocationID(runID, id), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	catchA := graph.Node{ID: "fail-a", Catch: []graph.CatchRule{{Errors: []string{"missing"}, Targets: []string{"handler"}}}}
	catchB := graph.Node{ID: "fail-b", Catch: []graph.CatchRule{{Targets: []string{"handler"}}}}
	if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, catchA.ID), Node: catchA, At: base.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	handlerGraph := graph.Graph{Nodes: []graph.Node{catchB, {ID: "handler"}, catchA}}
	if _, err := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: handlerGraph, InvocationID: invocationID(runID, "handler"), At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("incomplete owner set progression = %v", err)
	}
	if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, catchB.ID), Node: catchB, At: base.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	progress, progressErr := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: handlerGraph, InvocationID: invocationID(runID, "handler"), At: base.Add(4 * time.Second)})
	if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("multi-catch convergence = %#v, %v", progress, progressErr)
	}

	switchA := graph.Node{ID: "switch-a", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "false"}, Targets: []string{"shared"}}}}}
	switchB := graph.Node{ID: "switch-b", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "true"}, Targets: []string{"shared"}}}}}
	for _, node := range []graph.Node{switchB, switchA} {
		if _, err := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(runID, node.ID), Node: node, At: base.Add(3 * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	progress, progressErr = coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: graph.Graph{Nodes: []graph.Node{switchB, {ID: "shared"}, switchA}}, InvocationID: invocationID(runID, "shared"), At: base.Add(4 * time.Second)})
	if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("overlapping switch convergence = %#v, %v", progress, progressErr)
	}
}

func TestRouteKindsOnOneSourceTreatTerminalInapplicableOwnerAsUnselected(t *testing.T) {
	for _, test := range []struct {
		name   string
		status workflowruntime.NodeStatus
	}{
		{name: "success uses switch", status: workflowruntime.NodeSucceeded},
		{name: "failure uses catch", status: workflowruntime.NodeFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "dual-"+string(test.status))
			for _, id := range []string{"source", "target"} {
				createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
			}
			makeTerminalNode(t, store, invocationID(runID, "source"), test.status, base.Add(2*time.Second))
			source := graph.Node{ID: "source", Catch: []graph.CatchRule{{Targets: []string{"target"}}}, Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "true"}, Targets: []string{"target"}}}}}
			coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
			if test.status == workflowruntime.NodeSucceeded {
				if _, err := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(runID, "source"), Node: source, At: base.Add(3 * time.Second)}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, "source"), Node: source, At: base.Add(3 * time.Second)}); err != nil {
					t.Fatal(err)
				}
			}
			progress, err := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{
				Graph: graph.Graph{Nodes: []graph.Node{source, {ID: "target"}}}, InvocationID: invocationID(runID, "target"),
				Dependencies: []workflowruntime.DependencyRef{{InvocationID: invocationID(runID, "source")}}, At: base.Add(4 * time.Second),
			})
			if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
				t.Fatalf("dual route progression = %#v, %v", progress, err)
			}
		})
	}
}

func TestFinalizerSiblingScopesShareLayer(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope []string
	}{
		{name: "global siblings"},
		{name: "same explicit scope", scope: []string{"work"}},
		{name: "mixed length scope", scope: []string{"z", "aa"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			nodes := []graph.Node{
				{ID: "cleanup-b", Finally: &graph.FinallySpec{Scope: test.scope}},
				{ID: "work"},
				{ID: "cleanup-a", Finally: &graph.FinallySpec{Scope: test.scope}},
			}
			if test.name == "mixed length scope" {
				nodes = append(nodes, graph.Node{ID: "z"}, graph.Node{ID: "aa"})
			}
			workflow := graph.Graph{Nodes: nodes}
			scopes, err := workflowruntime.PlanFinalizerScopes(workflow, "run-finalizer-siblings")
			if err != nil || len(scopes) != 2 || scopes[0].Order != 0 || scopes[1].Order != 0 || scopes[0].Invocation.NodeID != "cleanup-a" || scopes[1].Invocation.NodeID != "cleanup-b" {
				t.Fatalf("sibling finalizers = %#v, %v", scopes, err)
			}
			if test.name == "mixed length scope" && (scopes[0].Scope[0].NodeID != "aa" || scopes[0].Scope[1].NodeID != "z") {
				t.Fatalf("mixed-length scope order = %#v", scopes[0].Scope)
			}
		})
	}
}

func TestFinallyRespectsOrdinaryExplicitDependencies(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "finally-explicit-dependency")
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "work"},
		{ID: "cleanup-a", Finally: &graph.FinallySpec{}},
		{ID: "cleanup-b", Needs: []graph.Need{{Node: "cleanup-a"}}, Finally: &graph.FinallySpec{}},
	}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "work"), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, _, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "finally-explicit-dependency", base.Add(3*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("begin terminal intent = %v", err)
	}
	blocked, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup-b"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(4*time.Second))
	if err != nil || blocked.Snapshot.Status != workflowruntime.NodeBlocked || blocked.Disposition != workflowruntime.ReadinessWaiting {
		t.Fatalf("dependent finalizer before prerequisite = %#v, %v", blocked, err)
	}
	readyA, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup-a"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(4*time.Second))
	if err != nil || readyA.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("prerequisite finalizer = %#v, %v", readyA, err)
	}
	makeTerminalNode(t, store, invocationID(runID, "cleanup-a"), workflowruntime.NodeSucceeded, base.Add(5*time.Second))
	readyB, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup-b"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(6*time.Second))
	if err != nil || readyB.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("dependent finalizer after prerequisite = %#v, %v", readyB, err)
	}
}

func TestControlDecisionRejectsKindOutcomeMismatchAndMalformedTarget(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "malformed")
	for _, id := range []string{"source", "target"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "source"), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	badTarget := graph.Node{ID: "source", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "true"}, Targets: []string{"Target"}}}}}
	if _, err := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(runID, "source"), Node: badTarget, At: base.Add(3 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("malformed target = %v", err)
	}
	decision := workflowruntime.ControlDecisionSnapshot{ID: workflowruntime.ControlDecisionID{Source: invocationID(runID, "source"), Kind: workflowruntime.ControlSwitch}, Outcome: workflowruntime.ControlContinued}
	if _, err := store.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ExpectedSourceGeneration: 2, At: base.Add(3 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("switch/continued malformed decision = %v", err)
	}
}

func TestCatchCoordinatorContentionConvergesOneTypedDecision(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "catch-contention")
	for _, id := range []string{"origin", "handler"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "origin"), workflowruntime.NodeFailed, base.Add(2*time.Second))
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	request := workflowruntime.DecideCatchRequest{Source: invocationID(runID, "origin"), Node: graph.Node{ID: "origin", Catch: []graph.CatchRule{{Targets: []string{"handler"}}}}, At: base.Add(3 * time.Second)}
	type outcome struct {
		result workflowruntime.RecordControlDecisionResult
		err    error
	}
	results := make(chan outcome, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := coordinator.DecideCatch(ctx, request)
			results <- outcome{result: result, err: err}
		}()
	}
	wg.Wait()
	close(results)
	applied := 0
	var errorRef *values.ValueSetRef
	for item := range results {
		if item.err != nil || item.result.Decision.Error == nil {
			t.Fatalf("contended catch = %#v, %v", item.result, item.err)
		}
		if item.result.Outcome == workflowruntime.IdempotencyApplied {
			applied++
		}
		if errorRef == nil {
			copyRef := *item.result.Decision.Error
			errorRef = &copyRef
		} else if *errorRef != *item.result.Decision.Error {
			t.Fatalf("contended refs differ: %#v != %#v", errorRef, item.result.Decision.Error)
		}
	}
	if applied != 1 {
		t.Fatalf("applied catch decisions = %d, want 1", applied)
	}
}

func TestNestedFinallyCancellationIntentFencesWorkAndCleanupFailureWins(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "finally-cancel")
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "work"},
		{ID: "inner-cleanup", Finally: &graph.FinallySpec{Scope: []string{"work"}}, If: &graph.Expression{Text: `run.status == "canceled" && run.error.code == "user_canceled" && run_scope.tenant == "alpha" && env.first`}},
		{ID: "outer-cleanup", Finally: &graph.FinallySpec{}},
	}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
	}
	work, _ := store.LoadNodeInvocation(ctx, invocationID(runID, "work"))
	_, _ = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: work.ID, ExpectedGeneration: work.Generation, To: workflowruntime.NodeReady, At: base.Add(time.Second)})
	scopes, planErr := workflowruntime.PlanFinalizerScopes(workflow, runID)
	if planErr != nil || len(scopes) != 2 || scopes[0].Invocation.NodeID != "inner-cleanup" || scopes[0].Order != 0 || scopes[1].Invocation.NodeID != "outer-cleanup" || scopes[1].Order != 1 {
		t.Fatalf("nested scopes = %#v, %v", scopes, planErr)
	}
	reordered := workflow
	reordered.Nodes = []graph.Node{workflow.Nodes[2], workflow.Nodes[1], workflow.Nodes[0]}
	reorderedScopes, reorderedErr := workflowruntime.PlanFinalizerScopes(reordered, runID)
	if reorderedErr != nil || !reflect.DeepEqual(scopes, reorderedScopes) {
		t.Fatalf("reordered scopes = %#v, %v; want %#v", reorderedScopes, reorderedErr, scopes)
	}
	run, _ := store.LoadRun(ctx, runID)
	request := workflowruntime.RequestRunCancellationRequest{RunID: runID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-with-cleanup", Reason: workflowruntime.Failure{Code: "user_canceled", Message: "user canceled"}, At: base.Add(2 * time.Second)}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	canceled, cancelErr := coordinator.RequestRunCancellationWithFinalizers(ctx, workflow, request)
	if cancelErr != nil || !canceled.Cancellation.Run.Status.Active() || canceled.Intent.IntendedStatus != workflowruntime.RunCanceled || canceled.Intent.Status != workflowruntime.TerminalIntentPending {
		t.Fatalf("cancellation with cleanup = %#v, %v", canceled, cancelErr)
	}
	if canceled.Intent.Error == nil {
		t.Fatal("cancellation intent omitted typed error")
	}
	if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{
		Source: invocationID(runID, "work"), Node: graph.Node{ID: "work", Catch: []graph.CatchRule{{Errors: []string{graph.CatchAllErrors}}}},
		Failure: &request.Reason, At: base.Add(3 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("post-intent catch decision = %v", err)
	}
	replay, replayErr := coordinator.RequestRunCancellationWithFinalizers(ctx, workflow, request)
	if replayErr != nil || replay.Cancellation.Outcome != workflowruntime.IdempotencyReplayed || replay.Intent.Generation != canceled.Intent.Generation || !reflect.DeepEqual(replay.Cancellation.Nodes, canceled.Cancellation.Nodes) || !reflect.DeepEqual(replay.Cancellation.Intents, canceled.Cancellation.Intents) || !reflect.DeepEqual(replay.Cancellation.Events, canceled.Cancellation.Events) {
		t.Fatalf("cancellation replay = %#v, %v", replay, replayErr)
	}
	claimed, claimErr := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: invocationID(runID, "work"), ExpectedClaimGeneration: 0, Owner: "late", Token: "late", IdempotencyKey: "late-work", Now: base.Add(3 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if claimErr != nil || claimed.Acquired {
		t.Fatalf("post-cancel work claim = %#v, %v", claimed, claimErr)
	}
	workAfterCancel, _ := store.LoadNodeInvocation(ctx, invocationID(runID, "work"))
	beforePublicEvents, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: workAfterCancel.ID, ExpectedGeneration: workAfterCancel.Generation, To: workflowruntime.NodeCanceled, At: base.Add(3 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public canceled transition during terminal intent = %v", err)
	}
	if _, err := store.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: runID, Invocation: &workAfterCancel.ID, Type: "ordinary.event", OccurredAt: base.Add(3 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public ordinary event during terminal intent = %v", err)
	}
	if _, err := store.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: runID, Type: "ordinary.run_event", OccurredAt: base.Add(3 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public anonymous event during terminal intent = %v", err)
	}
	afterPublicEvents, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if len(afterPublicEvents) != len(beforePublicEvents) {
		t.Fatalf("rejected public events changed history = %d/%d", len(beforePublicEvents), len(afterPublicEvents))
	}
	cleanupID := invocationID(runID, "inner-cleanup")
	if _, err := store.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: runID, Invocation: &cleanupID, Type: "cleanup.event", OccurredAt: base.Add(3 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		t.Fatalf("finalizer event during terminal intent = %v", err)
	}
	inner, innerErr := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "inner-cleanup"), values.ExpressionContext{RunScope: map[string]any{"tenant": "alpha"}, Env: boolInputs(t, true, false)}, values.ExpressionOptions{AllowEnv: true}, base.Add(3*time.Second))
	if innerErr != nil || inner.Snapshot.Status != workflowruntime.NodeReady || !inner.PredicateResult {
		t.Fatalf("inner finally = %#v, %v", inner, innerErr)
	}
	makeTerminalNode(t, store, invocationID(runID, "inner-cleanup"), workflowruntime.NodeSucceeded, base.Add(4*time.Second))
	outer, outerErr := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "outer-cleanup"), values.ExpressionContext{RunScope: map[string]any{"tenant": "alpha"}}, values.ExpressionOptions{}, base.Add(5*time.Second))
	if outerErr != nil || outer.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("outer finally = %#v, %v", outer, outerErr)
	}
	makeTerminalNode(t, store, invocationID(runID, "outer-cleanup"), workflowruntime.NodeFailed, base.Add(6*time.Second))
	currentRun, _ := store.LoadRun(ctx, runID)
	completed, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: currentRun.Generation, ExpectedIntentGeneration: canceled.Intent.Generation, At: base.Add(7 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunFailed || completed.Intent.IntendedStatus != workflowruntime.RunCanceled || completed.Event.Attributes["cleanup_failure"] != "outer-cleanup" {
		t.Fatalf("cleanup failure accounting = %#v, %v", completed, err)
	}
	if completed.Event.Values == nil || canceled.Intent.Error == nil || *completed.Event.Values != *canceled.Intent.Error {
		t.Fatalf("completion event error ref = %#v, want %#v", completed.Event.Values, canceled.Intent.Error)
	}
	if _, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: invocationID(runID, "late-node"), Status: workflowruntime.NodePending, CreatedAt: base.Add(8 * time.Second), UpdatedAt: base.Add(8 * time.Second)}}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("terminal parent node creation = %v", err)
	}
}

func TestFinallyOrderingBlocksOnlyNestedChain(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "disjoint-finally")
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "work-a"},
		{ID: "inner-a", Finally: &graph.FinallySpec{Scope: []string{"work-a"}}},
		{ID: "outer-a", Finally: &graph.FinallySpec{Scope: []string{"work-a", "inner-a"}}},
		{ID: "work-b"},
		{ID: "inner-b", Finally: &graph.FinallySpec{Scope: []string{"work-b"}}},
		{ID: "outer-b", Finally: &graph.FinallySpec{Scope: []string{"work-b", "inner-b"}}},
	}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "work-a"), workflowruntime.NodeSucceeded, base.Add(time.Second))
	makeTerminalNode(t, store, invocationID(runID, "work-b"), workflowruntime.NodeSucceeded, base.Add(time.Second))
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, _, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "disjoint-finally", base.Add(2*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("begin disjoint finalizers = %v", err)
	}
	if _, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "outer-a"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("outer-a before inner-a = %v", err)
	}
	innerA, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "inner-a"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second))
	if err != nil || innerA.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("inner-a = %#v, %v", innerA, err)
	}
	makeTerminalNode(t, store, invocationID(runID, "inner-a"), workflowruntime.NodeSucceeded, base.Add(4*time.Second))
	outerA, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "outer-a"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(5*time.Second))
	if err != nil || outerA.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("outer-a was blocked by disjoint inner-b = %#v, %v", outerA, err)
	}
	innerB, _ := store.LoadNodeInvocation(ctx, invocationID(runID, "inner-b"))
	if innerB.Status != workflowruntime.NodePending {
		t.Fatalf("disjoint inner-b changed = %#v", innerB)
	}
}

func TestNewFailureValueRejectsMismatchedAttemptOrigin(t *testing.T) {
	origin := invocationID("run-failure-constructor", "source")
	other := workflowruntime.AttemptID{Invocation: invocationID(origin.RunID, "other"), Number: 1}
	failure := workflowruntime.Failure{Code: "failed", Message: "failed"}
	if _, err := workflowruntime.NewFailureValue(origin, &other, workflowruntime.NodeFailed, "", failure); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("mismatched failure attempt = %v", err)
	}
	if _, err := workflowruntime.NewFailureValue(origin, nil, workflowruntime.NodeFailed, workflowruntime.TimeoutExecution, failure); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("timeout on non-timeout failure = %v", err)
	}
	if _, err := workflowruntime.NewFailureValue(origin, nil, workflowruntime.NodeTimedOut, workflowruntime.TimeoutKind("unknown"), failure); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("invalid timeout kind = %v", err)
	}
	timeoutFailure := workflowruntime.Failure{Code: "timed_out", Message: "timed out", Details: map[string]string{"timeout_kind": string(workflowruntime.TimeoutExecution)}}
	if _, err := workflowruntime.NewFailureValue(origin, nil, workflowruntime.NodeTimedOut, workflowruntime.TimeoutHeartbeat, timeoutFailure); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("mismatched timeout detail = %v", err)
	}
	if _, err := workflowruntime.NewFailureValue(origin, nil, workflowruntime.NodeTimedOut, workflowruntime.TimeoutExecution, timeoutFailure); err != nil {
		t.Fatalf("matching timeout detail = %v", err)
	}
	value, err := workflowruntime.NewFailureValue(origin, nil, workflowruntime.NodeFailed, "", failure)
	if err != nil {
		t.Fatal(err)
	}
	payload := value.Inline.(map[string]any)
	payload["unexpected"] = "not part of the durable envelope"
	if err := workflowruntime.ValidateControlErrorValues(values.ValueSet{"error": value}); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("extra control error field = %v", err)
	}
}

func TestControlFlowPublicHelpersRejectNilAndTypedNilStores(t *testing.T) {
	var typedNil *runtimetest.Store
	var state workflowruntime.StateStore = typedNil
	var control workflowruntime.ControlFlowStore = typedNil
	id := workflowruntime.ControlDecisionID{Source: workflowruntime.NodeInvocationID{RunID: "nil-run", NodeID: "source"}, Kind: workflowruntime.ControlCatch}
	if _, _, err := workflowruntime.CatchBinding(context.Background(), state, control, id); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("typed-nil CatchBinding = %v", err)
	}
	if _, _, err := workflowruntime.CatchBinding(nil, runtimetest.NewStore(), runtimetest.NewStore(), id); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) { //nolint:staticcheck // nil is the transport case under test.
		t.Fatalf("nil-context CatchBinding = %v", err)
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(state, control, nil)
	if _, err := coordinator.DecideSwitch(context.Background(), workflowruntime.DecideSwitchRequest{}); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("typed-nil coordinator = %v", err)
	}
}

func TestPendingRunRejectsUnclosableCancellationFinalizerIntent(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-control-pending-finalizer")
	createRun(t, store, runID, base)
	for _, id := range []string{"work", "cleanup"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	run, loadErr := store.LoadRun(ctx, runID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	beforeEvents, listErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if listErr != nil {
		t.Fatal(listErr)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	_, requestErr := workflowruntime.NewControlFlowCoordinator(store, store, nil).RequestRunCancellationWithFinalizers(ctx, workflow, workflowruntime.RequestRunCancellationRequest{
		RunID: runID, ExpectedGeneration: run.Generation, IdempotencyKey: "pending-finalizer-cancel",
		Reason: workflowruntime.Failure{Code: "canceled", Message: "canceled"}, At: base.Add(time.Second),
	})
	var transitionErr *workflowruntime.TransitionError
	if !errors.As(requestErr, &transitionErr) {
		t.Fatalf("pending cancellation with finalizer = %v, want transition error", requestErr)
	}
	afterRun, afterRunErr := store.LoadRun(ctx, runID)
	if afterRunErr != nil || !reflect.DeepEqual(afterRun, run) {
		t.Fatalf("rejected intent changed run = %#v, %v; want %#v", afterRun, afterRunErr, run)
	}
	for _, id := range []string{"work", "cleanup"} {
		node, loadErr := store.LoadNodeInvocation(ctx, invocationID(runID, id))
		if loadErr != nil || node.Status != workflowruntime.NodePending || node.Generation != 1 {
			t.Fatalf("rejected intent changed %s = %#v, %v", id, node, loadErr)
		}
	}
	if _, err := store.LoadTerminalIntent(ctx, runID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("rejected terminal intent = %v", err)
	}
	afterEvents, afterEventsErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if afterEventsErr != nil || !reflect.DeepEqual(afterEvents, beforeEvents) {
		t.Fatalf("rejected intent events = %#v, %v; want %#v", afterEvents, afterEventsErr, beforeEvents)
	}
}

func TestCancellationTreePreservesDirectChildFinalizersAndReplays(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	rootID, childID := workflowruntime.RunID("cancel-tree-root"), workflowruntime.RunID("cancel-tree-child")
	for _, runID := range []workflowruntime.RunID{rootID, childID} {
		createRun(t, store, runID, base)
		run, _ := store.LoadRun(ctx, runID)
		if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	rootNodeID := invocationID(rootID, "call-child")
	createNode(t, store, rootNodeID, workflowruntime.NodePending, 0, base)
	for _, id := range []string{"work", "cleanup"} {
		createNode(t, store, invocationID(childID, id), workflowruntime.NodePending, 0, base)
	}
	if err := store.RecordChildRun(ctx, workflowruntime.ChildRunLink{ParentRunID: rootID, Invocation: rootNodeID, ChildRunID: childID, Policy: graph.ParentCloseCancel, CreatedAt: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	rootGraph := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "call-child"}}}
	childGraph := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	root, _ := store.LoadRun(ctx, rootID)
	child, _ := store.LoadRun(ctx, childID)
	request := workflowruntime.RequestRunCancellationRequest{RunID: rootID, ExpectedGeneration: root.Generation, IdempotencyKey: "cancel-tree", Reason: workflowruntime.Failure{Code: "canceled", Message: "canceled"}, At: base.Add(2 * time.Second)}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	forged := rootGraph
	forged.ID = "another-plan"
	if _, err := coordinator.RequestRunCancellationTree(ctx, forged, request, []workflowruntime.CancellationDescendantGraph{{Run: child, Graph: childGraph}}); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("forged root graph binding = %v", err)
	}
	unchangedRoot, _ := store.LoadRun(ctx, rootID)
	if unchangedRoot.Generation != root.Generation || unchangedRoot.Status != root.Status {
		t.Fatalf("forged graph mutated root = %#v", unchangedRoot)
	}
	result, err := coordinator.RequestRunCancellationTree(ctx, rootGraph, request, []workflowruntime.CancellationDescendantGraph{{Run: child, Graph: childGraph}})
	if err != nil || result.Cancellation.Run.Status != workflowruntime.RunCanceled || result.Intent.RunID != "" || len(result.TerminalIntents) != 1 || result.TerminalIntents[0].RunID != childID {
		t.Fatalf("cancel tree = %#v, %v", result, err)
	}
	childAfter, _ := store.LoadRun(ctx, childID)
	workAfter, _ := store.LoadNodeInvocation(ctx, invocationID(childID, "work"))
	cleanupAfter, _ := store.LoadNodeInvocation(ctx, invocationID(childID, "cleanup"))
	if !childAfter.Status.Active() || workAfter.Status != workflowruntime.NodeCanceled || cleanupAfter.Status != workflowruntime.NodePending {
		t.Fatalf("child cancellation fence = run %#v work %#v cleanup %#v", childAfter, workAfter, cleanupAfter)
	}
	replayRoot, _ := store.LoadRun(ctx, rootID)
	replayChild, _ := store.LoadRun(ctx, childID)
	replayRequest := request
	replayRequest.ExpectedGeneration = replayRoot.Generation
	replay, err := coordinator.RequestRunCancellationTree(ctx, rootGraph, replayRequest, []workflowruntime.CancellationDescendantGraph{{Run: replayChild, Graph: childGraph}})
	if err != nil || replay.Cancellation.Outcome != workflowruntime.IdempotencyReplayed || len(replay.TerminalIntents) != 1 || replay.TerminalIntents[0].RunID != childID {
		t.Fatalf("cancel tree replay = %#v, %v", replay, err)
	}
	progress, err := coordinator.ProgressFinally(ctx, childGraph, invocationID(childID, "cleanup"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second))
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("child finalizer progression = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, invocationID(childID, "cleanup"), workflowruntime.NodeSucceeded, base.Add(4*time.Second))
	childNow, _ := store.LoadRun(ctx, childID)
	completed, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: childID, ExpectedRunGeneration: childNow.Generation, ExpectedIntentGeneration: result.TerminalIntents[0].Generation, At: base.Add(5 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("child cleanup completion = %#v, %v", completed, err)
	}
}

func TestCancellationWithFinalizersContentionAndLateAttemptFence(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "cancel-contention")
	for _, id := range []string{"work", "cleanup"} {
		createNode(t, store, invocationID(runID, id), workflowruntime.NodePending, 0, base)
	}
	work, _ := store.LoadNodeInvocation(ctx, invocationID(runID, "work"))
	ready, _ := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: work.ID, ExpectedGeneration: work.Generation, To: workflowruntime.NodeReady, At: base.Add(time.Second)})
	claim := claimNode(t, store, work.ID, 0, "worker", "token", "claim-running", base.Add(2*time.Second), base.Add(time.Hour))
	claimed, _ := store.LoadNodeInvocation(ctx, work.ID)
	started, startErr := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: work.ID, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if startErr != nil {
		t.Fatal(startErr)
	}
	_ = ready
	run, _ := store.LoadRun(ctx, runID)
	cancellation := workflowruntime.RequestRunCancellationRequest{RunID: runID, ExpectedGeneration: run.Generation, IdempotencyKey: "contended-cancel", Reason: workflowruntime.Failure{Code: "canceled", Message: "cancel"}, At: base.Add(3 * time.Second)}
	typedCancellation, valueErr := workflowruntime.NewRunFailureValue(runID, workflowruntime.RunCanceled, cancellation.Reason)
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	var wg sync.WaitGroup
	outcomes := make(chan workflowruntime.IdempotencyOutcome, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, callErr := coordinator.RequestRunCancellationWithFinalizers(ctx, workflow, cancellation)
			if callErr != nil {
				failures <- callErr
			} else {
				outcomes <- result.Cancellation.Outcome
			}
		}()
	}
	wg.Wait()
	close(outcomes)
	close(failures)
	if len(failures) != 0 {
		t.Fatalf("contended cancellation errors = %v", <-failures)
	}
	applied := 0
	for outcome := range outcomes {
		if outcome == workflowruntime.IdempotencyApplied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("applied cancellations = %d, want 1", applied)
	}
	claimReplay, claimErr := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: work.ID, ExpectedClaimGeneration: 0, Owner: "worker", Token: "token", IdempotencyKey: "claim-running", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if claimErr != nil || claimReplay.Acquired || !claimReplay.Replayed {
		t.Fatalf("post-intent claim replay = %#v, %v", claimReplay, claimErr)
	}
	intent, intentErr := store.LoadTerminalIntent(ctx, runID)
	if intentErr != nil || intent.Error == nil {
		t.Fatalf("contended typed cancellation intent = %#v, %v", intent, intentErr)
	}
	persisted, valuesErr := store.LoadValues(ctx, *intent.Error)
	if valuesErr != nil || persisted["error"].Digest != typedCancellation.Digest {
		t.Fatalf("contended cancellation error = %#v, %v", persisted, valuesErr)
	}
	if _, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: work.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(4 * time.Second)}); err == nil {
		t.Fatal("late non-finalizer completion was not fenced")
	}
	intents, recoveryErr := store.RecoverCancellationIntents(ctx, workflowruntime.CancellationIntentQuery{RunID: runID})
	if recoveryErr != nil || len(intents) != 1 {
		t.Fatalf("pending cancellation intents = %#v, %v", intents, recoveryErr)
	}
	if _, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(4*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("cleanup before cancellation reconciliation = %v", err)
	}
	if _, err := store.ResolveCancellationIntent(ctx, workflowruntime.ResolveCancellationIntentRequest{IntentID: intents[0].ID, ExpectedGeneration: intents[0].Generation, At: base.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	cleanup, progressErr := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(5*time.Second))
	if progressErr != nil || cleanup.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("cleanup after cancellation reconciliation = %#v, %v", cleanup, progressErr)
	}
}

func TestReconcileRunCompletionAccountsEveryOutcomeAndRetainsTypedFailure(t *testing.T) {
	for _, test := range []struct {
		node workflowruntime.NodeStatus
		run  workflowruntime.RunStatus
	}{
		{node: workflowruntime.NodeSucceeded, run: workflowruntime.RunSucceeded},
		{node: workflowruntime.NodeFailed, run: workflowruntime.RunFailed},
		{node: workflowruntime.NodeTimedOut, run: workflowruntime.RunTimedOut},
		{node: workflowruntime.NodeCrashed, run: workflowruntime.RunCrashed},
		{node: workflowruntime.NodeCanceled, run: workflowruntime.RunCanceled},
	} {
		t.Run(string(test.node), func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "account-"+string(test.node))
			workflow := graph.Graph{Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
			for _, node := range workflow.Nodes {
				createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
			}
			makeTerminalNode(t, store, invocationID(runID, "work"), test.node, base.Add(2*time.Second))
			coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
			run, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "account-"+string(test.node), base.Add(3*time.Second))
			if !errors.Is(err, workflowruntime.ErrControlFlowPending) || intent == nil || intent.IntendedStatus != test.run || !run.Status.Active() {
				t.Fatalf("pending completion = run %#v intent %#v err %v", run, intent, err)
			}
			if test.run == workflowruntime.RunSucceeded {
				if intent.Reason != nil || intent.Error != nil {
					t.Fatalf("successful intent failure context = %#v", intent)
				}
			} else {
				if intent.Reason == nil || intent.Error == nil {
					t.Fatalf("failed intent omitted typed context = %#v", intent)
				}
				set, loadErr := store.LoadValues(ctx, *intent.Error)
				if loadErr != nil || set["error"].Redaction != values.RedactionPrivate || set["error"].Retention != values.RetentionRun {
					t.Fatalf("typed terminal error = %#v, %v", set, loadErr)
				}
				if test.node == workflowruntime.NodeTimedOut && set["error"].Inline.(map[string]any)["timeout_kind"] != string(workflowruntime.TimeoutExecution) {
					t.Fatalf("typed terminal timeout = %#v", set["error"].Inline)
				}
			}
			makeTerminalNode(t, store, invocationID(runID, "cleanup"), workflowruntime.NodeSucceeded, base.Add(4*time.Second))
			completed, completedIntent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "account-"+string(test.node), base.Add(5*time.Second))
			if err != nil || completed.Status != test.run || completedIntent == nil || completedIntent.Status != workflowruntime.TerminalIntentCompleted {
				t.Fatalf("completed run = %#v intent %#v err %v", completed, completedIntent, err)
			}
			if _, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(6*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowConflict) {
				t.Fatalf("completed finalizer replay = %v", err)
			}
		})
	}
}

func TestPreAttemptTimeoutErrorProjectsFromTerminalIntentIntoFinalizerContext(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "pre-attempt-timeout-context")
	workflow := graph.Graph{Nodes: []graph.Node{
		{ID: "work"},
		{ID: "cleanup", Finally: &graph.FinallySpec{}, If: &graph.Expression{Text: `steps.work.error.code == "node_timed_out"`}},
	}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
	}
	work, loadErr := store.LoadNodeInvocation(ctx, invocationID(runID, "work"))
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: work.ID, ExpectedGeneration: work.Generation, To: workflowruntime.NodeTimedOut, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "pre-attempt-timeout", base.Add(2*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) || intent == nil || intent.Error == nil || intent.IntendedStatus != workflowruntime.RunTimedOut {
		t.Fatalf("begin timeout intent = %#v, %v", intent, err)
	}
	expressionContext, err := workflowruntime.BuildExpressionContext(ctx, store, store, workflow, runID)
	if err != nil || expressionContext.Steps["work"].Error == nil {
		t.Fatalf("pre-attempt timeout context = %#v, %v", expressionContext, err)
	}
	payload := expressionContext.Steps["work"].Error.Inline.(map[string]any)
	if payload["code"] != "node_timed_out" || payload["attempt"] != nil {
		t.Fatalf("pre-attempt timeout error = %#v", payload)
	}
	progress, err := coordinator.ProgressFinally(ctx, workflow, invocationID(runID, "cleanup"), values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second))
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("timeout-aware cleanup = %#v, %v", progress, err)
	}
}

func TestReconcileRunCompletionDistinguishesHandledUnmatchedAndCleanupFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		catch      []graph.CatchRule
		withTarget bool
		want       workflowruntime.RunStatus
	}{
		{name: "selected handler", catch: []graph.CatchRule{{Targets: []string{"handler"}}}, withTarget: true, want: workflowruntime.RunSucceeded},
		{name: "continued", catch: []graph.CatchRule{{Errors: []string{graph.CatchAllErrors}}}, want: workflowruntime.RunSucceeded},
		{name: "unmatched", catch: []graph.CatchRule{{Errors: []string{"different"}, Targets: []string{"handler"}}}, withTarget: true, want: workflowruntime.RunFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "handled-"+string(test.want)+"-"+test.name)
			workflow := graph.Graph{Nodes: []graph.Node{{ID: "origin", Catch: test.catch}}}
			if test.withTarget {
				workflow.Nodes = append(workflow.Nodes, graph.Node{ID: "handler"})
			}
			workflow.Nodes = append(workflow.Nodes, graph.Node{ID: "cleanup", Finally: &graph.FinallySpec{}})
			for _, node := range workflow.Nodes {
				createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
			}
			makeTerminalNode(t, store, invocationID(runID, "origin"), workflowruntime.NodeFailed, base.Add(2*time.Second))
			if test.withTarget {
				makeTerminalNode(t, store, invocationID(runID, "handler"), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
			}
			coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
			if _, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: invocationID(runID, "origin"), Node: workflow.Nodes[0], At: base.Add(3 * time.Second)}); err != nil {
				t.Fatal(err)
			}
			_, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "handled-"+test.name, base.Add(4*time.Second))
			if !errors.Is(err, workflowruntime.ErrControlFlowPending) || intent == nil || intent.IntendedStatus != test.want {
				t.Fatalf("handled accounting intent = %#v, %v", intent, err)
			}
			makeTerminalNode(t, store, invocationID(runID, "cleanup"), workflowruntime.NodeFailed, base.Add(5*time.Second))
			completed, completedIntent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "handled-"+test.name, base.Add(6*time.Second))
			if err != nil || completed.Status != workflowruntime.RunFailed || completedIntent.IntendedStatus != test.want {
				t.Fatalf("cleanup failure result = %#v intent %#v err %v", completed, completedIntent, err)
			}
		})
	}
}

func TestFailedReconcileContentionPersistsOneIntentAndError(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "reconcile-contention")
	workflow := graph.Graph{Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, invocationID(runID, "work"), workflowruntime.NodeFailed, base.Add(2*time.Second))
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "reconcile-contention", base.Add(3*time.Second))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, workflowruntime.ErrControlFlowPending) {
			t.Fatalf("contended reconcile = %v", err)
		}
	}
	intent, err := store.LoadTerminalIntent(ctx, runID)
	if err != nil || intent.Error == nil {
		t.Fatalf("contended terminal intent = %#v, %v", intent, err)
	}
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	intentEvents := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventTerminalIntent {
			intentEvents++
		}
	}
	if intentEvents != 1 {
		t.Fatalf("terminal intent events = %d, want 1", intentEvents)
	}
}

func TestCompletedReconcileReplaysAndConvergesUnderContention(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "completion-contention")
	workflow := graph.Graph{Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	for _, node := range workflow.Nodes {
		createNode(t, store, invocationID(runID, node.ID), workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, invocationID(runID, node.ID), workflowruntime.NodeSucceeded, base.Add(2*time.Second))
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	type result struct {
		run    workflowruntime.RunSnapshot
		intent *workflowruntime.TerminalIntentSnapshot
		err    error
	}
	results := make(chan result, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "completion-contention", base.Add(3*time.Second))
			results <- result{run: run, intent: intent, err: err}
		}()
	}
	wg.Wait()
	close(results)
	for item := range results {
		if item.err != nil || item.run.Status != workflowruntime.RunSucceeded || item.intent == nil || item.intent.Status != workflowruntime.TerminalIntentCompleted {
			t.Fatalf("contended completion = run %#v intent %#v err %v", item.run, item.intent, item.err)
		}
	}
	replayedRun, replayedIntent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "completion-contention", base.Add(time.Hour))
	if err != nil || replayedRun.Status != workflowruntime.RunSucceeded || replayedIntent == nil || replayedIntent.Status != workflowruntime.TerminalIntentCompleted {
		t.Fatalf("completed reconcile replay = run %#v intent %#v err %v", replayedRun, replayedIntent, err)
	}
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	intentEvents, completionEvents := 0, 0
	for _, event := range events {
		if event.Type == workflowruntime.EventTerminalIntent {
			intentEvents++
		}
		if event.Type == workflowruntime.EventRunStatusChanged && event.Attributes["intended_status"] == string(workflowruntime.RunSucceeded) {
			completionEvents++
		}
	}
	if intentEvents != 1 || completionEvents != 1 {
		t.Fatalf("control completion events = intent %d completion %d", intentEvents, completionEvents)
	}
}

func controlFixture(t *testing.T, suffix string) (context.Context, *runtimetest.Store, time.Time, workflowruntime.RunID) {
	t.Helper()
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-control-" + suffix)
	createRun(t, store, runID, base)
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	return ctx, store, base, runID
}

func boolInputs(t *testing.T, first, second bool) values.ValueSet {
	t.Helper()
	metadata := values.Metadata{Producer: values.Producer{Kind: "test", Reference: "switch"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
	one, err := values.NewInline(first, metadata)
	if err != nil {
		t.Fatal(err)
	}
	two, err := values.NewInline(second, metadata)
	if err != nil {
		t.Fatal(err)
	}
	return values.ValueSet{"first": one, "second": two}
}
