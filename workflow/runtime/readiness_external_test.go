package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestReadinessRuleTruthTables(t *testing.T) {
	tests := []struct {
		name     string
		rule     graph.ReadyRule
		statuses []workflowruntime.NodeStatus
		want     workflowruntime.ReadinessDisposition
	}{
		{"all_success zero", graph.ReadyAllSuccess, nil, workflowruntime.ReadinessReady},
		{"all_success partial", graph.ReadyAllSuccess, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodePending}, workflowruntime.ReadinessWaiting},
		{"all_success impossible early", graph.ReadyAllSuccess, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodePending}, workflowruntime.ReadinessSkip},
		{"all_success terminal true", graph.ReadyAllSuccess, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeSucceeded}, workflowruntime.ReadinessReady},
		{"all_success terminal skip", graph.ReadyAllSuccess, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeSkipped}, workflowruntime.ReadinessSkip},

		{"all_done zero", graph.ReadyAllDone, nil, workflowruntime.ReadinessReady},
		{"all_done partial", graph.ReadyAllDone, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodeRunning}, workflowruntime.ReadinessWaiting},
		{"all_done terminal", graph.ReadyAllDone, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodeSkipped}, workflowruntime.ReadinessReady},

		{"one_failed zero", graph.ReadyOneFailed, nil, workflowruntime.ReadinessSkip},
		{"one_failed partial without failure", graph.ReadyOneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodePending}, workflowruntime.ReadinessWaiting},
		{"one_failed partial early", graph.ReadyOneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeTimedOut, workflowruntime.NodeRunning}, workflowruntime.ReadinessReady},
		{"one_failed terminal false", graph.ReadyOneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeSkipped}, workflowruntime.ReadinessSkip},
		{"one_failed terminal true", graph.ReadyOneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeFailed}, workflowruntime.ReadinessReady},

		{"all_failed zero", graph.ReadyAllFailed, nil, workflowruntime.ReadinessSkip},
		{"all_failed partial", graph.ReadyAllFailed, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodePending}, workflowruntime.ReadinessWaiting},
		{"all_failed impossible early", graph.ReadyAllFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodePending}, workflowruntime.ReadinessSkip},
		{"all_failed terminal true", graph.ReadyAllFailed, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodeCrashed}, workflowruntime.ReadinessReady},
		{"all_failed terminal false", graph.ReadyAllFailed, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodeSkipped}, workflowruntime.ReadinessSkip},

		{"none_failed zero", graph.ReadyNoneFailed, nil, workflowruntime.ReadinessReady},
		{"none_failed partial", graph.ReadyNoneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeBlocked}, workflowruntime.ReadinessWaiting},
		{"none_failed impossible early", graph.ReadyNoneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeCanceled, workflowruntime.NodePending}, workflowruntime.ReadinessSkip},
		{"none_failed terminal true", graph.ReadyNoneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeSkipped}, workflowruntime.ReadinessReady},
		{"none_failed terminal false", graph.ReadyNoneFailed, []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeTimedOut}, workflowruntime.ReadinessSkip},

		{"always zero", graph.ReadyAlways, nil, workflowruntime.ReadinessReady},
		{"always partial early", graph.ReadyAlways, []workflowruntime.NodeStatus{workflowruntime.NodeRunning, workflowruntime.NodePending}, workflowruntime.ReadinessReady},
		{"always terminal", graph.ReadyAlways, []workflowruntime.NodeStatus{workflowruntime.NodeFailed, workflowruntime.NodeSkipped}, workflowruntime.ReadinessReady},
		{"default is all_success", "", []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded}, workflowruntime.ReadinessReady},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evaluation, err := workflowruntime.EvaluateReadiness(test.rule, readinessDependencies("run-truth", test.statuses...))
			if err != nil || evaluation.Disposition != test.want {
				t.Fatalf("EvaluateReadiness = %#v, %v; want %s", evaluation, err, test.want)
			}
			if test.want != workflowruntime.ReadinessReady {
				if evaluation.Reason == nil || evaluation.Reason.Details["terminal"] == "" || evaluation.Reason.Details["nonterminal"] == "" {
					t.Fatalf("non-ready evaluation lacks aggregate diagnostics: %#v", evaluation)
				}
			}
		})
	}
}

func TestReadinessHardFailureAndHandledRouteSemantics(t *testing.T) {
	for _, status := range []workflowruntime.NodeStatus{
		workflowruntime.NodeFailed, workflowruntime.NodeTimedOut,
		workflowruntime.NodeCanceled, workflowruntime.NodeCrashed,
	} {
		t.Run(string(status), func(t *testing.T) {
			dependency := readinessDependencies("run-hard", status)[0]
			oneFailed, err := workflowruntime.EvaluateReadiness(graph.ReadyOneFailed, []workflowruntime.DependencyState{dependency})
			if err != nil || oneFailed.Disposition != workflowruntime.ReadinessReady {
				t.Fatalf("one_failed(%s) = %#v, %v", status, oneFailed, err)
			}
			noneFailed, err := workflowruntime.EvaluateReadiness(graph.ReadyNoneFailed, []workflowruntime.DependencyState{dependency})
			if err != nil || noneFailed.Disposition != workflowruntime.ReadinessSkip {
				t.Fatalf("none_failed(%s) = %#v, %v", status, noneFailed, err)
			}
			dependency.FailureHandled = true
			allSuccess, err := workflowruntime.EvaluateReadiness(graph.ReadyAllSuccess, []workflowruntime.DependencyState{dependency})
			if err != nil || allSuccess.Disposition != workflowruntime.ReadinessReady {
				t.Fatalf("handled all_success(%s) = %#v, %v", status, allSuccess, err)
			}
			oneFailed, err = workflowruntime.EvaluateReadiness(graph.ReadyOneFailed, []workflowruntime.DependencyState{dependency})
			if err != nil || oneFailed.Disposition != workflowruntime.ReadinessSkip {
				t.Fatalf("handled one_failed(%s) = %#v, %v", status, oneFailed, err)
			}
			if oneFailed.Reason.Details["failed"] != "1" || oneFailed.Reason.Details["handled"] != "1" ||
				oneFailed.Reason.Details["succeeded"] != "0" || oneFailed.Reason.Details["success_equivalent"] != "1" {
				t.Fatalf("handled diagnostics(%s) = %#v", status, oneFailed.Reason.Details)
			}
		})
	}
}

func TestReadinessRejectsAmbiguousDependenciesAndCanonicalizesDiagnostics(t *testing.T) {
	first := workflowruntime.DependencyState{InvocationID: invocationID("run-order", "a"), Status: workflowruntime.NodePending}
	second := workflowruntime.DependencyState{InvocationID: invocationID("run-order", "b"), Status: workflowruntime.NodeSucceeded}
	evaluation, err := workflowruntime.EvaluateReadiness(graph.ReadyAllDone, []workflowruntime.DependencyState{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if got := evaluation.Reason.Dependencies; len(got) != 2 || got[0] != first.InvocationID || got[1] != second.InvocationID {
		t.Fatalf("canonical dependencies = %#v", got)
	}
	if _, err := workflowruntime.EvaluateReadiness(graph.ReadyAllDone, []workflowruntime.DependencyState{first, first}); !errors.Is(err, workflowruntime.ErrInvalidReadiness) {
		t.Fatalf("duplicate dependency = %v", err)
	}
	crossRun := second
	crossRun.InvocationID.RunID = "other-run"
	if _, err := workflowruntime.EvaluateReadiness(graph.ReadyAllDone, []workflowruntime.DependencyState{first, crossRun}); !errors.Is(err, workflowruntime.ErrInvalidReadiness) {
		t.Fatalf("cross-run dependencies = %v", err)
	}
	handledSuccess := second
	handledSuccess.FailureHandled = true
	if _, err := workflowruntime.EvaluateReadiness(graph.ReadyAllDone, []workflowruntime.DependencyState{handledSuccess}); !errors.Is(err, workflowruntime.ErrInvalidReadiness) {
		t.Fatalf("handled success = %v", err)
	}
	if _, err := workflowruntime.EvaluateReadiness("unsupported", nil); !errors.Is(err, workflowruntime.ErrInvalidReadiness) {
		t.Fatalf("unsupported rule = %v", err)
	}
}

func TestProgressNodeDefersPredicateAndRefreshesBlockedDiagnostics(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	firstDependencyID := invocationID("run-progress", "dependency-a")
	secondDependencyID := invocationID("run-progress", "dependency-b")
	targetID := invocationID("run-progress", "target")
	createNode(t, store, firstDependencyID, workflowruntime.NodePending, 0, now)
	createNode(t, store, secondDependencyID, workflowruntime.NodePending, 0, now)
	createNode(t, store, targetID, workflowruntime.NodePending, 0, now)
	evaluator := &predicateStub{value: true}
	coordinator := workflowruntime.NewProgressionCoordinator(store, evaluator)
	request := workflowruntime.ProgressNodeRequest{
		InvocationID: targetID, Dependencies: []workflowruntime.DependencyRef{
			{InvocationID: firstDependencyID}, {InvocationID: secondDependencyID},
		},
		Rule: graph.ReadyAllDone, Predicate: &graph.Expression{Text: "inputs.enabled"}, At: now.Add(time.Second),
	}
	waiting, err := coordinator.ProgressNode(ctx, request)
	if err != nil || waiting.Disposition != workflowruntime.ReadinessWaiting ||
		waiting.Snapshot.Status != workflowruntime.NodeBlocked || evaluator.calls.Load() != 0 {
		t.Fatalf("waiting progression = %#v, %v; predicate calls=%d", waiting, err, evaluator.calls.Load())
	}
	if waiting.Snapshot.Blocked.Details["terminal"] != "0" || waiting.Snapshot.Blocked.Details["nonterminal"] != "2" {
		t.Fatalf("waiting details = %#v", waiting.Snapshot.Blocked)
	}

	makeTerminalNode(t, store, firstDependencyID, workflowruntime.NodeSucceeded, now.Add(2*time.Second))
	request.At = now.Add(3 * time.Second)
	refreshed, err := coordinator.ProgressNode(ctx, request)
	if err != nil || refreshed.Disposition != workflowruntime.ReadinessWaiting ||
		refreshed.Snapshot.Status != workflowruntime.NodeBlocked || evaluator.calls.Load() != 0 {
		t.Fatalf("refreshed progression = %#v, %v; predicate calls=%d", refreshed, err, evaluator.calls.Load())
	}
	if refreshed.Snapshot.Generation <= waiting.Snapshot.Generation ||
		refreshed.Snapshot.Blocked.Details["terminal"] != "1" || refreshed.Snapshot.Blocked.Details["nonterminal"] != "1" ||
		refreshed.Snapshot.Blocked.Details["succeeded"] != "1" {
		t.Fatalf("refreshed waiting details = %#v", refreshed.Snapshot.Blocked)
	}

	makeTerminalNode(t, store, secondDependencyID, workflowruntime.NodeSucceeded, now.Add(4*time.Second))
	request.At = now.Add(5 * time.Second)
	ready, err := coordinator.ProgressNode(ctx, request)
	if err != nil || ready.Disposition != workflowruntime.ReadinessReady || ready.Snapshot.Status != workflowruntime.NodeReady ||
		!ready.PredicateEvaluated || !ready.PredicateResult || evaluator.calls.Load() != 1 {
		t.Fatalf("blocked-to-ready = %#v, %v; predicate calls=%d", ready, err, evaluator.calls.Load())
	}
}

func TestProgressNodePredicateFalseTrueErrorAndIdempotency(t *testing.T) {
	now := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	t.Run("false_and_idempotent", func(t *testing.T) {
		store := runtimetest.NewStore()
		id := invocationID("run-false", "target")
		createNode(t, store, id, workflowruntime.NodePending, 0, now)
		evaluator := &predicateStub{value: false}
		coordinator := workflowruntime.NewProgressionCoordinator(store, evaluator)
		request := workflowruntime.ProgressNodeRequest{
			InvocationID: id, Predicate: &graph.Expression{Text: "inputs.enabled"}, At: now.Add(time.Second),
		}
		result, err := coordinator.ProgressNode(context.Background(), request)
		if err != nil || result.Snapshot.Status != workflowruntime.NodeSkipped || !result.PredicateEvaluated || result.PredicateResult ||
			result.Reason.Code != workflowruntime.ReasonPredicateFalse || evaluator.calls.Load() != 1 {
			t.Fatalf("false predicate = %#v, %v", result, err)
		}
		assertExplanationEvent(t, result.Event, result.Reason)
		before, err := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: id.RunID})
		if err != nil {
			t.Fatal(err)
		}
		replay, err := coordinator.ProgressNode(context.Background(), request)
		after, listErr := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: id.RunID})
		if err != nil || listErr != nil || replay.Snapshot.Status != workflowruntime.NodeSkipped || evaluator.calls.Load() != 1 || len(after) != len(before) {
			t.Fatalf("skip replay = %#v, %v events=%d/%d calls=%d", replay, err, len(before), len(after), evaluator.calls.Load())
		}
	})

	t.Run("true", func(t *testing.T) {
		store := runtimetest.NewStore()
		id := invocationID("run-true", "target")
		createNode(t, store, id, workflowruntime.NodePending, 0, now)
		result, err := workflowruntime.NewProgressionCoordinator(store, &predicateStub{value: true}).ProgressNode(
			context.Background(), workflowruntime.ProgressNodeRequest{
				InvocationID: id, Predicate: &graph.Expression{Text: "inputs.enabled"}, At: now.Add(time.Second),
			},
		)
		if err != nil || result.Snapshot.Status != workflowruntime.NodeReady || !result.PredicateEvaluated || !result.PredicateResult {
			t.Fatalf("true predicate = %#v, %v", result, err)
		}
	})

	t.Run("error_preserves_pending", func(t *testing.T) {
		store := runtimetest.NewStore()
		id := invocationID("run-error", "target")
		createNode(t, store, id, workflowruntime.NodePending, 0, now)
		failure := errors.New("predicate failed")
		_, err := workflowruntime.NewProgressionCoordinator(store, &predicateStub{err: failure}).ProgressNode(
			context.Background(), workflowruntime.ProgressNodeRequest{
				InvocationID: id, Predicate: &graph.Expression{Text: "inputs.enabled"}, At: now.Add(time.Second),
			},
		)
		loaded, loadErr := store.LoadNodeInvocation(context.Background(), id)
		if !errors.Is(err, failure) || loadErr != nil || loaded.Status != workflowruntime.NodePending || loaded.Generation != 1 {
			t.Fatalf("predicate error = %v; node=%#v, %v", err, loaded, loadErr)
		}
	})
}

func TestProgressNodeUsesTypedExpressionEngineAndPreservesSourceDiagnostic(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 16, 30, 0, 0, time.UTC)
	trueID := invocationID("run-expression", "true-target")
	errorID := invocationID("run-expression", "error-target")
	createNode(t, store, trueID, workflowruntime.NodePending, 0, now)
	createNode(t, store, errorID, workflowruntime.NodePending, 0, now)
	coordinator := workflowruntime.NewProgressionCoordinator(store, nil)
	expressionContext := values.ExpressionContext{Inputs: testValueSet(t, true)}

	result, err := coordinator.ProgressNode(ctx, workflowruntime.ProgressNodeRequest{
		InvocationID: trueID, Predicate: &graph.Expression{Text: "inputs.payload"},
		ExpressionContext: expressionContext, At: now.Add(time.Second),
	})
	if err != nil || result.Snapshot.Status != workflowruntime.NodeReady ||
		!result.PredicateEvaluated || !result.PredicateResult {
		t.Fatalf("typed true predicate = %#v, %v", result, err)
	}

	source := &graph.SourceRef{
		Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 9, StartColumn: 7,
		Path: []string{"steps", "error-target", "if"},
	}
	_, err = coordinator.ProgressNode(ctx, workflowruntime.ProgressNodeRequest{
		InvocationID: errorID, Predicate: &graph.Expression{Text: "inputs.missing", Source: source},
		ExpressionContext: expressionContext, At: now.Add(time.Second),
	})
	var expressionError *values.ExpressionError
	if !errors.As(err, &expressionError) || expressionError.Diagnostic.Code != values.CodeExpressionUnresolved ||
		!reflect.DeepEqual(expressionError.Diagnostic.Source, source) {
		t.Fatalf("source-mapped predicate error = %#v, %v", expressionError, err)
	}
	loaded, loadErr := store.LoadNodeInvocation(ctx, errorID)
	if loadErr != nil || loaded.Status != workflowruntime.NodePending || loaded.Generation != 1 {
		t.Fatalf("expression error mutated target = %#v, %v", loaded, loadErr)
	}
}

func TestProgressNodeFailureTimeoutHandledAndBlockedToSkip(t *testing.T) {
	now := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	for index, test := range []struct {
		name    string
		status  workflowruntime.NodeStatus
		rule    graph.ReadyRule
		handled bool
		want    workflowruntime.NodeStatus
	}{
		{"default failure skips", workflowruntime.NodeFailed, "", false, workflowruntime.NodeSkipped},
		{"default timeout skips", workflowruntime.NodeTimedOut, "", false, workflowruntime.NodeSkipped},
		{"one_failed handles failure", workflowruntime.NodeFailed, graph.ReadyOneFailed, false, workflowruntime.NodeReady},
		{"one_failed handles timeout", workflowruntime.NodeTimedOut, graph.ReadyOneFailed, false, workflowruntime.NodeReady},
		{"handled timeout permits default", workflowruntime.NodeTimedOut, "", true, workflowruntime.NodeReady},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := runtimetest.NewStore()
			runID := workflowruntime.RunID(fmt.Sprintf("run-propagation-%d", index))
			dependencyID := invocationID(runID, "dependency")
			targetID := invocationID(runID, "target")
			createNode(t, store, dependencyID, workflowruntime.NodeReady, 0, now)
			makeTerminalNode(t, store, dependencyID, test.status, now.Add(time.Second))
			createNode(t, store, targetID, workflowruntime.NodePending, 0, now)
			result, err := workflowruntime.NewProgressionCoordinator(store, nil).ProgressNode(
				context.Background(), workflowruntime.ProgressNodeRequest{
					InvocationID: targetID, Rule: test.rule,
					Dependencies: []workflowruntime.DependencyRef{{InvocationID: dependencyID, FailureHandled: test.handled}},
					At:           now.Add(3 * time.Second),
				},
			)
			if err != nil || result.Snapshot.Status != test.want {
				t.Fatalf("progression = %#v, %v; want %s", result, err, test.want)
			}
			if test.want == workflowruntime.NodeSkipped {
				assertExplanationEvent(t, result.Event, result.Reason)
			}
		})
	}

	store := runtimetest.NewStore()
	dependencyID := invocationID("run-blocked-skip", "dependency")
	targetID := invocationID("run-blocked-skip", "target")
	createNode(t, store, dependencyID, workflowruntime.NodePending, 0, now)
	createNode(t, store, targetID, workflowruntime.NodePending, 0, now)
	coordinator := workflowruntime.NewProgressionCoordinator(store, nil)
	request := workflowruntime.ProgressNodeRequest{
		InvocationID: targetID, Dependencies: []workflowruntime.DependencyRef{{InvocationID: dependencyID}}, At: now.Add(time.Second),
	}
	waiting, err := coordinator.ProgressNode(context.Background(), request)
	if err != nil || waiting.Snapshot.Status != workflowruntime.NodeBlocked {
		t.Fatalf("initial block = %#v, %v", waiting, err)
	}
	makeTerminalNode(t, store, dependencyID, workflowruntime.NodeFailed, now.Add(2*time.Second))
	request.At = now.Add(4 * time.Second)
	skipped, err := coordinator.ProgressNode(context.Background(), request)
	if err != nil || skipped.Snapshot.Status != workflowruntime.NodeSkipped || skipped.Reason.Code != workflowruntime.ReasonReadinessUnsatisfied {
		t.Fatalf("blocked-to-skip = %#v, %v", skipped, err)
	}
	assertExplanationEvent(t, skipped.Event, skipped.Reason)
}

func TestProgressNodeRejectsDependencyIdentityErrorsBeforeMutation(t *testing.T) {
	now := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	targetID := invocationID("run-identities", "target")
	dependencyID := invocationID("run-identities", "dependency")
	crossRunID := invocationID("other-run", "dependency")
	createNode(t, store, targetID, workflowruntime.NodePending, 0, now)
	createNode(t, store, dependencyID, workflowruntime.NodePending, 0, now)
	createNode(t, store, crossRunID, workflowruntime.NodePending, 0, now)
	coordinator := workflowruntime.NewProgressionCoordinator(store, nil)
	for _, test := range []struct {
		name string
		refs []workflowruntime.DependencyRef
	}{
		{"duplicate", []workflowruntime.DependencyRef{{InvocationID: dependencyID}, {InvocationID: dependencyID}}},
		{"cross_run", []workflowruntime.DependencyRef{{InvocationID: crossRunID}}},
		{"self", []workflowruntime.DependencyRef{{InvocationID: targetID}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := coordinator.ProgressNode(context.Background(), workflowruntime.ProgressNodeRequest{
				InvocationID: targetID, Dependencies: test.refs, At: now.Add(time.Second),
			})
			loaded, loadErr := store.LoadNodeInvocation(context.Background(), targetID)
			if !errors.Is(err, workflowruntime.ErrInvalidReadiness) || loadErr != nil || loaded.Status != workflowruntime.NodePending || loaded.Generation != 1 {
				t.Fatalf("identity error=%v node=%#v load=%v", err, loaded, loadErr)
			}
		})
	}
}

func TestSkippedTransitionExplanationReplayAndConflict(t *testing.T) {
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 19, 0, 0, 0, time.UTC)
	id := invocationID("run-explanation", "target")
	createNode(t, store, id, workflowruntime.NodePending, 0, now)
	reason := &workflowruntime.BlockedReason{
		Code: workflowruntime.ReasonPredicateFalse, Message: "if predicate evaluated to false",
		Dependencies: []workflowruntime.NodeInvocationID{invocationID(id.RunID, "z"), invocationID(id.RunID, "a")},
		Details:      map[string]string{"rule": "all_success", "result": "false"},
	}
	applied, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 1, To: workflowruntime.NodeSkipped,
		Explanation: reason, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertExplanationEvent(t, applied.Event, reason)
	replayReason := cloneBlockedForTest(reason)
	replayReason.Dependencies[0], replayReason.Dependencies[1] = replayReason.Dependencies[1], replayReason.Dependencies[0]
	replay, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: applied.Snapshot.Generation, To: workflowruntime.NodeSkipped,
		Explanation: replayReason, At: now.Add(time.Second),
	})
	if err != nil || replay.Outcome != workflowruntime.TransitionNoOp || replay.Event != nil {
		t.Fatalf("explanation replay = %#v, %v", replay, err)
	}
	different := cloneBlockedForTest(reason)
	different.Message = "different"
	if _, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: applied.Snapshot.Generation, To: workflowruntime.NodeSkipped,
		Explanation: different, At: now.Add(time.Second),
	}); !errors.Is(err, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("different explanation = %v", err)
	}
	if _, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: applied.Snapshot.Generation, To: workflowruntime.NodeTimedOut,
		Explanation: reason, At: now.Add(2 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("non-skipped explanation = %v", err)
	}
}

func TestBlockedTransitionDiagnosticRefreshContract(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 19, 30, 0, 0, time.UTC)
	id := invocationID("run-blocked-refresh", "target")
	createNode(t, store, id, workflowruntime.NodePending, 0, now)
	initialReason := &workflowruntime.BlockedReason{
		Code: workflowruntime.ReasonReadinessWaiting, Message: "waiting",
		Details: map[string]string{"terminal": "0", "nonterminal": "2"},
	}
	initial, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 1, To: workflowruntime.NodeBlocked,
		Blocked: initialReason, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: initial.Snapshot.Generation, To: workflowruntime.NodeBlocked,
		Blocked: cloneBlockedForTest(initialReason), At: now.Add(time.Second),
	})
	if err != nil || exact.Outcome != workflowruntime.TransitionNoOp || exact.Event != nil {
		t.Fatalf("exact blocked replay = %#v, %v", exact, err)
	}
	changedReason := cloneBlockedForTest(initialReason)
	changedReason.Details["terminal"] = "1"
	changedReason.Details["nonterminal"] = "1"
	if _, transitionErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: initial.Snapshot.Generation, To: workflowruntime.NodeBlocked,
		Blocked: changedReason, At: now.Add(time.Second),
	}); !errors.Is(transitionErr, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("equal-time changed reason = %v", transitionErr)
	}
	if _, transitionErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: initial.Snapshot.Generation, To: workflowruntime.NodeBlocked,
		Blocked: changedReason, At: now,
	}); !errors.Is(transitionErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("stale changed reason = %v", transitionErr)
	}
	refreshed, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: initial.Snapshot.Generation, To: workflowruntime.NodeBlocked,
		Blocked: changedReason, At: now.Add(2 * time.Second),
	})
	if err != nil || refreshed.Outcome != workflowruntime.TransitionApplied ||
		refreshed.Snapshot.Generation != initial.Snapshot.Generation+1 ||
		!reflect.DeepEqual(refreshed.Snapshot.Blocked, changedReason) || refreshed.Event == nil {
		t.Fatalf("later blocked refresh = %#v, %v", refreshed, err)
	}
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: refreshed.Snapshot.Generation, To: workflowruntime.NodeBlocked,
		Blocked: cloneBlockedForTest(changedReason), At: now.Add(3 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("later identical reason = %v", err)
	}
}

type predicateStub struct {
	value bool
	err   error
	calls atomic.Int64
}

func (s *predicateStub) EvaluateBool(graph.Expression, values.ExpressionContext, values.ExpressionOptions) (bool, error) {
	s.calls.Add(1)
	return s.value, s.err
}

func readinessDependencies(runID workflowruntime.RunID, statuses ...workflowruntime.NodeStatus) []workflowruntime.DependencyState {
	result := make([]workflowruntime.DependencyState, len(statuses))
	for i, status := range statuses {
		result[i] = workflowruntime.DependencyState{
			InvocationID: workflowruntime.NodeInvocationID{RunID: runID, NodeID: "dependency-" + string(rune('a'+i))},
			Status:       status,
		}
	}
	return result
}

func makeTerminalNode(t *testing.T, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, at time.Time) {
	t.Helper()
	node, err := store.LoadNodeInvocation(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if node.Status == workflowruntime.NodePending {
		transition, transitionErr := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
			InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at,
		})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		node = transition.Snapshot
	}
	if status == workflowruntime.NodeSkipped || status == workflowruntime.NodeCanceled {
		_, err = store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
			InvocationID: id, ExpectedGeneration: node.Generation, To: status, At: at,
		})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	claim := claimNode(t, store, id, node.ClaimGeneration, "terminal-worker", "terminal-token-"+id.NodeID, "terminal-claim-"+id.NodeID, at, at.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: claimed.Generation,
		Claim: claim, Executor: testExecutor(), At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	var failure *workflowruntime.Failure
	if status != workflowruntime.NodeSucceeded {
		failure = &workflowruntime.Failure{Code: "test_" + string(status), Message: "test terminal outcome"}
	}
	_, err = store.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: status, NextNodeStatus: status, Failure: failure, At: at,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertExplanationEvent(t *testing.T, event *workflowruntime.Event, reason *workflowruntime.BlockedReason) {
	t.Helper()
	if event == nil || event.Attributes["explanation"] == "" || event.Attributes["explanation_code"] != reason.Code {
		t.Fatalf("explanation event = %#v", event)
	}
	var decoded workflowruntime.BlockedReason
	if err := json.Unmarshal([]byte(event.Attributes["explanation"]), &decoded); err != nil {
		t.Fatalf("decode explanation: %v", err)
	}
	want := cloneBlockedForTest(reason)
	sortInvocationIDs(want.Dependencies)
	if !reflect.DeepEqual(decoded, *want) {
		t.Fatalf("decoded explanation = %#v, want %#v", decoded, *want)
	}
}

func sortInvocationIDs(ids []workflowruntime.NodeInvocationID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0; j-- {
			left, right := ids[j-1], ids[j]
			less := right.RunID < left.RunID || right.RunID == left.RunID &&
				(right.NodeID < left.NodeID || right.NodeID == left.NodeID && right.Iteration < left.Iteration)
			if !less {
				break
			}
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
