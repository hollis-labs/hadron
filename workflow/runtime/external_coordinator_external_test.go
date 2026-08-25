package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestExternalOperationCoordinatorPersistsProgressSuccessAndFinalizeWarning(t *testing.T) {
	fixture := dispatchExternalOperation(t, "external-reconcile-success")
	observations, heartbeats, finalized := 0, 0, 0
	fixture.kind.HeartbeatFunc = func(context.Context, stepkind.ExternalOperationRef) error {
		heartbeats++
		return nil
	}
	fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		observations++
		if observations == 1 {
			return stepkind.Observation{State: stepkind.ObservationPending, Progress: map[string]string{"percent": "50"}}, nil
		}
		result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "complete")}}
		return stepkind.Observation{State: stepkind.ObservationSucceeded, Progress: map[string]string{"percent": "100"}, Result: &result}, nil
	}
	fixture.kind.FinalizeFunc = func(_ context.Context, finalization stepkind.Finalization) error {
		finalized++
		if finalization.Invocation.State != nil || finalization.Invocation.Invocation.Identity.Attempt != 1 || finalization.Result.Outcome != stepkind.StepCompleted {
			t.Fatalf("Finalize() = %#v", finalization)
		}
		return errors.New("cleanup warning contains process-only detail")
	}
	coordinator := fixture.coordinator(t)
	pending, err := coordinator.Reconcile(context.Background(), fixture.attempt)
	if err != nil || pending.Operation.Status != stepkind.ObservationPending || pending.Operation.Progress["percent"] != "50" ||
		pending.Operation.LastHeartbeatAt.IsZero() || pending.Operation.LastObservedAt.IsZero() || pending.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("first Reconcile() = %#v, %v", pending, err)
	}
	fixture.now = fixture.now.Add(time.Second)
	completed, err := coordinator.Reconcile(context.Background(), fixture.attempt)
	if err != nil || completed.Operation.Status != stepkind.ObservationSucceeded || completed.Node.Status != workflowruntime.NodeSucceeded ||
		completed.Attempt.Status != workflowruntime.NodeSucceeded || completed.Outputs == nil || len(completed.Warnings) != 1 || completed.Warnings[0].Event == nil ||
		heartbeats != 2 || finalized != 1 {
		t.Fatalf("second Reconcile() = %#v, %v heartbeats=%d finalized=%d", completed, err, heartbeats, finalized)
	}
	loaded, err := fixture.store.LoadValues(context.Background(), *completed.Outputs)
	if err != nil || loaded["result"].Inline != "complete" {
		t.Fatalf("LoadValues(external outputs) = %#v, %v", loaded, err)
	}
	recovered, err := coordinator.Recover(context.Background(), workflowruntime.ExternalOperationQuery{})
	if err != nil || len(recovered) != 0 {
		t.Fatalf("Recover(terminal) = %#v, %v", recovered, err)
	}
}

func TestExternalOperationCoordinatorCancelIntentReplayPersistsCanceled(t *testing.T) {
	fixture := dispatchExternalOperation(t, "external-cancel-replay")
	operation, err := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
	if err != nil {
		t.Fatal(err)
	}
	requested, err := fixture.store.RequestExternalOperationCancel(context.Background(), workflowruntime.RequestExternalOperationCancelRequest{
		Attempt: fixture.attempt, ExpectedOperationGeneration: operation.Generation, At: fixture.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancels := 0
	fixture.kind.CancelFunc = func(context.Context, stepkind.ExternalOperationRef) error {
		cancels++
		if cancels == 1 {
			return errors.New("cancel transport unavailable")
		}
		return nil
	}
	fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		failure := &stepkind.ExecutionError{Code: "remote-canceled", Message: "remote operation canceled", Classification: stepkind.RetryPermanent}
		return stepkind.Observation{State: stepkind.ObservationCanceled, Failure: failure}, nil
	}
	fixture.now = fixture.now.Add(time.Second)
	coordinator := fixture.coordinator(t)
	pending, cancelErr := coordinator.RequestCancel(context.Background(), fixture.attempt)
	if cancelErr == nil || pending.Operation.Status != stepkind.ObservationPending || pending.Operation.CancelRequestedAt.IsZero() ||
		pending.Attempt.Status != workflowruntime.NodeRunning || *fixture.executions != 1 {
		t.Fatalf("RequestCancel(transport error) = %#v, %v executions=%d", pending, cancelErr, *fixture.executions)
	}
	fixture.now = fixture.now.Add(time.Second)
	result, reconcileErr := coordinator.RequestCancel(context.Background(), fixture.attempt)
	var dispatchErr *workflowruntime.DispatchError
	if !errors.As(reconcileErr, &dispatchErr) || dispatchErr.Stage != workflowruntime.DispatchObserve || dispatchErr.Kind != "external-kind" || dispatchErr.Version != "v1" ||
		result.Operation.Status != stepkind.ObservationCanceled || result.Node.Status != workflowruntime.NodeCanceled || result.Attempt.Status != workflowruntime.NodeCanceled ||
		result.Operation.Generation <= requested.Operation.Generation || cancels != 2 || *fixture.executions != 1 {
		t.Fatalf("RequestCancel() = %#v, %v cancels=%d", result, reconcileErr, cancels)
	}
}

func TestExternalOperationCoordinatorAppliesPersistedVerificationModifier(t *testing.T) {
	t.Run("deterministic pass", func(t *testing.T) {
		fixture := dispatchExternalOperationWithVerification(t, "external-verification-pass", &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}})
		fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
			result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "complete")}}
			return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
		}
		completed, err := fixture.coordinator(t).Reconcile(context.Background(), fixture.attempt)
		if err != nil || completed.Node.Status != workflowruntime.NodeSucceeded || completed.Verification == nil || completed.Verification.Report.Status != verification.ReportPassed {
			t.Fatalf("Reconcile() = %#v, %v", completed, err)
		}
	})

	t.Run("missing process-local activity fails closed", func(t *testing.T) {
		fixture := dispatchExternalOperationWithVerification(t, "external-verification-evidence", &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "remote.write"}}}})
		fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
			result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "complete")}}
			return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
		}
		completed, err := fixture.coordinator(t).Reconcile(context.Background(), fixture.attempt)
		if err == nil || completed.Node.Status != workflowruntime.NodeFailed || completed.Attempt.Failure == nil || completed.Attempt.Failure.Code != "verification_failed" || completed.Verification == nil {
			t.Fatalf("Reconcile() = %#v, %v", completed, err)
		}
	})
}

func TestExternalOperationCoordinatorAtomicallyFencesCompetingVerifiedObservers(t *testing.T) {
	fixture := dispatchExternalOperationWithVerification(t, "external-verification-contention", &graph.VerificationSpec{
		Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}},
	})
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	completedValue := dispatchValue(t, "result", "complete")
	fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		entered <- struct{}{}
		<-release
		result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{
			"result": completedValue,
		}}
		return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
	}
	coordinators := []*workflowruntime.ExternalOperationCoordinator{fixture.coordinator(t), fixture.coordinator(t)}
	type outcome struct {
		result workflowruntime.ExternalOperationResult
		err    error
	}
	outcomes := make(chan outcome, len(coordinators))
	var group sync.WaitGroup
	for _, coordinator := range coordinators {
		group.Add(1)
		go func(coordinator *workflowruntime.ExternalOperationCoordinator) {
			defer group.Done()
			result, err := coordinator.Reconcile(context.Background(), fixture.attempt)
			outcomes <- outcome{result: result, err: err}
		}(coordinator)
	}
	<-entered
	<-entered
	close(release)
	group.Wait()
	close(outcomes)

	succeeded, stale := 0, 0
	var winner workflowruntime.ExternalOperationResult
	for outcome := range outcomes {
		switch {
		case outcome.err == nil:
			succeeded++
			winner = outcome.result
		case errors.Is(outcome.err, workflowruntime.ErrCASMismatch):
			stale++
		default:
			t.Fatalf("competing Reconcile() = %#v, %v", outcome.result, outcome.err)
		}
	}
	if succeeded != 1 || stale != 1 || winner.Verification == nil || winner.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("contention succeeded=%d stale=%d winner=%#v", succeeded, stale, winner)
	}
	events, err := fixture.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: fixture.attempt.Invocation.RunID})
	if err != nil {
		t.Fatal(err)
	}
	verificationEvents, finishedEvents := 0, 0
	verificationSequence, finishedSequence := uint64(0), uint64(0)
	for _, event := range events {
		switch event.Type {
		case workflowruntime.EventNodeVerificationCompleted:
			verificationEvents++
			verificationSequence = event.Sequence
		case workflowruntime.EventNodeAttemptFinished:
			finishedEvents++
			finishedSequence = event.Sequence
		}
	}
	if verificationEvents != 1 || finishedEvents != 1 || verificationSequence >= finishedSequence {
		t.Fatalf("atomic verification event history = %#v", events)
	}
	replayed, err := workflowruntime.PersistVerificationForTest(
		context.Background(), fixture.store, nil, nil, fixture.attempt,
		winner.Verification.Report, fixture.now.Add(time.Second),
	)
	if err != nil || !replayed.Replayed || replayed.Ref != winner.Verification.Ref {
		t.Fatalf("verification replay after contention = %#v, %v", replayed, err)
	}
}

func TestExternalOperationCoordinatorContextCancellationLeavesRemotePending(t *testing.T) {
	fixture := dispatchExternalOperation(t, "external-context-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := fixture.coordinator(t).Reconcile(ctx, fixture.attempt)
	if !errors.Is(err, context.Canceled) || result.Operation.Status != stepkind.ObservationPending || result.Node.Status != workflowruntime.NodeWaiting || result.Attempt.Status != workflowruntime.NodeRunning {
		t.Fatalf("Reconcile(canceled) = %#v, %v", result, err)
	}
	loaded, loadErr := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
	if loadErr != nil || loaded.Status != stepkind.ObservationPending || loaded.Generation != result.Operation.Generation {
		t.Fatalf("canceled reconcile mutated durable operation: %#v, %v", loaded, loadErr)
	}
}

func TestExternalOperationCoordinatorPreObserveFailuresDoNotSetObservedTime(t *testing.T) {
	t.Run("heartbeat transport", func(t *testing.T) {
		fixture := dispatchExternalOperation(t, "external-heartbeat-failure")
		fixture.kind.HeartbeatFunc = func(context.Context, stepkind.ExternalOperationRef) error {
			return errors.New("heartbeat transport unavailable")
		}
		result, err := fixture.coordinator(t).Reconcile(context.Background(), fixture.attempt)
		if err == nil || result.Operation.Status != stepkind.ObservationPending || !result.Operation.LastObservedAt.IsZero() || result.Operation.Generation != 1 {
			t.Fatalf("Reconcile(heartbeat failure) = %#v, %v", result, err)
		}
	})
	t.Run("missing registration", func(t *testing.T) {
		fixture := dispatchExternalOperation(t, "external-missing-registration")
		coordinator, err := workflowruntime.NewExternalOperationCoordinator(workflowruntime.ExternalOperationOptions{
			Store: fixture.store, Registry: stepkind.NewRegistry(), Now: func() time.Time { return fixture.now },
		})
		if err != nil {
			t.Fatal(err)
		}
		result, reconcileErr := coordinator.Reconcile(context.Background(), fixture.attempt)
		if reconcileErr == nil || result.Operation.Status != stepkind.ObservationFailed || !result.Operation.LastObservedAt.IsZero() || result.Attempt.Status != workflowruntime.NodeFailed {
			t.Fatalf("Reconcile(missing registration) = %#v, %v", result, reconcileErr)
		}
	})
}

func TestExternalOperationCoordinatorObserveErrorRemainsPendingAndRetries(t *testing.T) {
	fixture := dispatchExternalOperation(t, "external-observe-failure")
	observations := 0
	fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		observations++
		if observations == 1 {
			return stepkind.Observation{}, errors.New("raw provider detail")
		}
		result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "recovered")}}
		return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
	}
	coordinator := fixture.coordinator(t)
	result, err := coordinator.Reconcile(context.Background(), fixture.attempt)
	var dispatchErr *workflowruntime.DispatchError
	if !errors.As(err, &dispatchErr) || dispatchErr.Kind != "external-kind" || dispatchErr.Version != "v1" ||
		result.Operation.Status != stepkind.ObservationPending || result.Operation.LastHeartbeatAt.IsZero() || result.Operation.LastObservedAt.IsZero() ||
		result.Attempt.Status != workflowruntime.NodeRunning || result.Node.Status != workflowruntime.NodeWaiting || *fixture.executions != 1 {
		t.Fatalf("Reconcile(observe failure) = %#v, %v", result, err)
	}
	fixture.now = fixture.now.Add(time.Second)
	recovered, err := coordinator.Reconcile(context.Background(), fixture.attempt)
	if err != nil || recovered.Operation.Status != stepkind.ObservationSucceeded || recovered.Attempt.Status != workflowruntime.NodeSucceeded || *fixture.executions != 1 {
		t.Fatalf("Reconcile(retry) = %#v, %v executions=%d", recovered, err, *fixture.executions)
	}
}

type dispatchedExternalFixture struct {
	store      *inmemory.Store
	registry   *stepkind.MemoryRegistry
	kind       *stepkindtest.LifecycleKind
	attempt    workflowruntime.AttemptID
	now        time.Time
	executions *int
}

func dispatchExternalOperation(t *testing.T, run string) *dispatchedExternalFixture {
	return dispatchExternalOperationWithVerification(t, run, nil)
}

func dispatchExternalOperationWithVerification(t *testing.T, run string, modifier *graph.VerificationSpec) *dispatchedExternalFixture {
	t.Helper()
	store, claim, node, base := dispatchFixture(t, run)
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("external-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{Kind: "job", ID: "job-" + run}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	now := base.Add(3 * time.Second)
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "external-kind", KindVersion: "v1", Config: graph.Config{"large": json.Number("9007199254740993")}, Verification: modifier},
	})
	if err != nil || result.External == nil || result.Node.Status != workflowruntime.NodeWaiting || result.Attempt.Status != workflowruntime.NodeRunning {
		t.Fatalf("Dispatch(external) = %#v, %v", result, err)
	}
	large, ok := result.External.Invocation.Config["large"].(json.Number)
	if !ok || large.String() != "9007199254740993" {
		t.Fatalf("fake external invocation large number = %#v (%T)", result.External.Invocation.Config["large"], result.External.Invocation.Config["large"])
	}
	loaded, err := store.LoadExternalOperation(context.Background(), result.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	large, ok = loaded.Invocation.Config["large"].(json.Number)
	if !ok || large.String() != "9007199254740993" {
		t.Fatalf("loaded fake external invocation large number = %#v (%T)", loaded.Invocation.Config["large"], loaded.Invocation.Config["large"])
	}
	return &dispatchedExternalFixture{store: store, registry: registry, kind: kind, attempt: result.Attempt.ID, now: base.Add(4 * time.Second), executions: &executions}
}

func (f *dispatchedExternalFixture) coordinator(t *testing.T) *workflowruntime.ExternalOperationCoordinator {
	t.Helper()
	coordinator, err := workflowruntime.NewExternalOperationCoordinator(workflowruntime.ExternalOperationOptions{
		Store: f.store, Registry: f.registry, Now: func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}
