package checkpoint_test

import (
	"errors"
	"testing"
	"time"

	checkpointadapter "github.com/hollis-labs/hadron/workflow/adapters/checkpoint"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestCheckpointDispatcherResumeReplayRestartAndTimeout(t *testing.T) {
	t.Run("resume-replay-restart", func(t *testing.T) {
		fixture := newCheckpointRuntime(t, "checkpoint-resume-runtime")
		first := fixture.dispatch(t)
		if first.Node.Status != workflowruntime.NodeWaiting || first.Wait == nil || first.Attempt.Status != workflowruntime.NodeRunning {
			t.Fatalf("initial checkpoint dispatch = %#v", first)
		}
		payload := checkpointResume(t, map[string]any{"decision": "approve"}, values.RedactionPrivate, values.RetentionRun)
		command := workflowruntime.ResumeCommand{
			WaitID: first.Wait.Ref.ID, Correlation: first.Wait.Correlation, WakeSource: workflowwait.WakeGate,
			Responder: workflowwait.Responder{Kind: "user", Reference: "reviewer-1"}, Payload: payload,
			IdempotencyKey: "checkpoint-resume-1", ReceivedAt: checkpointTime.Add(time.Minute),
		}
		coordinator := workflowruntime.WaitCoordinator{Store: fixture.store}
		resumed, err := coordinator.Resume(t.Context(), command)
		if err != nil || resumed.Outcome != workflowruntime.ResumeApplied || resumed.Node.Status != workflowruntime.NodeReady {
			t.Fatalf("Resume = %#v, %v", resumed, err)
		}
		replayed, err := coordinator.Resume(t.Context(), command)
		if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Values != resumed.Values {
			t.Fatalf("resume replay = %#v, %v", replayed, err)
		}
		conflict := command
		conflict.Payload = checkpointResume(t, map[string]any{"decision": "reject"}, values.RedactionPrivate, values.RetentionRun)
		if _, conflictErr := coordinator.Resume(t.Context(), conflict); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
			t.Fatalf("conflicting resume = %v", conflictErr)
		}

		// Reconstruct the registry and dispatcher after the durable resume to
		// prove no adapter process memory is required for continuation.
		registry := stepkind.NewRegistry()
		if _, registerErr := checkpointadapter.Register(registry, checkpointOptions(nil, nil)); registerErr != nil {
			t.Fatal(registerErr)
		}
		claim, ok, err := workflowruntime.NewReadyQueueCoordinator(fixture.store, nil).ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
			RunID: fixture.runID, Owner: "worker-restarted", Token: "claim-restarted", IdempotencyKey: "claim-restarted",
			Now: checkpointTime.Add(2 * time.Minute), LeaseUntil: checkpointTime.Add(time.Hour),
		})
		if err != nil || !ok {
			t.Fatalf("ClaimNext(restarted) = %#v, %t, %v", claim, ok, err)
		}
		dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: fixture.store, Registry: registry, Now: func() time.Time { return checkpointTime.Add(3 * time.Minute) }})
		if err != nil {
			t.Fatal(err)
		}
		completed, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: fixture.node})
		if err != nil || completed.Node.Status != workflowruntime.NodeSucceeded || completed.Outputs == nil {
			t.Fatalf("continued Dispatch = %#v, %v", completed, err)
		}
		outputs, err := fixture.store.LoadValues(t.Context(), *completed.Outputs)
		if err != nil || outputs["decision"].Inline != "approve" || outputs["timed_out"].Inline != false {
			t.Fatalf("durable checkpoint outputs = %#v, %v", outputs, err)
		}
	})

	t.Run("timeout-is-typed-failure", func(t *testing.T) {
		fixture := newCheckpointRuntime(t, "checkpoint-timeout-runtime")
		first := fixture.dispatch(t)
		recovered, err := (workflowruntime.WaitCoordinator{Store: fixture.store}).RecoverWaits(t.Context(), workflowruntime.OpenWaitQuery{RunID: fixture.runID}, checkpointTime.Add(25*time.Hour))
		if err != nil || len(recovered.TimedOut) != 1 || len(recovered.Woken) != 0 {
			t.Fatalf("RecoverWaits = %#v, %v", recovered, err)
		}
		timedOut := recovered.TimedOut[0]
		if timedOut.Node.Status != workflowruntime.NodeTimedOut || timedOut.Attempt.Status != workflowruntime.NodeTimedOut || timedOut.Attempt.Failure == nil || timedOut.Attempt.Failure.Code != "wait_timeout" {
			t.Fatalf("typed checkpoint timeout = %#v", timedOut)
		}
		if first.Wait.Status != workflowruntime.WaitOpen || timedOut.Wait.Status != workflowruntime.WaitTimedOut {
			t.Fatalf("checkpoint timeout lifecycle = %#v / %#v", first.Wait, timedOut.Wait)
		}
	})

	t.Run("run-cancellation-closes-wait", func(t *testing.T) {
		fixture := newCheckpointRuntime(t, "checkpoint-cancel-runtime")
		first := fixture.dispatch(t)
		run, err := fixture.store.LoadRun(t.Context(), fixture.runID)
		if err != nil {
			t.Fatal(err)
		}
		canceled, err := fixture.store.RequestRunCancellation(t.Context(), workflowruntime.RequestRunCancellationRequest{
			RunID: fixture.runID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-checkpoint",
			Reason: workflowruntime.Failure{Code: "caller_canceled", Message: "caller canceled checkpoint"}, At: checkpointTime.Add(time.Minute),
		})
		if err != nil || canceled.Run.Status != workflowruntime.RunCanceled || len(canceled.Nodes) != 1 || canceled.Nodes[0].Status != workflowruntime.NodeCanceled {
			t.Fatalf("RequestRunCancellation = %#v, %v", canceled, err)
		}
		wait, err := fixture.store.LoadWait(t.Context(), first.Wait.Ref.ID)
		if err != nil || wait.Status != workflowruntime.WaitCanceled || wait.Resolution == nil || wait.Resolution.Responder.Kind != "system" {
			t.Fatalf("canceled checkpoint wait = %#v, %v", wait, err)
		}
	})
}

type checkpointRuntime struct {
	store    *inmemory.Store
	registry *stepkind.MemoryRegistry
	runID    workflowruntime.RunID
	node     graph.Node
	claim    workflowruntime.ReadyClaim
}

func newCheckpointRuntime(t *testing.T, name string) checkpointRuntime {
	t.Helper()
	store := inmemory.NewStore()
	runID := workflowruntime.RunID(name)
	plan := workflowruntime.PlanRef{ID: "checkpoint-plan", Version: "v1", Digest: values.SHA256Digest([]byte("checkpoint-plan")), SchemaVersion: "workflow.execution-plan/v1"}
	if _, _, err := store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
		ID: runID, Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + name, CreatedAt: checkpointTime,
	}); err != nil {
		t.Fatal(err)
	}
	inputs, err := store.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-inputs", RunID: runID}, Values: values.ValueSet{}})
	if err != nil {
		t.Fatal(err)
	}
	invocation := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "release-approval"}
	created, err := store.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: invocation, Status: workflowruntime.NodePending, Inputs: &inputs, CreatedAt: checkpointTime, UpdatedAt: checkpointTime,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: invocation, ExpectedGeneration: created.Generation, To: workflowruntime.NodeReady, At: checkpointTime.Add(time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
		RunID: runID, Owner: "worker", Token: "claim-token", IdempotencyKey: "claim-" + name,
		Now: checkpointTime.Add(2 * time.Second), LeaseUntil: checkpointTime.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimNext = %#v, %t, %v", claim, ok, err)
	}
	registry := stepkind.NewRegistry()
	if _, err := checkpointadapter.Register(registry, checkpointOptions(nil, nil)); err != nil {
		t.Fatal(err)
	}
	return checkpointRuntime{
		store: store, registry: registry, runID: runID, claim: claim,
		node: graph.Node{ID: "release-approval", Kind: checkpointadapter.KindName, KindVersion: checkpointadapter.KindVersion, Config: checkpointConfig()},
	}
}

func (f checkpointRuntime) dispatch(t *testing.T) workflowruntime.DispatchResult {
	t.Helper()
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: f.store, Registry: f.registry, Now: func() time.Time { return checkpointTime.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: f.claim, Node: f.node})
	if err != nil {
		t.Fatalf("Dispatch = %#v, %v", result, err)
	}
	return result
}
