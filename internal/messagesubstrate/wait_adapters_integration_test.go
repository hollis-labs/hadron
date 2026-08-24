package messagesubstrate_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	gateadapter "github.com/hollis-labs/hadron/workflow/adapters/gate"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	workflowgate "github.com/hollis-labs/hadron/workflow/gate"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestSleepDispatcherSuccessfulTimerContinuationEndToEnd(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-sleep-timer")
	now := base.Add(3 * time.Second)
	registry := stepkind.NewRegistry()
	if err := registry.Register(waitadapter.NewSleep(func() time.Time { return now })); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	coordinator := &workflowruntime.WaitCoordinator{Store: store, Scheduler: scheduler, Authorizer: allowAuthorizer{}}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }, WaitCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"duration": "1m"}
	first, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: waitadapter.SleepName, KindVersion: waitadapter.Version, Config: config}})
	wakeAt := base.Add(63 * time.Second)
	if err != nil || first.Wait == nil || first.Node.Status != workflowruntime.NodeWaiting || !first.Wait.WakeAt.Equal(wakeAt) || !first.Wait.Deadline.IsZero() || len(scheduler.scheduled) != 1 {
		t.Fatalf("initial sleep = %#v, schedules %#v, %v", first, scheduler.scheduled, err)
	}
	if _, wakeErr := coordinator.WakeTimer(t.Context(), workflowruntime.TimerWakeCommand{WaitID: first.Wait.Ref.ID, FiredAt: wakeAt.Add(-time.Nanosecond)}); !errors.Is(wakeErr, workflowruntime.ErrWaitWakeNotDue) {
		t.Fatalf("early wake = %v", wakeErr)
	}
	woken, err := coordinator.WakeTimer(t.Context(), workflowruntime.TimerWakeCommand{WaitID: first.Wait.Ref.ID, FiredAt: wakeAt.Add(time.Hour)})
	if err != nil || woken.Node.Status != workflowruntime.NodeReady || !woken.Wait.ResolvedAt.Equal(wakeAt) {
		t.Fatalf("timer wake = %#v, %v", woken, err)
	}
	replayed, err := coordinator.WakeTimer(t.Context(), workflowruntime.TimerWakeCommand{WaitID: first.Wait.Ref.ID, FiredAt: wakeAt.Add(2 * time.Hour)})
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Values != woken.Values || !replayed.Wait.ResolvedAt.Equal(wakeAt) {
		t.Fatalf("timer replay = %#v, %v", replayed, err)
	}
	now = wakeAt.Add(time.Second)
	second := dispatchContinuation(t, store, dispatcher, node.ID.NodeID, waitadapter.SleepName, config, now)
	if second.Node.Status != workflowruntime.NodeSucceeded || second.Result == nil || second.Result.Outputs["woke_at"].Inline != wakeAt.Format(time.RFC3339Nano) || second.Result.Outputs["timed_out"].Inline != false {
		t.Fatalf("completed sleep = %#v", second)
	}
}

func TestHumanGateDispatcherResumesThroughCanonicalWaitCoordinator(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-human-gate")
	now := base.Add(3 * time.Second)
	registry := stepkind.NewRegistry()
	executor, err := gateadapter.Register(registry, gateadapter.Options{
		Authority: workflowgate.AuthorityResolverFunc(func(context.Context, workflowgate.AuthorizationRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: workflowgate.AuthorityGatePolicy, Reference: "test-policy"}, nil
		}),
		Payloads: workflowgate.PayloadStoreFunc(func(context.Context, workflowgate.PayloadRequest) (values.ValueSetRef, error) {
			return values.ValueSetRef{ID: "gate-payload-dispatch", Digest: values.SHA256Digest([]byte("gate-payload-dispatch"))}, nil
		}),
		Now: func() time.Time { return now },
	})
	if err != nil || executor == nil {
		t.Fatal(err)
	}
	coordinator := &workflowruntime.WaitCoordinator{Store: store, Authorizer: allowAuthorizer{}}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }, WaitCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{
		"prompt": "Release version?", "options": []any{map[string]any{"id": "approve", "label": "Approve"}, map[string]any{"id": "reject", "label": "Reject"}},
		"environment": "production", "timeout": "1h", "optional": false, "blocking": true,
	}
	first, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: gateadapter.Name, KindVersion: gateadapter.Version, Config: config}})
	if err != nil || first.Wait == nil || first.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("initial gate dispatch = %#v, %v", first, err)
	}
	resumed, err := coordinator.Resume(t.Context(), workflowruntime.ResumeCommand{
		WaitID: first.Wait.Ref.ID, Correlation: first.Wait.Correlation, WakeSource: workflowwait.WakeGate,
		Responder: workflowwait.Responder{Kind: "user", Reference: "reviewer"}, Payload: mustWaitValue(t, map[string]any{"decision": "approve"}),
		IdempotencyKey: "gate-approve", ReceivedAt: now.Add(time.Minute),
	})
	if err != nil || resumed.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("gate resume = %#v, %v", resumed, err)
	}
	now = now.Add(2 * time.Minute)
	second := dispatchContinuation(t, store, dispatcher, node.ID.NodeID, gateadapter.Name, config, now)
	if second.Node.Status != workflowruntime.NodeSucceeded || second.Result == nil || second.Result.Outputs["decision"].Inline != "approve" || second.Result.Outputs["timed_out"].Inline != false {
		t.Fatalf("completed gate = %#v", second)
	}
}

func TestMessageWaitDispatcherResumesThroughMessageBridge(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-message-wait")
	now := base.Add(3 * time.Second)
	registry := stepkind.NewRegistry()
	registration, err := waitadapter.Register(registry, waitadapter.Options{
		Authority: waitadapter.AuthorityResolverFunc(func(context.Context, waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: "message_destination", Reference: "msg://workflow/project/run-1"}, nil
		}),
		Now: func() time.Time { return now },
	})
	if err != nil || registration.MessageWait == nil {
		t.Fatal(err)
	}
	coordinator := &workflowruntime.WaitCoordinator{Store: store, Authorizer: allowAuthorizer{}}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }, WaitCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{
		"substrate": "local", "to": "msg://workflow/project/run-1", "correlation": "thread-1", "timeout": "1h",
		"payload_schema": graph.Schema{"type": "object", "required": []any{"approved"}},
	}
	first, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: waitadapter.MessageWaitName, KindVersion: waitadapter.Version, Config: config}})
	if err != nil || first.Wait == nil || first.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("initial message wait = %#v, %v", first, err)
	}
	wake := messagesubstrate.MessageWake{
		WaitID: first.Wait.Ref.ID, Substrate: "local", Correlation: "thread-1", ReceivedAt: now.Add(time.Minute),
		Envelope: messaging.Envelope{
			ID: "message-dispatch-1", Kind: messaging.MsgKindResponse,
			From: messaging.Address{Kind: messaging.KindUser, Authority: "project", ID: "reviewer"}, To: messaging.Address{Kind: messaging.KindWorkflow, Authority: "project", ID: "run-1"},
			ThreadID: "thread-1", ContentType: "application/json", Payload: json.RawMessage(`{"approved":true}`),
		},
	}
	resumed, err := (messagesubstrate.WaitBridge{Resumer: coordinator}).ResumeMessage(t.Context(), wake)
	if err != nil || resumed.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("message bridge resume = %#v, %v", resumed, err)
	}
	now = now.Add(2 * time.Minute)
	second := dispatchContinuation(t, store, dispatcher, node.ID.NodeID, waitadapter.MessageWaitName, config, now)
	if second.Node.Status != workflowruntime.NodeSucceeded || second.Result == nil {
		t.Fatalf("completed message wait = %#v", second)
	}
	payload, ok := second.Result.Outputs["message"].Inline.(map[string]any)
	if !ok || payload["approved"] != true || second.Result.Outputs["timed_out"].Inline != false {
		t.Fatalf("completed message outputs = %#v", second.Result.Outputs)
	}
}

func TestWaitBackedDispatcherTimeoutIsTypedFailureNotSuccessfulOutput(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-wait-timeout")
	now := base.Add(3 * time.Second)
	registry := stepkind.NewRegistry()
	_, err := waitadapter.Register(registry, waitadapter.Options{
		Authority: waitadapter.AuthorityResolverFunc(func(context.Context, waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
			return workflowwait.ResponderAuthority{Kind: "signal_policy", Reference: "test"}, nil
		}),
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &workflowruntime.WaitCoordinator{Store: store}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }, WaitCoordinator: coordinator})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"signal": "approval", "correlation": "approval-1", "timeout": "1m"}
	first, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: waitadapter.WaitForName, KindVersion: waitadapter.Version, Config: config}})
	if err != nil || first.Wait == nil || first.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("initial timeout wait = %#v, %v", first, err)
	}
	recovered, err := coordinator.RecoverWaits(t.Context(), workflowruntime.OpenWaitQuery{}, now.Add(2*time.Minute))
	if err != nil || len(recovered.TimedOut) != 1 || recovered.TimedOut[0].Node.Status != workflowruntime.NodeTimedOut || recovered.TimedOut[0].Attempt.Failure == nil || recovered.TimedOut[0].Attempt.Failure.Code != "wait_timeout" || recovered.TimedOut[0].Attempt.Outputs != nil {
		t.Fatalf("typed wait timeout = %#v, %v", recovered, err)
	}
}

func dispatchFixture(t *testing.T, run string) (*runtimetest.Store, workflowruntime.ReadyClaim, workflowruntime.NodeInvocationSnapshot, time.Time) {
	t.Helper()
	store := runtimetest.NewStore()
	now := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID(run)
	_, _, err := store.CreateRun(t.Context(), workflowruntime.CreateRunRequest{
		ID: runID, Plan: workflowruntime.PlanRef{ID: "plan", Version: "v1", Digest: values.SHA256Digest([]byte("plan")), SchemaVersion: "workflow.execution-plan/v1"},
		Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + run, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	inputRef, err := store.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "node-inputs", RunID: runID}, Values: values.ValueSet{"input": mustWaitValue(t, "hello")},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "node"}
	node, err := store.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: id, Status: workflowruntime.NodePending, Inputs: &inputRef, CreatedAt: now, UpdatedAt: now}})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := store.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
		Owner: "worker", Token: "token", IdempotencyKey: "claim-" + run, Now: now.Add(2 * time.Second), LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("claim = %#v, %v, %v", claim, ok, err)
	}
	claimed, err := store.LoadNodeInvocation(t.Context(), id)
	if err != nil || claimed.Generation <= ready.Snapshot.Generation {
		t.Fatalf("claimed node = %#v, %v", claimed, err)
	}
	return store, claim, claimed, now
}

func dispatchContinuation(t *testing.T, store workflowruntime.StateStore, dispatcher *workflowruntime.StepDispatcher, nodeID, kind string, config graph.Config, now time.Time) workflowruntime.DispatchResult {
	t.Helper()
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{Owner: "continuation-worker", Token: "continuation-token-" + nodeID, IdempotencyKey: "continuation-claim-" + nodeID, Now: now, LeaseUntil: now.Add(time.Hour)})
	if err != nil || !ok {
		t.Fatalf("claim continuation = %#v, %v, %v", claim, ok, err)
	}
	result, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: nodeID, Kind: kind, KindVersion: "v1", Config: config}})
	if err != nil {
		t.Fatalf("dispatch continuation = %#v, %v", result, err)
	}
	return result
}

func mustWaitValue(t *testing.T, inline any) values.Value {
	t.Helper()
	value, err := values.NewInline(inline, values.Metadata{Producer: values.Producer{Kind: "wait_response", Reference: "integration"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type allowAuthorizer struct{}

func (allowAuthorizer) AuthorizeResume(context.Context, workflowwait.AuthorizationRequest) error {
	return nil
}

type recordingScheduler struct {
	scheduled []workflowwait.Activation
	canceled  []workflowwait.ActivationID
}

func (s *recordingScheduler) Schedule(_ context.Context, activation workflowwait.Activation) error {
	s.scheduled = append(s.scheduled, activation)
	return nil
}

func (s *recordingScheduler) Cancel(_ context.Context, id workflowwait.ActivationID) error {
	s.canceled = append(s.canceled, id)
	return nil
}
