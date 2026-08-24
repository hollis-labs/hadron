package runtime_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestStepDispatcherDurableWaitContinuationEndToEnd(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-wait-continuation")
	token := "one-time-resume-token"
	tokenDigest, err := workflowwait.DigestToken(token)
	if err != nil {
		t.Fatal(err)
	}
	resumeSchema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "string"})
	if err != nil {
		t.Fatal(err)
	}
	record := workflowwait.Record{
		Kind: workflowwait.KindSignal, Correlation: "continuation-1", ResumeSchema: resumeSchema,
		ResumeTokenDigest: tokenDigest, Visibility: workflowwait.VisibilityPrivate,
		Authority: workflowwait.ResponderAuthority{Kind: "test"}, WakeSource: workflowwait.WakeSignal,
		Status: workflowwait.StatusOpen,
	}
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("waiter", "v1")
	kind.SpecValue.CanSuspend = true
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executions := 0
	kind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		if executions == 1 {
			if prepared.Invocation.Continuation != nil {
				t.Fatal("initial invocation unexpectedly contained continuation")
			}
			return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{ID: "wait-continuation-1", Record: record, ResumeToken: token}}, nil
		}
		continuation := prepared.Invocation.Continuation
		if continuation == nil || continuation.ID != "wait-continuation-1" || continuation.Record.Status != workflowwait.StatusResumed ||
			continuation.Values[workflowruntime.ResumeValueName].Inline != "accepted" || prepared.Invocation.Identity.Attempt != 1 {
			t.Fatalf("resumed invocation = %#v", prepared.Invocation)
		}
		encoded, marshalErr := json.Marshal(prepared.Invocation)
		if marshalErr != nil || string(encoded) == "" || containsJSONText(encoded, token) {
			t.Fatalf("durable invocation leaked resume token: %s, %v", encoded, marshalErr)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "complete")}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	now := base.Add(3 * time.Second)
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	graphNode := graph.Node{ID: node.ID.NodeID, Kind: "waiter", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}}}
	first, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graphNode})
	if err != nil || first.Node.Status != workflowruntime.NodeWaiting || first.Node.Lease != nil || first.Wait == nil || first.Attempt.ID.Number != 1 || first.Verification != nil {
		t.Fatalf("first Dispatch() = %#v, %v", first, err)
	}
	payload := dispatchValue(t, workflowruntime.ResumeValueName, "accepted")
	now = base.Add(4 * time.Second)
	resumed, err := (workflowruntime.WaitCoordinator{Store: store}).Resume(context.Background(), workflowruntime.ResumeCommand{
		WaitID: first.Wait.Ref.ID, Correlation: record.Correlation, Token: token,
		WakeSource: record.WakeSource, Responder: workflowwait.Responder{Kind: "test", Reference: "responder"},
		Payload: payload, IdempotencyKey: "resume-continuation", ReceivedAt: now,
	})
	if err != nil || resumed.Node.Status != workflowruntime.NodeReady || resumed.Attempt.ID.Number != 1 {
		t.Fatalf("Resume() = %#v, %v", resumed, err)
	}
	now = base.Add(5 * time.Second)
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	secondClaim, ok, err := queue.ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{
		Owner: "worker-2", Token: "token-2", IdempotencyKey: "claim-continuation-2", Now: now, LeaseUntil: base.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimNext(resumed) = %#v, %v, %v", secondClaim, ok, err)
	}
	now = base.Add(6 * time.Second)
	second, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: secondClaim, Node: graphNode})
	if err != nil || second.Node.Status != workflowruntime.NodeSucceeded || second.Attempt.Status != workflowruntime.NodeSucceeded ||
		second.Attempt.ID.Number != 1 || executions != 2 || second.Verification == nil || second.Verification.Report.Status != verification.ReportPassed {
		t.Fatalf("second Dispatch() = %#v, %v; executions=%d", second, err, executions)
	}
}

func containsJSONText(encoded []byte, value string) bool {
	for index := 0; index+len(value) <= len(encoded); index++ {
		if string(encoded[index:index+len(value)]) == value {
			return true
		}
	}
	return false
}
