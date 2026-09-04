package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func TestWorkflowNamedSignalUpdatePersistsIntentBeforeCanonicalResume(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "run-controls.db"))
	fixture := prepareWorkflowSQLiteWait(t, state, "named-signal", workflowTestTime(), time.Hour)
	fixture.request.Wait.Kind = workflowwait.KindSignal
	fixture.request.Wait.SignalName = "review.completed"
	fixture.request.Wait.WakeSource = workflowwait.WakeSignal
	fixture.request.Wait.ResumeTokenDigest = ""
	fixture.request.Wait.ResumeURL = ""
	fixture.request.Wait.Authority = workflowwait.ResponderAuthority{Kind: "policy", Reference: "reviewers"}
	if _, err := (workflowruntime.WaitCoordinator{Store: state}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request}); err != nil {
		t.Fatal(err)
	}
	selector := workflowruntime.SignalSelector{RunID: fixture.invocation.RunID, Name: "review.completed", Correlation: fixture.correlation}
	wait, err := state.FindOpenSignalWait(context.Background(), selector)
	if err != nil || wait.Ref.ID != fixture.waitID {
		t.Fatalf("FindOpenSignalWait = %#v, %v", wait, err)
	}
	payload, _ := values.NewInline("accepted", values.Metadata{Producer: values.Producer{Kind: "update", Reference: "fixture"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	begin := workflowruntime.BeginRunUpdateRequest{IdempotencyKey: "update-1", Selector: selector, WaitID: wait.Ref.ID,
		Responder: workflowwait.Responder{Kind: "test", Reference: "operator"}, Payload: payload, ReceivedAt: fixture.base.Add(4 * time.Second)}
	regressed := begin
	regressed.IdempotencyKey = "update-regressed"
	regressed.ReceivedAt = wait.UpdatedAt.Add(-time.Nanosecond)
	if _, _, beginErr := state.BeginRunUpdate(context.Background(), regressed); beginErr == nil {
		t.Fatal("run update older than the selected wait was persisted")
	}
	if _, loadErr := state.LoadRunUpdate(context.Background(), regressed.IdempotencyKey); loadErr == nil {
		t.Fatal("rejected regressed update left a pending intent")
	}
	pending, outcome, err := state.BeginRunUpdate(context.Background(), begin)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || pending.Status != workflowruntime.RunUpdatePending {
		t.Fatalf("BeginRunUpdate = %#v / %s, %v", pending, outcome, err)
	}
	resumed, err := (workflowruntime.WaitCoordinator{Store: state}).Resume(context.Background(), workflowruntime.ResumeCommand{WaitID: wait.Ref.ID, Correlation: selector.Correlation,
		WakeSource: workflowwait.WakeSignal, Responder: begin.Responder, Payload: payload, IdempotencyKey: begin.IdempotencyKey, ReceivedAt: begin.ReceivedAt})
	if err != nil || resumed.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("Resume = %#v, %v", resumed, err)
	}
	receipt := workflowruntime.RunUpdateReceipt{Outcome: resumed.Outcome, WaitID: wait.Ref.ID, WaitStatus: resumed.Wait.Status, ResolvedAt: resumed.Wait.ResolvedAt, Values: &resumed.Values}
	sealed, err := state.CompleteRunUpdate(context.Background(), workflowruntime.CompleteRunUpdateRequest{IdempotencyKey: begin.IdempotencyKey, ExpectedGeneration: pending.Generation, Status: workflowruntime.RunUpdateApplied, Receipt: receipt, At: fixture.base.Add(5 * time.Second)})
	if err != nil || sealed.Status != workflowruntime.RunUpdateApplied || sealed.Receipt == nil || sealed.Receipt.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("CompleteRunUpdate = %#v, %v", sealed, err)
	}
	replayed, replayOutcome, err := state.BeginRunUpdate(context.Background(), begin)
	if err != nil || replayOutcome != workflowruntime.IdempotencyReplayed || replayed.Status != workflowruntime.RunUpdateApplied {
		t.Fatalf("BeginRunUpdate replay = %#v / %s, %v", replayed, replayOutcome, err)
	}
	replayWait, err := state.FindSignalWait(context.Background(), selector, begin.IdempotencyKey)
	if err != nil || replayWait.Status != workflowruntime.WaitResumed || replayWait.Resolution == nil || replayWait.Resolution.IdempotencyKey != begin.IdempotencyKey {
		t.Fatalf("FindSignalWait replay = %#v, %v", replayWait, err)
	}
	view, err := state.QueryRunState(context.Background(), workflowruntime.RunStateQuery{RunID: selector.RunID, Limit: 10})
	if err != nil || len(view.Waits) != 1 || len(view.Nodes) != 1 || len(view.Attempts) != 1 || len(view.Events) == 0 {
		t.Fatalf("QueryRunState = %#v, %v", view, err)
	}
}
