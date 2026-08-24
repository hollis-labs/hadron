package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestWakeTimerPublicContractRejectsForgeryAndReplaysExactly(t *testing.T) {
	base := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	running := prepareSuccessfulTimer(t, "public-contract", base, 10*time.Second, 0)
	scheduler := &recordingScheduler{}
	authorizer := &recordingAuthorizer{}
	coordinator := workflowruntime.WaitCoordinator{Store: running.store, Scheduler: scheduler, Authorizer: authorizer}
	suspended, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request})
	if err != nil {
		t.Fatal(err)
	}
	wakeAt := suspended.Wait.WakeAt
	if _, wakeErr := coordinator.WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: running.waitID, FiredAt: wakeAt.Add(-time.Nanosecond)}); !errors.Is(wakeErr, workflowruntime.ErrWaitWakeNotDue) {
		t.Fatalf("early wake = %v", wakeErr)
	}
	unchanged, err := running.store.LoadWait(context.Background(), running.waitID)
	if err != nil || unchanged.Status != workflowruntime.WaitOpen || unchanged.Generation != suspended.Wait.Generation {
		t.Fatalf("early wake mutated wait = %#v, %v", unchanged, err)
	}
	forged, err := values.NewInline(map[string]any{"woke_at": wakeAt.Format(time.RFC3339Nano)}, values.Metadata{
		Producer: values.Producer{Kind: "forger", Reference: "external"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, resumeErr := coordinator.Resume(context.Background(), workflowruntime.ResumeCommand{
		WaitID: running.waitID, Correlation: suspended.Wait.Correlation, WakeSource: workflowwait.WakeTimer,
		Responder: workflowwait.Responder{Kind: "system", Reference: "wait-timer"}, Payload: forged,
		IdempotencyKey: "forged-timer", ReceivedAt: wakeAt,
	}); !errors.Is(resumeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("generic timer forgery = %v", resumeErr)
	}
	woken, err := coordinator.WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: running.waitID, FiredAt: wakeAt.Add(time.Hour)})
	if err != nil || woken.Outcome != workflowruntime.ResumeApplied || woken.Wait.Status != workflowruntime.WaitResumed || !woken.Wait.ResolvedAt.Equal(wakeAt) || authorizer.calls != 1 || len(scheduler.canceled) != 1 {
		t.Fatalf("timer wake = %#v, canceled %#v, auth=%d, %v", woken, scheduler.canceled, authorizer.calls, err)
	}
	stored, err := running.store.LoadValues(context.Background(), woken.Values)
	object, ok := stored[workflowruntime.ResumeValueName].Inline.(map[string]any)
	if err != nil || !ok || object["woke_at"] != wakeAt.Format(time.RFC3339Nano) || stored[workflowruntime.ResumeValueName].Redaction != values.RedactionPrivate || stored[workflowruntime.ResumeValueName].Retention != values.RetentionRun {
		t.Fatalf("timer values = %#v, %v", stored, err)
	}
	if len(woken.Events) != 2 || woken.Events[0].Type != workflowruntime.EventWaitResumed || woken.Events[0].Attributes["wake_at"] != wakeAt.Format(time.RFC3339Nano) || !woken.Events[0].OccurredAt.Equal(wakeAt) {
		t.Fatalf("timer events = %#v", woken.Events)
	}
	replayed, err := coordinator.WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: running.waitID, FiredAt: wakeAt.Add(2 * time.Hour)})
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Values != woken.Values || !replayed.Wait.ResolvedAt.Equal(wakeAt) {
		t.Fatalf("timer replay = %#v, %v", replayed, err)
	}
}

func TestTimerRecoveryPrefersWakeAndSchedulerFailureIsRecoverable(t *testing.T) {
	base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("schedule-%d", failAt), func(t *testing.T) {
			running := prepareSuccessfulTimer(t, fmt.Sprintf("timer-recovery-%d", failAt), base, 10*time.Second, 20*time.Second)
			failing := &orderedFailScheduler{failAt: failAt}
			coordinator := workflowruntime.WaitCoordinator{Store: running.store, Scheduler: failing}
			suspended, suspendErr := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request})
			var postCommit *workflowruntime.PostCommitError
			if !errors.As(suspendErr, &postCommit) || suspended.Node.Status != workflowruntime.NodeWaiting || len(failing.scheduled) != failAt || failing.scheduled[0].Kind != workflowruntime.ActivationWaitWake {
				t.Fatalf("partial scheduling = %#v, %#v, %v", suspended, failing.scheduled, suspendErr)
			}
			if failAt == 2 && failing.scheduled[1].Kind != "wait_timeout" {
				t.Fatalf("schedule order = %#v", failing.scheduled)
			}
			restartedScheduler := &orderedFailScheduler{}
			restarted := workflowruntime.WaitCoordinator{Store: running.store, Scheduler: restartedScheduler}
			future, err := restarted.RecoverWaits(context.Background(), workflowruntime.OpenWaitQuery{}, base.Add(5*time.Second))
			if err != nil || len(future.Woken) != 0 || len(future.TimedOut) != 0 || len(restartedScheduler.scheduled) != 2 ||
				restartedScheduler.scheduled[0].Kind != workflowruntime.ActivationWaitWake || restartedScheduler.scheduled[1].Kind != "wait_timeout" {
				t.Fatalf("future recovery = %#v, schedules %#v, %v", future, restartedScheduler.scheduled, err)
			}
			recovered, err := restarted.RecoverWaits(context.Background(), workflowruntime.OpenWaitQuery{}, base.Add(time.Minute))
			if err != nil || len(recovered.Woken) != 1 || len(recovered.TimedOut) != 0 || !recovered.Woken[0].Wait.ResolvedAt.Equal(base.Add(10*time.Second)) || len(restartedScheduler.canceled) != 2 {
				t.Fatalf("timer recovery = %#v, canceled %#v, %v", recovered, restartedScheduler.canceled, err)
			}
		})
	}
}

func TestConcurrentTimerWakeAndRunCancellationConverge(t *testing.T) {
	base := time.Date(2026, time.August, 24, 19, 0, 0, 0, time.UTC)
	running := prepareRunningWait(t, "timer-cancel-race", workflowwait.KindTimer, workflowwait.WakeTimer, base, time.Hour)
	createRun(t, running.store, running.invocation.RunID, base)
	schema, _ := workflowwait.NewSchemaRef(graph.Schema{"type": "object"})
	running.request.Wait.ResumeSchema = schema
	running.request.Wait.ResumeTokenDigest = ""
	running.request.Wait.ResumeURL = ""
	running.request.Wait.WakeAt = base.Add(10 * time.Second)
	running.request.Wait.Deadline = time.Time{}
	coordinator := workflowruntime.WaitCoordinator{Store: running.store}
	if _, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request}); err != nil {
		t.Fatal(err)
	}
	run, err := running.store.LoadRun(context.Background(), running.invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	errorsFound := make(chan error, 2)
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		_, wakeErr := coordinator.WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: running.waitID, FiredAt: base.Add(10 * time.Second)})
		errorsFound <- wakeErr
	}()
	go func() {
		defer group.Done()
		<-start
		_, cancelErr := running.store.RequestRunCancellation(context.Background(), workflowruntime.RequestRunCancellationRequest{
			RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-timer-race",
			Reason: workflowruntime.Failure{Code: "canceled", Message: "test cancellation"}, At: base.Add(10 * time.Second),
		})
		errorsFound <- cancelErr
	}()
	close(start)
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil && !errors.Is(err, workflowruntime.ErrWaitClosed) && !errors.Is(err, workflowruntime.ErrCASMismatch) && !errors.Is(err, workflowruntime.ErrInvalidTransition) {
			t.Fatalf("race error = %v", err)
		}
	}
	loaded, err := running.store.LoadWait(context.Background(), running.waitID)
	if err != nil || loaded.Status != workflowruntime.WaitResumed && loaded.Status != workflowruntime.WaitCanceled {
		t.Fatalf("terminal raced wait = %#v, %v", loaded, err)
	}
}

func TestTimerRecoveryOrdersEarliestActionAndRespectsLimit(t *testing.T) {
	base := time.Date(2026, time.August, 24, 20, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	suspendSuccessfulTimerOnStore(t, store, "late", base, 30*time.Second, 0)
	suspendSuccessfulTimerOnStore(t, store, "early", base, 10*time.Second, 0)
	suspendSuccessfulTimerOnStore(t, store, "middle", base, 20*time.Second, 0)
	suspendTimeoutWaitOnStore(t, store, "timeout-only", base, 15*time.Second)

	candidates, err := store.RecoverOpenWaits(context.Background(), workflowruntime.OpenWaitQuery{Limit: 3})
	if err != nil || len(candidates) != 3 || candidates[0].Ref.ID != "wait-early" || candidates[1].Ref.ID != "wait-timeout-only" || candidates[2].Ref.ID != "wait-middle" {
		t.Fatalf("ordered candidates = %#v, %v", candidates, err)
	}
	coordinator := workflowruntime.WaitCoordinator{Store: store}
	result, err := coordinator.RecoverWaits(context.Background(), workflowruntime.OpenWaitQuery{Limit: 1}, base.Add(12*time.Second))
	if err != nil || len(result.Woken) != 1 || result.Woken[0].Wait.Ref.ID != "wait-early" {
		t.Fatalf("limited due recovery = %#v, %v", result, err)
	}
	candidates, err = store.RecoverOpenWaits(context.Background(), workflowruntime.OpenWaitQuery{Limit: 3})
	if err != nil || len(candidates) != 3 || candidates[0].Ref.ID != "wait-timeout-only" || candidates[1].Ref.ID != "wait-middle" || candidates[2].Ref.ID != "wait-late" {
		t.Fatalf("remaining candidates = %#v, %v", candidates, err)
	}
	scheduler := &orderedFailScheduler{}
	result, err = (workflowruntime.WaitCoordinator{Store: store, Scheduler: scheduler}).RecoverWaits(context.Background(), workflowruntime.OpenWaitQuery{Limit: 1}, base.Add(12*time.Second))
	if err != nil || len(result.Woken) != 0 || len(scheduler.scheduled) != 1 || scheduler.scheduled[0].WaitID != "wait-timeout-only" || scheduler.scheduled[0].Kind != "wait_timeout" {
		t.Fatalf("limited future recovery = %#v, schedules %#v, %v", result, scheduler.scheduled, err)
	}
}

func TestTimerWakeDeterministicallyPrecedesLaterDeadline(t *testing.T) {
	for iteration := range 20 {
		t.Run(fmt.Sprintf("iteration-%02d", iteration), func(t *testing.T) {
			base := time.Date(2026, time.August, 24, 21, iteration, 0, 0, time.UTC)
			running := prepareSuccessfulTimer(t, fmt.Sprintf("wake-timeout-%d", iteration), base, 10*time.Second, 20*time.Second)
			coordinator := workflowruntime.WaitCoordinator{Store: running.store}
			suspended, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request})
			if err != nil {
				t.Fatal(err)
			}
			timeoutRequest := workflowruntime.TimeoutWaitRequest{
				WaitID: suspended.Wait.Ref.ID, ExpectedWaitGeneration: suspended.Wait.Generation,
				ExpectedNodeGeneration: suspended.Node.Generation, IdempotencyKey: fmt.Sprintf("deadline-race-%d", iteration),
				Deadline: suspended.Wait.Deadline, Now: suspended.Wait.Deadline,
			}
			start := make(chan struct{})
			wakeResults := make(chan workflowruntime.ResumeWaitResult, 1)
			wakeErrors := make(chan error, 1)
			timeoutResults := make(chan workflowruntime.WaitTimeoutResult, 1)
			timeoutErrors := make(chan error, 1)
			go func() {
				<-start
				result, wakeErr := coordinator.WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: suspended.Wait.Ref.ID, FiredAt: suspended.Wait.Deadline})
				wakeResults <- result
				wakeErrors <- wakeErr
			}()
			go func() {
				<-start
				result, timeoutErr := running.store.TimeoutWait(context.Background(), timeoutRequest)
				timeoutResults <- result
				timeoutErrors <- timeoutErr
			}()
			close(start)
			wakeResult, wakeErr := <-wakeResults, <-wakeErrors
			timeoutResult, timeoutErr := <-timeoutResults, <-timeoutErrors
			if wakeErr != nil || wakeResult.Wait.Status != workflowruntime.WaitResumed {
				t.Fatalf("wake result = %#v, %v", wakeResult, wakeErr)
			}
			if timeoutErr != nil && !errors.Is(timeoutErr, workflowruntime.ErrWaitWakePending) || timeoutErr == nil && timeoutResult.Applied {
				t.Fatalf("timeout result = %#v, %v", timeoutResult, timeoutErr)
			}
			stored, err := running.store.LoadWait(context.Background(), suspended.Wait.Ref.ID)
			if err != nil || stored.Status != workflowruntime.WaitResumed || !stored.ResolvedAt.Equal(stored.WakeAt) {
				t.Fatalf("converged wait = %#v, %v", stored, err)
			}
		})
	}
}

func TestInMemoryTimeoutWaitFencesEarlierSuccessfulWake(t *testing.T) {
	base := time.Date(2026, time.August, 24, 22, 0, 0, 0, time.UTC)
	running := prepareSuccessfulTimer(t, "timeout-fence", base, 10*time.Second, 20*time.Second)
	suspended, err := (workflowruntime.WaitCoordinator{Store: running.store}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request})
	if err != nil {
		t.Fatal(err)
	}
	if _, timeoutErr := running.store.TimeoutWait(context.Background(), workflowruntime.TimeoutWaitRequest{
		WaitID: suspended.Wait.Ref.ID, ExpectedWaitGeneration: suspended.Wait.Generation,
		ExpectedNodeGeneration: suspended.Node.Generation, IdempotencyKey: "timeout-fenced",
		Deadline: suspended.Wait.Deadline, Now: suspended.Wait.Deadline,
	}); !errors.Is(timeoutErr, workflowruntime.ErrWaitWakePending) {
		t.Fatalf("timeout fence = %v", timeoutErr)
	}
	loaded, err := running.store.LoadWait(context.Background(), suspended.Wait.Ref.ID)
	if err != nil || loaded.Status != workflowruntime.WaitOpen || loaded.Generation != suspended.Wait.Generation {
		t.Fatalf("fenced timeout mutated wait = %#v, %v", loaded, err)
	}
}

func TestTimerRecoveryRetainsCommittedWakeOnCleanupFailure(t *testing.T) {
	base := time.Date(2026, time.August, 24, 22, 30, 0, 0, time.UTC)
	running := prepareSuccessfulTimer(t, "cleanup-failure", base, 10*time.Second, 0)
	cancelErr := errors.New("scheduler cancel unavailable")
	scheduler := &recordingScheduler{cancelErr: cancelErr}
	coordinator := workflowruntime.WaitCoordinator{Store: running.store, Scheduler: scheduler}
	if _, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request}); err != nil {
		t.Fatal(err)
	}
	recovered, err := coordinator.RecoverWaits(context.Background(), workflowruntime.OpenWaitQuery{}, base.Add(time.Minute))
	var postCommit *workflowruntime.PostCommitError
	if !errors.Is(err, cancelErr) || !errors.As(err, &postCommit) || len(recovered.Woken) != 1 || recovered.Woken[0].Outcome != workflowruntime.ResumeApplied || recovered.Woken[0].Wait.Status != workflowruntime.WaitResumed {
		t.Fatalf("recovery post-commit result = %#v, %v", recovered, err)
	}
	loaded, loadErr := running.store.LoadWait(context.Background(), running.waitID)
	if loadErr != nil || loaded.Status != workflowruntime.WaitResumed {
		t.Fatalf("committed wake = %#v, %v", loaded, loadErr)
	}
}

type orderedFailScheduler struct {
	mu        sync.Mutex
	scheduled []workflowwait.Activation
	canceled  []workflowwait.ActivationID
	failAt    int
}

func (s *orderedFailScheduler) Schedule(_ context.Context, activation workflowwait.Activation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = append(s.scheduled, activation)
	if s.failAt > 0 && len(s.scheduled) == s.failAt {
		return errors.New("schedule failed")
	}
	return nil
}

func (s *orderedFailScheduler) Cancel(_ context.Context, id workflowwait.ActivationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, id)
	return nil
}

func prepareSuccessfulTimer(t *testing.T, suffix string, base time.Time, wakeAfter, deadlineAfter time.Duration) runningWaitFixture {
	t.Helper()
	running := prepareRunningWait(t, suffix, workflowwait.KindTimer, workflowwait.WakeTimer, base, time.Hour)
	schema, err := workflowwait.NewSchemaRef(graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"woke_at"},
		"properties": map[string]any{"woke_at": map[string]any{"type": "string"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	running.request.Wait.ResumeSchema = schema
	running.request.Wait.ResumeTokenDigest = ""
	running.request.Wait.ResumeURL = ""
	running.request.Wait.Authority = workflowwait.ResponderAuthority{Kind: "system_timer", Reference: "runtime"}
	running.request.Wait.WakeAt = base.Add(wakeAfter)
	if deadlineAfter > 0 {
		running.request.Wait.Deadline = base.Add(deadlineAfter)
	} else {
		running.request.Wait.Deadline = time.Time{}
	}
	return running
}

func suspendSuccessfulTimerOnStore(t *testing.T, store *runtimetest.Store, suffix string, base time.Time, wakeAfter, deadlineAfter time.Duration) workflowruntime.SuspendWaitResult {
	t.Helper()
	invocation := invocationID("run-timer-order", "node-"+suffix)
	createNode(t, store, invocation, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, invocation, 0, "timer-worker", "timer-token-"+suffix, "timer-claim-"+suffix, base.Add(time.Second), base.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{
		InvocationID: invocation, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"woke_at"},
		"properties": map[string]any{"woke_at": map[string]any{"type": "string"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	record := workflowwait.Record{
		Kind: workflowwait.KindTimer, Correlation: "timer:" + suffix, WakeAt: base.Add(wakeAfter),
		ResumeSchema: schema, Visibility: workflowwait.VisibilityPrivate,
		Authority:  workflowwait.ResponderAuthority{Kind: "system_timer", Reference: "runtime"},
		WakeSource: workflowwait.WakeTimer, Status: workflowwait.StatusOpen,
	}
	if deadlineAfter > 0 {
		record.Deadline = base.Add(deadlineAfter)
	}
	result, err := (workflowruntime.WaitCoordinator{Store: store}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{
		Wait:                   workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: workflowruntime.WaitID("wait-" + suffix)}, Invocation: invocation, Record: record},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, At: base.Add(3 * time.Second),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func suspendTimeoutWaitOnStore(t *testing.T, store *runtimetest.Store, suffix string, base time.Time, timeoutAfter time.Duration) workflowruntime.SuspendWaitResult {
	t.Helper()
	invocation := invocationID("run-timer-order", "node-"+suffix)
	createNode(t, store, invocation, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, invocation, 0, "timeout-worker", "timeout-token-"+suffix, "timeout-claim-"+suffix, base.Add(time.Second), base.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{
		InvocationID: invocation, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "object"})
	if err != nil {
		t.Fatal(err)
	}
	record := workflowwait.Record{
		Kind: workflowwait.KindSignal, Correlation: "timeout:" + suffix, Deadline: base.Add(timeoutAfter),
		ResumeSchema: schema, Visibility: workflowwait.VisibilityPrivate,
		Authority:  workflowwait.ResponderAuthority{Kind: "signal_policy", Reference: "runtime"},
		WakeSource: workflowwait.WakeSignal, Status: workflowwait.StatusOpen,
	}
	result, err := (workflowruntime.WaitCoordinator{Store: store}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{
		Wait:                   workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: workflowruntime.WaitID("wait-" + suffix)}, Invocation: invocation, Record: record},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, At: base.Add(3 * time.Second),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
