package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
)

func TestWaitTimeoutStoreConformance(t *testing.T) {
	var _ workflowruntime.WaitTimeoutStore = runtimetest.NewStore()
}

func TestTimeoutWaitBoundaryAtomicOutcomeAndUTCReplay(t *testing.T) {
	ctx := context.Background()
	fixture := prepareWaitingWait(t, "boundary", time.Date(2026, time.August, 24, 20, 0, 0, 0, time.UTC))
	request := fixture.request
	request.Now = request.Deadline.Add(-time.Nanosecond)

	beforeWait, err := fixture.store.LoadWait(ctx, request.WaitID)
	if err != nil {
		t.Fatal(err)
	}
	beforeNode, err := fixture.store.LoadNodeInvocation(ctx, fixture.invocation)
	if err != nil {
		t.Fatal(err)
	}
	beforeAttempt, err := fixture.store.LoadAttempt(ctx, fixture.attempt)
	if err != nil {
		t.Fatal(err)
	}
	beforeEvents, err := fixture.store.ListEvents(ctx, workflowruntime.EventQuery{RunID: fixture.invocation.RunID})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = fixture.store.TimeoutWait(ctx, request); !errors.Is(err, workflowruntime.ErrWaitTimeoutNotDue) {
		t.Fatalf("before deadline error = %v", err)
	}
	assertWaitTimeoutState(t, fixture, beforeWait, beforeNode, beforeAttempt, beforeEvents)

	request.Now = request.Deadline
	result, err := fixture.store.TimeoutWait(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Replayed || result.Wait.Status != workflowruntime.WaitTimedOut ||
		result.Node.Status != workflowruntime.NodeTimedOut || result.Node.Wait != nil || result.Node.Lease != nil ||
		result.Attempt.Status != workflowruntime.NodeTimedOut || result.Attempt.Failure == nil ||
		result.Attempt.Failure.Code != "wait_timeout" || len(result.Events) != 3 {
		t.Fatalf("timeout result = %#v", result)
	}
	if got := result.Attempt.Failure.Details["deadline"]; got != request.Deadline.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("failure deadline = %q", got)
	}
	if result.Events[0].Type != workflowruntime.EventNodeAttemptFinished ||
		result.Events[0].Attributes["from_status"] != string(workflowruntime.NodeRunning) ||
		result.Events[0].Attributes["to_status"] != string(workflowruntime.NodeTimedOut) ||
		result.Events[1].Type != workflowruntime.EventNodeStatusChanged ||
		result.Events[1].Attributes["from_status"] != string(workflowruntime.NodeWaiting) ||
		result.Events[1].Attributes["to_status"] != string(workflowruntime.NodeTimedOut) ||
		result.Events[2].Type != workflowruntime.EventWaitTimedOut ||
		result.Events[2].Attributes["from_status"] != string(workflowruntime.WaitOpen) ||
		result.Events[2].Attributes["to_status"] != string(workflowruntime.WaitTimedOut) ||
		result.Events[2].Attributes["wait_id"] != string(request.WaitID) ||
		result.Events[2].Attributes["deadline"] != request.Deadline.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("timeout events = %#v", result.Events)
	}
	for _, event := range result.Events {
		if event.Invocation == nil || *event.Invocation != fixture.invocation ||
			event.Attempt == nil || *event.Attempt != fixture.attempt ||
			!event.OccurredAt.Equal(request.Now) || event.OccurredAt.Location() != time.UTC ||
			event.Attributes["wait_id"] != string(request.WaitID) {
			t.Fatalf("timeout event linkage/time = %#v", event)
		}
	}

	zone := time.FixedZone("same-instant", -7*60*60)
	replayRequest := request
	replayRequest.Deadline = request.Deadline.In(zone)
	replayRequest.Now = request.Now.In(zone)
	replay, err := fixture.store.TimeoutWait(ctx, replayRequest)
	if err != nil || !replay.Applied || !replay.Replayed || !reflect.DeepEqual(replay.Events, result.Events) {
		t.Fatalf("same-instant replay = %#v, %v", replay, err)
	}
	afterEvents, err := fixture.store.ListEvents(ctx, workflowruntime.EventQuery{RunID: fixture.invocation.RunID})
	if err != nil || len(afterEvents) != len(beforeEvents)+3 {
		t.Fatalf("replay appended events: %d -> %d, %v", len(beforeEvents), len(afterEvents), err)
	}

	changed := request
	changed.Now = request.Now.Add(time.Second)
	if _, err = fixture.store.TimeoutWait(ctx, changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed idempotency request = %v", err)
	}
	stale := request
	stale.IdempotencyKey = "timeout-boundary-stale"
	if _, err = fixture.store.TimeoutWait(ctx, stale); !errors.Is(err, workflowruntime.ErrCASMismatch) {
		t.Fatalf("stale timeout CAS = %v", err)
	}

	handlerID := invocationID(fixture.invocation.RunID, "timeout-handler")
	createNode(t, fixture.store, handlerID, workflowruntime.NodePending, 0, request.Now)
	handled, err := workflowruntime.NewProgressionCoordinator(fixture.store, nil).ProgressNode(ctx, workflowruntime.ProgressNodeRequest{
		InvocationID: handlerID, Rule: graph.ReadyOneFailed,
		Dependencies: []workflowruntime.DependencyRef{{InvocationID: fixture.invocation}},
		At:           request.Now.Add(time.Second),
	})
	if err != nil || handled.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("timeout-aware downstream progression = %#v, %v", handled, err)
	}
}

func TestTimeoutWaitMalformedPathIsAtomic(t *testing.T) {
	t.Run("deadline_before_creation", func(t *testing.T) {
		base := time.Date(2026, time.August, 24, 21, 0, 0, 0, time.UTC)
		fixture := prepareWaitingWait(t, "malformed-deadline", base)
		request := fixture.request
		request.Deadline = base.Add(2 * time.Second)
		request.Now = base.Add(20 * time.Second)
		assertMalformedTimeoutIsAtomic(t, fixture, request)
	})

	t.Run("node_references_another_wait", func(t *testing.T) {
		ctx := context.Background()
		base := time.Date(2026, time.August, 24, 21, 30, 0, 0, time.UTC)
		fixture := prepareWaitingWait(t, "malformed-reference", base)
		node, err := fixture.store.LoadNodeInvocation(ctx, fixture.invocation)
		if err != nil {
			t.Fatal(err)
		}
		node.Wait = &workflowruntime.WaitRef{ID: "another-wait"}
		node.UpdatedAt = base.Add(4 * time.Second)
		node, err = fixture.store.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{
			Snapshot: node, ExpectedGeneration: node.Generation,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := fixture.request
		request.ExpectedNodeGeneration = node.Generation
		assertMalformedTimeoutIsAtomic(t, fixture, request)
	})
}

func TestTimeoutWaitConcurrentSameRequestReplaysOneAtomicWinner(t *testing.T) {
	fixture := prepareWaitingWait(t, "same-key", time.Date(2026, time.August, 24, 22, 0, 0, 0, time.UTC))
	const contenders = 32
	results := make(chan timeoutCall, contenders)
	start := make(chan struct{})
	var group sync.WaitGroup
	for range contenders {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			result, err := fixture.store.TimeoutWait(context.Background(), fixture.request)
			results <- timeoutCall{result: result, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	fresh, replayed := 0, 0
	for call := range results {
		if call.err != nil || !call.result.Applied {
			t.Fatalf("same-key timeout = %#v, %v", call.result, call.err)
		}
		if call.result.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != contenders-1 {
		t.Fatalf("fresh/replayed = %d/%d", fresh, replayed)
	}
}

func TestTimeoutWaitConcurrentDistinctKeysHaveOneWinner(t *testing.T) {
	fixture := prepareWaitingWait(t, "distinct-keys", time.Date(2026, time.August, 24, 23, 0, 0, 0, time.UTC))
	const contenders = 32
	results := make(chan timeoutCall, contenders)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range contenders {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			request := fixture.request
			request.IdempotencyKey = fmt.Sprintf("timeout-distinct-%d", index)
			result, err := fixture.store.TimeoutWait(context.Background(), request)
			results <- timeoutCall{result: result, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	applied, lostCAS := 0, 0
	for call := range results {
		switch {
		case call.err == nil && call.result.Applied && !call.result.Replayed:
			applied++
		case errors.Is(call.err, workflowruntime.ErrCASMismatch):
			lostCAS++
		default:
			t.Fatalf("distinct-key timeout = %#v, %v", call.result, call.err)
		}
	}
	if applied != 1 || lostCAS != contenders-1 {
		t.Fatalf("applied/lost CAS = %d/%d", applied, lostCAS)
	}
}

func TestTimeoutWaitRacesResumeWithOneDurableWinner(t *testing.T) {
	for iteration := range 30 {
		fixture := prepareWaitingWait(t, fmt.Sprintf("resume-race-%d", iteration), time.Date(2026, time.August, 25, 0, 0, iteration, 0, time.UTC))
		start := make(chan struct{})
		timeoutDone := make(chan error, 1)
		resumeDone := make(chan error, 1)
		go func() {
			<-start
			_, err := fixture.store.TimeoutWait(context.Background(), fixture.request)
			timeoutDone <- err
		}()
		go func() {
			<-start
			_, _, err := fixture.store.ResumeWait(context.Background(), workflowruntime.ResumeWaitRequest{
				WaitID: fixture.request.WaitID, IdempotencyKey: fmt.Sprintf("resume-race-%d", iteration),
				ResumedAt: fixture.request.Now,
			})
			resumeDone <- err
		}()
		close(start)
		timeoutErr, resumeErr := <-timeoutDone, <-resumeDone
		wait, err := fixture.store.LoadWait(context.Background(), fixture.request.WaitID)
		if err != nil {
			t.Fatal(err)
		}
		switch wait.Status {
		case workflowruntime.WaitTimedOut:
			if timeoutErr != nil || !errors.Is(resumeErr, workflowruntime.ErrAlreadyResumed) {
				t.Fatalf("timeout winner errors = timeout:%v resume:%v", timeoutErr, resumeErr)
			}
		case workflowruntime.WaitResumed:
			if resumeErr != nil || !errors.Is(timeoutErr, workflowruntime.ErrCASMismatch) {
				t.Fatalf("resume winner errors = timeout:%v resume:%v", timeoutErr, resumeErr)
			}
		default:
			t.Fatalf("race left wait in %q", wait.Status)
		}
	}
}

type waitTimeoutFixture struct {
	store      *runtimetest.Store
	invocation workflowruntime.NodeInvocationID
	attempt    workflowruntime.AttemptID
	request    workflowruntime.TimeoutWaitRequest
}

type timeoutCall struct {
	result workflowruntime.WaitTimeoutResult
	err    error
}

func prepareWaitingWait(t *testing.T, suffix string, base time.Time) waitTimeoutFixture {
	t.Helper()
	ctx := context.Background()
	store := runtimetest.NewStore()
	invocation := invocationID(workflowruntime.RunID("run-timeout-"+suffix), "wait-node")
	waitID := workflowruntime.WaitID("wait-" + suffix)
	createNode(t, store, invocation, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, invocation, 0, "timeout-worker", "timeout-token-"+suffix,
		"timeout-claim-"+suffix, base.Add(time.Second), base.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: invocation, ExpectedNodeGeneration: claimed.Generation,
		Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	wait, err := store.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: waitID}, Invocation: invocation, Status: workflowruntime.WaitOpen,
		CreatedAt: base.Add(3 * time.Second), UpdatedAt: base.Add(3 * time.Second),
	}})
	if err != nil {
		t.Fatal(err)
	}
	nodeWithWait := started.Node
	nodeWithWait.Wait = &workflowruntime.WaitRef{ID: waitID}
	nodeWithWait.UpdatedAt = base.Add(3 * time.Second)
	nodeWithWait, err = store.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{
		Snapshot: nodeWithWait, ExpectedGeneration: started.Node.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: invocation, ExpectedGeneration: nodeWithWait.Generation,
		To: workflowruntime.NodeWaiting, Claim: &claim, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	return waitTimeoutFixture{
		store: store, invocation: invocation, attempt: started.Attempt.ID,
		request: workflowruntime.TimeoutWaitRequest{
			WaitID: waitID, ExpectedWaitGeneration: wait.Generation,
			ExpectedNodeGeneration: waiting.Snapshot.Generation,
			IdempotencyKey:         "timeout-" + suffix,
			Deadline:               base.Add(10 * time.Second), Now: base.Add(10 * time.Second),
		},
	}
}

func assertWaitTimeoutState(
	t *testing.T,
	fixture waitTimeoutFixture,
	wantWait workflowruntime.WaitSnapshot,
	wantNode workflowruntime.NodeInvocationSnapshot,
	wantAttempt workflowruntime.AttemptSnapshot,
	wantEvents []workflowruntime.Event,
) {
	t.Helper()
	ctx := context.Background()
	gotWait, waitErr := fixture.store.LoadWait(ctx, fixture.request.WaitID)
	gotNode, nodeErr := fixture.store.LoadNodeInvocation(ctx, fixture.invocation)
	gotAttempt, attemptErr := fixture.store.LoadAttempt(ctx, fixture.attempt)
	gotEvents, eventErr := fixture.store.ListEvents(ctx, workflowruntime.EventQuery{RunID: fixture.invocation.RunID})
	if waitErr != nil || nodeErr != nil || attemptErr != nil || eventErr != nil ||
		!reflect.DeepEqual(gotWait, wantWait) || !reflect.DeepEqual(gotNode, wantNode) ||
		!reflect.DeepEqual(gotAttempt, wantAttempt) || !reflect.DeepEqual(gotEvents, wantEvents) {
		t.Fatalf("timeout failure mutated state: wait=%#v/%v node=%#v/%v attempt=%#v/%v events=%#v/%v",
			gotWait, waitErr, gotNode, nodeErr, gotAttempt, attemptErr, gotEvents, eventErr)
	}
}

func assertMalformedTimeoutIsAtomic(t *testing.T, fixture waitTimeoutFixture, request workflowruntime.TimeoutWaitRequest) {
	t.Helper()
	ctx := context.Background()
	wantWait, err := fixture.store.LoadWait(ctx, request.WaitID)
	if err != nil {
		t.Fatal(err)
	}
	wantNode, err := fixture.store.LoadNodeInvocation(ctx, fixture.invocation)
	if err != nil {
		t.Fatal(err)
	}
	wantAttempt, err := fixture.store.LoadAttempt(ctx, fixture.attempt)
	if err != nil {
		t.Fatal(err)
	}
	wantEvents, err := fixture.store.ListEvents(ctx, workflowruntime.EventQuery{RunID: fixture.invocation.RunID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.store.TimeoutWait(ctx, request); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("malformed timeout = %v", err)
	}
	assertWaitTimeoutState(t, fixture, wantWait, wantNode, wantAttempt, wantEvents)
}
