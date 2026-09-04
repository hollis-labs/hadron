package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func TestWorkflowActivationRegistrationScheduleFireRecoveryAndObserver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation.db")
	store, state := openWorkflowStateTest(t, path)
	adapter, adapterErr := NewWorkflowActivationStore(store)
	if adapterErr != nil {
		t.Fatal(adapterErr)
	}
	registration := workflowActivationFixture(t, "nightly", hoststate.ActivationSourceSchedule)
	created, outcome, registerErr := adapter.RegisterActivation(t.Context(), registration)
	if registerErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RegisterActivation = %#v, %q, %v", created, outcome, registerErr)
	}
	registration.Source.Config["cron"] = "mutated"
	created.InputBindings["message"] = graph.Binding{Kind: graph.BindingLiteral, Literal: "mutated"}
	loaded, loadErr := adapter.LoadActivation(t.Context(), "nightly")
	if loadErr != nil || loaded.Source.Config["cron"] != "* * * * *" || loaded.InputBindings["message"].Literal != "scheduled" {
		t.Fatalf("defensive activation = %#v, %v", loaded, loadErr)
	}
	next, nextErr := gosched.NextRun(loaded.Source.Config["cron"].(string), loaded.CreatedAt)
	if nextErr != nil {
		t.Fatal(nextErr)
	}
	due, dueErr := adapter.ListDueSchedules(t.Context(), next, 10)
	if dueErr != nil || len(due) != 1 || !due[0].NextRun.Equal(next) {
		t.Fatalf("ListDueSchedules = %#v, %v", due, dueErr)
	}
	fire := gosched.Fire{ID: gosched.DeriveFireID(loaded.ID, next), ScheduleID: loaded.ID, ScheduledAt: next,
		Status: gosched.FirePending, NextAttemptAt: next, Retry: due[0].Retry, JobType: due[0].JobType, Payload: due[0].Payload}
	createdFire, createErr := adapter.CreateFire(t.Context(), gosched.FireCreation{ScheduleID: loaded.ID, ExpectedNext: next, NextRun: next.Add(time.Minute), Fire: fire})
	if createErr != nil || !createdFire {
		t.Fatalf("CreateFire = %v, %v", createdFire, createErr)
	}
	claimedAt := next.Add(time.Second)
	claimed, won, claimErr := adapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire.ID, ExpectedStatus: gosched.FirePending, ExpectedAttempt: 0, ClaimedAt: claimedAt})
	if claimErr != nil || !won || claimed.Attempt != 1 {
		t.Fatalf("ClaimFire = %#v, %v, %v", claimed, won, claimErr)
	}
	if oldApplied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: fire.ID, Attempt: 1, From: gosched.FireClaimed, To: gosched.FireSucceeded, At: claimedAt.Add(workflowActivationClaimLease + time.Nanosecond)}); err != nil || oldApplied {
		t.Fatalf("expired transition before recovery = %v, %v", oldApplied, err)
	}
	// Create another fire and prove a process-loss lease is reclaimed exactly
	// once while the stale claimant loses both before and after the next claim.
	next2 := next.Add(time.Minute)
	fire2 := gosched.Fire{ID: gosched.DeriveFireID(loaded.ID, next2), ScheduleID: loaded.ID, ScheduledAt: next2,
		Status: gosched.FirePending, NextAttemptAt: next2, Retry: due[0].Retry, JobType: due[0].JobType, Payload: due[0].Payload}
	if ok, err := adapter.CreateFire(t.Context(), gosched.FireCreation{ScheduleID: loaded.ID, ExpectedNext: next2, NextRun: next2.Add(time.Minute), Fire: fire2}); err != nil || !ok {
		t.Fatalf("CreateFire 2 = %v, %v", ok, err)
	}
	first, won, firstClaimErr := adapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire2.ID, ExpectedStatus: gosched.FirePending, ExpectedAttempt: 0, ClaimedAt: next2.Add(time.Second)})
	if firstClaimErr != nil || !won {
		t.Fatalf("first claim = %#v, %v", first, firstClaimErr)
	}
	recoveryStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = recoveryStore.Close() })
	recoveryAdapter, _ := NewWorkflowActivationStore(recoveryStore)
	recoveryAt := first.FiredAt.Add(workflowActivationClaimLease)
	recovered, recoverErr := recoveryAdapter.ListDueFires(t.Context(), recoveryAt, 10)
	if recoverErr != nil {
		t.Fatalf("recovered fires = %#v, %v", recovered, recoverErr)
	}
	var recoveredSecond *gosched.Fire
	for index := range recovered {
		if recovered[index].ID == fire2.ID {
			recoveredSecond = &recovered[index]
		}
	}
	if recoveredSecond == nil || recoveredSecond.Status != gosched.FireRetrying || recoveredSecond.Attempt != 1 {
		t.Fatalf("recovered second fire = %#v", recovered)
	}
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: fire2.ID, Attempt: 1, From: gosched.FireClaimed, To: gosched.FireSucceeded, At: recoveryAt}); err != nil || applied {
		t.Fatalf("stale transition before reclaim = %v, %v", applied, err)
	}
	second, won, secondClaimErr := recoveryAdapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire2.ID, ExpectedStatus: gosched.FireRetrying, ExpectedAttempt: 1, ClaimedAt: recoveryAt})
	if secondClaimErr != nil || !won || second.Attempt != 2 {
		t.Fatalf("second claim = %#v, %v, %v", second, won, secondClaimErr)
	}
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: fire2.ID, Attempt: 1, From: gosched.FireClaimed, To: gosched.FireSucceeded, At: recoveryAt}); err != nil || applied {
		t.Fatalf("stale transition after reclaim = %v, %v", applied, err)
	}
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: fire2.ID, Attempt: 2, From: gosched.FireClaimed, To: gosched.FireSucceeded, At: recoveryAt.Add(time.Second)}); err != nil || !applied {
		t.Fatalf("current transition = %v, %v", applied, err)
	}
	secretErr := errors.New("bearer never-persist-this")
	if err := adapter.RecordActivationObserver(t.Context(), gosched.ObserverEvent{Kind: gosched.ObserverEngineError, At: recoveryAt, Fire: second, Err: secretErr}); err != nil {
		t.Fatal(err)
	}
	var history string
	if err := state.db.QueryRow(`SELECT group_concat(CAST(event_json AS TEXT), '') FROM workflow_activation_events`).Scan(&history); err != nil || strings.Contains(history, "never-persist-this") {
		t.Fatalf("observer history leaked raw error: %q, %v", history, err)
	}
	if attempts := workflowRowCount(t, store, "workflow_activation_attempts"); attempts != 3 {
		t.Fatalf("attempt rows = %d", attempts)
	}
	if _, err := state.db.Exec(`UPDATE workflow_activation_attempts SET claimed_at = claimed_at WHERE fire_id = ? AND attempt = 1`, fire.ID); err == nil {
		t.Fatal("immutable activation attempt update succeeded")
	}
	if _, err := state.db.Exec(`DELETE FROM workflow_activation_events WHERE sequence = (SELECT MIN(sequence) FROM workflow_activation_events)`); err == nil {
		t.Fatal("append-only activation event delete succeeded")
	}
	if attempts := workflowRowCount(t, store, "workflow_activation_attempts"); attempts != 3 {
		t.Fatalf("tamper damaged attempt rows = %d", attempts)
	}
}

func TestWorkflowActivationExternalReplayPoliciesAndTwoHandleContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "external.db")
	firstStore, _ := openWorkflowStateTest(t, path)
	first, _ := NewWorkflowActivationStore(firstStore)
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, _ := NewWorkflowActivationStore(secondStore)

	registration := workflowActivationFixture(t, "webhook", hoststate.ActivationSourceWebhook)
	registration.Policy.Overlap = graph.OverlapAllow
	registration.Policy.RunIDReuse = graph.RunIDReuseAllowDuplicate
	if _, _, err := first.RegisterActivation(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	event := workflowActivationEvent(t, registration.ID, "delivery-one", "one")
	fire, outcome, recordErr := first.RecordActivationEvent(t.Context(), event)
	if recordErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordActivationEvent = %#v, %q, %v", fire, outcome, recordErr)
	}
	replayed, outcome, replayErr := second.RecordActivationEvent(t.Context(), event)
	if replayErr != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.ID != fire.ID {
		t.Fatalf("external replay = %#v, %q, %v", replayed, outcome, replayErr)
	}
	changed := event
	changed.Payload = workflowActivationValues(t, event.RegistrationID, "different")
	if _, _, err := first.RecordActivationEvent(t.Context(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed external event = %v", err)
	}

	var winners atomic.Int32
	var wg sync.WaitGroup
	for _, adapter := range []*WorkflowActivationStore{first, second} {
		wg.Add(1)
		go func(adapter *WorkflowActivationStore) {
			defer wg.Done()
			if _, won, _ := adapter.ClaimFire(context.Background(), gosched.FireClaim{FireID: fire.ID, ExpectedStatus: gosched.FirePending, ExpectedAttempt: 0, ClaimedAt: event.OccurredAt}); won {
				winners.Add(1)
			}
		}(adapter)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("claim winners = %d", winners.Load())
	}

	// Per-fire defaults remain distinct even under reject; explicit shared
	// logical IDs exercise reject, allow_duplicate, and terminate_existing.
	for _, fixture := range []struct {
		name    string
		reuse   graph.RunIDReusePolicy
		overlap graph.OverlapPolicy
		want    hoststate.ActivationDispatchStatus
		replace bool
	}{
		{"reject", graph.RunIDReuseReject, graph.OverlapAllow, hoststate.ActivationDispatchSkipped, false},
		{"allow", graph.RunIDReuseAllowDuplicate, graph.OverlapAllow, hoststate.ActivationDispatchStarting, false},
		{"terminate", graph.RunIDReuseTerminateExisting, graph.OverlapAllow, hoststate.ActivationDispatchStarting, true},
		{"forbid", graph.RunIDReuseAllowDuplicate, graph.OverlapForbid, hoststate.ActivationDispatchSkipped, false},
		{"replace", graph.RunIDReuseAllowDuplicate, graph.OverlapReplace, hoststate.ActivationDispatchStarting, true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			reg := workflowActivationFixture(t, "policy-"+fixture.name, hoststate.ActivationSourceExternal)
			reg.RunScope.ID = "project-" + fixture.name
			reg.Policy.RunIDReuse, reg.Policy.Overlap = fixture.reuse, fixture.overlap
			if _, _, err := first.RegisterActivation(t.Context(), reg); err != nil {
				t.Fatal(err)
			}
			one := claimExternalActivation(t, first, workflowActivationEvent(t, reg.ID, "one", "one"))
			preparedOne, err := first.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
				FireID: one.ID, Attempt: one.Attempt, ScheduledAt: one.ScheduledAt, ObservedAt: one.FiredAt, LogicalRunID: "shared"})
			if err != nil || preparedOne.Dispatch.Status != hoststate.ActivationDispatchStarting {
				t.Fatalf("first prepare = %#v, %v", preparedOne, err)
			}
			two := claimExternalActivation(t, first, workflowActivationEvent(t, reg.ID, "two", "two"))
			preparedTwo, err := first.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
				FireID: two.ID, Attempt: two.Attempt, ScheduledAt: two.ScheduledAt, ObservedAt: two.FiredAt, LogicalRunID: "shared"})
			if err != nil || preparedTwo.Dispatch.Status != fixture.want || fixture.replace != (len(preparedTwo.ReplaceRuns) == 1) {
				t.Fatalf("second prepare = %#v, %v", preparedTwo, err)
			}
		})
	}

	// Logical-run reuse is intentionally scoped across registrations rather
	// than being only a per-registration overlap rule.
	for index, id := range []string{"scope-first", "scope-second"} {
		reg := workflowActivationFixture(t, id, hoststate.ActivationSourceExternal)
		reg.RunScope.ID = "shared-project"
		reg.Policy.RunIDReuse = graph.RunIDReuseReject
		if _, _, err := first.RegisterActivation(t.Context(), reg); err != nil {
			t.Fatal(err)
		}
		fire := claimExternalActivation(t, first, workflowActivationEvent(t, reg.ID, "event", "payload"))
		prepared, err := first.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{
			RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1, FireID: fire.ID, Attempt: fire.Attempt,
			ScheduledAt: fire.ScheduledAt, ObservedAt: fire.FiredAt, LogicalRunID: "scope-shared",
		})
		if err != nil {
			t.Fatal(err)
		}
		want := hoststate.ActivationDispatchStarting
		if index == 1 {
			want = hoststate.ActivationDispatchSkipped
		}
		if prepared.Dispatch.Status != want {
			t.Fatalf("cross-registration dispatch %d = %s, want %s", index, prepared.Dispatch.Status, want)
		}
	}
}

func TestWorkflowActivationDeadlineCatchupExhaustionAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deadline.db")
	store, _ := openWorkflowStateTest(t, path)
	adapter, _ := NewWorkflowActivationStore(store)
	missed := workflowActivationFixture(t, "missed-schedule", hoststate.ActivationSourceSchedule)
	missed.Policy.StartingDeadline = time.Minute
	if _, _, err := adapter.RegisterActivation(t.Context(), missed); err != nil {
		t.Fatal(err)
	}
	missedNext, nextErr := gosched.NextRun(missed.Source.Config["cron"].(string), missed.CreatedAt)
	if nextErr != nil {
		t.Fatal(nextErr)
	}
	due, dueErr := adapter.ListDueSchedules(t.Context(), missedNext, 10)
	if dueErr != nil || len(due) != 1 {
		t.Fatalf("missed due schedule = %#v, %v", due, dueErr)
	}
	missedFire := gosched.Fire{ID: gosched.DeriveFireID(missed.ID, missedNext), ScheduleID: missed.ID, ScheduledAt: missedNext,
		Status: gosched.FirePending, NextAttemptAt: missedNext, Retry: due[0].Retry, JobType: due[0].JobType, Payload: due[0].Payload}
	if created, err := adapter.CreateFire(t.Context(), gosched.FireCreation{ScheduleID: missed.ID, ExpectedNext: missedNext,
		NextRun: missedNext.Add(time.Minute), Fire: missedFire}); err != nil || !created {
		t.Fatalf("missed CreateFire = %v, %v", created, err)
	}
	missedClaim, won, claimErr := adapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: missedFire.ID, ExpectedStatus: gosched.FirePending,
		ExpectedAttempt: 0, ClaimedAt: missedNext.Add(2 * time.Minute)})
	if claimErr != nil || !won {
		t.Fatalf("missed ClaimFire = %#v, %v, %v", missedClaim, won, claimErr)
	}
	missedResult, prepareErr := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: missed.ID,
		ExpectedRegistrationGeneration: 1, FireID: missedClaim.ID, Attempt: missedClaim.Attempt, ScheduledAt: missedClaim.ScheduledAt,
		ObservedAt: missedClaim.FiredAt, LogicalRunID: missedClaim.ID})
	if prepareErr != nil || missedResult.Dispatch.Status != hoststate.ActivationDispatchSkipped || missedResult.Dispatch.ReasonCode != "starting_deadline_missed" {
		t.Fatalf("missed schedule = %#v, %v", missedResult, prepareErr)
	}
	for _, catchup := range []bool{false, true} {
		reg := workflowActivationFixture(t, "deadline-"+map[bool]string{false: "skip", true: "catchup"}[catchup], hoststate.ActivationSourceExternal)
		reg.Policy.StartingDeadline, reg.Policy.Catchup = time.Minute, catchup
		if _, _, err := adapter.RegisterActivation(t.Context(), reg); err != nil {
			t.Fatal(err)
		}
		fire := claimExternalActivation(t, adapter, workflowActivationEvent(t, reg.ID, "event", "payload"))
		prepared, err := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
			FireID: fire.ID, Attempt: fire.Attempt, ScheduledAt: fire.ScheduledAt, ObservedAt: fire.ScheduledAt.Add(2 * time.Minute), LogicalRunID: fire.ID})
		want := hoststate.ActivationDispatchSkipped
		if catchup {
			want = hoststate.ActivationDispatchStarting
		}
		if err != nil || prepared.Dispatch.Status != want {
			t.Fatalf("catchup %v = %#v, %v", catchup, prepared, err)
		}
	}

	reg := workflowActivationFixture(t, "exhaust-overlap", hoststate.ActivationSourceExternal)
	reg.Policy.Overlap, reg.Policy.Retry.MaxAttempts = graph.OverlapForbid, 2
	if _, _, err := adapter.RegisterActivation(t.Context(), reg); err != nil {
		t.Fatal(err)
	}
	one := claimExternalActivation(t, adapter, workflowActivationEvent(t, reg.ID, "one", "one"))
	prepared, firstPrepareErr := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
		FireID: one.ID, Attempt: one.Attempt, ScheduledAt: one.ScheduledAt, ObservedAt: one.FiredAt, LogicalRunID: one.ID})
	if firstPrepareErr != nil {
		t.Fatal(firstPrepareErr)
	}
	retryAt := one.FiredAt.Add(time.Second)
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: one.ID, Attempt: one.Attempt, From: gosched.FireClaimed,
		To: gosched.FireRetrying, At: retryAt, NextAttemptAt: retryAt, Reason: "dispatch_failed"}); err != nil || !applied {
		t.Fatalf("retry = %v, %v", applied, err)
	}
	retrying, won, retryClaimErr := adapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: one.ID, ExpectedStatus: gosched.FireRetrying,
		ExpectedAttempt: one.Attempt, ClaimedAt: retryAt})
	if retryClaimErr != nil || !won || retrying.Attempt != 2 {
		t.Fatalf("retry claim = %#v, %v, %v", retrying, won, retryClaimErr)
	}
	replayedPrepare, replayPrepareErr := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
		FireID: retrying.ID, Attempt: retrying.Attempt, ScheduledAt: retrying.ScheduledAt, ObservedAt: retrying.FiredAt, LogicalRunID: retrying.ID})
	if replayPrepareErr != nil || replayedPrepare.Outcome != workflowruntime.IdempotencyReplayed || replayedPrepare.Dispatch.Attempt != prepared.Dispatch.Attempt {
		t.Fatalf("replayed prepare = %#v, %v", replayedPrepare, replayPrepareErr)
	}
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: retrying.ID, Attempt: retrying.Attempt, From: gosched.FireClaimed,
		To: gosched.FireExhausted, At: retrying.FiredAt.Add(time.Second), Reason: "maximum_attempts_reached"}); err != nil || !applied {
		t.Fatalf("exhaust = %v, %v", applied, err)
	}
	exhausted, loadDispatchErr := loadWorkflowActivationDispatch(t.Context(), adapter.db, one.ID)
	if loadDispatchErr != nil || exhausted.Status != hoststate.ActivationDispatchExhausted || exhausted.PhysicalRunID != prepared.Dispatch.PhysicalRunID {
		t.Fatalf("exhausted dispatch = %#v, %v", exhausted, loadDispatchErr)
	}
	two := claimExternalActivation(t, adapter, workflowActivationEvent(t, reg.ID, "two", "two"))
	second, secondPrepareErr := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: reg.ID, ExpectedRegistrationGeneration: 1,
		FireID: two.ID, Attempt: two.Attempt, ScheduledAt: two.ScheduledAt, ObservedAt: two.FiredAt, LogicalRunID: two.ID})
	if secondPrepareErr != nil || second.Dispatch.Status != hoststate.ActivationDispatchStarting {
		t.Fatalf("post-exhaustion prepare = %#v, %v", second, secondPrepareErr)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowActivationStore(reopenedStore)
	loaded, err := reopened.LoadActivation(t.Context(), reg.ID)
	if err != nil || !reflect.DeepEqual(loaded, reg) {
		t.Fatalf("reopened activation = %#v, %v", loaded, err)
	}
}

func TestWorkflowActivationPreparedStartReplaysAfterExpiredClaim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prepared-replay.db")
	store, _ := openWorkflowStateTest(t, path)
	adapter, _ := NewWorkflowActivationStore(store)
	registration := workflowActivationFixture(t, "prepared-replay", hoststate.ActivationSourceExternal)
	registration.Policy.RunIDReuse = graph.RunIDReuseAllowDuplicate
	if _, _, err := adapter.RegisterActivation(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	first := claimExternalActivation(t, adapter, workflowActivationEvent(t, registration.ID, "delivery", "payload"))
	prepared, prepareErr := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: registration.ID, ExpectedRegistrationGeneration: 1,
		FireID: first.ID, Attempt: first.Attempt, ScheduledAt: first.ScheduledAt, ObservedAt: first.FiredAt, LogicalRunID: first.ID})
	if prepareErr != nil {
		t.Fatal(prepareErr)
	}
	started, completeErr := adapter.CompleteActivation(t.Context(), hoststate.ActivationCompleteRequest{FireID: first.ID, ExpectedGeneration: prepared.Dispatch.Generation,
		Attempt: prepared.Dispatch.Attempt, Status: hoststate.ActivationDispatchStarted, At: first.FiredAt.Add(time.Second)})
	if completeErr != nil {
		t.Fatal(completeErr)
	}
	expiredAt := first.FiredAt.Add(workflowActivationClaimLease)
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: first.ID, Attempt: first.Attempt, From: gosched.FireClaimed,
		To: gosched.FireSucceeded, At: expiredAt}); err != nil || applied {
		t.Fatalf("expired first result = %v, %v", applied, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	adapter, _ = NewWorkflowActivationStore(reopenedStore)
	due, err := adapter.ListDueFires(t.Context(), expiredAt, 10)
	if err != nil || len(due) != 1 || due[0].ID != first.ID {
		t.Fatalf("recovered fire = %#v, %v", due, err)
	}
	second, won, err := adapter.ClaimFire(t.Context(), gosched.FireClaim{FireID: first.ID, ExpectedStatus: gosched.FireRetrying,
		ExpectedAttempt: first.Attempt, ClaimedAt: expiredAt})
	if err != nil || !won || second.Attempt != first.Attempt+1 {
		t.Fatalf("reclaimed fire = %#v, %v, %v", second, won, err)
	}
	replayed, err := adapter.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{RegistrationID: registration.ID, ExpectedRegistrationGeneration: 1,
		FireID: second.ID, Attempt: second.Attempt, ScheduledAt: second.ScheduledAt, ObservedAt: second.FiredAt, LogicalRunID: second.ID})
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Dispatch.Attempt != first.Attempt ||
		replayed.Dispatch.PhysicalRunID != started.PhysicalRunID {
		t.Fatalf("prepared replay = %#v, %v", replayed, err)
	}
	completed, err := adapter.CompleteActivation(t.Context(), hoststate.ActivationCompleteRequest{FireID: second.ID,
		ExpectedGeneration: replayed.Dispatch.Generation, Attempt: replayed.Dispatch.Attempt, Status: hoststate.ActivationDispatchStarted, At: second.FiredAt})
	if err != nil || completed.Generation != started.Generation {
		t.Fatalf("dispatch completion replay = %#v, %v", completed, err)
	}
	if applied, err := adapter.TransitionFire(t.Context(), gosched.FireTransition{FireID: second.ID, Attempt: second.Attempt, From: gosched.FireClaimed,
		To: gosched.FireSucceeded, At: second.FiredAt.Add(time.Second)}); err != nil || !applied {
		t.Fatalf("reclaimed result = %v, %v", applied, err)
	}
}

func TestWorkflowCallbackCredentialPayloadReplayExpiryAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "callback.db")
	store, state := openWorkflowStateTest(t, path)
	adapter, _ := NewWorkflowActivationStore(store)
	createdAt := workflowTestTime()
	credential := "raw-callback-credential"
	digest, digestErr := hoststate.DigestCallbackCredential(credential)
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	registration := hoststate.CallbackRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: "callback-one", WaitID: "wait-one", Correlation: "callback:one",
		WakeSource: workflowwait.WakeCallback, Responder: workflowwait.Responder{Kind: "service", Reference: "approver"},
		ValueSchema: graph.Schema{"type": "string"}, CredentialDigest: digest, ExposureRef: "route-one",
		ExpiresAt: createdAt.Add(time.Hour), CreatedAt: createdAt, Generation: 1,
	}
	created, outcome, createErr := adapter.CreateCallback(t.Context(), registration)
	if createErr != nil || outcome != workflowruntime.IdempotencyApplied || !reflect.DeepEqual(created, registration) {
		t.Fatalf("CreateCallback = %#v, %q, %v", created, outcome, createErr)
	}
	request := hoststate.CallbackBeginRequest{CallbackID: registration.ID, IdempotencyKey: "delivery-one", CredentialDigest: digest,
		PayloadDigest: values.SHA256Digest([]byte("accepted")), ReceivedAt: createdAt.Add(time.Minute)}
	if delivery, err := adapter.BeginCallback(t.Context(), request); err != nil || delivery.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("BeginCallback = %#v, %v", delivery, err)
	}
	changed := request
	changed.PayloadDigest = values.SHA256Digest([]byte("changed"))
	if _, err := adapter.BeginCallback(t.Context(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed callback payload = %v", err)
	}
	wrong := request
	wrong.IdempotencyKey, wrong.CredentialDigest = "delivery-two", values.SHA256Digest([]byte("wrong"))
	if _, err := adapter.BeginCallback(t.Context(), wrong); !errors.Is(err, hoststate.ErrCallbackCredential) {
		t.Fatalf("wrong callback credential = %v", err)
	}
	var persisted string
	if err := state.db.QueryRow(`SELECT group_concat(CAST(registration_json AS TEXT), '') || group_concat(CAST(request_digest AS TEXT), '')
FROM workflow_callback_registrations, workflow_callback_deliveries`).Scan(&persisted); err != nil || strings.Contains(persisted, credential) {
		t.Fatalf("raw callback credential persisted: %q, %v", persisted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowActivationStore(reopenedStore)
	if delivery, err := reopened.BeginCallback(t.Context(), request); err != nil || delivery.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("reopened callback replay = %#v, %v", delivery, err)
	}
	completed, err := reopened.CompleteCallback(t.Context(), registration.ID, request.IdempotencyKey, request.ReceivedAt)
	if err != nil || completed.Registration.ConsumedAt.IsZero() {
		t.Fatalf("CompleteCallback = %#v, %v", completed, err)
	}

	expired := registration
	expired.ID, expired.WaitID, expired.Correlation = "callback-expired", "wait-expired", "callback:expired"
	if _, _, err := reopened.CreateCallback(t.Context(), expired); err != nil {
		t.Fatal(err)
	}
	expiredRequest := request
	expiredRequest.CallbackID, expiredRequest.IdempotencyKey, expiredRequest.ReceivedAt = expired.ID, "expired", expired.ExpiresAt
	if _, err := reopened.BeginCallback(t.Context(), expiredRequest); !errors.Is(err, hoststate.ErrCallbackExpired) {
		t.Fatalf("expired callback = %v", err)
	}
}

func workflowActivationFixture(t *testing.T, id string, kind hoststate.ActivationSourceKind) hoststate.ActivationRegistration {
	t.Helper()
	base := workflowTestTime()
	config := graph.Config{}
	switch kind {
	case hoststate.ActivationSourceSchedule:
		config["cron"] = "* * * * *"
	case hoststate.ActivationSourceTimer:
		config["fire_at"] = base.Add(time.Minute).Format(time.RFC3339Nano)
	case hoststate.ActivationSourceWebhook:
		config["path"] = "/hooks/" + id
	case hoststate.ActivationSourceFile:
		config["path"] = "/drop/" + id
		config["events"] = []string{"create"}
	case hoststate.ActivationSourceExternal:
		config["topic"] = "events." + id
	case hoststate.ActivationSourceCallback:
		config["path"] = "/callbacks/" + id
		config["ttl"] = "15m"
	}
	return hoststate.ActivationRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: id,
		Definition:    graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "definition", Version: "v1", Digest: values.SHA256Digest([]byte("definition"))},
		InputBindings: map[string]graph.Binding{"message": {Kind: graph.BindingLiteral, Literal: "scheduled"}},
		Principal:     hoststate.ActivationPrincipal{Principal: "service:activation", SourceAuthority: "activation", Trust: "trusted", ExposureRef: "exposure-" + id},
		RunScope:      hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "project-one"},
		Source: hoststate.ActivationSource{Kind: kind, Reference: "source-" + id, Config: config,
			OneShot: kind == hoststate.ActivationSourceCallback || kind == hoststate.ActivationSourceTimer},
		Authority:  hoststate.ActivationAuthorityProject,
		Provenance: graph.Provenance{Authority: "project", Origin: "workflow-source", Digest: values.SHA256Digest([]byte("source"))},
		Policy: hoststate.ActivationPolicy{Overlap: graph.OverlapAllow, RunIDReuse: graph.RunIDReuseReject,
			Retry: hoststate.ActivationRetryPolicy{MaxAttempts: 3, Strategy: "constant", Initial: time.Second, Maximum: time.Minute}},
		Enabled: true, CreatedAt: base, UpdatedAt: base, Generation: 1,
	}
}

func workflowActivationEvent(t *testing.T, registrationID, key, text string) hoststate.ActivationEvent {
	t.Helper()
	return hoststate.ActivationEvent{RegistrationID: registrationID, IdempotencyKey: key, OccurredAt: workflowTestTime().Add(time.Minute),
		Payload: workflowActivationValues(t, registrationID, text), SourceRef: "delivery-" + key}
}

func workflowActivationValues(t *testing.T, reference, text string) values.ValueSet {
	t.Helper()
	value, err := values.NewInline(text, values.Metadata{Producer: values.Producer{Kind: "trigger", Reference: reference, Output: "message"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return values.ValueSet{"message": value}
}

func claimExternalActivation(t *testing.T, store *WorkflowActivationStore, event hoststate.ActivationEvent) gosched.Fire {
	t.Helper()
	fire, _, err := store.RecordActivationEvent(t.Context(), event)
	if err != nil {
		t.Fatal(err)
	}
	claimed, won, err := store.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire.ID, ExpectedStatus: fire.Status, ExpectedAttempt: fire.Attempt, ClaimedAt: event.OccurredAt})
	if err != nil || !won {
		t.Fatalf("ClaimFire = %#v, %v, %v", claimed, won, err)
	}
	return claimed
}
