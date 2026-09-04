package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func TestWorkflowSQLiteAtomicSuspendResumeAndReplay(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "wait.db"))
	fixture := prepareWorkflowSQLiteWait(t, state, "atomic", workflowTestTime(), time.Hour)
	coordinator := workflowruntime.WaitCoordinator{Store: state}
	suspended, suspendErr := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token})
	if suspendErr != nil || suspended.Node.Status != workflowruntime.NodeWaiting || suspended.Node.Lease != nil || suspended.Attempt.Status != workflowruntime.NodeRunning {
		t.Fatalf("Suspend = %#v, %v", suspended, suspendErr)
	}
	command := fixture.resumeCommand(t, "resume-atomic", fixture.base.Add(4*time.Second))
	resumed, resumeErr := coordinator.Resume(context.Background(), command)
	if resumeErr != nil || resumed.Outcome != workflowruntime.ResumeApplied || resumed.Wait.Status != workflowruntime.WaitResumed || resumed.Node.Status != workflowruntime.NodeReady || len(resumed.Events) != 2 {
		t.Fatalf("Resume = %#v, %v", resumed, resumeErr)
	}
	command.ReceivedAt = fixture.base.Add(2 * time.Hour)
	replayed, replayErr := coordinator.Resume(context.Background(), command)
	if replayErr != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Wait.ResolvedAt != resumed.Wait.ResolvedAt {
		t.Fatalf("replay after deadline = %#v, %v", replayed, replayErr)
	}
	command.IdempotencyKey = "other-key"
	if _, err := coordinator.Resume(context.Background(), command); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("different-key duplicate = %v", err)
	}
	stored, loadErr := state.LoadValues(context.Background(), resumed.Values)
	if loadErr != nil || stored[workflowruntime.ResumeValueName].Inline != "accepted" {
		t.Fatalf("LoadValues = %#v, %v", stored, loadErr)
	}
}

func TestWorkflowSQLiteLateResumeTimeoutAndRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store, state := openWorkflowStateTest(t, path)
	fixture := prepareWorkflowSQLiteWait(t, state, "late", workflowTestTime(), 10*time.Second)
	coordinator := workflowruntime.WaitCoordinator{Store: state}
	if _, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err != nil {
		t.Fatal(err)
	}
	result, resumeErr := coordinator.Resume(context.Background(), fixture.resumeCommand(t, "late", fixture.base.Add(time.Minute)))
	if !errors.Is(resumeErr, workflowruntime.ErrWaitClosed) || result.Wait.Status != workflowruntime.WaitTimedOut || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "wait_timeout" {
		t.Fatalf("late resume = %#v, %v", result, resumeErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restarted, stateErr := NewWorkflowStateStore(reopened)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	waits, recoveryErr := restarted.RecoverOpenWaits(context.Background(), workflowruntime.OpenWaitQuery{})
	if recoveryErr != nil || len(waits) != 0 {
		t.Fatalf("recovered terminal wait candidates = %#v, %v", waits, recoveryErr)
	}
	loaded, loadErr := restarted.LoadWait(context.Background(), fixture.waitID)
	if loadErr != nil || loaded.Status != workflowruntime.WaitTimedOut || !loaded.Deadline.Equal(fixture.base.Add(10*time.Second)) {
		t.Fatalf("reopened wait = %#v, %v", loaded, loadErr)
	}
}

func TestWorkflowSQLiteSuccessfulTimerReopensAndMatchesRuntimeContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timer-wake.db")
	store, state := openWorkflowStateTest(t, path)
	fixture := prepareWorkflowSQLiteWait(t, state, "timer-wake", workflowTestTime(), time.Hour)
	schema, err := workflowwait.NewSchemaRef(graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"woke_at"},
		"properties": map[string]any{"woke_at": map[string]any{"type": "string"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wakeAt := fixture.base.Add(10 * time.Second)
	deadline := fixture.base.Add(20 * time.Second)
	fixture.request.Wait.Kind = workflowwait.KindTimer
	fixture.request.Wait.WakeSource = workflowwait.WakeTimer
	fixture.request.Wait.ResumeSchema = schema
	fixture.request.Wait.ResumeTokenDigest = ""
	fixture.request.Wait.ResumeURL = ""
	fixture.request.Wait.Authority = workflowwait.ResponderAuthority{Kind: "system_timer", Reference: "runtime"}
	fixture.request.Wait.WakeAt = wakeAt
	fixture.request.Wait.Deadline = deadline
	coordinator := workflowruntime.WaitCoordinator{Store: state}
	suspended, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request})
	if err != nil {
		t.Fatal(err)
	}
	if _, timeoutErr := state.TimeoutWait(context.Background(), workflowruntime.TimeoutWaitRequest{
		WaitID: suspended.Wait.Ref.ID, ExpectedWaitGeneration: suspended.Wait.Generation,
		ExpectedNodeGeneration: suspended.Node.Generation, IdempotencyKey: "timer-timeout-must-lose",
		Deadline: deadline, Now: deadline,
	}); !errors.Is(timeoutErr, workflowruntime.ErrWaitWakePending) {
		t.Fatalf("timer timeout precedence = %v", timeoutErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, err := NewWorkflowStateStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.LoadWait(context.Background(), fixture.waitID)
	if err != nil || loaded.Status != workflowruntime.WaitOpen || !loaded.WakeAt.Equal(wakeAt) || !loaded.Deadline.Equal(deadline) {
		t.Fatalf("reopened timer = %#v, %v", loaded, err)
	}
	woken, err := (workflowruntime.WaitCoordinator{Store: reopened}).WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: fixture.waitID, FiredAt: deadline.Add(time.Hour)})
	if err != nil || woken.Outcome != workflowruntime.ResumeApplied || woken.Wait.Status != workflowruntime.WaitResumed || !woken.Wait.ResolvedAt.Equal(wakeAt) || woken.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("SQLite timer wake = %#v, %v", woken, err)
	}
	set, err := reopened.LoadValues(context.Background(), woken.Values)
	object, ok := set[workflowruntime.ResumeValueName].Inline.(map[string]any)
	if err != nil || !ok || object["woke_at"] != wakeAt.Format(time.RFC3339Nano) || set[workflowruntime.ResumeValueName].Redaction != values.RedactionPrivate || set[workflowruntime.ResumeValueName].Retention != values.RetentionRun {
		t.Fatalf("SQLite timer values = %#v, %v", set, err)
	}
	if len(woken.Events) != 2 || woken.Events[0].Type != workflowruntime.EventWaitResumed || woken.Events[0].Attributes["wake_at"] != wakeAt.Format(time.RFC3339Nano) || !woken.Events[0].OccurredAt.Equal(wakeAt) {
		t.Fatalf("SQLite timer events = %#v", woken.Events)
	}
	replayed, err := (workflowruntime.WaitCoordinator{Store: reopened}).WakeTimer(context.Background(), workflowruntime.TimerWakeCommand{WaitID: fixture.waitID, FiredAt: deadline.Add(2 * time.Hour)})
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Values != woken.Values {
		t.Fatalf("SQLite timer replay = %#v, %v", replayed, err)
	}
}

func TestWorkflowSQLiteConcurrentResumeHasOneAtomicWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contention.db")
	firstStore, first := openWorkflowStateTest(t, path)
	fixture := prepareWorkflowSQLiteWait(t, first, "contention", workflowTestTime(), time.Hour)
	if _, err := (workflowruntime.WaitCoordinator{Store: first}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err != nil {
		t.Fatal(err)
	}
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, stateErr := NewWorkflowStateStore(secondStore)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	_ = firstStore
	command := fixture.resumeCommand(t, "same-key", fixture.base.Add(4*time.Second))
	stores := []workflowruntime.WaitStore{first, second}
	results := make(chan workflowruntime.ResumeWaitResult, 2)
	errs := make(chan error, 2)
	var group sync.WaitGroup
	for _, store := range stores {
		group.Add(1)
		go func(store workflowruntime.WaitStore) {
			defer group.Done()
			result, resumeErr := (workflowruntime.WaitCoordinator{Store: store}).Resume(context.Background(), command)
			results <- result
			errs <- resumeErr
		}(store)
	}
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent resume: %v", err)
		}
	}
	applied, replayed := 0, 0
	for result := range results {
		if result.Outcome == workflowruntime.ResumeApplied {
			applied++
		}
		if result.Outcome == workflowruntime.ResumeReplayed {
			replayed++
		}
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("outcomes applied=%d replayed=%d", applied, replayed)
	}
	loaded, loadErr := first.LoadNodeInvocation(context.Background(), fixture.invocation)
	if loadErr != nil || loaded.Status != workflowruntime.NodeReady {
		t.Fatalf("node = %#v, %v", loaded, loadErr)
	}
}

func TestWorkflowSQLiteResumeRacesTimeoutWithDurableLoserOutcome(t *testing.T) {
	for iteration := range 20 {
		t.Run(fmt.Sprintf("iteration-%02d", iteration), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "resume-timeout.db")
			firstStore, first := openWorkflowStateTest(t, path)
			fixture := prepareWorkflowSQLiteWait(t, first, fmt.Sprintf("race-%d", iteration), workflowTestTime().Add(time.Duration(iteration)*time.Minute), 10*time.Second)
			suspended, suspendErr := (workflowruntime.WaitCoordinator{Store: first}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token})
			if suspendErr != nil {
				t.Fatal(suspendErr)
			}
			secondStore, openErr := Open(path)
			if openErr != nil {
				t.Fatal(openErr)
			}
			t.Cleanup(func() { _ = secondStore.Close() })
			second, stateErr := NewWorkflowStateStore(secondStore)
			if stateErr != nil {
				t.Fatal(stateErr)
			}
			_ = firstStore

			command := fixture.resumeCommand(t, fmt.Sprintf("resume-race-%d", iteration), suspended.Wait.Deadline.Add(-time.Nanosecond))
			digest, digestErr := workflowwait.DigestToken(command.Token)
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			resumeRequest := workflowruntime.ResumeNodeWaitRequest{
				WaitID: suspended.Wait.Ref.ID, ExpectedWaitGeneration: suspended.Wait.Generation,
				ExpectedNodeGeneration: suspended.Node.Generation, ExpectedAttemptGeneration: suspended.Attempt.Generation,
				Correlation: command.Correlation, PresentedTokenDigest: digest, WakeSource: command.WakeSource,
				Responder: command.Responder, Payload: command.Payload, IdempotencyKey: command.IdempotencyKey, ReceivedAt: command.ReceivedAt,
			}
			timeoutRequest := workflowruntime.TimeoutWaitRequest{
				WaitID: suspended.Wait.Ref.ID, ExpectedWaitGeneration: suspended.Wait.Generation,
				ExpectedNodeGeneration: suspended.Node.Generation, IdempotencyKey: fmt.Sprintf("timeout-race-%d", iteration),
				Deadline: suspended.Wait.Deadline, Now: suspended.Wait.Deadline,
			}

			start := make(chan struct{})
			timeouts := make(chan workflowSQLiteTimeoutCall, 1)
			resumes := make(chan workflowSQLiteResumeCall, 1)
			go func() {
				<-start
				result, timeoutErr := first.TimeoutWait(context.Background(), timeoutRequest)
				timeouts <- workflowSQLiteTimeoutCall{result: result, err: timeoutErr}
			}()
			go func() {
				<-start
				result, resumeErr := second.ResumeNodeWait(context.Background(), resumeRequest)
				resumes <- workflowSQLiteResumeCall{result: result, err: resumeErr}
			}()
			close(start)
			timeoutCall, resumeCall := <-timeouts, <-resumes
			stored, loadErr := first.LoadWait(context.Background(), suspended.Wait.Ref.ID)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			switch stored.Status {
			case workflowruntime.WaitTimedOut:
				if timeoutCall.err != nil || !timeoutCall.result.Applied || !errors.Is(resumeCall.err, workflowruntime.ErrWaitClosed) || resumeCall.result.Wait.Status != stored.Status {
					t.Fatalf("timeout winner = timeout:%#v/%v resume:%#v/%v", timeoutCall.result, timeoutCall.err, resumeCall.result, resumeCall.err)
				}
			case workflowruntime.WaitResumed:
				if resumeCall.err != nil || resumeCall.result.Wait.Status != stored.Status || timeoutCall.err != nil || timeoutCall.result.Applied || timeoutCall.result.Wait.Status != stored.Status || timeoutCall.result.Node.Status != workflowruntime.NodeReady {
					t.Fatalf("resume winner = timeout:%#v/%v resume:%#v/%v", timeoutCall.result, timeoutCall.err, resumeCall.result, resumeCall.err)
				}
			default:
				t.Fatalf("race left wait in %q", stored.Status)
			}
		})
	}
}

func TestMigration0015BackfillsValidUnresumableLegacyWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, openErr := sql.Open("sqlite", path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	migration, readErr := migrationsFS.ReadFile("migrations/0014_workflow_state.sql")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if _, execErr := db.Exec(string(migration)); execErr != nil {
		t.Fatal(execErr)
	}
	if _, execErr := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); execErr != nil {
		t.Fatal(execErr)
	}
	for version := 1; version <= 14; version++ {
		if _, execErr := db.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)`, version, workflowTime(workflowTestTime())); execErr != nil {
			t.Fatal(execErr)
		}
	}
	at := workflowTime(workflowTestTime())
	if _, execErr := db.Exec(`INSERT INTO workflow_waits(wait_id, run_id, node_id, iteration, status, generation, created_at, updated_at) VALUES ('legacy-wait','legacy-run','legacy-node','','open',1,?,?)`, at, at); execErr != nil {
		t.Fatal(execErr)
	}
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	store, storeErr := Open(path)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, stateErr := NewWorkflowStateStore(store)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	wait, loadErr := state.LoadWait(context.Background(), "legacy-wait")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if wait.Status != workflowruntime.WaitOpen || wait.Authority.Kind != "legacy_unresumable" || wait.ResumeTokenDigest == "" {
		t.Fatalf("legacy backfill = %#v", wait)
	}
	payload := workflowTestValue(t, "accepted")
	_, resumeErr := (workflowruntime.WaitCoordinator{Store: state}).Resume(context.Background(), workflowruntime.ResumeCommand{WaitID: wait.Ref.ID, Correlation: wait.Correlation, WakeSource: wait.WakeSource, Payload: payload, Responder: workflowwait.Responder{Kind: "test", Reference: "test"}, ReceivedAt: workflowTestTime().Add(time.Second)})
	if !errors.Is(resumeErr, workflowruntime.ErrWaitUnresumable) {
		t.Fatalf("legacy resume = %v", resumeErr)
	}
}

type workflowSQLiteWaitFixture struct {
	base        time.Time
	waitID      workflowruntime.WaitID
	invocation  workflowruntime.NodeInvocationID
	token       string
	source      workflowwait.WakeSource
	correlation string
	request     workflowruntime.SuspendNodeWaitRequest
}

type workflowSQLiteTimeoutCall struct {
	result workflowruntime.WaitTimeoutResult
	err    error
}

type workflowSQLiteResumeCall struct {
	result workflowruntime.ResumeWaitResult
	err    error
}

func prepareWorkflowSQLiteWait(t *testing.T, state *WorkflowStateStore, suffix string, base time.Time, timeout time.Duration) workflowSQLiteWaitFixture {
	t.Helper()
	run := createWorkflowTestRun(t, state, "run-wait-"+suffix, base)
	node := createWorkflowTestNode(t, state, run.ID, "wait-node", base)
	ready, err := state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "worker", Token: "claim-" + suffix, IdempotencyKey: "claim-key-" + suffix, Now: base.Add(time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !claim.Acquired {
		t.Fatalf("ClaimNode = %#v, %v", claim, err)
	}
	claimed, err := state.LoadNodeInvocation(context.Background(), node.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := state.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "test", Version: "v1"}, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "string"})
	if err != nil {
		t.Fatal(err)
	}
	token := "resume-" + suffix
	digest, err := workflowwait.DigestToken(token)
	if err != nil {
		t.Fatal(err)
	}
	waitID := workflowruntime.WaitID("wait-" + suffix)
	correlation := "correlation-" + suffix
	record := workflowwait.Record{Kind: workflowwait.KindCallback, Correlation: correlation, Deadline: base.Add(timeout), ResumeSchema: schema, ResumeTokenDigest: digest, ResumeURL: "https://example.test/waits/" + suffix, Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "test"}, WakeSource: workflowwait.WakeCallback, Status: workflowruntime.WaitOpen}
	request := workflowruntime.SuspendNodeWaitRequest{Wait: workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: waitID}, Invocation: node.ID, Record: record}, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, At: base.Add(3 * time.Second)}
	return workflowSQLiteWaitFixture{base: base, waitID: waitID, invocation: node.ID, token: token, source: record.WakeSource, correlation: correlation, request: request}
}

func (f workflowSQLiteWaitFixture) resumeCommand(t *testing.T, key string, at time.Time) workflowruntime.ResumeCommand {
	t.Helper()
	value, err := values.NewInline("accepted", values.Metadata{Producer: values.Producer{Kind: "wait_response", Reference: string(f.waitID)}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return workflowruntime.ResumeCommand{WaitID: f.waitID, Correlation: f.correlation, Token: f.token, WakeSource: f.source, Responder: workflowwait.Responder{Kind: "test", Reference: "responder"}, Payload: value, IdempotencyKey: key, ReceivedAt: at}
}
