package runtime_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestAllWakeSourcesConvergeOnAtomicResume(t *testing.T) {
	cases := []struct {
		kind   workflowwait.Kind
		source workflowwait.WakeSource
	}{{workflowwait.KindGate, workflowwait.WakeGate}, {workflowwait.KindMessage, workflowwait.WakeMessage}, {workflowwait.KindTimer, workflowwait.WakeTimer}, {workflowwait.KindCallback, workflowwait.WakeCallback}, {workflowwait.KindChildRun, workflowwait.WakeChildRun}, {workflowwait.KindSignal, workflowwait.WakeSignal}}
	for _, test := range cases {
		t.Run(string(test.source), func(t *testing.T) {
			fixture := prepareAtomicWait(t, string(test.source), test.kind, test.source, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), time.Hour)
			result, err := fixture.coordinator.Resume(context.Background(), fixture.resumeCommand("resume-"+string(test.source), fixture.base.Add(4*time.Second)))
			if err != nil || result.Outcome != workflowruntime.ResumeApplied || result.Wait.Status != workflowruntime.WaitResumed || result.Node.Status != workflowruntime.NodeReady || result.Node.Wait != nil || len(result.Events) != 2 || result.Events[0].Type != workflowruntime.EventWaitResumed || result.Events[1].Type != workflowruntime.EventNodeStatusChanged {
				t.Fatalf("Resume = %#v, %v", result, err)
			}
			attempt, err := fixture.store.LoadAttempt(context.Background(), result.Attempt.ID)
			if err != nil || attempt.Status != workflowruntime.NodeRunning || !attempt.FinishedAt.IsZero() {
				t.Fatalf("resumed attempt = %#v, %v", attempt, err)
			}
			stored, err := fixture.store.LoadValues(context.Background(), result.Values)
			if err != nil || stored[workflowruntime.ResumeValueName].Inline != "accepted" {
				t.Fatalf("resume values = %#v, %v", stored, err)
			}
		})
	}
}

func TestResumeIdempotencyDeadlineAndAuthorizationOrdering(t *testing.T) {
	fixture := prepareAtomicWait(t, "idempotency", workflowwait.KindCallback, workflowwait.WakeCallback, time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC), 10*time.Second)
	first := fixture.resumeCommand("resume-key", fixture.base.Add(4*time.Second))
	applied, err := fixture.coordinator.Resume(context.Background(), first)
	if err != nil || applied.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("first resume = %#v, %v", applied, err)
	}
	replay := first
	replay.ReceivedAt = fixture.base.Add(20 * time.Second)
	replayed, err := fixture.coordinator.Resume(context.Background(), replay)
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Wait.ResolvedAt != applied.Wait.ResolvedAt {
		t.Fatalf("post-deadline replay = %#v, %v", replayed, err)
	}
	conflict := replay
	conflict.IdempotencyKey = "different-key"
	if _, err := fixture.coordinator.Resume(context.Background(), conflict); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("different-key duplicate = %v", err)
	}
	fixture.authorizer.deny = errors.New("not authorized")
	if _, err := fixture.coordinator.Resume(context.Background(), replay); err == nil || err.Error() != "not authorized" {
		t.Fatalf("unauthorized duplicate = %v", err)
	}
	if fixture.authorizer.calls != 4 {
		t.Fatalf("authority calls = %d, want 4", fixture.authorizer.calls)
	}
}

func TestResumeAuthorizationPrecedesSchemaValidation(t *testing.T) {
	fixture := prepareAtomicWait(t, "authorization-order", workflowwait.KindCallback, workflowwait.WakeCallback, time.Date(2026, 8, 24, 13, 15, 0, 0, time.UTC), time.Hour)
	denied := errors.New("not authorized for wait")
	fixture.authorizer.deny = denied
	command := fixture.resumeCommand("authorization-order", fixture.base.Add(4*time.Second))
	invalidPayload, valueErr := values.NewInline(42, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "authorization-order"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	command.Payload = invalidPayload
	if _, err := fixture.coordinator.Resume(context.Background(), command); !errors.Is(err, denied) || errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("unauthorized schema-invalid resume = %v", err)
	}
	if fixture.authorizer.calls != 1 {
		t.Fatalf("authority calls = %d, want 1", fixture.authorizer.calls)
	}
}

func TestSuspendReplayAndResumeValidationDoNotMutate(t *testing.T) {
	running := prepareRunningWait(t, "validation", workflowwait.KindCallback, workflowwait.WakeCallback, time.Date(2026, 8, 24, 13, 30, 0, 0, time.UTC), time.Hour)
	coordinator := workflowruntime.WaitCoordinator{Store: running.store}
	command := workflowruntime.SuspendCommand{Request: running.request, ResumeToken: running.token}
	first, suspendErr := coordinator.Suspend(context.Background(), command)
	if suspendErr != nil {
		t.Fatal(suspendErr)
	}
	replay, replayErr := coordinator.Suspend(context.Background(), command)
	if replayErr != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.Wait.Generation != first.Wait.Generation {
		t.Fatalf("suspend replay = %#v, %v", replay, replayErr)
	}
	changed := command
	changed.Request.Wait.Correlation = "different"
	if _, err := coordinator.Suspend(context.Background(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed suspension = %v", err)
	}

	fixture := atomicWaitFixture{store: running.store, coordinator: coordinator, base: running.base, waitID: running.waitID, invocation: running.invocation, token: running.token}
	badToken := fixture.resumeCommand("bad-token", running.base.Add(4*time.Second))
	badToken.Token = "incorrect"
	if _, err := coordinator.Resume(context.Background(), badToken); !errors.Is(err, workflowruntime.ErrInvalidResumeToken) || strings.Contains(err.Error(), badToken.Token) {
		t.Fatalf("invalid token = %v", err)
	}
	wrongDigest, digestErr := workflowwait.DigestToken("different-credential")
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	direct := fixture.resumeCommand("direct-bad-token", running.base.Add(4*time.Second))
	if _, err := running.store.ResumeNodeWait(context.Background(), workflowruntime.ResumeNodeWaitRequest{
		WaitID: running.waitID, ExpectedWaitGeneration: first.Wait.Generation,
		ExpectedNodeGeneration: first.Node.Generation, ExpectedAttemptGeneration: first.Attempt.Generation,
		Correlation: direct.Correlation, PresentedTokenDigest: wrongDigest, WakeSource: direct.WakeSource,
		Responder: direct.Responder, Payload: direct.Payload, IdempotencyKey: direct.IdempotencyKey, ReceivedAt: direct.ReceivedAt,
	}); !errors.Is(err, workflowruntime.ErrInvalidResumeToken) || strings.Contains(err.Error(), "different-credential") || strings.Contains(err.Error(), wrongDigest) {
		t.Fatalf("store invalid token digest = %v", err)
	}
	badPayload := fixture.resumeCommand("bad-payload", running.base.Add(4*time.Second))
	value, valueErr := values.NewInline(42, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "bad"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	badPayload.Payload = value
	if _, err := coordinator.Resume(context.Background(), badPayload); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("schema mismatch = %v", err)
	}
	wait, loadErr := running.store.LoadWait(context.Background(), running.waitID)
	if loadErr != nil || wait.Status != workflowruntime.WaitOpen || wait.Generation != 1 {
		t.Fatalf("rejected resume mutated wait = %#v, %v", wait, loadErr)
	}
}

func TestResumeWithoutKeyReturnsAcceptedResultAndLateOpenWaitTimesOut(t *testing.T) {
	fixture := prepareAtomicWait(t, "no-key", workflowwait.KindSignal, workflowwait.WakeSignal, time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC), time.Minute)
	command := fixture.resumeCommand("", fixture.base.Add(4*time.Second))
	first, err := fixture.coordinator.Resume(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.coordinator.Resume(context.Background(), command)
	if err != nil || second.Outcome != workflowruntime.ResumeAlreadyResumed || second.Values != first.Values {
		t.Fatalf("no-key duplicate = %#v, %v", second, err)
	}

	late := prepareAtomicWait(t, "late", workflowwait.KindMessage, workflowwait.WakeMessage, fixture.base.Add(time.Hour), 10*time.Second)
	result, err := late.coordinator.Resume(context.Background(), late.resumeCommand("late-key", late.base.Add(20*time.Second)))
	if !errors.Is(err, workflowruntime.ErrWaitClosed) || result.Wait.Status != workflowruntime.WaitTimedOut || result.Node.Status != workflowruntime.NodeTimedOut || result.Attempt.Status != workflowruntime.NodeTimedOut {
		t.Fatalf("late resume = %#v, %v", result, err)
	}
	stored, loadErr := late.store.LoadWait(context.Background(), late.waitID)
	if loadErr != nil || stored.Status != workflowruntime.WaitTimedOut {
		t.Fatalf("late wait was not durably timed out: %#v, %v", stored, loadErr)
	}
}

func TestResumeRejectsNonPersistableRetentionWithoutMutation(t *testing.T) {
	fixture := prepareAtomicWait(t, "retention-none", workflowwait.KindSignal, workflowwait.WakeSignal, time.Date(2026, 8, 24, 14, 30, 0, 0, time.UTC), time.Hour)
	payload, payloadErr := values.NewInline("ephemeral", values.Metadata{Producer: values.Producer{Kind: "wait_response", Reference: string(fixture.waitID)}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone})
	if payloadErr != nil {
		t.Fatal(payloadErr)
	}
	command := fixture.resumeCommand("retention-none", fixture.base.Add(4*time.Second))
	command.Payload = payload
	if _, err := fixture.coordinator.Resume(context.Background(), command); !errors.Is(err, values.ErrRetentionViolation) {
		t.Fatalf("retention-none resume = %v", err)
	}
	wait, loadErr := fixture.store.LoadWait(context.Background(), fixture.waitID)
	if loadErr != nil || wait.Status != workflowruntime.WaitOpen || wait.Generation != 1 {
		t.Fatalf("rejected retention mutated wait = %#v, %v", wait, loadErr)
	}
}

func TestSuspendTokenURLAndPostCommitErrors(t *testing.T) {
	fixture := prepareRunningWait(t, "url", workflowwait.KindCallback, workflowwait.WakeCallback, time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC), time.Minute)
	fixture.request.Wait.ResumeURL = "https://example.test/waits/" + fixture.token
	coordinator := workflowruntime.WaitCoordinator{Store: fixture.store}
	if result, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err == nil || result.Wait.Generation != 0 {
		t.Fatalf("token-bearing URL suspend = %#v, %v", result, err)
	}
	node, _ := fixture.store.LoadNodeInvocation(context.Background(), fixture.invocation)
	if node.Status != workflowruntime.NodeRunning {
		t.Fatalf("rejected suspension mutated node: %#v", node)
	}

	fixture.request.Wait.ResumeURL = "https://example.test/%zz"
	if _, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err == nil {
		t.Fatal("malformed URL did not fail")
	}
	fixture.request.Wait.ResumeURL = "https://" + fixture.token + ".example.test/waits/url"
	if _, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err == nil {
		t.Fatal("token-bearing URL hostname did not fail")
	}
	fixture.request.Wait.ResumeURL = "https://example.test/waits/url"
	scheduler := &recordingScheduler{scheduleErr: errors.New("scheduler unavailable")}
	coordinator.Scheduler = scheduler
	result, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token})
	var operational *workflowruntime.PostCommitError
	if !errors.As(err, &operational) || result.Wait.Status != workflowruntime.WaitOpen || result.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("post-commit scheduling result = %#v, %v", result, err)
	}
}

func TestRecoveryAndCleanupUseDurableWaits(t *testing.T) {
	base := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	future := prepareAtomicWait(t, "future", workflowwait.KindTimer, workflowwait.WakeTimer, base, time.Hour)
	restartedScheduler := &recordingScheduler{}
	restarted := workflowruntime.WaitCoordinator{Store: future.store, Scheduler: restartedScheduler}
	results, err := restarted.Recover(context.Background(), workflowruntime.OpenWaitQuery{}, base.Add(10*time.Second))
	if err != nil || len(results) != 0 || len(restartedScheduler.scheduled) != 1 {
		t.Fatalf("future recovery = %#v, schedules=%d, %v", results, len(restartedScheduler.scheduled), err)
	}

	due := prepareAtomicWait(t, "due", workflowwait.KindTimer, workflowwait.WakeTimer, base.Add(2*time.Hour), 10*time.Second)
	results, err = (workflowruntime.WaitCoordinator{Store: due.store}).Recover(context.Background(), workflowruntime.OpenWaitQuery{}, due.base.Add(time.Minute))
	if err != nil || len(results) != 1 || !results[0].Applied || results[0].Wait.Status != workflowruntime.WaitTimedOut {
		t.Fatalf("due recovery = %#v, %v", results, err)
	}

	cleanup := prepareAtomicWait(t, "cleanup", workflowwait.KindCallback, workflowwait.WakeCallback, base.Add(4*time.Hour), time.Hour)
	cleanup.coordinator.Scheduler = &recordingScheduler{cancelErr: errors.New("cancel failed")}
	cleanup.coordinator.Materializer = &recordingMaterializer{resolveErr: errors.New("resolve failed")}
	result, err := cleanup.coordinator.Resume(context.Background(), cleanup.resumeCommand("cleanup-key", cleanup.base.Add(4*time.Second)))
	if result.Outcome != workflowruntime.ResumeApplied || err == nil || !errors.Is(err, cleanup.coordinator.Scheduler.(*recordingScheduler).cancelErr) || !errors.Is(err, cleanup.coordinator.Materializer.(*recordingMaterializer).resolveErr) {
		t.Fatalf("joined cleanup warning = %#v, %v", result, err)
	}
}

type atomicWaitFixture struct {
	store       *runtimetest.Store
	coordinator workflowruntime.WaitCoordinator
	authorizer  *recordingAuthorizer
	base        time.Time
	waitID      workflowruntime.WaitID
	invocation  workflowruntime.NodeInvocationID
	token       string
}
type runningWaitFixture struct {
	store      *runtimetest.Store
	base       time.Time
	waitID     workflowruntime.WaitID
	invocation workflowruntime.NodeInvocationID
	token      string
	request    workflowruntime.SuspendNodeWaitRequest
}

func prepareAtomicWait(t *testing.T, suffix string, kind workflowwait.Kind, source workflowwait.WakeSource, base time.Time, timeout time.Duration) atomicWaitFixture {
	t.Helper()
	running := prepareRunningWait(t, suffix, kind, source, base, timeout)
	authorizer := &recordingAuthorizer{}
	coordinator := workflowruntime.WaitCoordinator{Store: running.store, Authorizer: authorizer}
	result, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: running.request, ResumeToken: running.token})
	if err != nil || result.Wait.Status != workflowruntime.WaitOpen || result.Node.Status != workflowruntime.NodeWaiting || result.Node.Lease != nil || len(result.Events) != 2 {
		t.Fatalf("Suspend = %#v, %v", result, err)
	}
	return atomicWaitFixture{store: running.store, coordinator: coordinator, authorizer: authorizer, base: base, waitID: running.waitID, invocation: running.invocation, token: running.token}
}

func prepareRunningWait(t *testing.T, suffix string, kind workflowwait.Kind, source workflowwait.WakeSource, base time.Time, timeout time.Duration) runningWaitFixture {
	t.Helper()
	store := runtimetest.NewStore()
	invocation := invocationID(workflowruntime.RunID("run-wait-"+suffix), "wait-node")
	waitID := workflowruntime.WaitID("wait-" + suffix)
	createNode(t, store, invocation, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, invocation, 0, "wait-worker", "claim-token-"+suffix, "claim-"+suffix, base.Add(time.Second), base.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(context.Background(), invocation)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: invocation, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "string"})
	if err != nil {
		t.Fatal(err)
	}
	token := "resume-token-" + suffix
	digest, err := workflowwait.DigestToken(token)
	if err != nil {
		t.Fatal(err)
	}
	record := workflowwait.Record{Kind: kind, Correlation: "correlation-" + suffix, Deadline: base.Add(timeout), ResumeSchema: schema, ResumeTokenDigest: digest, ResumeURL: "https://example.test/waits/" + suffix, Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "test", Reference: "fixture"}, WakeSource: source, Status: workflowruntime.WaitOpen}
	request := workflowruntime.SuspendNodeWaitRequest{Wait: workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: waitID}, Invocation: invocation, Record: record}, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, At: base.Add(3 * time.Second)}
	return runningWaitFixture{store: store, base: base, waitID: waitID, invocation: invocation, token: token, request: request}
}

func (f atomicWaitFixture) resumeCommand(key string, at time.Time) workflowruntime.ResumeCommand {
	payload, err := values.NewInline("accepted", values.Metadata{Producer: values.Producer{Kind: "wait_response", Reference: string(f.waitID)}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		panic(err)
	}
	return workflowruntime.ResumeCommand{WaitID: f.waitID, Correlation: "correlation-" + string(f.waitID[len("wait-"):]), Token: f.token, WakeSource: f.waitSource(), Responder: workflowwait.Responder{Kind: "test", Reference: "responder"}, Payload: payload, IdempotencyKey: key, ReceivedAt: at}
}

func (f atomicWaitFixture) waitSource() workflowwait.WakeSource {
	snapshot, err := f.store.LoadWait(context.Background(), f.waitID)
	if err != nil {
		panic(err)
	}
	return snapshot.WakeSource
}

type recordingAuthorizer struct {
	mu    sync.Mutex
	calls int
	deny  error
}

func (a *recordingAuthorizer) AuthorizeResume(_ context.Context, _ workflowwait.AuthorizationRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.deny
}

type recordingScheduler struct {
	mu                     sync.Mutex
	scheduled              []workflowwait.Activation
	canceled               []workflowwait.ActivationID
	scheduleErr, cancelErr error
}

func (s *recordingScheduler) Schedule(_ context.Context, activation workflowwait.Activation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheduled = append(s.scheduled, activation)
	return s.scheduleErr
}
func (s *recordingScheduler) Cancel(_ context.Context, id workflowwait.ActivationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = append(s.canceled, id)
	return s.cancelErr
}

type recordingMaterializer struct{ resolveErr error }

func (*recordingMaterializer) Materialize(context.Context, workflowwait.Materialization) error {
	return nil
}
func (m *recordingMaterializer) Resolve(context.Context, workflowwait.Materialization) error {
	return m.resolveErr
}

func TestWaitStorePublicConformance(t *testing.T) {
	var _ workflowruntime.WaitStore = runtimetest.NewStore()
}
