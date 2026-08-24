package appworkflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestActivationExternalUsesHostStartPathAndReplaysStableFire(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	identity := testIdentityBinding("service:activation", "activation")
	identity.Extension = map[string]string{"exposure_ref": "webhook-main"}
	host := hostWithFixedIdentity(t, fixture, identity)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	store, storeErr := persistence.NewWorkflowActivationStore(fixture.store)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	now := fixture.now.Add(time.Minute)
	service := appworkflow.ActivationService{Host: host, Store: store, Clock: appworkflow.ClockFunc(func() time.Time { return now })}
	registration := activationHostRegistration(t, fixture, identity, "external-main")
	if err := registration.Validate(); err != nil {
		t.Fatalf("registration fixture: %v (%#v)", err, registration.Definition)
	}
	if _, outcome, err := service.Register(t.Context(), registration); err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("Register = %q, %v", outcome, err)
	}
	if err := service.Enqueue(t.Context(), gosched.Job{ScheduleID: registration.ID, FireID: "malformed-fire", Attempt: 1,
		ScheduledAt: now, FiredAt: now, Payload: []byte(`{"payload":{} trailing`)}); !errors.Is(err, appworkflow.ErrInvalidActivation) {
		t.Fatalf("malformed scheduler payload = %v", err)
	}

	request := appworkflow.ExternalActivationRequest{
		RegistrationID: registration.ID, IdempotencyKey: "delivery-one", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "from webhook"}, SourceRef: "delivery-source",
	}
	started, activationErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), request)
	if activationErr != nil || started.Start.Run == nil || started.Dispatch.Status != hoststate.ActivationDispatchStarted ||
		started.Start.Run.ID != started.Dispatch.PhysicalRunID || started.Start.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("ActivateExternal = %#v, %v", started, activationErr)
	}
	stored, loadErr := fixture.journal.LoadStartByKey(t.Context(), started.Dispatch.HostStartKey)
	if loadErr != nil || stored.Record.Activation == nil || stored.Record.Activation.ActivationID != registration.ID {
		t.Fatalf("host start record = %#v, %v", stored, loadErr)
	}

	replayed, replayErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), request)
	if replayErr != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Start.Outcome != workflowruntime.IdempotencyReplayed ||
		replayed.Dispatch.PhysicalRunID != started.Dispatch.PhysicalRunID {
		t.Fatalf("activation replay = %#v, %v", replayed, replayErr)
	}
	changed := request
	changed.Payload = map[string]any{"message": "changed"}
	if _, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed delivery = %v", err)
	}

	// A Host start may commit after its scheduler claim has expired. The late
	// claimant loses, while reclaiming the same fire replays the one physical
	// run and completes with the original dispatch provenance.
	late := request
	late.IdempotencyKey, late.OccurredAt, late.ReceivedAt = "delivery-late", now.Add(time.Minute), now.Add(time.Minute)
	now = late.ReceivedAt.Add(2 * time.Minute)
	lateFirst, lateErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), late)
	if !errors.Is(lateErr, gosched.ErrTransitionConflict) || lateFirst.Start.Run == nil {
		t.Fatalf("late completion = %#v, %v", lateFirst, lateErr)
	}
	if _, err := store.ListDueFires(t.Context(), now, 10); err != nil {
		t.Fatal(err)
	}
	late.ReceivedAt = now
	now = now.Add(time.Second)
	lateReplay, lateReplayErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), late)
	if lateReplayErr != nil || lateReplay.Dispatch.PhysicalRunID != lateFirst.Dispatch.PhysicalRunID || lateReplay.Start.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("late activation replay = %#v, %v", lateReplay, lateReplayErr)
	}

	dedup := activationHostRegistration(t, fixture, identity, "dedup-main")
	dedup.Policy.RunIDReuse = graph.RunIDReuseReject
	dedup.Policy.DeduplicationKey = &graph.Expression{Text: "inputs.message"}
	if _, _, err := service.Register(t.Context(), dedup); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	dedupFirst, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), appworkflow.ExternalActivationRequest{
		RegistrationID: dedup.ID, IdempotencyKey: "dedup-one", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "same"}, SourceRef: "dedup-source-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	dedupSecond, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), appworkflow.ExternalActivationRequest{
		RegistrationID: dedup.ID, IdempotencyKey: "dedup-two", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "same"}, SourceRef: "dedup-source-two",
	})
	if !errors.Is(err, appworkflow.ErrActivationSkipped) || dedupSecond.Dispatch.Status != hoststate.ActivationDispatchSkipped ||
		dedupSecond.Dispatch.LogicalRunID != dedupFirst.Dispatch.LogicalRunID {
		t.Fatalf("deduplicated activation = %#v, %v", dedupSecond, err)
	}

	for _, reuse := range []graph.RunIDReusePolicy{graph.RunIDReuseReject, graph.RunIDReuseAllowDuplicate, graph.RunIDReuseTerminateExisting} {
		id := "schedule-" + strings.ReplaceAll(string(reuse), "_", "-")
		registration := activationHostRegistration(t, fixture, identity, id)
		registration.Source = hoststate.ActivationSource{Kind: hoststate.ActivationSourceSchedule, Reference: "schedule-source-" + id, Config: graph.Config{"cron": "* * * * *"}}
		registration.InputBindings = map[string]graph.Binding{"message": {Kind: graph.BindingLiteral, Literal: "scheduled"}}
		registration.Policy.RunIDReuse = reuse
		if _, _, err := service.Register(t.Context(), registration); err != nil {
			t.Fatal(err)
		}
		first := dispatchScheduleOccurrence(t, service, store, registration, registration.CreatedAt)
		second := dispatchScheduleOccurrence(t, service, store, registration, first.ScheduledAt)
		if first.PhysicalRunID == second.PhysicalRunID || first.LogicalRunID == second.LogicalRunID {
			t.Fatalf("%s schedule occurrences collapsed: %#v %#v", reuse, first, second)
		}
	}
}

func TestActivationCallbackBindsDurableWaitAndPayloadIdempotency(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	started := seedRunningCallParent(t, fixture)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	store, storeErr := persistence.NewWorkflowActivationStore(fixture.store)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	service := appworkflow.ActivationService{Host: fixture.host, Store: store}

	schema, schemaErr := workflowwait.NewSchemaRef(graph.Schema{"type": "string"})
	if schemaErr != nil {
		t.Fatal(schemaErr)
	}
	token := "one-time-callback-credential"
	tokenDigest, digestErr := workflowwait.DigestToken(token)
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	suspendedAt := fixture.now.Add(time.Nanosecond)
	waitID := workflowruntime.WaitID("callback-wait")
	wait := workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: waitID}, Invocation: started.Node.ID,
		Record: workflowwait.Record{
			Kind: workflowwait.KindCallback, Correlation: "callback:invoice", Deadline: fixture.now.Add(45 * time.Second),
			ResumeSchema: schema, ResumeTokenDigest: tokenDigest, ResumeURL: "https://callbacks.example.test/invoice",
			Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "service", Reference: "invoice-approver"},
			WakeSource: workflowwait.WakeCallback, Status: workflowwait.StatusOpen,
		},
	}
	lease := started.Node.Lease
	if lease == nil {
		t.Fatal("started attempt has no lease")
	}
	if _, err := fixture.state.SuspendNodeWait(t.Context(), workflowruntime.SuspendNodeWaitRequest{
		Wait: wait, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: workflowruntime.ClaimProof{Owner: lease.Owner, Token: lease.Token, Generation: lease.Generation}, At: suspendedAt,
	}); err != nil {
		t.Fatal(err)
	}
	requested := hoststate.CallbackRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: "invoice-callback", WaitID: waitID,
		ExposureRef: "callback-route", ExpiresAt: fixture.now.Add(30 * time.Second), CreatedAt: suspendedAt, Generation: 1,
	}
	created, outcome, registerErr := service.RegisterCallback(t.Context(), appworkflow.CallbackRegistrationRequest{Registration: requested, Credential: token})
	if registerErr != nil || outcome != workflowruntime.IdempotencyApplied || created.Correlation != wait.Correlation ||
		created.Responder.Reference != wait.Authority.Reference || created.CredentialDigest == token {
		t.Fatalf("RegisterCallback = %#v, %q, %v", created, outcome, registerErr)
	}
	wrong := requested
	wrong.ID = "wrong-callback"
	wrong.Correlation = "forged"
	if _, _, err := service.RegisterCallback(t.Context(), appworkflow.CallbackRegistrationRequest{Registration: wrong, Credential: token}); !errors.Is(err, appworkflow.ErrInvalidActivation) {
		t.Fatalf("mismatched callback = %v", err)
	}

	payload := callbackPayload(t, waitID, "approved")
	resumeAt := fixture.now.Add(20 * time.Second)
	resume := appworkflow.CallbackResumeRequest{CallbackID: created.ID, IdempotencyKey: "delivery-one", Credential: token,
		Responder: workflowruntime.ResumeCommand{Payload: payload, ReceivedAt: resumeAt}}
	resumed, err := service.ResumeCallback(t.Context(), resume)
	if err != nil || resumed.Outcome != workflowruntime.ResumeApplied || resumed.Wait.Status != workflowruntime.WaitResumed {
		t.Fatalf("ResumeCallback = %#v, %v", resumed, err)
	}
	replayed, err := service.ResumeCallback(t.Context(), resume)
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed {
		t.Fatalf("ResumeCallback replay = %#v, %v", replayed, err)
	}
	changed := resume
	changed.Responder.Payload = callbackPayload(t, waitID, "rejected")
	if _, err := service.ResumeCallback(t.Context(), changed); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed callback payload = %v", err)
	}
	badCredential := resume
	badCredential.IdempotencyKey = "delivery-two"
	badCredential.Credential = "wrong-callback-credential"
	if _, err := service.ResumeCallback(t.Context(), badCredential); !errors.Is(err, appworkflow.ErrCallbackCredential) {
		t.Fatalf("wrong callback credential = %v", err)
	}
	closed := requested
	closed.ID = "closed-callback"
	if _, _, err := service.RegisterCallback(t.Context(), appworkflow.CallbackRegistrationRequest{Registration: closed, Credential: token}); !errors.Is(err, appworkflow.ErrInvalidActivation) {
		t.Fatalf("closed wait callback = %v", err)
	}
}

func TestActivationOverlapReplaceFencesFinalizerRunBeforeReplacement(t *testing.T) {
	plan := compileFinalizerHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	identity := testIdentityBinding("service:activation", "activation")
	identity.Extension = map[string]string{"exposure_ref": "replace-route"}
	host := hostWithFixedIdentity(t, fixture, identity)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	store, storeErr := persistence.NewWorkflowActivationStore(fixture.store)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	now := fixture.now.Add(time.Minute)
	service := appworkflow.ActivationService{Host: host, Store: store, Clock: appworkflow.ClockFunc(func() time.Time { return now })}
	registration := activationHostRegistration(t, fixture, identity, "replace-finalizer")
	registration.Policy.Overlap = graph.OverlapReplace
	if _, _, err := service.Register(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	first, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), appworkflow.ExternalActivationRequest{
		RegistrationID: registration.ID, IdempotencyKey: "first", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "first"}, SourceRef: "delivery-first",
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	second, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), appworkflow.ExternalActivationRequest{
		RegistrationID: registration.ID, IdempotencyKey: "second", OccurredAt: now, ReceivedAt: now,
		Payload: map[string]any{"message": "second"}, SourceRef: "delivery-second",
	})
	if err != nil || second.Start.Run == nil || second.Start.Run.ID == first.Start.Run.ID {
		t.Fatalf("replacement activation = %#v, %v", second, err)
	}
	prior, err := fixture.state.LoadRun(t.Context(), first.Start.Run.ID)
	if err != nil || !prior.Status.Active() {
		t.Fatalf("prior run = %#v, %v", prior, err)
	}
	intent, err := fixture.state.LoadTerminalIntent(t.Context(), prior.ID)
	if err != nil || intent.Status != workflowruntime.TerminalIntentPending || intent.IntendedStatus != workflowruntime.RunCanceled {
		t.Fatalf("replacement fence = %#v, %v", intent, err)
	}
	work, _ := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: prior.ID, NodeID: "echo"})
	cleanup, _ := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: prior.ID, NodeID: "cleanup"})
	if work.Status != workflowruntime.NodeCanceled || cleanup.Status != workflowruntime.NodePending {
		t.Fatalf("finalizer-aware replacement = work %#v cleanup %#v", work, cleanup)
	}
}

func activationHostRegistration(t *testing.T, fixture *hostFixture, identity hoststate.IdentityBinding, id string) hoststate.ActivationRegistration {
	t.Helper()
	target := identity.ExecutionTarget.Clone()
	expression := graph.Expression{Text: "inputs.message"}
	definition := fixture.plan.Definition
	definition.Authority = "project"
	return hoststate.ActivationRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: id, Definition: definition,
		InputBindings: map[string]graph.Binding{"message": {Kind: graph.BindingExpression, Expression: &expression}},
		Principal: hoststate.ActivationPrincipal{Principal: identity.Principal, SourceAuthority: identity.SourceAuthority, Trust: identity.Trust,
			Grants: append([]string(nil), identity.Grants...), ExposureRef: identity.Extension["exposure_ref"]},
		RunScope: identity.RunScope.Clone(), ExecutionTarget: &target,
		Source:     hoststate.ActivationSource{Kind: hoststate.ActivationSourceExternal, Reference: "webhook-source", Config: graph.Config{"topic": "events.main"}},
		Authority:  hoststate.ActivationAuthorityProject,
		Provenance: graph.Provenance{Authority: "project", Origin: "workflow-source", Digest: values.SHA256Digest([]byte("activation-source"))},
		Policy: hoststate.ActivationPolicy{Overlap: graph.OverlapAllow, RunIDReuse: graph.RunIDReuseAllowDuplicate,
			Retry: hoststate.ActivationRetryPolicy{MaxAttempts: 3, Strategy: "constant", Initial: time.Second, Maximum: time.Minute}},
		Enabled: true, CreatedAt: fixture.now, UpdatedAt: fixture.now, Generation: 1,
	}
}

func callbackPayload(t *testing.T, waitID workflowruntime.WaitID, input string) values.Value {
	t.Helper()
	value, err := values.NewInline(input, values.Metadata{Producer: values.Producer{Kind: "callback", Reference: string(waitID), Output: workflowruntime.ResumeValueName},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func dispatchScheduleOccurrence(t *testing.T, service appworkflow.ActivationService, store *persistence.WorkflowActivationStore, registration hoststate.ActivationRegistration, from time.Time) hoststate.ActivationDispatch {
	t.Helper()
	next, nextErr := gosched.NextRun(registration.Source.Config["cron"].(string), from)
	if nextErr != nil {
		t.Fatal(nextErr)
	}
	var schedule gosched.Schedule
	due, dueErr := store.ListDueSchedules(t.Context(), next, 100)
	if dueErr != nil {
		t.Fatal(dueErr)
	}
	for _, candidate := range due {
		if candidate.ID == registration.ID {
			schedule = candidate
			break
		}
	}
	if schedule.ID == "" || !schedule.NextRun.Equal(next) {
		t.Fatalf("due schedule %s missing from %#v", registration.ID, due)
	}
	fire := gosched.Fire{ID: gosched.DeriveFireID(registration.ID, next), ScheduleID: registration.ID, ScheduledAt: next,
		Status: gosched.FirePending, NextAttemptAt: next, Retry: schedule.Retry, JobType: schedule.JobType, Payload: schedule.Payload}
	nextRun, nextRunErr := gosched.NextRun(registration.Source.Config["cron"].(string), next)
	if nextRunErr != nil {
		t.Fatal(nextRunErr)
	}
	if created, err := store.CreateFire(t.Context(), gosched.FireCreation{ScheduleID: registration.ID, ExpectedNext: next, NextRun: nextRun, Fire: fire}); err != nil || !created {
		t.Fatalf("CreateFire(%s) = %v, %v", registration.ID, created, err)
	}
	claimed, won, claimErr := store.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire.ID, ExpectedStatus: gosched.FirePending, ExpectedAttempt: 0, ClaimedAt: next})
	if claimErr != nil || !won {
		t.Fatalf("ClaimFire(%s) = %#v, %v, %v", registration.ID, claimed, won, claimErr)
	}
	if err := service.Enqueue(authenticatedContext(t.Context(), "service:activation"), gosched.Job{ScheduleID: registration.ID, FireID: claimed.ID,
		RunID: claimed.ID, JobType: claimed.JobType, Payload: claimed.Payload, ScheduledAt: claimed.ScheduledAt, FiredAt: claimed.FiredAt, Attempt: claimed.Attempt}); err != nil {
		t.Fatalf("Enqueue(%s) = %v", registration.ID, err)
	}
	dispatch, loadErr := store.LoadActivationDispatch(t.Context(), claimed.ID)
	if loadErr != nil || dispatch.Status != hoststate.ActivationDispatchStarted {
		t.Fatalf("LoadActivationDispatch(%s) = %#v, %v", registration.ID, dispatch, loadErr)
	}
	if applied, err := store.TransitionFire(t.Context(), gosched.FireTransition{FireID: claimed.ID, Attempt: claimed.Attempt,
		From: gosched.FireClaimed, To: gosched.FireSucceeded, At: claimed.FiredAt.Add(time.Second)}); err != nil || !applied {
		t.Fatalf("TransitionFire(%s) = %v, %v", registration.ID, applied, err)
	}
	return dispatch
}
