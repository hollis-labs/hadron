package rundiagnostics

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

func TestPersistenceActivationAttemptsJoinReopenBoundsAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "activation-diagnostics.db")
	database, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(database)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := persistence.NewWorkflowActivationStore(database)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	registration := diagnosticActivationRegistration(base)
	if _, outcome, registerErr := activations.RegisterActivation(t.Context(), registration); registerErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RegisterActivation = %q, %v", outcome, registerErr)
	}
	payload, valueErr := values.NewInline("activation input", values.Metadata{Producer: values.Producer{Kind: "activation", Reference: registration.ID}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	event := hoststate.ActivationEvent{RegistrationID: registration.ID, IdempotencyKey: "delivery-one", OccurredAt: base.Add(time.Minute), Payload: values.ValueSet{"message": payload}, SourceRef: "delivery-one"}
	fire, _, err := activations.RecordActivationEvent(t.Context(), event)
	if err != nil {
		t.Fatal(err)
	}
	first, won, err := activations.ClaimFire(t.Context(), gosched.FireClaim{FireID: fire.ID, ExpectedStatus: fire.Status, ExpectedAttempt: fire.Attempt, ClaimedAt: event.OccurredAt})
	if err != nil || !won {
		t.Fatalf("ClaimFire(first) = %#v, %v, %v", first, won, err)
	}
	prepared, err := activations.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{
		RegistrationID: registration.ID, ExpectedRegistrationGeneration: registration.Generation,
		FireID: first.ID, Attempt: first.Attempt, ScheduledAt: first.ScheduledAt,
		ObservedAt: first.FiredAt, LogicalRunID: "activation-run",
	})
	if err != nil {
		t.Fatal(err)
	}
	retryAt := first.FiredAt.Add(time.Second)
	if applied, transitionErr := activations.TransitionFire(t.Context(), gosched.FireTransition{
		FireID: first.ID, Attempt: first.Attempt, From: gosched.FireClaimed, To: gosched.FireRetrying,
		At: retryAt, NextAttemptAt: retryAt.Add(time.Second), Reason: "temporary_unavailable",
	}); transitionErr != nil || !applied {
		t.Fatalf("TransitionFire(retry) = %v, %v", applied, transitionErr)
	}
	second, won, err := activations.ClaimFire(t.Context(), gosched.FireClaim{FireID: first.ID, ExpectedStatus: gosched.FireRetrying, ExpectedAttempt: first.Attempt, ClaimedAt: retryAt.Add(time.Second)})
	if err != nil || !won {
		t.Fatalf("ClaimFire(second) = %#v, %v, %v", second, won, err)
	}
	replayed, err := activations.PrepareActivation(t.Context(), hoststate.ActivationPrepareRequest{
		RegistrationID: registration.ID, ExpectedRegistrationGeneration: registration.Generation,
		FireID: second.ID, Attempt: second.Attempt, ScheduledAt: second.ScheduledAt,
		ObservedAt: second.FiredAt, LogicalRunID: "activation-run",
	})
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Dispatch != prepared.Dispatch {
		t.Fatalf("PrepareActivation(replay) = %#v, %v", replayed, err)
	}

	planDigest := values.SHA256Digest([]byte("activation-diagnostic-plan"))
	planRef := workflowruntime.PlanRef{ID: "activation-diagnostic", Version: "v1", Digest: planDigest, SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if recordPlanErr := state.RecordPlan(t.Context(), planRef); recordPlanErr != nil {
		t.Fatal(recordPlanErr)
	}
	run, runOutcome, err := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: prepared.Dispatch.PhysicalRunID, Plan: planRef, Status: workflowruntime.RunPending, StartIdempotencyKey: prepared.Dispatch.HostStartKey, CreatedAt: second.FiredAt})
	if err != nil || runOutcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = %#v, %q, %v", run, runOutcome, err)
	}
	if _, completeErr := activations.CompleteActivation(t.Context(), hoststate.ActivationCompleteRequest{
		FireID: prepared.Dispatch.FireID, ExpectedGeneration: prepared.Dispatch.Generation,
		Attempt: prepared.Dispatch.Attempt, Status: hoststate.ActivationDispatchStarted, At: second.FiredAt,
	}); completeErr != nil {
		t.Fatal(completeErr)
	}
	if applied, transitionErr := activations.TransitionFire(t.Context(), gosched.FireTransition{FireID: second.ID, Attempt: second.Attempt, From: gosched.FireClaimed, To: gosched.FireSucceeded, At: second.FiredAt.Add(time.Second)}); transitionErr != nil || !applied {
		t.Fatalf("TransitionFire(success) = %v, %v", applied, transitionErr)
	}
	if closeErr := database.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	reopened, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedState, err := persistence.NewWorkflowStateStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	reopenedActivations, err := persistence.NewWorkflowActivationStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	plan := compile.ExecutionPlan{SchemaVersion: planRef.SchemaVersion, ID: planRef.ID, Digest: planRef.Digest,
		Definition: graph.DefinitionRef{ID: planRef.ID, Version: planRef.Version, Digest: values.SHA256Digest([]byte("activation-source"))},
		Graph: graph.Graph{ID: planRef.ID, Version: planRef.Version, Digest: values.SHA256Digest([]byte("activation-graph")),
			Nodes: []graph.Node{{ID: "noop", Kind: "transform", KindVersion: "v1"}}}}
	service := Service{State: reopenedState, Plans: fixedPlanSource{plan: workflowruntime.RecoveryPlan{Ref: planRef, Plan: plan,
		Visibility: compile.ValueVisibilityPlan{Nodes: map[string]compile.ValueScope{"noop": {}}}}}, Activations: PersistenceActivationAttemptSource{Store: reopenedActivations}}
	result, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour), ActivationLimit: 1})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if !result.Capabilities.ActivationAttempts || !result.Truncated.Activations || len(result.Activations) != 1 {
		t.Fatalf("activation capability/bound = %#v, %#v", result.Capabilities, result.Truncated)
	}
	firstAttempt := result.Activations[0]
	if firstAttempt.FireID != fire.ID || firstAttempt.ActivationID != registration.ID || firstAttempt.RunID != run.ID ||
		firstAttempt.Attempt != 1 || firstAttempt.Status != string(gosched.FireRetrying) || firstAttempt.FailureCode != "temporary_unavailable" ||
		firstAttempt.Source != string(hoststate.ActivationSourceExternal) || !firstAttempt.ScheduledAt.Equal(event.OccurredAt) || !firstAttempt.FiredAt.Equal(first.FiredAt) {
		t.Fatalf("first activation diagnostic = %#v", firstAttempt)
	}
	all, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour), ActivationLimit: 2})
	if err != nil || all.Truncated.Activations || len(all.Activations) != 2 || all.Activations[1].Attempt != 2 || all.Activations[1].Status != string(gosched.FireSucceeded) {
		t.Fatalf("all activation diagnostics = %#v, truncated=%v, err=%v", all.Activations, all.Truncated.Activations, err)
	}
	encoded, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"activation input", prepared.Dispatch.HostStartKey, "claim_expires_at", "payload_json", "dispatch_json"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("activation diagnostics exposed private operational material %q: %s", forbidden, encoded)
		}
	}

	// The join validates duplicated columns against the canonical dispatch
	// snapshot; malformed durable state is reported as corruption, not absence.
	if _, err := reopened.DB().Exec(`UPDATE workflow_activation_dispatches SET scheduled_at = observed_at WHERE fire_id = ?`, fire.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Inspect(t.Context(), Query{RunID: run.ID, Now: base.Add(time.Hour)}); !errors.Is(err, ErrCorruptRunState) {
		t.Fatalf("corrupt activation diagnostic error = %v", err)
	}
}

func diagnosticActivationRegistration(at time.Time) hoststate.ActivationRegistration {
	return hoststate.ActivationRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: "diagnostic-activation",
		Definition:    graph.DefinitionRef{Authority: "registry", Kind: "workflow", ID: "diagnostic", Version: "v1", Digest: values.SHA256Digest([]byte("activation-definition"))},
		InputBindings: map[string]graph.Binding{},
		Principal:     hoststate.ActivationPrincipal{Principal: "service:activation", SourceAuthority: "activation", Trust: "trusted", ExposureRef: "diagnostic-exposure"},
		RunScope:      hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "diagnostic-project"},
		Source:        hoststate.ActivationSource{Kind: hoststate.ActivationSourceExternal, Reference: "diagnostic-source", Config: graph.Config{"type": "diagnostic.event"}},
		Authority:     hoststate.ActivationAuthorityProject,
		Provenance:    graph.Provenance{Authority: "project", Origin: "workflow-source", Digest: values.SHA256Digest([]byte("activation-source"))},
		Policy: hoststate.ActivationPolicy{Overlap: graph.OverlapAllow, RunIDReuse: graph.RunIDReuseAllowDuplicate,
			Retry: hoststate.ActivationRetryPolicy{MaxAttempts: 3, Strategy: "constant", Initial: time.Second, Maximum: time.Minute}},
		Enabled: true, CreatedAt: at, UpdatedAt: at, Generation: 1,
	}
}
