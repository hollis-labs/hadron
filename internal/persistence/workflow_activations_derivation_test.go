package persistence

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestWorkflowDerivedActivationReconcilePreservesHistoryAndOperatorRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "derived.db")
	base, _ := openWorkflowStateTest(t, path)
	first, _ := NewWorkflowActivationStore(base)
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, _ := NewWorkflowActivationStore(secondStore)

	owner := values.SHA256Digest([]byte("source-owner"))
	planOne := values.SHA256Digest([]byte("plan-one"))
	planTwo := values.SHA256Digest([]byte("plan-two"))
	at := workflowTestTime()
	operator := workflowActivationFixture(t, "operator-route", hoststate.ActivationSourceExternal)
	operator.Authority = hoststate.ActivationAuthorityOperator
	if _, _, err := first.RegisterActivation(t.Context(), operator); err != nil {
		t.Fatal(err)
	}
	one := derivedActivationFixture(t, owner, planOne, "source-route-one", "route")
	created, reconcileErr := first.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, PlanDigest: planOne, Registrations: []hoststate.ActivationRegistration{one}, At: at,
	})
	if reconcileErr != nil || created.Outcome != workflowruntime.IdempotencyApplied || created.SourceGeneration != 1 || len(created.Registrations) != 1 {
		t.Fatalf("first reconcile = %#v, %v", created, reconcileErr)
	}
	created.Registrations[0].Source.Config["topic"] = "mutated"
	created.Registrations[0].Derivation.SourceOwnerKey = values.SHA256Digest([]byte("mutated"))
	replayed, reconcileErr := second.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, PlanDigest: planOne, Registrations: []hoststate.ActivationRegistration{one}, At: at.Add(time.Second),
	})
	if reconcileErr != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.SourceGeneration != 1 ||
		replayed.Registrations[0].Source.Config["topic"] == "mutated" || replayed.Registrations[0].Derivation.SourceOwnerKey != owner {
		t.Fatalf("reconcile replay = %#v, %v", replayed, reconcileErr)
	}
	claimed := claimExternalActivation(t, first, workflowActivationEvent(t, one.ID, "delivery", "payload"))
	if claimed.Attempt != 1 {
		t.Fatalf("activation attempt = %#v", claimed)
	}
	credential, credentialErr := hoststate.DigestCallbackCredential("derived-history-callback-credential")
	if credentialErr != nil {
		t.Fatal(credentialErr)
	}
	callback := hoststate.CallbackRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: "derived-history-callback", WaitID: "derived-history-wait",
		Correlation: "callback:derived-history", WakeSource: workflowwait.WakeCallback,
		Responder: workflowwait.Responder{Kind: "service", Reference: "approver"}, ValueSchema: graph.Schema{"type": "string"},
		CredentialDigest: credential, ExposureRef: "derived-history-route", ExpiresAt: at.Add(time.Hour), CreatedAt: at, Generation: 1,
	}
	if _, _, callbackErr := first.CreateCallback(t.Context(), callback); callbackErr != nil {
		t.Fatal(callbackErr)
	}
	firesBefore := workflowRowCount(t, base, "workflow_activation_fires")
	attemptsBefore := workflowRowCount(t, base, "workflow_activation_attempts")
	callbacksBefore := workflowRowCount(t, base, "workflow_callback_registrations")
	eventsBefore := workflowRowCount(t, base, "workflow_activation_events")

	two := derivedActivationFixtureKind(t, owner, planTwo, "source-route-two", "route", hoststate.ActivationSourceSchedule)
	changed, reconcileErr := second.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: planOne, PlanDigest: planTwo,
		Registrations: []hoststate.ActivationRegistration{two}, At: at.Add(2 * time.Second),
	})
	if reconcileErr != nil || changed.Outcome != workflowruntime.IdempotencyApplied || changed.SourceGeneration != 2 || len(changed.Registrations) != 1 || changed.Registrations[0].ID != two.ID {
		t.Fatalf("changed reconcile = %#v, %v", changed, reconcileErr)
	}
	all, loadErr := first.ListDerivedActivations(t.Context(), owner)
	if loadErr != nil || len(all) != 2 || !all[0].Derivation.Retired || all[0].Enabled || all[1].Derivation.Retired {
		t.Fatalf("derived history = %#v, %v", all, loadErr)
	}
	operatorAfter, loadErr := first.LoadActivation(t.Context(), operator.ID)
	if loadErr != nil || !operatorAfter.Enabled || operatorAfter.Generation != operator.Generation || operatorAfter.Derivation != nil {
		t.Fatalf("operator registration changed = %#v, %v", operatorAfter, loadErr)
	}
	if workflowRowCount(t, base, "workflow_activation_fires") != firesBefore ||
		workflowRowCount(t, base, "workflow_activation_attempts") != attemptsBefore ||
		workflowRowCount(t, base, "workflow_callback_registrations") != callbacksBefore ||
		workflowRowCount(t, base, "workflow_activation_events") != eventsBefore+2 {
		t.Fatal("declaration change erased or duplicated activation history")
	}

	rowsBefore := workflowRowCount(t, base, "workflow_activation_registrations")
	eventsBefore = workflowRowCount(t, base, "workflow_activation_events")
	var scheduleBefore string
	if err := base.db.QueryRow(`SELECT cron_expr || '|' || IFNULL(last_run_at, '') || '|' || next_run_at || '|' || enabled || '|' || generation || '|' || hex(retry_json) || '|' || hex(payload_json)
FROM workflow_activation_schedules WHERE registration_id = ?`, two.ID).Scan(&scheduleBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: planOne, PlanDigest: planOne,
		Registrations: []hoststate.ActivationRegistration{one}, At: at.Add(3 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("stale plan replay = %v", err)
	}
	if workflowRowCount(t, base, "workflow_activation_registrations") != rowsBefore || workflowRowCount(t, base, "workflow_activation_events") != eventsBefore {
		t.Fatal("stale reconciliation changed durable state")
	}
	var scheduleAfter string
	if err := base.db.QueryRow(`SELECT cron_expr || '|' || IFNULL(last_run_at, '') || '|' || next_run_at || '|' || enabled || '|' || generation || '|' || hex(retry_json) || '|' || hex(payload_json)
FROM workflow_activation_schedules WHERE registration_id = ?`, two.ID).Scan(&scheduleAfter); err != nil || scheduleAfter != scheduleBefore {
		t.Fatalf("stale reconciliation changed schedule = %v", err)
	}

	retired, reconcileErr := second.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: planTwo, At: at.Add(4 * time.Second),
	})
	if reconcileErr != nil || retired.Outcome != workflowruntime.IdempotencyApplied || retired.CurrentPlanDigest != "" || len(retired.Registrations) != 0 {
		t.Fatalf("retire = %#v, %v", retired, reconcileErr)
	}
	if replay, err := first.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: planTwo, At: at.Add(5 * time.Second),
	}); err != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.SourceGeneration != retired.SourceGeneration {
		t.Fatalf("retire replay = %#v, %v", replay, err)
	}

	if err := secondStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowActivationStore(reopenedStore)
	history, err := reopened.ListDerivedActivations(t.Context(), owner)
	if err != nil || len(history) != 2 || !history[0].Derivation.Retired || !history[1].Derivation.Retired ||
		history[0].Derivation.SourceGeneration != retired.SourceGeneration {
		t.Fatalf("reopened history = %#v, %v", history, err)
	}
}

func TestWorkflowDerivedActivationTwoHandleCASAndValidationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "derived-contention.db")
	base, _ := openWorkflowStateTest(t, path)
	first, _ := NewWorkflowActivationStore(base)
	otherStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = otherStore.Close() })
	second, _ := NewWorkflowActivationStore(otherStore)
	owner := values.SHA256Digest([]byte("owner"))
	initialPlan := values.SHA256Digest([]byte("initial-plan"))
	at := workflowTestTime()
	initial := derivedActivationFixture(t, owner, initialPlan, "source-initial", "route")
	if _, _, err := first.RegisterActivation(t.Context(), initial); err == nil {
		t.Fatal("derived registration bypassed atomic reconciliation")
	}
	if _, err := first.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, PlanDigest: initialPlan, Registrations: []hoststate.ActivationRegistration{initial}, At: at,
	}); err != nil {
		t.Fatal(err)
	}
	collisionPlan := values.SHA256Digest([]byte("collision-plan"))
	collision := derivedActivationFixture(t, owner, collisionPlan, "source-collision", "route")
	operator := workflowActivationFixture(t, collision.ID, hoststate.ActivationSourceExternal)
	operator.Authority = hoststate.ActivationAuthorityOperator
	if _, _, registerErr := first.RegisterActivation(t.Context(), operator); registerErr != nil {
		t.Fatal(registerErr)
	}
	eventsBefore := workflowRowCount(t, base, "workflow_activation_events")
	if _, err := second.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: initialPlan, PlanDigest: collisionPlan,
		Registrations: []hoststate.ActivationRegistration{collision}, At: at.Add(time.Second),
	}); err == nil {
		t.Fatal("derived registration overwrote an ad-hoc operator identity")
	}
	initialAfterCollision, err := first.LoadActivation(t.Context(), initial.ID)
	if err != nil || initialAfterCollision.Derivation.Retired || !initialAfterCollision.Enabled || initialAfterCollision.Generation != 1 ||
		workflowRowCount(t, base, "workflow_activation_events") != eventsBefore {
		t.Fatalf("authority collision partially changed source state = %#v, %v", initialAfterCollision, err)
	}
	operatorAfterCollision, err := first.LoadActivation(t.Context(), operator.ID)
	if err != nil || operatorAfterCollision.Authority != hoststate.ActivationAuthorityOperator || operatorAfterCollision.Generation != 1 {
		t.Fatalf("authority collision changed operator = %#v, %v", operatorAfterCollision, err)
	}
	disabled := initial
	disabled.Enabled = false
	var disabledApplied atomic.Int32
	var disabledReplayed atomic.Int32
	var disableWG sync.WaitGroup
	for _, adapter := range []*WorkflowActivationStore{first, second} {
		disableWG.Add(1)
		go func(adapter *WorkflowActivationStore) {
			defer disableWG.Done()
			result, err := adapter.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
				SourceOwnerKey: owner, ExpectedCurrentPlanDigest: initialPlan, PlanDigest: initialPlan,
				Registrations: []hoststate.ActivationRegistration{disabled}, At: at.Add(2 * time.Second),
			})
			if err != nil {
				t.Errorf("disable contender = %v", err)
				return
			}
			switch result.Outcome {
			case workflowruntime.IdempotencyApplied:
				disabledApplied.Add(1)
			case workflowruntime.IdempotencyReplayed:
				disabledReplayed.Add(1)
			default:
				t.Errorf("disable outcome = %q", result.Outcome)
			}
		}(adapter)
	}
	disableWG.Wait()
	if disabledApplied.Load() != 1 || disabledReplayed.Load() != 1 {
		t.Fatalf("disable contention = applied %d replayed %d", disabledApplied.Load(), disabledReplayed.Load())
	}

	corrupt := derivedActivationFixture(t, owner, values.SHA256Digest([]byte("corrupt-plan")), "source-corrupt", "route")
	corrupt.Derivation.SourceDigest = values.SHA256Digest([]byte("forged-source"))
	rowsBefore := workflowRowCount(t, base, "workflow_activation_registrations")
	if _, err := first.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: initialPlan, PlanDigest: corrupt.Derivation.PlanDigest,
		Registrations: []hoststate.ActivationRegistration{corrupt}, At: at.Add(3 * time.Second),
	}); err == nil {
		t.Fatal("forged source digest reconciled")
	}
	if workflowRowCount(t, base, "workflow_activation_registrations") != rowsBefore {
		t.Fatal("invalid reconciliation left partial rows")
	}

	registrations := []hoststate.ActivationRegistration{
		derivedActivationFixture(t, owner, values.SHA256Digest([]byte("next-plan-0")), "source-next-0", "route"),
		derivedActivationFixture(t, owner, values.SHA256Digest([]byte("next-plan-1")), "source-next-1", "route"),
	}
	var applied atomic.Int32
	var conflicts atomic.Int32
	var wg sync.WaitGroup
	for index, adapter := range []*WorkflowActivationStore{first, second} {
		wg.Add(1)
		go func(index int, adapter *WorkflowActivationStore) {
			defer wg.Done()
			plan := values.SHA256Digest([]byte(fmt.Sprintf("next-plan-%d", index)))
			registration := registrations[index]
			result, err := adapter.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
				SourceOwnerKey: owner, ExpectedCurrentPlanDigest: initialPlan, PlanDigest: plan,
				Registrations: []hoststate.ActivationRegistration{registration}, At: at.Add(4 * time.Second),
			})
			if err == nil && result.Outcome == workflowruntime.IdempotencyApplied {
				applied.Add(1)
			} else if errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
				conflicts.Add(1)
			} else {
				t.Errorf("contender %d = %#v, %v", index, result, err)
			}
		}(index, adapter)
	}
	wg.Wait()
	if applied.Load() != 1 || conflicts.Load() != 1 {
		t.Fatalf("contention = applied %d conflicts %d", applied.Load(), conflicts.Load())
	}
	var id string
	if err := base.db.QueryRow(`SELECT registration_id FROM workflow_activation_registrations
WHERE json_extract(CAST(registration_json AS TEXT), '$.derivation.source_owner_key') = ? ORDER BY registration_id LIMIT 1`, owner).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := base.db.Exec(`UPDATE workflow_activation_registrations
SET registration_json = json_set(CAST(registration_json AS TEXT), '$.derivation.source_digest', ?)
WHERE registration_id = ?`, values.SHA256Digest([]byte("tampered-source")), id); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ListDerivedActivations(t.Context(), owner); err == nil {
		t.Fatal("corrupt source derivation was readable")
	}
}

func TestWorkflowDerivedActivationHistoryIsBounded(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "derived-bound.db"))
	adapter, _ := NewWorkflowActivationStore(store)
	owner := values.SHA256Digest([]byte("bounded-owner"))
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index <= hoststate.MaximumDerivedActivationHistory; index++ {
		id := fmt.Sprintf("historical-%04d", index)
		snapshot := fmt.Sprintf(`{"authority":"%s","derivation":{"source_owner_key":"%s"}}`, hoststate.ActivationAuthorityProject, owner)
		if _, err := tx.Exec(`INSERT INTO workflow_activation_registrations(
registration_id, version, source_kind, scope_key, enabled, generation, created_at, updated_at, registration_json
) VALUES (?, 'v1', 'external', 'scope', 0, 1, ?, ?, ?)`, id, workflowTime(workflowTestTime()), workflowTime(workflowTestTime()), []byte(snapshot)); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ListDerivedActivations(t.Context(), owner); err == nil {
		t.Fatal("unbounded derived activation history was accepted")
	}
}

func TestWorkflowDerivedActivationStoredIdentityCorruptionFailsClosed(t *testing.T) {
	for name, mutation := range map[string]struct {
		path  string
		value string
	}{
		"template digest": {
			path:  "$.derivation.template_digest",
			value: values.SHA256Digest([]byte("tampered-template")),
		},
		"registration id": {
			path:  "$.id",
			value: "tampered-derived-registration",
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "derived-corrupt.db"))
			adapter, _ := NewWorkflowActivationStore(store)
			owner := values.SHA256Digest([]byte("corruption-owner"))
			plan := values.SHA256Digest([]byte("corruption-plan"))
			registration := derivedActivationFixture(t, owner, plan, "source-corruption", "route")
			if _, reconcileErr := adapter.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
				SourceOwnerKey: owner, PlanDigest: plan, Registrations: []hoststate.ActivationRegistration{registration}, At: workflowTestTime(),
			}); reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			if _, updateErr := store.db.Exec(`UPDATE workflow_activation_registrations
SET registration_json = json_set(CAST(registration_json AS TEXT), ?, ?)
WHERE registration_id = ?`, mutation.path, mutation.value, registration.ID); updateErr != nil {
				t.Fatal(updateErr)
			}
			if _, loadErr := adapter.ListDerivedActivations(t.Context(), owner); loadErr == nil {
				t.Fatal("corrupt derived activation identity was readable")
			}
		})
	}
}

func TestWorkflowDerivedActivationMissingScheduleProjectionRollsBack(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "derived-missing-schedule.db"))
	adapter, _ := NewWorkflowActivationStore(store)
	owner := values.SHA256Digest([]byte("schedule-owner"))
	plan := values.SHA256Digest([]byte("schedule-plan"))
	at := workflowTestTime()
	registration := derivedActivationFixtureKind(t, owner, plan, "source-schedule", "schedule", hoststate.ActivationSourceSchedule)
	if _, reconcileErr := adapter.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, PlanDigest: plan, Registrations: []hoststate.ActivationRegistration{registration}, At: at,
	}); reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	before, loadErr := adapter.LoadActivation(t.Context(), registration.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	eventsBefore := workflowRowCount(t, store, "workflow_activation_events")
	if _, deleteErr := store.db.Exec(`DELETE FROM workflow_activation_schedules WHERE registration_id = ?`, registration.ID); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	disabled := registration
	disabled.Enabled = false
	if _, reconcileErr := adapter.ReconcileDerivedActivations(t.Context(), hoststate.ActivationReconcileRequest{
		SourceOwnerKey: owner, ExpectedCurrentPlanDigest: plan, PlanDigest: plan,
		Registrations: []hoststate.ActivationRegistration{disabled}, At: at.Add(time.Second),
	}); reconcileErr == nil {
		t.Fatal("missing schedule projection was silently accepted")
	}
	after, loadErr := adapter.LoadActivation(t.Context(), registration.ID)
	if loadErr != nil || after.Generation != before.Generation || after.Enabled != before.Enabled || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("schedule projection failure partially changed registration = %#v, %v", after, loadErr)
	}
	if workflowRowCount(t, store, "workflow_activation_events") != eventsBefore {
		t.Fatal("schedule projection failure appended an event")
	}
}

func derivedActivationFixture(t *testing.T, owner, planDigest, id, templateID string) hoststate.ActivationRegistration {
	return derivedActivationFixtureKind(t, owner, planDigest, id, templateID, hoststate.ActivationSourceExternal)
}

func derivedActivationFixtureKind(t *testing.T, owner, planDigest, id, templateID string, kind hoststate.ActivationSourceKind) hoststate.ActivationRegistration {
	t.Helper()
	registration := workflowActivationFixture(t, id, kind)
	registration.Definition.Digest = values.SHA256Digest([]byte("source-" + planDigest))
	digest, err := hoststate.ActivationMaterializationDigest(registration, templateID)
	if err != nil {
		t.Fatal(err)
	}
	registration.Derivation = &hoststate.ActivationDerivation{
		SourceOwnerKey: owner, SourceDigest: registration.Definition.Digest, PlanDigest: planDigest,
		TemplateID: templateID, TemplateDigest: values.SHA256Digest([]byte("template-" + id)), MaterializationDigest: digest,
		CurrentPlanDigest: planDigest, SourceGeneration: 1,
	}
	registration.ID, err = hoststate.DerivedActivationRegistrationID(owner, planDigest, templateID, registration.Derivation.TemplateDigest, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := registration.Validate(); err != nil {
		t.Fatalf("derived fixture: %v", err)
	}
	return registration
}
