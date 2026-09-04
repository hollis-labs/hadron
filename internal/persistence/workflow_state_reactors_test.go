package persistence

import (
	"context"
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

func TestWorkflowReactorQueuesAcrossContinuationAndReplaysDelivery(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor.db"))
	base := workflowTestTime()
	digest := values.SHA256Digest([]byte("reactor-plan"))
	provenance := values.SHA256Digest([]byte("reactor-provenance"))
	identity := workflowruntime.ReactorIdentity{ID: "reactor-fixture", RegistrationID: "events", RegistrationGeneration: 3, Correlation: "project-1",
		Definition: graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "fixture", Version: "v1", Digest: digest},
		Plan:       workflowruntime.PlanRef{ID: "fixture", Version: "v1", Digest: digest, SchemaVersion: "1"}, Provenance: graph.Provenance{Authority: "project", Origin: "source", Digest: provenance}}
	payload, err := values.NewInline(map[string]any{"sequence": 1}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "delivery"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	delivery := workflowruntime.ReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: "delivery-1", SignalName: "project.changed", Payload: payload,
		Responder: workflowwait.Responder{Kind: "test", Reference: "source"}, OccurredAt: base, ReceivedAt: base}
	reactor, first, outcome, err := state.BeginReactorDelivery(context.Background(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity, InitialRunID: "reactor-run-1", ContinueAfterEvents: 1, Delivery: delivery, At: base})
	if err != nil || outcome != workflowruntime.IdempotencyApplied || first.Status != workflowruntime.ReactorDeliveryPending || reactor.Status != workflowruntime.ReactorStarting {
		t.Fatalf("BeginReactorDelivery = %#v / %#v / %s, %v", reactor, first, outcome, err)
	}
	reactor, err = state.MarkReactorWaiting(context.Background(), identity.ID, reactor.Generation, base.Add(time.Second))
	if err != nil || reactor.Status != workflowruntime.ReactorWaiting {
		t.Fatalf("MarkReactorWaiting = %#v, %v", reactor, err)
	}
	claimed, err := state.ClaimReactorDelivery(context.Background(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: first.Generation, At: base.Add(2 * time.Second)})
	if err != nil || claimed.Status != workflowruntime.ReactorDeliveryApplying {
		t.Fatalf("ClaimReactorDelivery = %#v, %v", claimed, err)
	}
	receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: "reactor-run-1", ProcessedAt: base.Add(3 * time.Second)}
	reactor, completed, err := state.CompleteReactorDelivery(context.Background(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: claimed.Generation, Status: workflowruntime.ReactorDeliveryApplied, Receipt: receipt, At: base.Add(3 * time.Second)})
	if err != nil || completed.Status != workflowruntime.ReactorDeliveryApplied || reactor.EventCount != 1 {
		t.Fatalf("CompleteReactorDelivery = %#v / %#v, %v", reactor, completed, err)
	}
	_, replay, replayOutcome, err := state.BeginReactorDelivery(context.Background(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity, InitialRunID: "reactor-run-1", ContinueAfterEvents: 1, Delivery: delivery, At: base.Add(4 * time.Second)})
	if err != nil || replayOutcome != workflowruntime.IdempotencyReplayed || replay.Status != workflowruntime.ReactorDeliveryApplied {
		t.Fatalf("delivery replay = %#v / %s, %v", replay, replayOutcome, err)
	}

	payload2, _ := values.NewInline(map[string]any{"sequence": 2}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "delivery"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	delivery2 := delivery
	delivery2.IdempotencyKey, delivery2.Payload, delivery2.OccurredAt, delivery2.ReceivedAt = "delivery-2", payload2, base.Add(4*time.Second), base.Add(4*time.Second)
	reactor, queued, _, err := state.BeginReactorDelivery(context.Background(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity, InitialRunID: "reactor-run-1", ContinueAfterEvents: 1, Delivery: delivery2, At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, claimErr := state.ClaimReactorDelivery(context.Background(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: delivery2.IdempotencyKey, ExpectedGeneration: queued.Generation, WaitID: "wait-after-ceiling", At: base.Add(5 * time.Second)}); !errors.Is(claimErr, workflowruntime.ErrReactorRolling) {
		t.Fatalf("delivery beyond max_events ceiling = %v", claimErr)
	}
	stateValue, _ := values.NewInline("cursor-1", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "state"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	rolling, continuation, _, err := state.BeginReactorContinuation(context.Background(), workflowruntime.ReactorContinuationRequest{IdempotencyKey: "continue-1", ReactorID: identity.ID,
		ExpectedGeneration: reactor.Generation, FromGeneration: 1, FromRunID: "reactor-run-1", ToRunID: "reactor-run-2", State: values.ValueSet{"cursor": stateValue}, At: base.Add(5 * time.Second)})
	if err != nil || rolling.Status != workflowruntime.ReactorRolling {
		t.Fatalf("BeginReactorContinuation = %#v / %#v, %v", rolling, continuation, err)
	}
	if _, claimErr := state.ClaimReactorDelivery(context.Background(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: delivery2.IdempotencyKey, ExpectedGeneration: queued.Generation, At: base.Add(6 * time.Second)}); !errors.Is(claimErr, workflowruntime.ErrReactorRolling) {
		t.Fatalf("rolling delivery claim = %v", claimErr)
	}
	continued, sealed, err := state.CompleteReactorContinuation(context.Background(), continuation.Request.IdempotencyKey, continuation.Generation, base.Add(6*time.Second))
	if err != nil || sealed.Status != workflowruntime.ReactorContinuationCompleted || continued.CurrentGeneration != 2 || continued.CurrentRunID != "reactor-run-2" || continued.EventCount != 0 {
		t.Fatalf("CompleteReactorContinuation = %#v / %#v, %v", continued, sealed, err)
	}
	reassigned, err := state.LoadReactorDelivery(context.Background(), identity.ID, delivery2.IdempotencyKey)
	if err != nil || reassigned.ReactorGeneration != 2 || reassigned.RunID != "reactor-run-2" || reassigned.Status != workflowruntime.ReactorDeliveryPending {
		t.Fatalf("reassigned delivery = %#v, %v", reassigned, err)
	}
}

func TestWorkflowReactorTerminalFailureClosesPendingDeliveriesAndReplays(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor-terminal.db"))
	base := workflowTestTime()
	identity, firstRequest := workflowReactorFixture(t, "reactor-terminal", base.Add(time.Second))
	run, outcome, err := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: "reactor-terminal-run", Plan: identity.Plan,
		Status: workflowruntime.RunPending, StartIdempotencyKey: "reactor-terminal-start", CreatedAt: base})
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = %#v / %s, %v", run, outcome, err)
	}
	reactor, first, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: run.ID, ContinueAfterEvents: 5, Delivery: firstRequest, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err = state.MarkReactorWaiting(t.Context(), identity.ID, reactor.Generation, base.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimReactorDelivery(t.Context(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: firstRequest.IdempotencyKey, ExpectedGeneration: first.Generation, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	reactor, _, err = state.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: firstRequest.IdempotencyKey, ExpectedGeneration: claimed.Generation, Status: workflowruntime.ReactorDeliveryApplied,
		Receipt: workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: run.ID, ProcessedAt: firstRequest.ReceivedAt}, At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	_, pendingRequest := workflowReactorFixture(t, identity.ID, base.Add(8*time.Second))
	pendingRequest.IdempotencyKey = "reactor-terminal-pending"
	_, pending, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: run.ID, ContinueAfterEvents: reactor.ContinueAfterEvents, Delivery: pendingRequest, At: pendingRequest.ReceivedAt})
	if err != nil || pending.Status != workflowruntime.ReactorDeliveryPending {
		t.Fatalf("pending delivery = %#v, %v", pending, err)
	}
	terminal, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation,
		To: workflowruntime.RunCanceled, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	recoverable, err := state.RecoverReactors(t.Context(), 1)
	if err != nil || len(recoverable) != 1 || recoverable[0].Identity.ID != identity.ID {
		t.Fatalf("terminal reactor recovery eligibility = %#v, %v", recoverable, err)
	}
	failed, err := state.FailReactor(t.Context(), workflowruntime.FailReactorRequest{ReactorID: identity.ID, ExpectedGeneration: reactor.Generation,
		RunID: run.ID, RunStatus: terminal.Snapshot.Status, At: base.Add(7 * time.Second)})
	if err != nil || failed.Status != workflowruntime.ReactorFailed || !failed.UpdatedAt.Equal(pendingRequest.ReceivedAt) {
		t.Fatalf("FailReactor = %#v, %v", failed, err)
	}
	closed, err := state.LoadReactorDelivery(t.Context(), identity.ID, pendingRequest.IdempotencyKey)
	if err != nil || closed.Status != workflowruntime.ReactorDeliveryClosed || closed.Receipt == nil ||
		closed.Receipt.Kind != workflowruntime.ReactorDeliveryTerminalRun || closed.Receipt.RunStatus != workflowruntime.RunCanceled ||
		!closed.Receipt.ProcessedAt.Equal(pendingRequest.ReceivedAt) {
		t.Fatalf("closed pending delivery = %#v, %v", closed, err)
	}
	_, replay, replayOutcome, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: run.ID, ContinueAfterEvents: reactor.ContinueAfterEvents, Delivery: pendingRequest, At: base.Add(9 * time.Second)})
	if err != nil || replayOutcome != workflowruntime.IdempotencyReplayed || replay.Status != workflowruntime.ReactorDeliveryClosed {
		t.Fatalf("terminal delivery replay = %#v / %s, %v", replay, replayOutcome, err)
	}
	newRequest := pendingRequest
	newRequest.IdempotencyKey, newRequest.OccurredAt, newRequest.ReceivedAt = "reactor-terminal-late", base.Add(10*time.Second), base.Add(10*time.Second)
	if _, _, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: run.ID, ContinueAfterEvents: reactor.ContinueAfterEvents, Delivery: newRequest, At: newRequest.ReceivedAt}); !errors.Is(err, workflowruntime.ErrReactorTerminal) {
		t.Fatalf("late terminal delivery = %v", err)
	}
}

func TestWorkflowReactorDuplicateDeliveryAcrossTwoHandlesCountsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reactor-two-handles.db")
	firstDB, first := openWorkflowStateTest(t, path)
	secondDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	_ = firstDB
	base := workflowTestTime()
	digest := values.SHA256Digest([]byte("reactor-plan-two-handles"))
	identity := workflowruntime.ReactorIdentity{ID: "reactor-two-handles", RegistrationID: "events", RegistrationGeneration: 1, Correlation: "project-2",
		Definition: graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "fixture", Version: "v1", Digest: digest},
		Plan:       workflowruntime.PlanRef{ID: "fixture", Version: "v1", Digest: digest, SchemaVersion: "1"}, Provenance: graph.Provenance{Authority: "project", Origin: "source", Digest: values.SHA256Digest([]byte("reactor-source-two-handles"))}}
	payload, err := values.NewInline(map[string]any{"sequence": 1}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "delivery"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	delivery := workflowruntime.ReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: "same-delivery", SignalName: "project.changed", Payload: payload,
		Responder: workflowwait.Responder{Kind: "test", Reference: "source"}, OccurredAt: base, ReceivedAt: base}
	reactor, pending, _, err := first.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity, InitialRunID: "reactor-run-two-handles", ContinueAfterEvents: 10, Delivery: delivery, At: base})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err = first.MarkReactorWaiting(t.Context(), identity.ID, reactor.Generation, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	stores := []*WorkflowStateStore{first, second}
	claims := make(chan workflowruntime.ReactorDeliverySnapshot, len(stores))
	errs := make(chan error, len(stores))
	var group sync.WaitGroup
	for _, store := range stores {
		group.Add(1)
		go func(store *WorkflowStateStore) {
			defer group.Done()
			claim, claimErr := store.ClaimReactorDelivery(t.Context(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: pending.Generation, At: base.Add(2 * time.Second)})
			claims <- claim
			errs <- claimErr
		}(store)
	}
	group.Wait()
	close(claims)
	close(errs)
	for claimErr := range errs {
		if claimErr != nil {
			t.Fatalf("concurrent claim = %v", claimErr)
		}
	}
	for claim := range claims {
		if claim.Status != workflowruntime.ReactorDeliveryApplying || claim.Generation != 2 {
			t.Fatalf("concurrent claim = %#v", claim)
		}
	}

	receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: "reactor-run-two-handles", ProcessedAt: base.Add(3 * time.Second)}
	results := make(chan workflowruntime.ReactorSnapshot, len(stores))
	errs = make(chan error, len(stores))
	for index, store := range stores {
		group.Add(1)
		go func(store *WorkflowStateStore, completedAt time.Time) {
			defer group.Done()
			result, _, completeErr := store.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: 2, Status: workflowruntime.ReactorDeliveryApplied, Receipt: receipt, At: completedAt})
			results <- result
			errs <- completeErr
		}(store, base.Add(time.Duration(index+3)*time.Second))
	}
	group.Wait()
	close(results)
	close(errs)
	for completeErr := range errs {
		if completeErr != nil {
			t.Fatalf("concurrent completion = %v", completeErr)
		}
	}
	for result := range results {
		if result.EventCount != 1 {
			t.Fatalf("duplicate completion event count = %d", result.EventCount)
		}
	}
	loaded, err := second.LoadReactor(t.Context(), identity.ID)
	if err != nil || loaded.EventCount != 1 || loaded.Generation != reactor.Generation+1 {
		t.Fatalf("cross-handle reactor = %#v, %v", loaded, err)
	}
}

func TestWorkflowReactorCompletionFencesReactorTimestampRegression(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor-time-fence.db"))
	base := workflowTestTime()
	identity, delivery := workflowReactorFixture(t, "reactor-time-fence", base)
	reactor, pending, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{
		Identity: identity, InitialRunID: "reactor-time-fence-run", ContinueAfterEvents: 10, Delivery: delivery, At: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err = state.MarkReactorWaiting(t.Context(), identity.ID, reactor.Generation, base.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimReactorDelivery(t.Context(), workflowruntime.ClaimReactorDeliveryRequest{
		ReactorID: identity.ID, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: pending.Generation, At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: "reactor-time-fence-run", ProcessedAt: delivery.ReceivedAt}
	if _, _, completionErr := state.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: claimed.Generation, Status: workflowruntime.ReactorDeliveryApplied,
		Receipt: receipt, At: base.Add(3 * time.Second),
	}); completionErr == nil {
		t.Fatal("delivery completion regressed the reactor timestamp")
	}
	unchanged, err := state.LoadReactor(t.Context(), identity.ID)
	if err != nil || unchanged.Generation != reactor.Generation || !unchanged.UpdatedAt.Equal(base.Add(10*time.Second)) || unchanged.EventCount != 0 {
		t.Fatalf("rejected timestamp changed reactor = %#v, %v", unchanged, err)
	}
	completed, _, err := state.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: claimed.Generation, Status: workflowruntime.ReactorDeliveryApplied,
		Receipt: receipt, At: base.Add(10 * time.Second),
	})
	if err != nil || completed.EventCount != 1 || !completed.UpdatedAt.Equal(base.Add(10*time.Second)) {
		t.Fatalf("monotonic completion = %#v, %v", completed, err)
	}
}

func TestWorkflowReactorContinuationPreservesLaterPendingDeliveryTimestamp(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor-continuation-time.db"))
	base := workflowTestTime()
	identity, firstDelivery := workflowReactorFixture(t, "reactor-continuation-time", base)
	reactor, pending, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{
		Identity: identity, InitialRunID: "reactor-continuation-time-run-1", ContinueAfterEvents: 1, Delivery: firstDelivery, At: base,
	})
	if err != nil {
		t.Fatal(err)
	}
	reactor, err = state.MarkReactorWaiting(t.Context(), identity.ID, reactor.Generation, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimReactorDelivery(t.Context(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: firstDelivery.IdempotencyKey, ExpectedGeneration: pending.Generation, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: "reactor-continuation-time-run-1", ProcessedAt: firstDelivery.ReceivedAt}
	reactor, _, err = state.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: identity.ID,
		IdempotencyKey: firstDelivery.IdempotencyKey, ExpectedGeneration: claimed.Generation, Status: workflowruntime.ReactorDeliveryApplied,
		Receipt: receipt, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	stateValue, _ := values.NewInline("cursor", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "state"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	rolling, continuation, _, err := state.BeginReactorContinuation(t.Context(), workflowruntime.ReactorContinuationRequest{IdempotencyKey: "reactor-continuation-time-key",
		ReactorID: identity.ID, ExpectedGeneration: reactor.Generation, FromGeneration: 1, FromRunID: "reactor-continuation-time-run-1",
		ToRunID: "reactor-continuation-time-run-2", State: values.ValueSet{"cursor": stateValue}, At: base.Add(10 * time.Second)})
	if err != nil || rolling.Status != workflowruntime.ReactorRolling {
		t.Fatalf("begin continuation = %#v / %#v, %v", rolling, continuation, err)
	}
	later := base.Add(20 * time.Second)
	_, laterDelivery := workflowReactorFixture(t, identity.ID, later)
	laterDelivery.IdempotencyKey = "delivery-during-rolling"
	_, queued, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: "reactor-continuation-time-run-1", ContinueAfterEvents: 1, Delivery: laterDelivery, At: later})
	if err != nil || !queued.CreatedAt.Equal(later) {
		t.Fatalf("queue during rolling = %#v, %v", queued, err)
	}
	continued, _, err := state.CompleteReactorContinuation(t.Context(), continuation.Request.IdempotencyKey, continuation.Generation, base.Add(11*time.Second))
	if err != nil || continued.CurrentGeneration != 2 {
		t.Fatalf("complete continuation = %#v, %v", continued, err)
	}
	reassigned, err := state.LoadReactorDelivery(t.Context(), identity.ID, laterDelivery.IdempotencyKey)
	if err != nil || reassigned.ReactorGeneration != 2 || reassigned.RunID != "reactor-continuation-time-run-2" ||
		!reassigned.CreatedAt.Equal(later) || !reassigned.UpdatedAt.Equal(later) {
		t.Fatalf("reassigned later delivery = %#v, %v", reassigned, err)
	}
}

func TestWorkflowReactorRecoveryPrioritizesGenerationStartRegardlessOfTimestamp(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor-start-order.db"))
	base := workflowTestTime()
	identity, firstDelivery := workflowReactorFixture(t, "reactor-start-order", base.Add(10*time.Second))
	_, first, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: "reactor-start-order-run", ContinueAfterEvents: 10, Delivery: firstDelivery, At: base.Add(10 * time.Second)})
	if err != nil || !first.StartsGeneration {
		t.Fatalf("first delivery = %#v, %v", first, err)
	}
	_, laterDelivery := workflowReactorFixture(t, identity.ID, base.Add(5*time.Second))
	laterDelivery.IdempotencyKey = "older-non-start-delivery"
	_, later, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity,
		InitialRunID: "reactor-start-order-run", ContinueAfterEvents: 10, Delivery: laterDelivery, At: base.Add(5 * time.Second)})
	if err != nil || later.StartsGeneration {
		t.Fatalf("later delivery = %#v, %v", later, err)
	}
	recovered, err := state.RecoverReactorDeliveries(t.Context(), 1)
	if err != nil || len(recovered) != 1 || recovered[0].Request.IdempotencyKey != firstDelivery.IdempotencyKey || !recovered[0].StartsGeneration {
		t.Fatalf("starting recovery order = %#v, %v", recovered, err)
	}
}

func workflowReactorFixture(t *testing.T, id string, at time.Time) (workflowruntime.ReactorIdentity, workflowruntime.ReactorDeliveryRequest) {
	t.Helper()
	digest := values.SHA256Digest([]byte("plan-" + id))
	identity := workflowruntime.ReactorIdentity{ID: id, RegistrationID: "events-" + id, RegistrationGeneration: 1, Correlation: "correlation-" + id,
		Definition: graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "fixture", Version: "v1", Digest: digest},
		Plan:       workflowruntime.PlanRef{ID: "fixture", Version: "v1", Digest: digest, SchemaVersion: "1"},
		Provenance: graph.Provenance{Authority: "project", Origin: "source", Digest: values.SHA256Digest([]byte("source-" + id))}}
	payload, err := values.NewInline(map[string]any{"id": id}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: id},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	delivery := workflowruntime.ReactorDeliveryRequest{ReactorID: id, IdempotencyKey: "delivery-" + id, SignalName: "project.changed", Payload: payload,
		Responder: workflowwait.Responder{Kind: "test", Reference: "source"}, OccurredAt: at, ReceivedAt: at}
	return identity, delivery
}

func TestWorkflowReactorRecoveryEligibilityDoesNotStarveBehindIdlePage(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "reactor-recovery-page.db"))
	base := workflowTestTime()
	eligible := map[string]bool{}
	for index := range 7 {
		id := fmt.Sprintf("reactor-page-%d", index)
		digest := values.SHA256Digest([]byte("plan-" + id))
		identity := workflowruntime.ReactorIdentity{ID: id, RegistrationID: "events-" + id, RegistrationGeneration: 1, Correlation: "correlation-" + id,
			Definition: graph.DefinitionRef{Authority: "project", Kind: "workflow", ID: "fixture", Version: "v1", Digest: digest},
			Plan:       workflowruntime.PlanRef{ID: "fixture", Version: "v1", Digest: digest, SchemaVersion: "1"}, Provenance: graph.Provenance{Authority: "project", Origin: "source", Digest: values.SHA256Digest([]byte("source-" + id))}}
		payload, err := values.NewInline(map[string]any{"sequence": index}, values.Metadata{Producer: values.Producer{Kind: "test", Reference: id}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			t.Fatal(err)
		}
		at := base.Add(time.Duration(index) * time.Second)
		delivery := workflowruntime.ReactorDeliveryRequest{ReactorID: id, IdempotencyKey: "delivery-" + id, SignalName: "project.changed", Payload: payload,
			Responder: workflowwait.Responder{Kind: "test", Reference: "source"}, OccurredAt: at, ReceivedAt: at}
		threshold := uint64(100)
		if index >= 5 {
			threshold = 1
			eligible[id] = true
		}
		reactor, pending, _, err := state.BeginReactorDelivery(t.Context(), workflowruntime.BeginReactorDeliveryRequest{Identity: identity, InitialRunID: workflowruntime.RunID("run-" + id), ContinueAfterEvents: threshold, Delivery: delivery, At: at})
		if err != nil {
			t.Fatal(err)
		}
		reactor, err = state.MarkReactorWaiting(t.Context(), id, reactor.Generation, at.Add(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if threshold != 1 {
			continue
		}
		claim, err := state.ClaimReactorDelivery(t.Context(), workflowruntime.ClaimReactorDeliveryRequest{ReactorID: id, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: pending.Generation, At: at.Add(200 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryStartedRun, RunID: workflowruntime.RunID("run-" + id), ProcessedAt: at.Add(300 * time.Millisecond)}
		if _, _, err := state.CompleteReactorDelivery(t.Context(), workflowruntime.CompleteReactorDeliveryRequest{ReactorID: id, IdempotencyKey: delivery.IdempotencyKey, ExpectedGeneration: claim.Generation, Status: workflowruntime.ReactorDeliveryApplied, Receipt: receipt, At: at.Add(300 * time.Millisecond)}); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := state.RecoverReactors(t.Context(), 2)
	if err != nil || len(recovered) != 2 {
		t.Fatalf("RecoverReactors = %#v, %v", recovered, err)
	}
	for _, reactor := range recovered {
		if !eligible[reactor.Identity.ID] || reactor.EventCount < reactor.ContinueAfterEvents {
			t.Fatalf("recovered idle reactor = %#v", reactor)
		}
	}
}
