package appworkflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type ReactorDeliveryRequest struct {
	RegistrationID string
	IdempotencyKey string
	Correlation    string
	Payload        map[string]any
	OccurredAt     time.Time
	ReceivedAt     time.Time
}

type ReactorDeliveryResult struct {
	Reactor  runtime.ReactorSnapshot
	Delivery runtime.ReactorDeliverySnapshot
	Outcome  runtime.IdempotencyOutcome
}

// ReactorService accepts only a registration ID and correlation. It reloads
// the exact source-owned registration and plan on every delivery/recovery;
// plan, provenance, logical reactor identity, and physical run IDs are never
// caller-selectable.
type ReactorService struct {
	Host        *Host
	Activations hoststate.ActivationStore
	Store       runtime.ReactorStore
	Clock       Clock
}

func (s ReactorService) Deliver(ctx context.Context, request ReactorDeliveryRequest) (ReactorDeliveryResult, error) {
	if err := s.validate(); err != nil {
		return ReactorDeliveryResult{}, err
	}
	if request.ReceivedAt.IsZero() {
		request.ReceivedAt = request.OccurredAt
	}
	registration, plan, identity, signalName, err := s.resolve(ctx, request.RegistrationID, request.Correlation)
	if err != nil {
		return ReactorDeliveryResult{}, err
	}
	payload, err := values.NewInline(request.Payload, values.Metadata{
		Producer: values.Producer{Kind: "reactor-activation", Reference: request.IdempotencyKey}, MediaType: "application/json",
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return ReactorDeliveryResult{}, fmt.Errorf("reactor payload: %w", err)
	}
	initialRunID := reactorRunID(identity.ID, 1)
	continuePolicy, continued, policyErr := runtime.ParseContinueAsNew(plan.Graph)
	if policyErr != nil || !continued {
		return ReactorDeliveryResult{}, errors.New("reactor plan lacks a valid continue-as-new policy")
	}
	// ParseContinueAsNew bounds max_events to [1, 1_000_000].
	continueAfterEvents := uint64(continuePolicy.MaxEvents) // #nosec G115 -- validated bounded positive value
	deliveryRequest := runtime.ReactorDeliveryRequest{ReactorID: identity.ID, IdempotencyKey: request.IdempotencyKey, SignalName: signalName,
		Payload: payload, Responder: responderFromIdentity(activationIdentity(registration)), OccurredAt: request.OccurredAt.UTC(), ReceivedAt: request.ReceivedAt.UTC()}
	reactor, delivery, outcome, err := s.Store.BeginReactorDelivery(context.WithoutCancel(ctx), runtime.BeginReactorDeliveryRequest{
		Identity: identity, InitialRunID: initialRunID, ContinueAfterEvents: continueAfterEvents, Delivery: deliveryRequest, At: request.ReceivedAt.UTC(),
	})
	if err != nil {
		return ReactorDeliveryResult{}, err
	}
	if reactor.Status == runtime.ReactorStarting {
		if !delivery.StartsGeneration {
			return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, nil
		}
		persistedRequest, requestErr := reactorRequestFromDelivery(registration.ID, reactor.Identity.Correlation, delivery)
		if requestErr != nil {
			return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, requestErr
		}
		if startErr := s.startInitial(ctx, registration, plan, reactor, persistedRequest, false); startErr != nil {
			return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, startErr
		}
		reactor, err = s.Store.MarkReactorWaiting(context.WithoutCancel(ctx), reactor.Identity.ID, reactor.Generation, maxTime(s.now(), reactor.UpdatedAt))
		if err != nil {
			return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, err
		}
	}
	delivery, err = s.applyDelivery(ctx, registration, reactor, delivery, false)
	if errors.Is(err, runtime.ErrSignalNotOpen) || errors.Is(err, runtime.ErrReactorRolling) || errors.Is(err, runtime.ErrReactorBusy) {
		return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, nil
	}
	if err != nil {
		return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, err
	}
	reactor, _ = s.Store.LoadReactor(ctx, reactor.Identity.ID)
	return ReactorDeliveryResult{Reactor: reactor, Delivery: delivery, Outcome: outcome}, nil
}

func (s ReactorService) Recover(ctx context.Context, limit int) error {
	if err := s.validate(); err != nil {
		return err
	}
	continuations, err := s.Store.RecoverReactorContinuations(ctx, limit)
	if err != nil {
		return err
	}
	for _, continuation := range continuations {
		if resumeErr := s.resumeContinuation(ctx, continuation); resumeErr != nil {
			return resumeErr
		}
	}
	reactors, err := s.Store.RecoverReactors(ctx, limit)
	if err != nil {
		return err
	}
	for _, reactor := range reactors {
		registration, plan, identity, _, resolveErr := s.resolve(ctx, reactor.Identity.RegistrationID, reactor.Identity.Correlation)
		if resolveErr != nil {
			return resolveErr
		}
		if !reactorIdentityEqual(identity, reactor.Identity) {
			return errors.New("reactor recovery registration or exact plan changed")
		}
		policy, ok, policyErr := runtime.ParseContinueAsNew(plan.Graph)
		// ParseContinueAsNew bounds max_events to [1, 1_000_000].
		policyMaxEvents := uint64(policy.MaxEvents) // #nosec G115 -- validated bounded positive value
		if policyErr != nil || !ok || reactor.ContinueAfterEvents != policyMaxEvents {
			return errors.New("reactor recovery continuation policy changed")
		}
		if reactor.Status == runtime.ReactorWaiting {
			if continueErr := s.continueIfTerminal(ctx, registration, plan, reactor, policy); continueErr != nil && !errors.Is(continueErr, runtime.ErrControlFlowPending) && !errors.Is(continueErr, runtime.ErrReactorBusy) {
				return continueErr
			}
		}
	}
	deliveries, err := s.Store.RecoverReactorDeliveries(ctx, limit)
	if err != nil {
		return err
	}
	for _, delivery := range deliveries {
		reactor, loadErr := s.Store.LoadReactor(ctx, delivery.Request.ReactorID)
		if loadErr != nil {
			return loadErr
		}
		registration, plan, _, _, resolveErr := s.resolve(ctx, reactor.Identity.RegistrationID, reactor.Identity.Correlation)
		if resolveErr != nil {
			return resolveErr
		}
		if reactor.Status == runtime.ReactorStarting {
			if !delivery.StartsGeneration {
				continue
			}
			request, requestErr := reactorRequestFromDelivery(registration.ID, reactor.Identity.Correlation, delivery)
			if requestErr != nil {
				return requestErr
			}
			if startErr := s.startInitial(ctx, registration, plan, reactor, request, true); startErr != nil {
				return startErr
			}
			reactor, err = s.Store.MarkReactorWaiting(ctx, reactor.Identity.ID, reactor.Generation, maxTime(s.now(), reactor.UpdatedAt))
			if err != nil {
				return err
			}
		}
		applied, applyErr := s.applyDelivery(ctx, registration, reactor, delivery, true)
		if applyErr != nil && !errors.Is(applyErr, runtime.ErrSignalNotOpen) && !errors.Is(applyErr, runtime.ErrReactorRolling) && !errors.Is(applyErr, runtime.ErrReactorBusy) {
			var postCommit *runtime.PostCommitError
			if !errors.As(applyErr, &postCommit) || applied.Status != runtime.ReactorDeliveryApplied {
				return applyErr
			}
		}
	}
	return nil
}

func (s ReactorService) validate() error {
	if s.Host == nil || nilInterface(s.Activations) || nilInterface(s.Store) {
		return fmt.Errorf("%w: reactor service requires host, activation registration store, and reactor store", ErrInvalidHost)
	}
	return nil
}

func (s ReactorService) resolve(ctx context.Context, registrationID, correlation string) (hoststate.ActivationRegistration, *compile.ExecutionPlan, runtime.ReactorIdentity, string, error) {
	registration, err := s.Activations.LoadActivation(ctx, registrationID)
	if err != nil {
		return registration, nil, runtime.ReactorIdentity{}, "", err
	}
	if !registration.Enabled || registration.Authority != hoststate.ActivationAuthorityProject || registration.Derivation == nil || registration.Derivation.Retired || registration.Source.Kind != hoststate.ActivationSourceExternal || registration.Source.OneShot || strings.TrimSpace(correlation) == "" || len(correlation) > 1024 {
		return registration, nil, runtime.ReactorIdentity{}, "", fmt.Errorf("%w: reactor requires an enabled message/event registration and bounded correlation", ErrInvalidActivation)
	}
	plan, err := s.Host.definitions.ResolvePlan(ctx, registration.Definition)
	if err != nil {
		return registration, nil, runtime.ReactorIdentity{}, "", err
	}
	plan, err = cloneExecutionPlan(plan)
	if err != nil {
		return registration, nil, runtime.ReactorIdentity{}, "", err
	}
	if !reflect.DeepEqual(plan.Definition, registration.Definition) || plan.Graph.Provenance.Digest == "" || runtime.EffectiveDurability(plan.Graph) != graph.DurabilitySteps || registration.Derivation.PlanDigest != plan.Digest || registration.Derivation.CurrentPlanDigest != plan.Digest {
		return registration, nil, runtime.ReactorIdentity{}, "", errors.New("reactor registration does not resolve to its exact durable plan and provenance")
	}
	if _, ok, policyErr := runtime.ParseContinueAsNew(plan.Graph); policyErr != nil || !ok {
		return registration, nil, runtime.ReactorIdentity{}, "", errors.New("reactor plan lacks canonical continue-as-new policy")
	}
	declaration, ok := sourceReactorDeclaration(plan.Graph, registration)
	if !ok {
		return registration, nil, runtime.ReactorIdentity{}, "", errors.New("reactor registration is not the exact source-derived message/event declaration")
	}
	signalName := ""
	if eventType, ok := declaration.Config["type"].(string); ok {
		signalName = eventType
	} else if topic, ok := declaration.Config["to"].(string); ok {
		signalName = topic
	} else if topic, ok := declaration.Config["topic"].(string); ok {
		signalName = topic
	}
	if strings.TrimSpace(signalName) == "" {
		return registration, nil, runtime.ReactorIdentity{}, "", errors.New("reactor registration has no exact message/event signal name")
	}
	ref := runtime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	identity := runtime.ReactorIdentity{RegistrationID: registration.ID, RegistrationGeneration: registration.Generation, Correlation: correlation,
		Definition: registration.Definition, Plan: ref, Provenance: plan.Graph.Provenance}
	identity.ID = reactorID(identity)
	if err := identity.Validate(); err != nil {
		return registration, nil, identity, "", err
	}
	return registration, plan, identity, signalName, nil
}

func sourceReactorDeclaration(workflow graph.Graph, registration hoststate.ActivationRegistration) (graph.ActivationDeclaration, bool) {
	if registration.Derivation == nil || registration.Provenance.Authority != "project" && registration.Provenance.Authority != "project_source" {
		return graph.ActivationDeclaration{}, false
	}
	for _, declaration := range workflow.Activations {
		if declaration.ID != registration.Derivation.TemplateID || declaration.Kind != "message" && declaration.Kind != "event" {
			continue
		}
		templateDigest, err := hoststate.ActivationTemplateDigest(declaration)
		if err != nil || templateDigest != registration.Derivation.TemplateDigest {
			return graph.ActivationDeclaration{}, false
		}
		reference := declaration.Provenance.Locator
		if reference == "" {
			reference = declaration.ID
		}
		if !reflect.DeepEqual(declaration.Config, registration.Source.Config) || !reflect.DeepEqual(declaration.Inputs, registration.InputBindings) || !reflect.DeepEqual(declaration.Provenance, registration.Provenance) || registration.Source.Reference != reference {
			return graph.ActivationDeclaration{}, false
		}
		return declaration, true
	}
	return graph.ActivationDeclaration{}, false
}

func (s ReactorService) handlesSourceRegistration(ctx context.Context, registrationID string) (bool, error) {
	registration, err := s.Activations.LoadActivation(ctx, registrationID)
	if err != nil {
		return false, err
	}
	if !registration.Enabled || registration.Authority != hoststate.ActivationAuthorityProject || registration.Derivation == nil || registration.Derivation.Retired || registration.Source.Kind != hoststate.ActivationSourceExternal || registration.Source.OneShot {
		return false, nil
	}
	plan, err := s.Host.definitions.ResolvePlan(ctx, registration.Definition)
	if err != nil {
		return false, err
	}
	plan, err = cloneExecutionPlan(plan)
	if err != nil {
		return false, err
	}
	_, continued, policyErr := runtime.ParseContinueAsNew(plan.Graph)
	if policyErr != nil {
		return false, policyErr
	}
	if !continued {
		return false, nil
	}
	if !reflect.DeepEqual(plan.Definition, registration.Definition) || registration.Derivation.PlanDigest != plan.Digest || registration.Derivation.CurrentPlanDigest != plan.Digest {
		return false, errors.New("source reactor registration changed its exact plan")
	}
	_, ok := sourceReactorDeclaration(plan.Graph, registration)
	if !ok {
		return false, errors.New("source reactor registration changed its exact activation template")
	}
	return true, nil
}

func (s ReactorService) startInitial(ctx context.Context, registration hoststate.ActivationRegistration, plan *compile.ExecutionPlan, reactor runtime.ReactorSnapshot, request ReactorDeliveryRequest, allowRecovery bool) error {
	payloadContext, err := privateActivationContext(registration, request.Payload, request.IdempotencyKey, request.OccurredAt)
	if err != nil {
		return err
	}
	inputs, err := evaluateActivationInputs(registration, payloadContext, request.IdempotencyKey)
	if err != nil {
		return err
	}
	expectedIdentity := activationIdentity(registration)
	result, err := s.Host.startRunInternal(ctx, StartRunRequest{RunID: reactor.CurrentRunID, Definition: registration.Definition, Inputs: inputs,
		IdempotencyKey: reactorStartKey(reactor.Identity.ID, reactor.CurrentGeneration), Identity: activationIdentityRequest(registration),
		Activation: &hoststate.ActivationBinding{ActivationID: registration.ID, IdempotencyKey: request.IdempotencyKey, OccurredAt: request.OccurredAt.UTC()}}, &expectedIdentity, allowRecovery)
	if err != nil {
		return err
	}
	if result.Facts.Plan != reactor.Identity.Plan || result.Facts.Plan.Digest != plan.Digest {
		return errors.New("reactor start did not preserve exact plan")
	}
	return nil
}

func (s ReactorService) applyDelivery(ctx context.Context, registration hoststate.ActivationRegistration, reactor runtime.ReactorSnapshot, delivery runtime.ReactorDeliverySnapshot, allowRecovery bool) (runtime.ReactorDeliverySnapshot, error) {
	if delivery.Status == runtime.ReactorDeliveryApplied || delivery.Status == runtime.ReactorDeliveryClosed {
		return delivery, nil
	}
	if delivery.Status == runtime.ReactorDeliveryPending {
		var waitID runtime.WaitID
		if !delivery.StartsGeneration {
			controls, ok := s.Host.state.(runtime.RunControlStore)
			if !ok {
				return delivery, errors.New("reactor host lacks named signal controls")
			}
			wait, err := controls.FindOpenSignalWait(ctx, runtime.SignalSelector{RunID: delivery.RunID, Name: delivery.Request.SignalName, Correlation: reactor.Identity.Correlation})
			if err != nil {
				return delivery, err
			}
			waitID = wait.Ref.ID
		}
		claimed, err := s.Store.ClaimReactorDelivery(context.WithoutCancel(ctx), runtime.ClaimReactorDeliveryRequest{ReactorID: reactor.Identity.ID,
			IdempotencyKey: delivery.Request.IdempotencyKey, ExpectedGeneration: delivery.Generation, WaitID: waitID, At: maxTime(s.now(), delivery.UpdatedAt)})
		if err != nil {
			return delivery, err
		}
		delivery = claimed
	}
	completionReactor, err := s.Store.LoadReactor(ctx, reactor.Identity.ID)
	if err != nil {
		return delivery, err
	}
	if completionReactor.CurrentGeneration != delivery.ReactorGeneration || completionReactor.CurrentRunID != delivery.RunID {
		return delivery, runtime.ErrReactorRolling
	}
	completedAt := maxTime(maxTime(s.now(), delivery.UpdatedAt), completionReactor.UpdatedAt)
	if delivery.StartsGeneration {
		receipt := runtime.ReactorDeliveryReceipt{Kind: runtime.ReactorDeliveryStartedRun, RunID: delivery.RunID, ProcessedAt: delivery.Request.ReceivedAt}
		_, completed, completeErr := s.Store.CompleteReactorDelivery(context.WithoutCancel(ctx), runtime.CompleteReactorDeliveryRequest{ReactorID: reactor.Identity.ID,
			IdempotencyKey: delivery.Request.IdempotencyKey, ExpectedGeneration: delivery.Generation, Status: runtime.ReactorDeliveryApplied, Receipt: receipt, At: completedAt})
		return completed, completeErr
	}
	if delivery.ClaimedWaitID == "" {
		return delivery, errors.New("applying reactor delivery has no exact claimed wait")
	}
	wait, err := s.Host.state.LoadWait(ctx, delivery.ClaimedWaitID)
	if err != nil {
		return delivery, err
	}
	selector := runtime.SignalSelector{RunID: delivery.RunID, Name: delivery.Request.SignalName, Correlation: reactor.Identity.Correlation}
	updateKey := reactorUpdateKey(reactor.Identity.ID, delivery.Request.IdempotencyKey, delivery.ClaimedWaitID)
	var update runtime.RunUpdateSnapshot
	var updateErr error
	if allowRecovery {
		update, updateErr = s.Host.recoverRunUpdateAtWait(ctx, selector, delivery.Request.Payload, updateKey, wait, delivery.Request.Responder,
			maxTime(delivery.Request.ReceivedAt, wait.UpdatedAt))
	} else {
		update, updateErr = s.Host.updateRunAtWaitInternal(ctx, UpdateRunRequest{Selector: selector, Payload: delivery.Request.Payload,
			IdempotencyKey: updateKey, Identity: activationIdentityRequest(registration)}, wait, false)
	}
	if updateErr != nil && update.Status == "" {
		currentWait, loadErr := s.Host.state.LoadWait(ctx, delivery.ClaimedWaitID)
		if loadErr == nil && currentWait.Status != workflowwait.StatusOpen {
			released, releaseErr := s.Store.ReleaseReactorDelivery(context.WithoutCancel(ctx), runtime.ReleaseReactorDeliveryRequest{ReactorID: reactor.Identity.ID,
				IdempotencyKey: delivery.Request.IdempotencyKey, ExpectedGeneration: delivery.Generation, At: maxTime(completedAt, delivery.UpdatedAt)})
			return released, errors.Join(runtime.ErrSignalNotOpen, releaseErr)
		}
	}
	if update.Status == runtime.RunUpdateClosed || errors.Is(updateErr, runtime.ErrWaitClosed) {
		released, releaseErr := s.Store.ReleaseReactorDelivery(context.WithoutCancel(ctx), runtime.ReleaseReactorDeliveryRequest{ReactorID: reactor.Identity.ID,
			IdempotencyKey: delivery.Request.IdempotencyKey, ExpectedGeneration: delivery.Generation, At: maxTime(completedAt, delivery.UpdatedAt)})
		return released, errors.Join(runtime.ErrSignalNotOpen, releaseErr)
	}
	if updateErr != nil {
		var postCommit *runtime.PostCommitError
		if !errors.As(updateErr, &postCommit) || update.Status != runtime.RunUpdateApplied {
			return delivery, updateErr
		}
	}
	if update.Receipt == nil {
		return delivery, errors.New("reactor update completed without receipt")
	}
	receipt := runtime.ReactorDeliveryReceipt{Kind: runtime.ReactorDeliveryResumedWait, RunID: delivery.RunID, Update: update.Receipt,
		ProcessedAt: update.Receipt.ResolvedAt}
	_, completed, completeErr := s.Store.CompleteReactorDelivery(context.WithoutCancel(ctx), runtime.CompleteReactorDeliveryRequest{ReactorID: reactor.Identity.ID,
		IdempotencyKey: delivery.Request.IdempotencyKey, ExpectedGeneration: delivery.Generation, Status: runtime.ReactorDeliveryApplied, Receipt: receipt, At: completedAt})
	return completed, errors.Join(updateErr, completeErr)
}

func reactorRequestFromDelivery(registrationID, correlation string, delivery runtime.ReactorDeliverySnapshot) (ReactorDeliveryRequest, error) {
	payload, ok := delivery.Request.Payload.Inline.(map[string]any)
	if !ok {
		return ReactorDeliveryRequest{}, errors.New("persisted initial reactor payload is not an object")
	}
	return ReactorDeliveryRequest{RegistrationID: registrationID, IdempotencyKey: delivery.Request.IdempotencyKey, Correlation: correlation,
		Payload: payload, OccurredAt: delivery.Request.OccurredAt, ReceivedAt: delivery.Request.ReceivedAt}, nil
}

func (s ReactorService) continueIfTerminal(ctx context.Context, registration hoststate.ActivationRegistration, plan *compile.ExecutionPlan, reactor runtime.ReactorSnapshot, policy runtime.ContinueAsNewPolicy) error {
	run, err := s.Host.state.LoadRun(ctx, reactor.CurrentRunID)
	if err != nil {
		return err
	}
	if !run.Status.Terminal() {
		return runtime.ErrControlFlowPending
	}
	if run.Status != runtime.RunSucceeded {
		_, failErr := s.Store.FailReactor(context.WithoutCancel(ctx), runtime.FailReactorRequest{ReactorID: reactor.Identity.ID,
			ExpectedGeneration: reactor.Generation, RunID: reactor.CurrentRunID, RunStatus: run.Status,
			At: maxTime(maxTime(s.now(), reactor.UpdatedAt), run.UpdatedAt)})
		return failErr
	}
	if run.Outputs == nil {
		return errors.New("successful reactor run has no typed outputs to continue")
	}
	outputs, err := s.Host.state.LoadValues(ctx, *run.Outputs)
	if err != nil {
		return err
	}
	state := make(values.ValueSet, len(policy.Carry))
	inputs := make(map[string]any, len(policy.Carry))
	for _, name := range policy.Carry {
		value, ok := outputs[name]
		if !ok || value.Type == values.TypeArtifact || value.Type == values.TypeSecretRef {
			return fmt.Errorf("reactor carried output %q is absent or cannot cross the inline start boundary", name)
		}
		state[name], inputs[name] = value, value.Inline
	}
	key := reactorContinuationKey(reactor.Identity.ID, reactor.CurrentGeneration)
	toRunID := reactorRunID(reactor.Identity.ID, reactor.CurrentGeneration+1)
	_, continuation, _, err := s.Store.BeginReactorContinuation(context.WithoutCancel(ctx), runtime.ReactorContinuationRequest{IdempotencyKey: key, ReactorID: reactor.Identity.ID,
		ExpectedGeneration: reactor.Generation, FromGeneration: reactor.CurrentGeneration, FromRunID: reactor.CurrentRunID, ToRunID: toRunID, State: state, At: maxTime(s.now(), reactor.UpdatedAt)})
	if err != nil {
		return err
	}
	return s.resumeContinuationWith(ctx, registration, plan, continuation)
}

func (s ReactorService) resumeContinuation(ctx context.Context, continuation runtime.ReactorContinuationSnapshot) error {
	reactor, err := s.Store.LoadReactor(ctx, continuation.Request.ReactorID)
	if err != nil {
		return err
	}
	registration, plan, identity, _, err := s.resolve(ctx, reactor.Identity.RegistrationID, reactor.Identity.Correlation)
	if err != nil {
		return err
	}
	if !reactorIdentityEqual(identity, reactor.Identity) {
		return errors.New("continuation recovery changed reactor identity")
	}
	return s.resumeContinuationWith(ctx, registration, plan, continuation)
}

func (s ReactorService) resumeContinuationWith(ctx context.Context, registration hoststate.ActivationRegistration, plan *compile.ExecutionPlan, continuation runtime.ReactorContinuationSnapshot) error {
	inputs := make(map[string]any, len(continuation.Request.State))
	for name, value := range continuation.Request.State {
		if value.Type == values.TypeArtifact || value.Type == values.TypeSecretRef {
			return errors.New("continuation state cannot cross the inline start boundary")
		}
		inputs[name] = value.Inline
	}
	expectedIdentity := activationIdentity(registration)
	started, err := s.Host.startRunInternal(ctx, StartRunRequest{RunID: continuation.Request.ToRunID, Definition: registration.Definition, Inputs: inputs,
		IdempotencyKey: continuation.Request.IdempotencyKey, Identity: activationIdentityRequest(registration),
		Activation: &hoststate.ActivationBinding{ActivationID: registration.ID, IdempotencyKey: continuation.Request.IdempotencyKey, OccurredAt: continuation.Request.At}}, &expectedIdentity, true)
	if err != nil {
		return err
	}
	if started.Facts.Plan.Digest != plan.Digest {
		return errors.New("continued reactor changed exact plan")
	}
	_, _, err = s.Store.CompleteReactorContinuation(context.WithoutCancel(ctx), continuation.Request.IdempotencyKey, continuation.Generation, maxTime(s.now(), continuation.UpdatedAt))
	return err
}

func (s ReactorService) now() time.Time {
	if s.Clock != nil && !nilInterface(s.Clock) {
		return s.Clock.Now().UTC()
	}
	return s.Host.now()
}

func reactorID(identity runtime.ReactorIdentity) string {
	encoded, _ := json.Marshal(struct {
		RegistrationID string `json:"registration_id"`
		Generation     uint64 `json:"generation"`
		Correlation    string `json:"correlation"`
		Plan           string `json:"plan"`
		Provenance     string `json:"provenance"`
	}{identity.RegistrationID, identity.RegistrationGeneration, identity.Correlation, identity.Plan.Digest, identity.Provenance.Digest})
	digest := sha256.Sum256(encoded)
	return "reactor-" + hex.EncodeToString(digest[:])
}

func reactorRunID(id string, generation uint64) runtime.RunID {
	return runtime.RunID(fmt.Sprintf("%s-g%06d", id, generation))
}

func reactorStartKey(id string, generation uint64) string {
	return fmt.Sprintf("%s:start:%d", id, generation)
}
func reactorContinuationKey(id string, generation uint64) string {
	return fmt.Sprintf("%s:continue:%d", id, generation)
}
func reactorUpdateKey(id, key string, waitID runtime.WaitID) string {
	digest := sha256.Sum256([]byte(id + "\x00" + key + "\x00" + string(waitID)))
	return "reactor-update-" + hex.EncodeToString(digest[:])
}

func reactorIdentityEqual(left, right runtime.ReactorIdentity) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}
