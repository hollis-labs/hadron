package appworkflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

var (
	ErrInvalidActivation  = errors.New("invalid workflow activation")
	ErrActivationConflict = errors.New("workflow activation conflict")
	ErrActivationSkipped  = errors.New("workflow activation skipped")
	ErrCallbackExpired    = errors.New("workflow callback expired")
	ErrCallbackCredential = errors.New("workflow callback credential rejected")
)

type ActivationService struct {
	Host                *Host
	Store               hoststate.ActivationStore
	Clock               Clock
	CurrentRegistry     SourceActivationRegistry
	RequireCurrentFence bool
}

// SourceActivationCurrentFence serializes one derived activation admission
// with mutations of the registry's movable current alias. The supplied view
// must observe the same catalog state protected by the fence.
type SourceActivationCurrentFence interface {
	WithSourceActivationCurrent(context.Context, func(SourceActivationRegistry) error) error
}

func (s ActivationService) validate() error {
	if s.Host == nil || s.Store == nil || nilInterface(s.Store) {
		return fmt.Errorf("%w: activation service requires host and store", ErrInvalidActivation)
	}
	if s.RequireCurrentFence {
		fence, ok := s.CurrentRegistry.(SourceActivationCurrentFence)
		if s.CurrentRegistry == nil || nilInterface(s.CurrentRegistry) || !ok || nilInterface(fence) {
			return fmt.Errorf("%w: activation service requires a current-registry admission fence", ErrInvalidActivation)
		}
	}
	return nil
}

func (s ActivationService) Register(ctx context.Context, registration hoststate.ActivationRegistration) (hoststate.ActivationRegistration, runtime.IdempotencyOutcome, error) {
	if err := s.validate(); err != nil {
		return hoststate.ActivationRegistration{}, "", err
	}
	if ctx == nil {
		return hoststate.ActivationRegistration{}, "", fmt.Errorf("%w: context is required", ErrInvalidActivation)
	}
	cloned, err := registration.Clone()
	if err != nil || cloned.Validate() != nil {
		return hoststate.ActivationRegistration{}, "", fmt.Errorf("%w: registration is malformed", ErrInvalidActivation)
	}
	return s.Store.RegisterActivation(ctx, cloned)
}

// Enqueue is the common graph-native scheduler runner. Stable fire identity
// selects one physical run; delivery attempt remains durable scheduler history.
func (s ActivationService) Enqueue(ctx context.Context, job gosched.Job) error {
	payload, err := decodeActivationPayload(job.Payload)
	if err != nil {
		return err
	}
	_, err = s.Start(ctx, ActivationStartRequest{
		RegistrationID: job.ScheduleID,
		FireID:         job.FireID,
		Attempt:        job.Attempt,
		ScheduledAt:    job.ScheduledAt.UTC(),
		ObservedAt:     job.FiredAt.UTC(),
		Payload:        payload,
	})
	if errors.Is(err, ErrActivationSkipped) {
		return fmt.Errorf("activation skipped: %w", gosched.ErrDuplicateJob)
	}
	return err
}

type ActivationStartRequest struct {
	RegistrationID string
	FireID         string
	Attempt        int
	ScheduledAt    time.Time
	ObservedAt     time.Time
	Payload        map[string]any
	LogicalRunID   string
}

type ActivationStartResult struct {
	Dispatch hoststate.ActivationDispatch
	Start    StartRunResult
	Reactor  *ReactorDeliveryResult
	Outcome  runtime.IdempotencyOutcome
}

const (
	WorkflowActivationFireDirect  = "direct"
	WorkflowActivationFireReactor = "reactor"
)

// FireWorkflowActivationResult is the bounded public projection of either a
// direct durable run admission or a reactor delivery. Reactor request payload,
// responder authority, correlation, receipts, and internal dispatch state are
// deliberately excluded.
type FireWorkflowActivationResult struct {
	Kind    string                             `json:"kind"`
	Outcome runtime.IdempotencyOutcome         `json:"outcome"`
	Start   *StartRunResult                    `json:"start,omitempty"`
	Reactor *WorkflowReactorActivationDelivery `json:"reactor,omitempty"`
}

type WorkflowReactorActivationDelivery struct {
	ReactorID         string                        `json:"reactor_id"`
	CurrentGeneration uint64                        `json:"current_generation"`
	RunID             runtime.RunID                 `json:"run_id"`
	ReactorStatus     runtime.ReactorStatus         `json:"reactor_status"`
	DeliveryStatus    runtime.ReactorDeliveryStatus `json:"delivery_status"`
}

func SafeFireWorkflowActivationResult(result ActivationStartResult) (FireWorkflowActivationResult, error) {
	if !validActivationIdempotencyOutcome(result.Outcome) {
		return FireWorkflowActivationResult{}, fmt.Errorf("%w: activation result outcome is invalid", ErrInvalidActivation)
	}
	if result.Reactor != nil {
		if !reflect.DeepEqual(result.Start, StartRunResult{}) {
			return FireWorkflowActivationResult{}, fmt.Errorf("%w: activation result contains both direct and reactor branches", ErrInvalidActivation)
		}
		reactor := result.Reactor
		if !validActivationIdempotencyOutcome(reactor.Outcome) || reactor.Outcome != result.Outcome {
			return FireWorkflowActivationResult{}, fmt.Errorf("%w: reactor activation outcome is inconsistent", ErrInvalidActivation)
		}
		if reactor.Reactor.Identity.ID == "" || reactor.Reactor.CurrentGeneration == 0 || reactor.Delivery.RunID == "" ||
			!reactor.Reactor.Status.Valid() || !reactor.Delivery.Status.Valid() {
			return FireWorkflowActivationResult{}, fmt.Errorf("%w: reactor activation result is malformed", ErrInvalidActivation)
		}
		return FireWorkflowActivationResult{
			Kind: WorkflowActivationFireReactor, Outcome: reactor.Outcome,
			Reactor: &WorkflowReactorActivationDelivery{
				ReactorID: reactor.Reactor.Identity.ID, CurrentGeneration: reactor.Reactor.CurrentGeneration,
				RunID: reactor.Delivery.RunID, ReactorStatus: reactor.Reactor.Status, DeliveryStatus: reactor.Delivery.Status,
			},
		}, nil
	}
	if result.Start.Run == nil {
		return FireWorkflowActivationResult{}, fmt.Errorf("%w: direct activation result has no durable run", ErrInvalidActivation)
	}
	if !validActivationIdempotencyOutcome(result.Start.Outcome) || result.Start.Outcome != result.Outcome {
		return FireWorkflowActivationResult{}, fmt.Errorf("%w: direct activation outcome is inconsistent", ErrInvalidActivation)
	}
	start := result.Start
	return FireWorkflowActivationResult{Kind: WorkflowActivationFireDirect, Outcome: result.Outcome, Start: &start}, nil
}

func validActivationIdempotencyOutcome(outcome runtime.IdempotencyOutcome) bool {
	return outcome == runtime.IdempotencyApplied || outcome == runtime.IdempotencyReplayed
}

type ActivationMaterializationRequest struct {
	Declaration         graph.ActivationDeclaration
	Definition          graph.DefinitionRef
	Identity            hoststate.IdentityBinding
	ExposureRef         string
	DefaultLogicalRunID string
	Enabled             bool
	ExpiresAt           time.Time
	CreatedAt           time.Time
}

// MaterializeActivationRegistration maps one compiler-owned activation
// declaration into Hadron's operational model without reparsing workflow
// source. W05-T08 may reconcile these immutable snapshots later.
func MaterializeActivationRegistration(request ActivationMaterializationRequest) (hoststate.ActivationRegistration, error) {
	declaration := request.Declaration
	overlap := declaration.Policy.Overlap
	if overlap == "" {
		overlap = graph.OverlapAllow
	}
	reuse := declaration.Policy.RunIDReuse
	if reuse == "" {
		reuse = graph.RunIDReuseReject
	}
	deadline := time.Duration(0)
	var err error
	if declaration.Policy.StartingDeadline != "" {
		deadline, err = time.ParseDuration(string(declaration.Policy.StartingDeadline))
		if err != nil {
			return hoststate.ActivationRegistration{}, fmt.Errorf("%w: starting deadline is invalid", ErrInvalidActivation)
		}
	}
	sourceKind, oneShot, err := activationSourceKind(declaration.Kind)
	if err != nil {
		return hoststate.ActivationRegistration{}, err
	}
	reference := declaration.Provenance.Locator
	if reference == "" {
		reference = declaration.ID
	}
	attributes := cloneStringMap(request.Identity.Extension)
	delete(attributes, "exposure_ref")
	registration := hoststate.ActivationRegistration{
		Version: hoststate.ActivationRegistrationVersionV1, ID: declaration.ID, Definition: request.Definition,
		InputBindings: declaration.Inputs,
		Principal: hoststate.ActivationPrincipal{Principal: request.Identity.Principal, SourceAuthority: request.Identity.SourceAuthority,
			Trust: request.Identity.Trust, Grants: append([]string(nil), request.Identity.Grants...), ExposureRef: request.ExposureRef, Attributes: attributes},
		RunScope: request.Identity.RunScope.Clone(), Source: hoststate.ActivationSource{Kind: sourceKind, Reference: reference, Config: declaration.Config, OneShot: oneShot},
		Authority: hoststate.ActivationAuthorityProject, Provenance: declaration.Provenance,
		Policy: hoststate.ActivationPolicy{Overlap: overlap, StartingDeadline: deadline, Catchup: declaration.Policy.Catchup,
			RunIDReuse: reuse, DefaultLogicalRunID: request.DefaultLogicalRunID, DeduplicationKey: declaration.Policy.DeduplicationKey},
		Enabled: request.Enabled, ExpiresAt: request.ExpiresAt.UTC(), CreatedAt: request.CreatedAt.UTC(), UpdatedAt: request.CreatedAt.UTC(), Generation: 1,
	}
	if request.Identity.ExecutionTarget != nil {
		target := request.Identity.ExecutionTarget.Clone()
		registration.ExecutionTarget = &target
	}
	if declaration.Provenance.Authority != "project" && declaration.Provenance.Authority != "project_source" {
		registration.Authority = hoststate.ActivationAuthorityOperator
	}
	cloned, cloneErr := registration.Clone()
	if cloneErr != nil {
		return hoststate.ActivationRegistration{}, fmt.Errorf("%w: materialized registration cannot be cloned", ErrInvalidActivation)
	}
	if validationErr := cloned.Validate(); validationErr != nil {
		return hoststate.ActivationRegistration{}, fmt.Errorf("%w: materialized registration is invalid: %w", ErrInvalidActivation, validationErr)
	}
	return cloned, nil
}

func activationSourceKind(kind string) (hoststate.ActivationSourceKind, bool, error) {
	switch kind {
	case "schedule":
		return hoststate.ActivationSourceSchedule, false, nil
	case "webhook":
		return hoststate.ActivationSourceWebhook, false, nil
	case "file":
		return hoststate.ActivationSourceFile, false, nil
	case "message", "event":
		return hoststate.ActivationSourceExternal, false, nil
	case "one_shot":
		return hoststate.ActivationSourceCallback, true, nil
	default:
		return "", false, fmt.Errorf("%w: unsupported activation source kind", ErrInvalidActivation)
	}
}

type ExternalActivationRequest struct {
	RegistrationID string
	IdempotencyKey string
	OccurredAt     time.Time
	ReceivedAt     time.Time
	Payload        map[string]any
	LogicalRunID   string
	SourceRef      string
}

// FireWorkflowActivationRequest is the bounded transport-neutral event intent.
// ReceivedAt and SourceRef remain host facts and therefore are not caller
// fields. RegistrationID must agree with the transport path.
type FireWorkflowActivationRequest struct {
	RegistrationID string         `json:"registration_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Payload        map[string]any `json:"payload,omitempty"`
	LogicalRunID   string         `json:"logical_run_id,omitempty"`
}

func (s ActivationService) ActivateExternal(ctx context.Context, request ExternalActivationRequest) (ActivationStartResult, error) {
	if err := s.validate(); err != nil {
		return ActivationStartResult{}, err
	}
	if ctx == nil {
		return ActivationStartResult{}, fmt.Errorf("%w: external activation context is required", ErrInvalidActivation)
	}
	request.OccurredAt = request.OccurredAt.UTC()
	request.ReceivedAt = request.ReceivedAt.UTC()
	if request.OccurredAt.IsZero() || request.ReceivedAt.IsZero() || request.OccurredAt.After(request.ReceivedAt) {
		return ActivationStartResult{}, fmt.Errorf("%w: external activation timestamps are invalid", ErrInvalidActivation)
	}
	registration, err := s.Store.LoadActivation(ctx, request.RegistrationID)
	if err != nil {
		return ActivationStartResult{}, err
	}
	if !externalActivationSourceKind(registration.Source.Kind) {
		return ActivationStartResult{}, fmt.Errorf("%w: registration does not accept external activation events", ErrInvalidActivation)
	}
	if registration.Derivation == nil || s.CurrentRegistry == nil || nilInterface(s.CurrentRegistry) {
		return s.activateExternal(ctx, request)
	}
	if fence, ok := s.CurrentRegistry.(SourceActivationCurrentFence); ok && !nilInterface(fence) {
		var result ActivationStartResult
		err := fence.WithSourceActivationCurrent(ctx, func(registry SourceActivationRegistry) error {
			if validationErr := validateCurrentDerivedActivation(ctx, registry, registration); validationErr != nil {
				return validationErr
			}
			fencedCtx := context.WithValue(ctx, sourceActivationFenceContextKey{}, sourceActivationFenceContext{registry: registry})
			var activationErr error
			result, activationErr = s.activateExternal(fencedCtx, request)
			return activationErr
		})
		return result, err
	}
	if s.RequireCurrentFence {
		return ActivationStartResult{}, fmt.Errorf("%w: derived activation current-alias fence is unavailable", ErrActivationSkipped)
	}
	if err := validateCurrentDerivedActivation(ctx, s.CurrentRegistry, registration); err != nil {
		return ActivationStartResult{}, err
	}
	return s.activateExternal(ctx, request)
}

func externalActivationSourceKind(kind hoststate.ActivationSourceKind) bool {
	return kind == hoststate.ActivationSourceWebhook || kind == hoststate.ActivationSourceFile || kind == hoststate.ActivationSourceExternal
}

func (s ActivationService) activateExternal(ctx context.Context, request ExternalActivationRequest) (ActivationStartResult, error) {
	if err := s.validate(); err != nil {
		return ActivationStartResult{}, err
	}
	if request.ReceivedAt.IsZero() {
		request.ReceivedAt = request.OccurredAt
	}
	fireID := externalServiceFireID(request.RegistrationID, request.IdempotencyKey)
	payload, err := privateActivationPayload(request.Payload, fireID)
	if err != nil {
		return ActivationStartResult{}, err
	}
	canonicalPayload := make(map[string]any, len(payload))
	for name, value := range payload {
		canonicalPayload[name] = value.Inline
	}
	var reactorService ReactorService
	var reactorCorrelation string
	reactorService = ReactorService{Host: s.Host, Activations: s.Store, Store: s.Host.reactors, Clock: s.Clock}
	reactorDelivery, err := reactorService.handlesSourceRegistration(ctx, request.RegistrationID)
	if err != nil {
		return ActivationStartResult{}, err
	}
	if reactorDelivery {
		if s.Host.reactors == nil {
			return ActivationStartResult{}, fmt.Errorf("%w: source reactor persistence is unavailable", ErrInvalidHost)
		}
		if request.LogicalRunID != "" {
			return ActivationStartResult{}, fmt.Errorf("%w: reactor logical identity is derived by the host", ErrInvalidActivation)
		}
		registration, loadErr := s.Store.LoadActivation(ctx, request.RegistrationID)
		if loadErr != nil {
			return ActivationStartResult{}, loadErr
		}
		reactorCorrelation, err = deriveReactorCorrelation(registration, canonicalPayload, request.IdempotencyKey, request.OccurredAt)
		if err != nil {
			return ActivationStartResult{}, err
		}
	}
	fire, _, err := s.Store.RecordActivationEvent(context.WithoutCancel(ctx), hoststate.ActivationEvent{
		RegistrationID: request.RegistrationID, IdempotencyKey: request.IdempotencyKey,
		OccurredAt: request.OccurredAt.UTC(), Payload: payload, LogicalRunID: request.LogicalRunID, SourceRef: request.SourceRef,
	})
	if err != nil {
		return ActivationStartResult{}, err
	}
	if fire.Status == gosched.FirePending || fire.Status == gosched.FireRetrying {
		claimed, won, claimErr := s.Store.ClaimFire(context.WithoutCancel(ctx), gosched.FireClaim{
			FireID: fire.ID, ExpectedStatus: fire.Status, ExpectedAttempt: fire.Attempt, ClaimedAt: request.ReceivedAt.UTC(),
		})
		if claimErr != nil {
			return ActivationStartResult{}, claimErr
		}
		if !won {
			return ActivationStartResult{}, fmt.Errorf("%w: external activation claim was lost", ErrActivationConflict)
		}
		fire = claimed
	}
	if fire.Status == gosched.FireSucceeded || fire.Status == gosched.FireSkipped || fire.Status == gosched.FireExhausted {
		if reactorDelivery && fire.Status == gosched.FireSucceeded {
			reactor, deliveryErr := reactorService.Deliver(ctx, ReactorDeliveryRequest{RegistrationID: request.RegistrationID, IdempotencyKey: request.IdempotencyKey,
				Correlation: reactorCorrelation, Payload: canonicalPayload, OccurredAt: request.OccurredAt.UTC(), ReceivedAt: request.ReceivedAt.UTC()})
			return ActivationStartResult{Reactor: &reactor, Outcome: reactor.Outcome}, deliveryErr
		}
		return s.replayTerminalActivation(ctx, fire)
	}
	if fire.Status != gosched.FireClaimed {
		return ActivationStartResult{}, ErrActivationSkipped
	}
	var result ActivationStartResult
	var startErr error
	if reactorDelivery {
		reactor, deliveryErr := reactorService.Deliver(ctx, ReactorDeliveryRequest{RegistrationID: request.RegistrationID, IdempotencyKey: request.IdempotencyKey,
			Correlation: reactorCorrelation, Payload: canonicalPayload, OccurredAt: request.OccurredAt.UTC(), ReceivedAt: request.ReceivedAt.UTC()})
		result, startErr = ActivationStartResult{Reactor: &reactor, Outcome: reactor.Outcome}, deliveryErr
	} else {
		result, startErr = s.Start(ctx, ActivationStartRequest{
			RegistrationID: request.RegistrationID, FireID: fire.ID, Attempt: fire.Attempt,
			ScheduledAt: fire.ScheduledAt, ObservedAt: request.ReceivedAt.UTC(), Payload: canonicalPayload, LogicalRunID: request.LogicalRunID,
		})
	}
	completedAt := s.now()
	if completedAt.Before(request.ReceivedAt) {
		completedAt = request.ReceivedAt.UTC()
	}
	transition := gosched.FireTransition{FireID: fire.ID, Attempt: fire.Attempt, From: gosched.FireClaimed, At: completedAt}
	if startErr == nil {
		transition.To = gosched.FireSucceeded
	} else if errors.Is(startErr, ErrActivationSkipped) {
		transition.To, transition.Reason = gosched.FireSkipped, "activation_skipped"
	} else if fire.Retry.Exhausted(fire.Attempt) {
		transition.To, transition.Reason = gosched.FireExhausted, "maximum_attempts_reached"
	} else {
		transition.To, transition.Reason = gosched.FireRetrying, "dispatch_failed"
		transition.NextAttemptAt = transition.At.Add(fire.Retry.Backoff.DelayAfter(fire.Attempt))
	}
	applied, transitionErr := s.Store.TransitionFire(context.WithoutCancel(ctx), transition)
	if transitionErr == nil && !applied {
		transitionErr = gosched.ErrTransitionConflict
	}
	return result, errors.Join(startErr, transitionErr)
}

func deriveReactorCorrelation(registration hoststate.ActivationRegistration, payload map[string]any, deliveryKey string, occurredAt time.Time) (string, error) {
	if registration.Policy.DeduplicationKey == nil {
		return "", fmt.Errorf("%w: source reactor requires a compiler-owned deduplication key for correlation", ErrInvalidActivation)
	}
	contextValues, err := privateActivationContext(registration, payload, deliveryKey, occurredAt.UTC())
	if err != nil {
		return "", err
	}
	key, err := values.NewExpressionEngine().EvaluateRaw(*registration.Policy.DeduplicationKey, activationExpressionContext(contextValues), values.ExpressionOptions{})
	if err != nil {
		return "", fmt.Errorf("evaluate reactor correlation key: %w", err)
	}
	digest, err := values.DigestInline(key)
	if err != nil {
		return "", fmt.Errorf("digest reactor correlation key: %w", err)
	}
	return "reactor-correlation-" + strings.TrimPrefix(digest, "sha256:"), nil
}

func (s ActivationService) replayTerminalActivation(ctx context.Context, fire gosched.Fire) (ActivationStartResult, error) {
	dispatch, err := s.Store.LoadActivationDispatch(ctx, fire.ID)
	if err != nil {
		return ActivationStartResult{}, err
	}
	result := ActivationStartResult{Dispatch: dispatch, Outcome: runtime.IdempotencyReplayed}
	if fire.Status != gosched.FireSucceeded || dispatch.Status != hoststate.ActivationDispatchStarted {
		return result, ErrActivationSkipped
	}
	snapshot, err := s.Host.journal.LoadStartByKey(ctx, dispatch.HostStartKey)
	if err != nil {
		return result, err
	}
	result.Start, err = s.Host.startResult(ctx, snapshot, runtime.IdempotencyReplayed, nil)
	return result, err
}

func (s ActivationService) now() time.Time {
	if s.Clock != nil && !nilInterface(s.Clock) {
		return s.Clock.Now().UTC()
	}
	if s.Host != nil && s.Host.clock != nil && !nilInterface(s.Host.clock) {
		return s.Host.clock.Now().UTC()
	}
	return time.Now().UTC()
}

func (s ActivationService) Start(ctx context.Context, request ActivationStartRequest) (ActivationStartResult, error) {
	if err := s.validate(); err != nil {
		return ActivationStartResult{}, err
	}
	if ctx == nil || graph.ValidateID(request.RegistrationID) != nil || request.FireID == "" || request.Attempt < 1 ||
		request.ScheduledAt.IsZero() || request.ObservedAt.IsZero() {
		return ActivationStartResult{}, fmt.Errorf("%w: start request is malformed", ErrInvalidActivation)
	}
	registration, err := s.Store.LoadActivation(ctx, request.RegistrationID)
	if err != nil {
		return ActivationStartResult{}, err
	}
	if fenced, ok := ctx.Value(sourceActivationFenceContextKey{}).(sourceActivationFenceContext); ok && fenced.registry != nil && !nilInterface(fenced.registry) {
		return s.startLoadedActivation(ctx, request, registration, fenced.registry)
	}
	if registration.Derivation != nil && s.CurrentRegistry != nil && !nilInterface(s.CurrentRegistry) {
		if fence, ok := s.CurrentRegistry.(SourceActivationCurrentFence); ok && !nilInterface(fence) {
			var result ActivationStartResult
			err := fence.WithSourceActivationCurrent(ctx, func(registry SourceActivationRegistry) error {
				var startErr error
				result, startErr = s.startLoadedActivation(ctx, request, registration, registry)
				return startErr
			})
			return result, err
		}
		if s.RequireCurrentFence {
			return ActivationStartResult{}, fmt.Errorf("%w: derived activation current-alias fence is unavailable", ErrActivationSkipped)
		}
	}
	return s.startLoadedActivation(ctx, request, registration, s.CurrentRegistry)
}

type sourceActivationFenceContextKey struct{}

type sourceActivationFenceContext struct {
	registry SourceActivationRegistry
}

func (s ActivationService) startLoadedActivation(ctx context.Context, request ActivationStartRequest, registration hoststate.ActivationRegistration, registry SourceActivationRegistry) (ActivationStartResult, error) {
	if err := validateCurrentDerivedActivation(ctx, registry, registration); err != nil {
		return ActivationStartResult{}, err
	}
	logical := registration.Policy.DefaultLogicalRunID
	if request.LogicalRunID != "" {
		logical = request.LogicalRunID
	}
	payloadValues, err := privateActivationContext(registration, request.Payload, request.FireID, request.ScheduledAt)
	if err != nil {
		return ActivationStartResult{}, err
	}
	if registration.Policy.DeduplicationKey != nil {
		key, evaluationErr := values.NewExpressionEngine().EvaluateRaw(*registration.Policy.DeduplicationKey, activationExpressionContext(payloadValues), values.ExpressionOptions{})
		if evaluationErr != nil {
			return ActivationStartResult{}, fmt.Errorf("evaluate activation deduplication key: %w", evaluationErr)
		}
		digest, digestErr := values.DigestInline(key)
		if digestErr != nil {
			return ActivationStartResult{}, fmt.Errorf("digest activation deduplication key: %w", digestErr)
		}
		if logical == "" {
			logical = registration.ID
		}
		logical += "-dedup-" + strings.TrimPrefix(digest, "sha256:")[:16]
	} else if logical == "" {
		logical = request.FireID
	}
	prepared, err := s.Store.PrepareActivation(context.WithoutCancel(ctx), hoststate.ActivationPrepareRequest{
		RegistrationID: request.RegistrationID, ExpectedRegistrationGeneration: registration.Generation,
		FireID: request.FireID, Attempt: request.Attempt, ScheduledAt: request.ScheduledAt.UTC(),
		ObservedAt: request.ObservedAt.UTC(), LogicalRunID: logical,
	})
	if err != nil {
		return ActivationStartResult{}, err
	}
	if prepared.Dispatch.Status == hoststate.ActivationDispatchSkipped || prepared.Dispatch.Status == hoststate.ActivationDispatchExhausted {
		return ActivationStartResult{Dispatch: prepared.Dispatch, Outcome: prepared.Outcome}, ErrActivationSkipped
	}
	expectedIdentity := activationIdentity(prepared.Registration)
	executionCtx, err := WithAuthenticatedIdentity(ctx, expectedIdentity)
	if err != nil {
		return ActivationStartResult{Dispatch: prepared.Dispatch, Outcome: prepared.Outcome}, fmt.Errorf("%w: durable activation identity is invalid", ErrInvalidActivation)
	}
	for _, priorRun := range prepared.ReplaceRuns {
		_, _, cancelErr := s.Host.CancelRun(executionCtx, CancelRunRequest{
			RunID: priorRun, IdempotencyKey: activationCancelKey(request.FireID, priorRun),
			Reason: "replaced by workflow activation", At: request.ObservedAt.UTC(),
		})
		if cancelErr != nil {
			return ActivationStartResult{Dispatch: prepared.Dispatch, Outcome: prepared.Outcome}, cancelErr
		}
		if fencedErr := s.requireRunFenced(executionCtx, priorRun); fencedErr != nil {
			return ActivationStartResult{Dispatch: prepared.Dispatch, Outcome: prepared.Outcome}, fencedErr
		}
	}
	inputs, err := evaluateActivationInputs(prepared.Registration, payloadValues, request.FireID)
	if err != nil {
		return ActivationStartResult{Dispatch: prepared.Dispatch, Outcome: prepared.Outcome}, err
	}
	start, err := s.Host.startRun(executionCtx, StartRunRequest{
		RunID: prepared.Dispatch.PhysicalRunID, Definition: prepared.Registration.Definition,
		Inputs: inputs, IdempotencyKey: prepared.Dispatch.HostStartKey, Identity: durableActivationIdentityRequest(prepared.Registration),
		Activation: &hoststate.ActivationBinding{ActivationID: prepared.Registration.ID, IdempotencyKey: prepared.Dispatch.HostStartKey, OccurredAt: request.ScheduledAt.UTC()},
	}, &expectedIdentity)
	if err != nil {
		return ActivationStartResult{Dispatch: prepared.Dispatch, Start: start, Outcome: prepared.Outcome}, err
	}
	completed, err := s.Store.CompleteActivation(context.WithoutCancel(ctx), hoststate.ActivationCompleteRequest{
		FireID: request.FireID, ExpectedGeneration: prepared.Dispatch.Generation, Attempt: prepared.Dispatch.Attempt,
		Status: hoststate.ActivationDispatchStarted, At: request.ObservedAt.UTC(),
	})
	return ActivationStartResult{Dispatch: completed, Start: start, Outcome: prepared.Outcome}, err
}

func validateCurrentDerivedActivation(ctx context.Context, registry SourceActivationRegistry, registration hoststate.ActivationRegistration) error {
	if registration.Derivation == nil || registry == nil || nilInterface(registry) {
		return nil
	}
	definition := registration.Definition
	if definition.Kind != DefinitionKindRegistry || definition.ID == "" || definition.Version == "" || definition.Digest == "" {
		return fmt.Errorf("%w: derived activation has no exact registry execution identity", ErrActivationSkipped)
	}
	resolution, err := registry.ResolveWorkflow(ctx, hadronregistry.WorkflowQuery{Name: definition.ID})
	if err != nil {
		return fmt.Errorf("%w: derived activation workflow is no longer current", ErrActivationSkipped)
	}
	record := resolution.Record
	if !resolution.Movable || record.Name != definition.ID || record.Version != definition.Version || record.Digest != definition.Digest ||
		record.Authority != definition.Authority || record.PlanDigest != registration.Derivation.PlanDigest {
		return fmt.Errorf("%w: derived activation workflow is no longer current", ErrActivationSkipped)
	}
	return nil
}

func (s ActivationService) requireRunFenced(ctx context.Context, runID runtime.RunID) error {
	run, err := s.Host.state.LoadRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status.Terminal() {
		return nil
	}
	control, ok := s.Host.state.(runtime.ControlFlowStore)
	if !ok || nilInterface(control) {
		return fmt.Errorf("%w: replacement run is not durably fenced", ErrActivationConflict)
	}
	intent, err := control.LoadTerminalIntent(ctx, runID)
	if err != nil {
		return fmt.Errorf("%w: replacement run is not durably fenced", ErrActivationConflict)
	}
	if intent.Status != runtime.TerminalIntentPending {
		return fmt.Errorf("%w: replacement run has no pending terminal intent", ErrActivationConflict)
	}
	return nil
}

type CallbackResumeRequest struct {
	CallbackID     string
	IdempotencyKey string
	Credential     string
	Responder      runtime.ResumeCommand
}

type CallbackRegistrationRequest struct {
	Registration hoststate.CallbackRegistration
	Credential   string
}

func (s ActivationService) RegisterCallback(ctx context.Context, request CallbackRegistrationRequest) (hoststate.CallbackRegistration, runtime.IdempotencyOutcome, error) {
	if err := s.validate(); err != nil {
		return hoststate.CallbackRegistration{}, "", err
	}
	digest, err := hoststate.DigestCallbackCredential(request.Credential)
	if err != nil {
		return hoststate.CallbackRegistration{}, "", ErrCallbackCredential
	}
	wait, err := s.Host.state.LoadWait(ctx, request.Registration.WaitID)
	if err != nil {
		return hoststate.CallbackRegistration{}, "", err
	}
	if wait.Status != runtime.WaitOpen || wait.Kind != workflowwait.KindCallback || wait.WakeSource != workflowwait.WakeCallback ||
		!workflowwait.EqualTokenDigest(wait.ResumeTokenDigest, digest) {
		return hoststate.CallbackRegistration{}, "", fmt.Errorf("%w: callback does not match an open callback wait", ErrInvalidActivation)
	}
	if (request.Registration.Correlation != "" && request.Registration.Correlation != wait.Correlation) ||
		(request.Registration.WakeSource != "" && request.Registration.WakeSource != wait.WakeSource) ||
		(request.Registration.CredentialDigest != "" && !workflowwait.EqualTokenDigest(request.Registration.CredentialDigest, digest)) {
		return hoststate.CallbackRegistration{}, "", fmt.Errorf("%w: callback fields diverge from the durable wait", ErrInvalidActivation)
	}
	if request.Registration.Responder.Kind != "" &&
		(request.Registration.Responder.Kind != wait.Authority.Kind || request.Registration.Responder.Reference != wait.Authority.Reference) {
		return hoststate.CallbackRegistration{}, "", fmt.Errorf("%w: callback responder diverges from the durable wait", ErrInvalidActivation)
	}
	if request.Registration.ValueSchema != nil {
		provided, _ := json.Marshal(request.Registration.ValueSchema)
		durable, _ := json.Marshal(wait.ResumeSchema.Schema)
		if !bytes.Equal(provided, durable) {
			return hoststate.CallbackRegistration{}, "", fmt.Errorf("%w: callback schema diverges from the durable wait", ErrInvalidActivation)
		}
	}
	if !request.Registration.ExpiresAt.IsZero() && !wait.Deadline.IsZero() && request.Registration.ExpiresAt.After(wait.Deadline) {
		return hoststate.CallbackRegistration{}, "", fmt.Errorf("%w: callback expiry exceeds the wait deadline", ErrInvalidActivation)
	}
	registration := request.Registration
	registration.Correlation = wait.Correlation
	registration.WakeSource = wait.WakeSource
	registration.Responder = workflowwait.Responder{Kind: wait.Authority.Kind, Reference: wait.Authority.Reference, Attributes: cloneStringMap(wait.Authority.Attributes)}
	registration.ValueSchema = wait.ResumeSchema.Schema
	registration.CredentialDigest = digest
	if registration.ExpiresAt.IsZero() {
		registration.ExpiresAt = wait.Deadline
	}
	return s.Store.CreateCallback(context.WithoutCancel(ctx), registration)
}

func (s ActivationService) ResumeCallback(ctx context.Context, request CallbackResumeRequest) (runtime.ResumeWaitResult, error) {
	if err := s.validate(); err != nil {
		return runtime.ResumeWaitResult{}, err
	}
	digest, err := hoststate.DigestCallbackCredential(request.Credential)
	if err != nil {
		return runtime.ResumeWaitResult{}, ErrCallbackCredential
	}
	if validationErr := request.Responder.Payload.Validate(); validationErr != nil {
		return runtime.ResumeWaitResult{}, fmt.Errorf("%w: callback payload is invalid", ErrInvalidActivation)
	}
	begin, err := s.Store.BeginCallback(context.WithoutCancel(ctx), hoststate.CallbackBeginRequest{
		CallbackID: request.CallbackID, IdempotencyKey: request.IdempotencyKey,
		CredentialDigest: digest, PayloadDigest: request.Responder.Payload.Digest, ReceivedAt: request.Responder.ReceivedAt.UTC(),
	})
	if err != nil {
		if errors.Is(err, hoststate.ErrCallbackCredential) {
			return runtime.ResumeWaitResult{}, ErrCallbackCredential
		}
		if errors.Is(err, hoststate.ErrCallbackExpired) {
			return runtime.ResumeWaitResult{}, ErrCallbackExpired
		}
		return runtime.ResumeWaitResult{}, err
	}
	registration := begin.Registration
	command := request.Responder
	command.WaitID = registration.WaitID
	command.Correlation = registration.Correlation
	command.Token = request.Credential
	command.WakeSource = registration.WakeSource
	command.Responder = registration.Responder
	command.IdempotencyKey = "callback:" + request.CallbackID + ":" + request.IdempotencyKey
	result, err := s.Host.ResumeWait(ctx, command)
	if err != nil {
		return result, err
	}
	_, completeErr := s.Store.CompleteCallback(context.WithoutCancel(ctx), request.CallbackID, request.IdempotencyKey, request.Responder.ReceivedAt.UTC())
	return result, errors.Join(err, completeErr)
}

type ActivationObserver struct{ Store hoststate.ActivationStore }

func (o ActivationObserver) Observe(ctx context.Context, event gosched.ObserverEvent) error {
	if o.Store == nil || nilInterface(o.Store) {
		return fmt.Errorf("%w: observer store is required", ErrInvalidActivation)
	}
	// Err is process-local only. The store persists closed reason codes.
	event.Err = nil
	return o.Store.RecordActivationObserver(ctx, event)
}

func activationIdentity(registration hoststate.ActivationRegistration) hoststate.IdentityBinding {
	extension := make(map[string]string, len(registration.Principal.Attributes)+1)
	for key, value := range registration.Principal.Attributes {
		extension[key] = value
	}
	extension["exposure_ref"] = registration.Principal.ExposureRef
	identity := hoststate.IdentityBinding{
		Principal: registration.Principal.Principal, SourceAuthority: registration.Principal.SourceAuthority,
		Trust: registration.Principal.Trust, Grants: append([]string(nil), registration.Principal.Grants...),
		RunScope: registration.RunScope.Clone(), Extension: extension,
	}
	if registration.ExecutionTarget != nil {
		target := registration.ExecutionTarget.Clone()
		identity.ExecutionTarget = &target
	}
	return identity
}

func durableActivationIdentityRequest(registration hoststate.ActivationRegistration) IdentityRequest {
	return IdentityRequest{SourceAuthority: registration.Principal.SourceAuthority}
}

func evaluateActivationInputs(registration hoststate.ActivationRegistration, contextValues values.ValueSet, fireID string) (map[string]any, error) {
	engine := values.NewExpressionEngine()
	names := make([]string, 0, len(registration.InputBindings))
	for name := range registration.InputBindings {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(map[string]any, len(names))
	for _, name := range names {
		value, err := engine.EvaluateBinding(registration.InputBindings[name], activationExpressionContext(contextValues), values.ExpressionOptions{}, values.Metadata{
			Producer:  values.Producer{Kind: "activation-binding", Reference: fireID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return nil, fmt.Errorf("evaluate activation input %q: %w", name, err)
		}
		if value.Type == values.TypeArtifact || value.Type == values.TypeSecretRef {
			return nil, fmt.Errorf("%w: activation start inputs cannot inline classified references", ErrInvalidActivation)
		}
		result[name] = value.Inline
	}
	return result, nil
}

func activationExpressionContext(input values.ValueSet) values.ExpressionContext {
	locals := make(values.ValueSet)
	for _, name := range []string{"body", "event", "file", "message", "registration", "schedule", "source"} {
		if value, exists := input[name]; exists {
			locals[name] = value
		}
	}
	return values.ExpressionContext{Inputs: input, Locals: locals}
}

func privateActivationContext(registration hoststate.ActivationRegistration, payload map[string]any, fireID string, scheduledAt time.Time) (values.ValueSet, error) {
	contextValues, err := privateActivationPayload(payload, fireID)
	if err != nil {
		return nil, err
	}
	hostValues := map[string]any{
		"registration": map[string]any{"id": registration.ID, "authority": string(registration.Authority)},
		"schedule":     map[string]any{"fire_id": fireID, "scheduled_at": scheduledAt.UTC().Format(time.RFC3339Nano)},
		"source": map[string]any{
			"kind": string(registration.Source.Kind), "reference": registration.Source.Reference,
			"config": registration.Source.Config,
		},
	}
	for name, input := range hostValues {
		value, valueErr := values.NewInline(input, values.Metadata{
			Producer:  values.Producer{Kind: "activation-context", Reference: fireID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if valueErr != nil {
			return nil, fmt.Errorf("%w: activation context is invalid", ErrInvalidActivation)
		}
		contextValues[name] = value
	}
	return contextValues, values.ValidatePersistableSet(contextValues)
}

func privateActivationPayload(payload map[string]any, fireID string) (values.ValueSet, error) {
	contextValues := make(values.ValueSet, len(payload))
	for name, input := range payload {
		if hoststate.ValidatePublicText(name, 128, true) != nil {
			return nil, fmt.Errorf("%w: activation payload name is invalid", ErrInvalidActivation)
		}
		value, err := values.NewInline(input, values.Metadata{
			Producer:  values.Producer{Kind: "activation", Reference: fireID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: activation payload is invalid", ErrInvalidActivation)
		}
		contextValues[name] = value
	}
	return contextValues, values.ValidatePersistableSet(contextValues)
}

func decodeActivationPayload(encoded []byte) (map[string]any, error) {
	if len(encoded) == 0 {
		return map[string]any{}, nil
	}
	if len(encoded) > hoststate.MaximumActivationPayloadBytes {
		return nil, fmt.Errorf("%w: scheduler payload exceeds its byte limit", ErrInvalidActivation)
	}
	var payload struct {
		Payload map[string]any `json:"payload"`
	}
	if err := decodeActivationJSON(encoded, &payload); err != nil || payload.Payload == nil {
		return nil, fmt.Errorf("%w: scheduler payload is malformed", ErrInvalidActivation)
	}
	return payload.Payload, nil
}

func decodeActivationJSON(encoded []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("activation JSON contains trailing content")
	}
	return nil
}

func activationCancelKey(fireID string, runID runtime.RunID) string {
	sum := sha256.Sum256([]byte(fireID + "\x00" + string(runID)))
	return "activation-cancel-" + hex.EncodeToString(sum[:])
}

func externalServiceFireID(registrationID, key string) string {
	sum := sha256.Sum256([]byte(registrationID + "\x00" + key))
	return "fire-external-" + hex.EncodeToString(sum[:])
}
