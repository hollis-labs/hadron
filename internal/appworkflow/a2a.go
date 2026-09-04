package appworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

// A2ATaskCorrelationStore is Hadron's narrow durable application-state port.
// It stores only immutable correlation, never a duplicate task lifecycle.
type A2ATaskCorrelationStore interface {
	PutA2ATaskCorrelation(context.Context, hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error)
	GetA2ATaskCorrelation(context.Context, string) (hoststate.A2ATaskCorrelation, error)
}

// A2ATaskCorrelations validates and exposes durable task/run correlation to
// transports without granting any authority over the correlated run.
type A2ATaskCorrelations struct {
	host   *Host
	store  A2ATaskCorrelationStore
	access A2ATaskOperationAuthorizer
}

type A2ATaskOperation string

const (
	A2ATaskStartReplay A2ATaskOperation = "start_replay"
	A2ATaskInspect     A2ATaskOperation = "inspect"
	A2ATaskCancel      A2ATaskOperation = "cancel"
	A2ATaskResume      A2ATaskOperation = "resume"
)

func (o A2ATaskOperation) valid() bool {
	switch o {
	case A2ATaskStartReplay, A2ATaskInspect, A2ATaskCancel, A2ATaskResume:
		return true
	default:
		return false
	}
}

type A2ATaskAuthorization struct {
	Operation A2ATaskOperation          `json:"operation"`
	TaskID    string                    `json:"task_id"`
	RunID     workflowruntime.RunID     `json:"run_id"`
	Caller    hoststate.IdentityBinding `json:"caller"`
	Owner     hoststate.IdentityBinding `json:"owner"`
}

type A2ATaskOperationAuthorizer interface {
	AuthorizeA2ATaskOperation(context.Context, A2ATaskAuthorization) error
}

type A2ATaskOperationAuthorizerFunc func(context.Context, A2ATaskAuthorization) error

func (f A2ATaskOperationAuthorizerFunc) AuthorizeA2ATaskOperation(ctx context.Context, request A2ATaskAuthorization) error {
	return f(ctx, request)
}

type A2ATaskCorrelationsOptions struct {
	Host   *Host
	Store  A2ATaskCorrelationStore
	Access A2ATaskOperationAuthorizer
}

func NewA2ATaskCorrelations(options A2ATaskCorrelationsOptions) (*A2ATaskCorrelations, error) {
	if options.Host == nil || nilInterface(options.Store) {
		return nil, fmt.Errorf("%w: A2A correlation host and store are required", ErrInvalidHost)
	}
	access := options.Access
	if access != nil && nilInterface(access) {
		return nil, fmt.Errorf("%w: A2A task authorizer must not be typed nil", ErrInvalidHost)
	}
	if access == nil {
		access = A2ATaskOperationAuthorizerFunc(func(_ context.Context, request A2ATaskAuthorization) error {
			if !sameIdentity(request.Caller, request.Owner) {
				return ErrWorkflowHidden
			}
			return nil
		})
	}
	return &A2ATaskCorrelations{host: options.Host, store: options.Store, access: access}, nil
}

func (s *A2ATaskCorrelations) Put(ctx context.Context, identity IdentityRequest, correlation hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error) {
	if ctx == nil {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: A2A correlation context is required", ErrInvalidHost)
	}
	owner, err := s.host.bindIdentity(ctx, identity)
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, "", err
	}
	correlation.Owner = owner.Clone()
	if correlation.RunID != "" || correlation.HostStartKey != "" {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: A2A run and host-start identities are application-derived", hoststate.ErrInvalidRecord)
	}
	if correlation.TaskID != "" {
		if validationErr := hoststate.ValidateA2ATaskID(correlation.TaskID); validationErr != nil {
			return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
		}
	}
	if validationErr := hoststate.ValidateA2ADefinition(correlation.Definition); validationErr != nil {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	if validationErr := values.ValidateDigest(correlation.RequestDigest); validationErr != nil {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: A2A request digest: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	if validationErr := hoststate.ValidatePublicText(correlation.IdempotencyKey, 512, true); validationErr != nil {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: A2A idempotency key is invalid", hoststate.ErrInvalidRecord)
	}
	if correlation.TaskID == "" {
		correlation.TaskID, err = derivedA2ATaskID(owner, correlation.IdempotencyKey)
		if err != nil {
			return hoststate.A2ATaskCorrelation{}, "", err
		}
	}
	correlation.RunID = derivedA2ARunID(correlation.TaskID, correlation.RequestDigest)
	correlation.HostStartKey = derivedA2AHostStartKey(correlation.TaskID, correlation.IdempotencyKey)
	if correlation.CreatedAt.IsZero() {
		correlation.CreatedAt = s.host.now().UTC()
	}
	if validationErr := correlation.Validate(); validationErr != nil {
		return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	if authorizationErr := s.authorize(ctx, correlation, owner, A2ATaskStartReplay); authorizationErr != nil {
		return hoststate.A2ATaskCorrelation{}, "", authorizationErr
	}
	prior, loadErr := s.store.GetA2ATaskCorrelation(ctx, correlation.TaskID)
	if loadErr == nil {
		if authorizationErr := s.authorize(ctx, prior, owner, A2ATaskStartReplay); authorizationErr != nil {
			return hoststate.A2ATaskCorrelation{}, "", authorizationErr
		}
		return s.store.PutA2ATaskCorrelation(ctx, correlation)
	}
	if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		return hoststate.A2ATaskCorrelation{}, "", loadErr
	}
	stored, outcome, err := s.store.PutA2ATaskCorrelation(ctx, correlation)
	if err != nil {
		if errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
			winner, winnerErr := s.store.GetA2ATaskCorrelation(ctx, correlation.TaskID)
			if winnerErr == nil {
				if authorizationErr := s.authorize(ctx, winner, owner, A2ATaskStartReplay); authorizationErr != nil {
					return hoststate.A2ATaskCorrelation{}, "", authorizationErr
				}
				return hoststate.A2ATaskCorrelation{}, "", err
			}
			return hoststate.A2ATaskCorrelation{}, "", fmt.Errorf("%w: A2A correlation conflict could not be safely resolved", ErrHostNotReady)
		}
		return hoststate.A2ATaskCorrelation{}, "", err
	}
	return stored.Clone(), outcome, nil
}

func derivedA2ATaskID(owner hoststate.IdentityBinding, idempotencyKey string) (string, error) {
	identity := struct {
		Owner          hoststate.IdentityBinding `json:"owner"`
		IdempotencyKey string                    `json:"idempotency_key"`
	}{Owner: normalizeIdentity(owner), IdempotencyKey: idempotencyKey}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("%w: canonicalize A2A owner identity", ErrInvalidHost)
	}
	return "task-" + strings.TrimPrefix(values.SHA256Digest(encoded), "sha256:"), nil
}

func derivedA2ARunID(taskID, requestDigest string) workflowruntime.RunID {
	digest := values.SHA256Digest([]byte(taskID + "\x00" + requestDigest))
	return workflowruntime.RunID("a2a-" + strings.TrimPrefix(digest, "sha256:"))
}

func derivedA2AHostStartKey(taskID, idempotencyKey string) string {
	digest := values.SHA256Digest([]byte(taskID + "\x00" + idempotencyKey))
	return "a2a-start-" + strings.TrimPrefix(digest, "sha256:")
}

func (s *A2ATaskCorrelations) Get(ctx context.Context, identity IdentityRequest, taskID string, operation A2ATaskOperation) (hoststate.A2ATaskCorrelation, error) {
	if ctx == nil {
		return hoststate.A2ATaskCorrelation{}, fmt.Errorf("%w: A2A correlation context is required", ErrInvalidHost)
	}
	if err := hoststate.ValidateA2ATaskID(taskID); err != nil {
		return hoststate.A2ATaskCorrelation{}, fmt.Errorf("%w: A2A task", workflowruntime.ErrNotFound)
	}
	if !operation.valid() || operation == A2ATaskStartReplay {
		return hoststate.A2ATaskCorrelation{}, fmt.Errorf("%w: A2A task operation is invalid", ErrWorkflowInvalidRequest)
	}
	correlation, err := s.store.GetA2ATaskCorrelation(ctx, taskID)
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, err
	}
	if operation == A2ATaskResume {
		if _, bindErr := s.host.bindIdentity(ctx, identity); bindErr != nil {
			return hoststate.A2ATaskCorrelation{}, bindErr
		}
		return correlation.Clone(), nil
	}
	identity.RunScope = &hoststate.RunScopeSelector{Version: correlation.Owner.RunScope.Version, Kind: correlation.Owner.RunScope.Kind, ID: correlation.Owner.RunScope.ID}
	if correlation.Owner.ExecutionTarget != nil {
		identity.ExecutionTarget = &hoststate.ExecutionTargetSelector{Version: correlation.Owner.ExecutionTarget.Version, ID: correlation.Owner.ExecutionTarget.ID}
	} else {
		identity.ExecutionTarget = nil
	}
	caller, err := s.host.bindIdentity(ctx, identity)
	if err != nil {
		if errors.Is(err, ErrWorkflowUnauthenticated) {
			return hoststate.A2ATaskCorrelation{}, err
		}
		return hoststate.A2ATaskCorrelation{}, ErrWorkflowHidden
	}
	if err := s.authorize(ctx, correlation, caller, operation); err != nil {
		return hoststate.A2ATaskCorrelation{}, err
	}
	return correlation.Clone(), nil
}

func (s *A2ATaskCorrelations) authorize(ctx context.Context, correlation hoststate.A2ATaskCorrelation, caller hoststate.IdentityBinding, operation A2ATaskOperation) error {
	return s.access.AuthorizeA2ATaskOperation(ctx, A2ATaskAuthorization{Operation: operation, TaskID: correlation.TaskID, RunID: correlation.RunID, Caller: caller.Clone(), Owner: correlation.Owner.Clone()})
}
