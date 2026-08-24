package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventExternalOperationSuspended       = "external_operation.suspended"
	EventExternalOperationObserved        = "external_operation.observed"
	EventExternalOperationCancelRequested = "external_operation.cancel_requested"
)

// ExternalOperationSnapshot is the durable one-per-attempt binding between an
// unfinished attempt and adapter-owned work. Attempt, Ref, and Invocation are
// immutable after creation. Status, progress, observation/heartbeat times,
// cancellation intent, and the terminal outcome evolve only through CAS store
// operations.
type ExternalOperationSnapshot struct {
	Attempt           AttemptID                     `json:"attempt"`
	Ref               stepkind.ExternalOperationRef `json:"ref"`
	Invocation        stepkind.Invocation           `json:"invocation"`
	Status            stepkind.ObservationState     `json:"status"`
	Progress          map[string]string             `json:"progress,omitempty"`
	Outputs           *values.ValueSetRef           `json:"outputs,omitempty"`
	Failure           *Failure                      `json:"failure,omitempty"`
	CancelRequestedAt time.Time                     `json:"cancel_requested_at,omitempty"`
	LastObservedAt    time.Time                     `json:"last_observed_at"`
	LastHeartbeatAt   time.Time                     `json:"last_heartbeat_at,omitempty"`
	Generation        uint64                        `json:"generation"`
	CreatedAt         time.Time                     `json:"created_at"`
	UpdatedAt         time.Time                     `json:"updated_at"`
}

// Validate reports malformed durable external-operation state without
// assigning lifecycle transition policy.
func (s ExternalOperationSnapshot) Validate() error {
	if err := s.Attempt.Validate(); err != nil {
		return err
	}
	if err := s.Ref.Validate(); err != nil {
		return err
	}
	if err := s.Invocation.Validate(); err != nil {
		return err
	}
	identity := s.Invocation.Identity
	if identity.RunID != string(s.Attempt.Invocation.RunID) || identity.NodeID != s.Attempt.Invocation.NodeID ||
		identity.Iteration != s.Attempt.Invocation.Iteration || identity.Attempt != s.Attempt.Number {
		return fmt.Errorf("external operation invocation identity must match attempt")
	}
	if err := values.ValidatePersistableSet(s.Invocation.Inputs); err != nil {
		return fmt.Errorf("external operation invocation inputs: %w", err)
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported external operation status %q", s.Status)
	}
	if err := validateStringMap("external operation progress", s.Progress); err != nil {
		return err
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return err
	}
	if s.Failure != nil {
		if err := s.Failure.Validate(); err != nil {
			return err
		}
	}
	switch s.Status {
	case stepkind.ObservationPending:
		if s.Outputs != nil || s.Failure != nil {
			return fmt.Errorf("pending external operation must not contain a terminal outcome")
		}
	case stepkind.ObservationSucceeded:
		if s.Outputs == nil || s.Failure != nil {
			return fmt.Errorf("succeeded external operation requires only outputs")
		}
	case stepkind.ObservationFailed, stepkind.ObservationCanceled:
		if s.Outputs != nil || s.Failure == nil {
			return fmt.Errorf("unsuccessful external operation requires only a failure")
		}
	}
	if err := validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt); err != nil {
		return err
	}
	for _, field := range []struct {
		name      string
		timestamp time.Time
	}{
		{name: "cancel_requested_at", timestamp: s.CancelRequestedAt},
		{name: "last_observed_at", timestamp: s.LastObservedAt},
		{name: "last_heartbeat_at", timestamp: s.LastHeartbeatAt},
	} {
		if !field.timestamp.IsZero() && (field.timestamp.Before(s.CreatedAt) || field.timestamp.After(s.UpdatedAt)) {
			return fmt.Errorf("external operation %s must be within persisted chronology", field.name)
		}
	}
	return nil
}

// SuspendExternalOperationRequest atomically binds adapter work to the
// running attempt, sets the aggregate node waiting, and releases its claim.
type SuspendExternalOperationRequest struct {
	Operation                 ExternalOperationSnapshot
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	At                        time.Time
}

func (r SuspendExternalOperationRequest) Validate() error {
	if r.Operation.Generation != 0 || r.Operation.Status != stepkind.ObservationPending {
		return fmt.Errorf("new external operation must be pending with zero generation")
	}
	if r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 {
		return fmt.Errorf("external suspension requires positive node and attempt generations")
	}
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.At.IsZero() {
		return fmt.Errorf("external suspension time is required")
	}
	candidate := r.Operation
	candidate.Generation = 1
	candidate.CreatedAt, candidate.UpdatedAt = r.At, r.At
	return candidate.Validate()
}

type SuspendExternalOperationResult struct {
	Operation ExternalOperationSnapshot
	Node      NodeInvocationSnapshot
	Attempt   AttemptSnapshot
	Events    []Event
}

// RequestExternalOperationCancelRequest durably records cancellation intent
// before the adapter call. Recovery retries pending intent after a crash.
type RequestExternalOperationCancelRequest struct {
	Attempt                     AttemptID
	ExpectedOperationGeneration uint64
	At                          time.Time
}

func (r RequestExternalOperationCancelRequest) Validate() error {
	if err := r.Attempt.Validate(); err != nil {
		return err
	}
	if r.ExpectedOperationGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("external cancel request requires generation and timestamp")
	}
	return nil
}

type RequestExternalOperationCancelResult struct {
	Operation ExternalOperationSnapshot
	Event     *Event
}

// ApplyExternalOperationRequest atomically records one observation. Pending
// observations update only operational metadata. Terminal observations also
// close the unfinished attempt and set the aggregate node terminal or ready
// for an injected retry policy.
type ApplyExternalOperationRequest struct {
	Attempt                     AttemptID
	ExpectedOperationGeneration uint64
	ExpectedNodeGeneration      uint64
	ExpectedAttemptGeneration   uint64
	Status                      stepkind.ObservationState
	Progress                    map[string]string
	Outputs                     *values.ValueSetRef
	Failure                     *Failure
	NextNodeStatus              NodeStatus
	ObservedAt                  time.Time
	HeartbeatAt                 time.Time
	At                          time.Time
}

func (r ApplyExternalOperationRequest) Validate() error {
	if err := r.Attempt.Validate(); err != nil {
		return err
	}
	if r.ExpectedOperationGeneration == 0 || r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 {
		return fmt.Errorf("external observation requires positive operation, node, and attempt generations")
	}
	if !r.Status.Valid() || r.At.IsZero() {
		return fmt.Errorf("external observation requires status and timestamp")
	}
	if err := validateStringMap("external observation progress", r.Progress); err != nil {
		return err
	}
	if !r.HeartbeatAt.IsZero() && r.HeartbeatAt.After(r.At) {
		return fmt.Errorf("external heartbeat must not follow observation time")
	}
	if !r.ObservedAt.IsZero() && r.ObservedAt.After(r.At) {
		return fmt.Errorf("external observed_at must not follow mutation time")
	}
	if err := validateOptionalValueSetRef(r.Outputs); err != nil {
		return err
	}
	if r.Failure != nil {
		if err := r.Failure.Validate(); err != nil {
			return err
		}
	}
	switch r.Status {
	case stepkind.ObservationPending:
		if r.Outputs != nil || r.Failure != nil || r.NextNodeStatus != "" {
			return fmt.Errorf("pending external observation must not contain terminal state")
		}
		if r.ObservedAt.IsZero() && r.HeartbeatAt.IsZero() {
			return fmt.Errorf("pending external observation must record an observation or heartbeat")
		}
	case stepkind.ObservationSucceeded:
		if r.Outputs == nil || r.Failure != nil || r.NextNodeStatus != NodeSucceeded {
			return fmt.Errorf("succeeded external observation requires outputs and succeeded node")
		}
	case stepkind.ObservationFailed:
		if r.Outputs != nil || r.Failure == nil || (r.NextNodeStatus != NodeFailed && r.NextNodeStatus != NodeReady) {
			return fmt.Errorf("failed external observation requires failure and failed or ready node")
		}
	case stepkind.ObservationCanceled:
		if r.Outputs != nil || r.Failure == nil || r.NextNodeStatus != NodeCanceled {
			return fmt.Errorf("canceled external observation requires failure and canceled node")
		}
	}
	return nil
}

type ApplyExternalOperationResult struct {
	Operation ExternalOperationSnapshot
	Node      NodeInvocationSnapshot
	Attempt   AttemptSnapshot
	Events    []Event
}

// ExternalOperationQuery selects pending recovery work in deterministic
// updated-time then attempt-identity order. Limit zero means unlimited.
type ExternalOperationQuery struct {
	RunID RunID
	Limit int
}

// ExternalOperationStore is the recovery-facing persistence surface. Adapter
// calls happen outside these atomic mutations; their immutable ref plus CAS
// generations make retries and competing observers fail closed.
type ExternalOperationStore interface {
	LoadExternalOperation(context.Context, AttemptID) (ExternalOperationSnapshot, error)
	SuspendExternalOperation(context.Context, SuspendExternalOperationRequest) (SuspendExternalOperationResult, error)
	RequestExternalOperationCancel(context.Context, RequestExternalOperationCancelRequest) (RequestExternalOperationCancelResult, error)
	ApplyExternalOperation(context.Context, ApplyExternalOperationRequest) (ApplyExternalOperationResult, error)
	RecoverExternalOperations(context.Context, ExternalOperationQuery) ([]ExternalOperationSnapshot, error)
}
