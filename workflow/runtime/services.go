package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventServiceSuspended = "service.suspended"
	EventServiceReady     = "service.ready"
	EventServiceStopping  = "service.stopping"
	EventServiceStopped   = "service.stopped"
	EventServiceFailed    = "service.failed"
)

var (
	ErrInvalidService = errors.New("invalid workflow service")
	ErrServicePending = errors.New("workflow service is not terminal")
)

// ServiceStatus is the closed, durable lifecycle of one service-start
// invocation. Ready is intentionally active: dependents may run, but recovery
// must continue heartbeat observation until the generated finalizer stops it.
type ServiceStatus string

const (
	ServiceStarting  ServiceStatus = "starting"
	ServiceLaunching ServiceStatus = "launching"
	ServiceReady     ServiceStatus = "ready"
	ServiceStopping  ServiceStatus = "stopping"
	ServiceStopped   ServiceStatus = "stopped"
	ServiceFailed    ServiceStatus = "failed"
)

func (s ServiceStatus) valid() bool {
	switch s {
	case ServiceLaunching, ServiceStarting, ServiceReady, ServiceStopping, ServiceStopped, ServiceFailed:
		return true
	default:
		return false
	}
}

// ServiceSnapshot is the restart-safe binding from an immutable service-start
// attempt to its provider reference and optional generated teardown attempt.
// Provider handles remain opaque inside Ref; raw credentials are forbidden by
// stepkind.ExternalOperationRef validation.
type ServiceSnapshot struct {
	Start              AttemptID                     `json:"start"`
	Invocation         stepkind.Invocation           `json:"invocation"`
	Ref                stepkind.ExternalOperationRef `json:"ref"`
	Status             ServiceStatus                 `json:"status"`
	HeartbeatTimeout   graph.Duration                `json:"heartbeat_timeout,omitempty"`
	ReadyCheck         *graph.VerificationSpec       `json:"ready_check,omitempty"`
	Outputs            *values.ValueSetRef           `json:"outputs,omitempty"`
	Failure            *Failure                      `json:"failure,omitempty"`
	Teardown           *AttemptID                    `json:"teardown,omitempty"`
	TeardownInvocation *stepkind.Invocation          `json:"teardown_invocation,omitempty"`
	StopRequestedAt    time.Time                     `json:"stop_requested_at,omitempty"`
	LastObservedAt     time.Time                     `json:"last_observed_at,omitempty"`
	LastHeartbeatAt    time.Time                     `json:"last_heartbeat_at,omitempty"`
	ReadyAt            time.Time                     `json:"ready_at,omitempty"`
	Generation         uint64                        `json:"generation"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
}

func (s ServiceSnapshot) Validate() error {
	if err := s.Start.Validate(); err != nil {
		return err
	}
	if err := s.Invocation.Validate(); err != nil {
		return err
	}
	if s.Invocation.Identity.RunID != string(s.Start.Invocation.RunID) || s.Invocation.Identity.NodeID != s.Start.Invocation.NodeID || s.Invocation.Identity.Iteration != s.Start.Invocation.Iteration || s.Invocation.Identity.Attempt != s.Start.Number {
		return fmt.Errorf("service invocation identity must match start attempt")
	}
	if s.Status == ServiceLaunching {
		if s.Ref.Kind != "" || s.Ref.ID != "" || len(s.Ref.Metadata) != 0 {
			return fmt.Errorf("launching service cannot contain a provider reference")
		}
	} else if err := s.Ref.Validate(); err != nil {
		return err
	}
	if !s.Status.valid() || s.Generation == 0 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return fmt.Errorf("service status, generation, and chronology are required")
	}
	if s.HeartbeatTimeout != "" {
		duration, err := time.ParseDuration(string(s.HeartbeatTimeout))
		if err != nil || duration <= 0 {
			return fmt.Errorf("service heartbeat timeout must be positive")
		}
	}
	if err := validateOptionalValueSetRef(s.Outputs); err != nil {
		return err
	}
	if s.Failure != nil {
		if err := s.Failure.Validate(); err != nil {
			return err
		}
	}
	if s.Teardown != nil {
		if err := s.Teardown.Validate(); err != nil {
			return err
		}
		if s.Teardown.Invocation.RunID != s.Start.Invocation.RunID || s.Teardown.Invocation.Iteration != s.Start.Invocation.Iteration {
			return fmt.Errorf("service teardown must share run and iteration with start")
		}
		if s.TeardownInvocation == nil {
			return fmt.Errorf("service teardown requires its immutable invocation")
		}
		if err := s.TeardownInvocation.Validate(); err != nil {
			return fmt.Errorf("service teardown invocation: %w", err)
		}
		if s.TeardownInvocation.Identity.RunID != string(s.Teardown.Invocation.RunID) || s.TeardownInvocation.Identity.NodeID != s.Teardown.Invocation.NodeID || s.TeardownInvocation.Identity.Iteration != s.Teardown.Invocation.Iteration || s.TeardownInvocation.Identity.Attempt != s.Teardown.Number {
			return fmt.Errorf("service teardown invocation identity must match attempt")
		}
	} else if s.TeardownInvocation != nil {
		return fmt.Errorf("service teardown invocation requires a teardown attempt")
	}
	for _, timestamp := range []time.Time{s.StopRequestedAt, s.LastObservedAt, s.LastHeartbeatAt, s.ReadyAt} {
		if !timestamp.IsZero() && (timestamp.Before(s.CreatedAt) || timestamp.After(s.UpdatedAt)) {
			return fmt.Errorf("service timestamp is outside persisted chronology")
		}
	}
	switch s.Status {
	case ServiceLaunching:
		if s.Outputs != nil || s.Failure != nil || s.Teardown != nil || !s.StopRequestedAt.IsZero() {
			return fmt.Errorf("launching service cannot contain terminal or teardown state")
		}
	case ServiceStarting:
		if s.Outputs != nil || s.Failure != nil || s.Teardown != nil || !s.StopRequestedAt.IsZero() {
			return fmt.Errorf("starting service cannot contain terminal or teardown state")
		}
	case ServiceReady:
		if s.Outputs == nil || s.Failure != nil || s.Teardown != nil || !s.StopRequestedAt.IsZero() || s.ReadyAt.IsZero() || s.LastHeartbeatAt.IsZero() {
			return fmt.Errorf("ready service requires only readiness outputs")
		}
	case ServiceStopping:
		if s.Teardown == nil || s.StopRequestedAt.IsZero() {
			return fmt.Errorf("stopping service requires teardown attempt and durable stop intent")
		}
	case ServiceStopped:
		if s.Teardown == nil || s.StopRequestedAt.IsZero() {
			return fmt.Errorf("stopped service requires successful teardown state")
		}
	case ServiceFailed:
		if s.Failure == nil {
			return fmt.Errorf("failed service requires typed failure")
		}
	}
	return nil
}

// PrepareServiceStartRequest persists acquisition intent after the attempt is
// durable and immediately before provider Start. Recovery may safely invoke
// Start again with the immutable invocation idempotency key.
type PrepareServiceStartRequest struct {
	Service                   ServiceSnapshot
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	At                        time.Time
}

// RecoverServiceStartRequest attaches the exact reacquired provider reference
// after a crash. No worker claim is required; generations fence live workers.
type RecoverServiceStartRequest struct {
	Start                     AttemptID
	Ref                       stepkind.ExternalOperationRef
	ExpectedServiceGeneration uint64
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	At                        time.Time
}

// SuspendServiceStartRequest atomically persists provider work and releases
// the running service-start attempt while it awaits readiness.
type SuspendServiceStartRequest struct {
	Service                   ServiceSnapshot
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	At                        time.Time
}

type SuspendServiceStartResult struct {
	Service ServiceSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Events  []Event
}

// ApplyServiceReadyRequest records either another starting observation or the
// readiness milestone. Outputs is required only when Ready is true.
type ApplyServiceReadyRequest struct {
	Start                     AttemptID
	ExpectedServiceGeneration uint64
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Ready                     bool
	Outputs                   *values.ValueSetRef
	Failure                   *Failure
	ObservedAt                time.Time
	HeartbeatAt               time.Time
	At                        time.Time
}

type ApplyServiceReadyResult struct {
	Service ServiceSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Events  []Event
}

// ApplyServiceHeartbeatRequest records ready-service liveness or an
// unexpected terminal provider failure without rewriting the succeeded start
// node. The returned failed service remains visible for finalizer recovery.
type ApplyServiceHeartbeatRequest struct {
	Start                     NodeInvocationID
	ExpectedServiceGeneration uint64
	ObservedAt                time.Time
	HeartbeatAt               time.Time
	Failure                   *Failure
	At                        time.Time
}

// SuspendServiceTeardownRequest binds the generated finally attempt to the
// exact start record and durably records stop intent before any host call.
type SuspendServiceTeardownRequest struct {
	Start                     NodeInvocationID
	Teardown                  AttemptID
	Invocation                stepkind.Invocation
	ExpectedServiceGeneration uint64
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	At                        time.Time
}

type SuspendServiceTeardownResult struct {
	Service ServiceSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Events  []Event
}

// ApplyServiceStopRequest records another pending stop observation or closes
// the generated teardown attempt and service record.
type ApplyServiceStopRequest struct {
	Start                     NodeInvocationID
	ExpectedServiceGeneration uint64
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Stopped                   bool
	Failure                   *Failure
	ObservedAt                time.Time
	HeartbeatAt               time.Time
	At                        time.Time
}

type ApplyServiceStopResult struct {
	Service ServiceSnapshot
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Events  []Event
}

// ServiceQuery selects active records in updated-time and identity order.
type ServiceQuery struct {
	RunID RunID
	Limit int
}

// ServiceStore is deliberately separate from StateStore so hosts that cannot
// execute service nodes are not forced to advertise incomplete durability.
type ServiceStore interface {
	LoadService(context.Context, NodeInvocationID) (ServiceSnapshot, error)
	PrepareServiceStart(context.Context, PrepareServiceStartRequest) (ServiceSnapshot, error)
	RecoverServiceStart(context.Context, RecoverServiceStartRequest) (SuspendServiceStartResult, error)
	SuspendServiceStart(context.Context, SuspendServiceStartRequest) (SuspendServiceStartResult, error)
	ApplyServiceReady(context.Context, ApplyServiceReadyRequest) (ApplyServiceReadyResult, error)
	ApplyServiceHeartbeat(context.Context, ApplyServiceHeartbeatRequest) (ServiceSnapshot, error)
	SuspendServiceTeardown(context.Context, SuspendServiceTeardownRequest) (SuspendServiceTeardownResult, error)
	ApplyServiceStop(context.Context, ApplyServiceStopRequest) (ApplyServiceStopResult, error)
	RecoverServices(context.Context, ServiceQuery) ([]ServiceSnapshot, error)
}

func serviceTimeFloor(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
