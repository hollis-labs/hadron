package stepkind

import (
	"context"
	"fmt"
	"sort"

	"github.com/hollis-labs/hadron/workflow/values"
)

// ServicePhase distinguishes provider start/readiness from durable teardown.
type ServicePhase string

const (
	ServiceStart    ServicePhase = "start"
	ServiceTeardown ServicePhase = "teardown"
)

// Valid reports whether p is part of the closed service lifecycle vocabulary.
func (p ServicePhase) Valid() bool { return p == ServiceStart || p == ServiceTeardown }

// ServiceBinding is injected by the runtime only for generated teardown
// nodes. Ref is the immutable provider reference returned by the original
// service-start attempt; adapters must not accept an authored substitute.
type ServiceBinding struct {
	Phase           ServicePhase         `json:"phase"`
	StartInvocation InvocationIdentity   `json:"start_invocation"`
	Ref             ExternalOperationRef `json:"ref"`
	// Absent is set only by the runtime when the start node never materialized
	// provider work. It lets the generated finalizer succeed as an exact no-op.
	Absent bool `json:"absent,omitempty"`
}

// Validate reports malformed service continuation state.
func (b ServiceBinding) Validate() error {
	if b.Phase != ServiceTeardown {
		return fmt.Errorf("service binding phase must be teardown")
	}
	if b.Absent {
		if b.StartInvocation != (InvocationIdentity{}) || b.Ref.Kind != "" || b.Ref.ID != "" || len(b.Ref.Metadata) != 0 {
			return fmt.Errorf("absent service binding cannot carry an invocation or reference")
		}
		return nil
	}
	if err := b.StartInvocation.Validate(); err != nil {
		return fmt.Errorf("service start invocation: %w", err)
	}
	if err := b.Ref.Validate(); err != nil {
		return fmt.Errorf("service reference: %w", err)
	}
	return nil
}

// ServiceObservationState is the provider-neutral durable service state.
type ServiceObservationState string

const (
	ServiceObservationStarting ServiceObservationState = "starting"
	ServiceObservationReady    ServiceObservationState = "ready"
	ServiceObservationStopped  ServiceObservationState = "stopped"
	ServiceObservationFailed   ServiceObservationState = "failed"
)

// Valid reports whether s is a supported service observation state.
func (s ServiceObservationState) Valid() bool {
	switch s {
	case ServiceObservationStarting, ServiceObservationReady, ServiceObservationStopped, ServiceObservationFailed:
		return true
	default:
		return false
	}
}

// ServiceObservation carries readiness/stopped outcomes plus bounded,
// non-secret operational progress. Outputs are published only at readiness.
type ServiceObservation struct {
	State     ServiceObservationState `json:"state"`
	Progress  map[string]string       `json:"progress,omitempty"`
	Outputs   values.ValueSet         `json:"outputs,omitempty"`
	Failure   *ExecutionError         `json:"failure,omitempty"`
	Heartbeat bool                    `json:"heartbeat,omitempty"`
}

// Validate enforces the mutually exclusive service observation shapes.
func (o ServiceObservation) Validate() error {
	if !o.State.Valid() {
		return fmt.Errorf("unsupported service observation state %q", o.State)
	}
	if err := validateRuntimeStringMap("service progress", o.Progress); err != nil {
		return err
	}
	if len(o.Progress) > 32 {
		return fmt.Errorf("service progress exceeds 32 entries")
	}
	progressBytes := 0
	keys := make([]string, 0, len(o.Progress))
	for key := range o.Progress {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if len(key) > 128 || len(o.Progress[key]) > 1024 {
			return fmt.Errorf("service progress entry exceeds its byte limit")
		}
		progressBytes += len(key) + len(o.Progress[key])
	}
	if progressBytes > 8<<10 {
		return fmt.Errorf("service progress exceeds its total byte limit")
	}
	if o.Outputs != nil {
		if err := values.ValidatePersistableSet(o.Outputs); err != nil {
			return fmt.Errorf("service outputs: %w", err)
		}
	}
	if o.Failure != nil {
		if err := o.Failure.Validate(); err != nil {
			return err
		}
	}
	switch o.State {
	case ServiceObservationStarting:
		if o.Outputs != nil || o.Failure != nil {
			return fmt.Errorf("starting service cannot carry terminal data")
		}
	case ServiceObservationReady:
		if o.Outputs == nil || o.Failure != nil {
			return fmt.Errorf("ready service requires only outputs")
		}
	case ServiceObservationStopped:
		if o.Outputs != nil || o.Failure != nil || o.Heartbeat {
			return fmt.Errorf("stopped service cannot carry outputs, failure, or heartbeat")
		}
	case ServiceObservationFailed:
		if o.Outputs != nil || o.Failure == nil || o.Heartbeat {
			return fmt.Errorf("failed service requires only a failure")
		}
	}
	return nil
}

// ServiceController is the extraction-ready host lifecycle port for one
// registered service kind. RequestStop must be idempotent for the immutable
// invocation identity and reference; ObserveService must be side-effect free.
type ServiceController interface {
	ObserveService(context.Context, ExternalOperationRef) (ServiceObservation, error)
	RequestStop(context.Context, ExternalOperationRef, string) error
}
