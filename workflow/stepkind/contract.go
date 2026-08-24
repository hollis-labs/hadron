package stepkind

import (
	"context"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

// RetrySafety describes when the engine may retry an invocation. It is
// metadata only; runtime retry classification is added by W04-T01.
type RetrySafety string

const (
	// RetryUnsupported means the kind cannot safely be retried.
	RetryUnsupported RetrySafety = "unsupported"
	// RetrySafe means repeated execution is intrinsically safe.
	RetrySafe RetrySafety = "safe"
	// RetryRequiresIdempotency means retry requires an invocation-level
	// idempotency declaration accepted by policy.
	RetryRequiresIdempotency RetrySafety = "requires-idempotency"
)

// Valid reports whether s is a supported retry-safety declaration.
func (s RetrySafety) Valid() bool {
	switch s {
	case RetryUnsupported, RetrySafe, RetryRequiresIdempotency:
		return true
	default:
		return false
	}
}

// CancellationMode describes how an invocation responds to cancellation.
type CancellationMode string

const (
	// CancellationNone means the kind does not support cancellation after
	// execution begins.
	CancellationNone CancellationMode = "none"
	// CancellationContext means Execute observes context cancellation.
	CancellationContext CancellationMode = "context"
	// CancellationExplicit means the kind implements Canceler for an external
	// operation.
	CancellationExplicit CancellationMode = "explicit"
)

// Valid reports whether m is a supported cancellation mode.
func (m CancellationMode) Valid() bool {
	switch m {
	case CancellationNone, CancellationContext, CancellationExplicit:
		return true
	default:
		return false
	}
}

// CancellationSpec advertises cancellation behavior without embedding a
// concrete adapter's operation type.
type CancellationSpec struct {
	Mode CancellationMode `json:"mode"`
}

// ObservationMode describes whether external work can be observed.
type ObservationMode string

const (
	// ObservationNone means the kind has no external observation hook.
	ObservationNone ObservationMode = "none"
	// ObservationPoll means the kind implements Observer for polling external
	// operation state.
	ObservationPoll ObservationMode = "poll"
)

// Valid reports whether m is a supported observation mode.
func (m ObservationMode) Valid() bool {
	switch m {
	case ObservationNone, ObservationPoll:
		return true
	default:
		return false
	}
}

// ObservationSpec advertises observation behavior.
type ObservationSpec struct {
	Mode ObservationMode `json:"mode"`
}

// LifecycleSpec advertises optional lifecycle hooks not otherwise described by
// cancellation or observation metadata.
type LifecycleSpec struct {
	Prepare  bool `json:"prepare,omitempty"`
	Finalize bool `json:"finalize,omitempty"`
}

// StepKindSpec is immutable metadata used by compilers, policy evaluators, and
// runtimes before adapter execution. Empty schemas are valid JSON Schemas;
// nil schemas are missing metadata.
type StepKindSpec struct {
	Name                  string                `json:"name"`
	Version               string                `json:"version"`
	ConfigSchema          graph.Schema          `json:"config_schema"`
	InputSchema           graph.Schema          `json:"input_schema"`
	OutputSchema          graph.Schema          `json:"output_schema"`
	Effects               graph.EffectSet       `json:"effects"`
	RequiredCapabilities  []string              `json:"required_capabilities,omitempty"`
	Idempotency           graph.IdempotencyMode `json:"idempotency"`
	RetrySafety           RetrySafety           `json:"retry_safety"`
	Cancellation          CancellationSpec      `json:"cancellation"`
	Observation           ObservationSpec       `json:"observation"`
	Lifecycle             LifecycleSpec         `json:"lifecycle,omitempty"`
	CanSuspend            bool                  `json:"can_suspend,omitempty"`
	EmbeddedModeSupported bool                  `json:"embedded_mode_supported,omitempty"`
}

// Invocation is the application-neutral input to optional preparation. W04-T01
// extends it with runtime context and typed values without replacing the
// executor interfaces.
type Invocation struct {
	Config graph.Config
}

// PreparedInvocation is the required input to Execute. Runtimes wrap an
// Invocation directly when a kind does not implement Preparer.
type PreparedInvocation struct {
	Invocation Invocation
}

// StepResult is the deliberately minimal execution result envelope. Typed
// outputs, waits, and retry classification are W04-T01 extensions.
type StepResult struct{}

// ExternalOperationRef identifies adapter-owned work for observation or
// cancellation. Durable reference details are W04-T01 extensions.
type ExternalOperationRef struct {
	ID string
}

// Observation is the minimal external-operation observation envelope.
type Observation struct {
	Complete bool
}

// Finalization supplies the execution outcome to an optional Finalizer.
type Finalization struct {
	Invocation     PreparedInvocation
	Result         StepResult
	ExecutionError error
}

// StepKind is the required executor lifecycle shared by all adapters.
type StepKind interface {
	Spec() StepKindSpec
	ValidateConfig(ctx context.Context, config graph.Config) []diagnostic.Diagnostic
	Execute(ctx context.Context, invocation PreparedInvocation) (StepResult, error)
}

// Preparer optionally prepares an invocation before Execute.
type Preparer interface {
	Prepare(ctx context.Context, invocation Invocation) (PreparedInvocation, error)
}

// Observer optionally polls an adapter-owned external operation.
type Observer interface {
	Observe(ctx context.Context, ref ExternalOperationRef) (Observation, error)
}

// Canceler optionally cancels an adapter-owned external operation.
type Canceler interface {
	Cancel(ctx context.Context, ref ExternalOperationRef) error
}

// Finalizer optionally releases resources after execution completes.
type Finalizer interface {
	Finalize(ctx context.Context, finalization Finalization) error
}

// Registry exposes deterministic registration and lookup by name and version.
type Registry interface {
	Register(kind StepKind) error
	Lookup(name, version string) (StepKind, bool)
	List() []StepKindSpec
}
