package stepkind

import (
	"context"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
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
	Mode      ObservationMode `json:"mode"`
	Heartbeat bool            `json:"heartbeat,omitempty"`
}

// LifecycleSpec advertises optional lifecycle hooks not otherwise described by
// cancellation or observation metadata.
type LifecycleSpec struct {
	Prepare  bool `json:"prepare,omitempty"`
	Finalize bool `json:"finalize,omitempty"`
}

// MemoizationSupport is an executor's immutable opt-in to result reuse.
// Default permits the runtime's safe read/compute default. Approved is the
// additional executor assertion required before materialize effects may be
// reused; host policy must still approve. Disabled rejects all memoization.
type MemoizationSupport string

const (
	MemoizationDefault  MemoizationSupport = ""
	MemoizationApproved MemoizationSupport = "approved"
	MemoizationDisabled MemoizationSupport = "disabled"
)

// Valid reports whether m is a supported memoization declaration.
func (m MemoizationSupport) Valid() bool {
	return m == MemoizationDefault || m == MemoizationApproved || m == MemoizationDisabled
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
	Memoization           MemoizationSupport    `json:"memoization,omitempty"`
	CanSuspend            bool                  `json:"can_suspend,omitempty"`
	EmbeddedModeSupported bool                  `json:"embedded_mode_supported,omitempty"`
}

// Invocation is the application-neutral input to optional preparation. W04-T01
// extends it with runtime context and typed values without replacing the
// executor interfaces.
type Invocation struct {
	Identity     InvocationIdentity `json:"identity"`
	Config       graph.Config       `json:"config"`
	Inputs       values.ValueSet    `json:"inputs"`
	Call         *CallInvocation    `json:"call,omitempty"`
	Continuation *WaitContinuation  `json:"continuation,omitempty"`
	// Verification is the immutable graph modifier carried through durable
	// external-operation recovery. Activity is a runtime-issued, process-local
	// recorder; it is deliberately excluded from durable invocation JSON.
	Verification   *graph.VerificationSpec        `json:"verification,omitempty"`
	Activity       *verification.ActivityRecorder `json:"-"`
	IdempotencyKey string                         `json:"idempotency_key,omitempty"`
	Deadline       time.Time                      `json:"deadline,omitempty"`
}

// CallInvocation carries the graph-native call declaration and the immutable
// active definition path supplied by the runtime host. Lineage contains the
// parent definition followed by every active inline/run ancestor; call
// executors append the newly resolved child only after cycle/depth checks.
// Hosts reconstruct this path from durable run/call state during recovery.
type CallInvocation struct {
	Spec    graph.CallSpec        `json:"spec"`
	Lineage []graph.DefinitionRef `json:"lineage"`
}

// WaitContinuation is the durable resolved wait delivered when the runtime
// resumes the same logical attempt. Values are loaded from Record.ResumeValues
// and digest-checked; the raw one-time resume token never enters this envelope.
type WaitContinuation struct {
	ID     string              `json:"id"`
	Record workflowwait.Record `json:"record"`
	Values values.ValueSet     `json:"values"`
}

// InvocationIdentity is the application-neutral execution identity visible to
// adapters. String identities deliberately avoid coupling step kinds to a
// runtime store's concrete ID types.
type InvocationIdentity struct {
	RunID     string `json:"run_id"`
	NodeID    string `json:"node_id"`
	Iteration string `json:"iteration,omitempty"`
	Attempt   int    `json:"attempt"`
}

// PreparedInvocation is the required input to Execute. Runtimes wrap an
// Invocation directly when a kind does not implement Preparer.
type PreparedInvocation struct {
	Invocation Invocation `json:"invocation"`
	// State is process-local adapter state. Runtimes never persist, serialize,
	// compare, or expose it outside the adapter lifecycle.
	State any `json:"-"`
}

// StepOutcome is the closed execution handoff produced by Execute.
type StepOutcome string

const (
	StepCompleted StepOutcome = "completed"
	StepWaiting   StepOutcome = "waiting"
	StepExternal  StepOutcome = "external"
)

// Valid reports whether o is a supported execution handoff.
func (o StepOutcome) Valid() bool {
	switch o {
	case StepCompleted, StepWaiting, StepExternal:
		return true
	default:
		return false
	}
}

// WaitResult is the adapter-facing generic wait handoff. Record is the
// canonical workflow/wait contract and must be open. ResumeToken is a
// one-time process-local capability; only its matching digest enters Record.
type WaitResult struct {
	ID          string              `json:"id"`
	Record      workflowwait.Record `json:"record"`
	ResumeToken string              `json:"-"`
}

// StepResult is one mutually exclusive completed, waiting, or external
// outcome. Completed outputs are typed and persistable. Waiting delegates to
// the canonical generic-wait contract. External delegates to a durable
// operation record that recovery can observe independently of worker leases.
type StepResult struct {
	Outcome  StepOutcome           `json:"outcome"`
	Outputs  values.ValueSet       `json:"outputs,omitempty"`
	Wait     *WaitResult           `json:"wait,omitempty"`
	External *ExternalOperationRef `json:"external,omitempty"`
}

// ExternalOperationRef identifies adapter-owned work for observation or
// cancellation. Every field crosses the durable persistence and event
// boundary, so Kind, ID, and Metadata must be stable non-secret identifiers
// and metadata. They must never contain bearer tokens, credentials, or
// resolved secret material; adapters resolve authorization separately.
type ExternalOperationRef struct {
	Kind     string            `json:"kind"`
	ID       string            `json:"id"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ObservationState is the adapter-reported state of external work.
type ObservationState string

const (
	ObservationPending   ObservationState = "pending"
	ObservationSucceeded ObservationState = "succeeded"
	ObservationFailed    ObservationState = "failed"
	ObservationCanceled  ObservationState = "canceled"
)

// Valid reports whether s is a supported observation state.
func (s ObservationState) Valid() bool {
	switch s {
	case ObservationPending, ObservationSucceeded, ObservationFailed, ObservationCanceled:
		return true
	default:
		return false
	}
}

// Observation is a typed external-operation observation envelope. Progress is
// operational metadata, not workflow output data.
type Observation struct {
	State    ObservationState  `json:"state"`
	Progress map[string]string `json:"progress,omitempty"`
	Result   *StepResult       `json:"result,omitempty"`
	Failure  *ExecutionError   `json:"failure,omitempty"`
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

// Heartbeater optionally refreshes or probes an adapter-owned external
// operation independently of the runtime's own claim lease heartbeat.
type Heartbeater interface {
	Heartbeat(ctx context.Context, ref ExternalOperationRef) error
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
