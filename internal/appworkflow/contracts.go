package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var (
	ErrInvalidHost          = errors.New("invalid Hadron workflow host")
	ErrHostNotReady         = errors.New("hadron workflow host is not ready")
	ErrPolicyDenied         = errors.New("workflow policy denied operation")
	ErrConfirmationRequired = errors.New("workflow operation requires confirmation")
	ErrDryRunUnsupported    = errors.New("workflow dry-run is not supported by every step kind")
	ErrExecutionTarget      = errors.New("workflow execution target does not satisfy the plan")
)

type DefinitionProvider interface {
	ResolvePlan(context.Context, graph.DefinitionRef) (*compile.ExecutionPlan, error)
	LoadPlan(context.Context, string) (*compile.ExecutionPlan, error)
}

type IdentityRequest struct {
	PrincipalHint   string                             `json:"principal_hint,omitempty"`
	SourceAuthority string                             `json:"source_authority"`
	RunScope        *hoststate.RunScopeSelector        `json:"run_scope,omitempty"`
	ExecutionTarget *hoststate.ExecutionTargetSelector `json:"execution_target,omitempty"`
	Attributes      map[string]string                  `json:"attributes,omitempty"`
}

type IdentityProvider interface {
	BindIdentity(context.Context, IdentityRequest) (hoststate.IdentityBinding, error)
}

type PolicyEvaluator interface {
	EvaluatePolicy(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error)
}

type PolicyEvaluatorFunc func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error)

func (f PolicyEvaluatorFunc) EvaluatePolicy(ctx context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
	return f(ctx, facts)
}

// DryRunSupport is an explicit host assertion. StepKindSpec does not claim
// dry-run semantics, so absence of this collaborator always fails closed.
type DryRunSupport interface {
	SupportsDryRun(context.Context, stepkind.StepKindSpec) (bool, error)
}

type TelemetryObservation struct {
	RunID      runtime.RunID
	Event      string
	OccurredAt time.Time
	Attributes map[string]string
}

type TelemetrySink interface {
	ObserveWorkflow(context.Context, TelemetryObservation)
}

type RecoveryHook interface {
	RecoverWorkflow(context.Context, runtime.RecoverySnapshot, time.Time) error
}

type RecoveryHookFunc func(context.Context, runtime.RecoverySnapshot, time.Time) error

func (f RecoveryHookFunc) RecoverWorkflow(ctx context.Context, snapshot runtime.RecoverySnapshot, now time.Time) error {
	return f(ctx, snapshot, now)
}

// ChildRunRecoverySource exposes atomically created call.mode:run requests
// whose child run is still pending. WorkflowHostStore implements this port.
type ChildRunRecoverySource interface {
	RecoverPendingChildRuns(context.Context, int) ([]calladapter.ChildRunRequest, error)
}

// ChildRunDefinitionSource loads the exact immutable request that created a
// call.mode:run child. Cancellation uses its pinned resolved graph and never
// synthesizes a root start record or re-resolves the definition.
type ChildRunDefinitionSource interface {
	LoadChildRunRequest(context.Context, runtime.RunID) (calladapter.ChildRunRequest, error)
}

// ChildRunMaterializer is the injected W05-T03 seam that turns a resolved,
// pinned child definition into runnable child graph state. It must be
// idempotent by ChildRunRequest.IdempotencyKey.
type ChildRunMaterializer interface {
	MaterializeChildRun(context.Context, calladapter.ChildRunRequest) error
}

type Clock interface{ Now() time.Time }
type ClockFunc func() time.Time

func (f ClockFunc) Now() time.Time { return f() }

type KindRef struct{ Name, Version string }

type StartRunRequest struct {
	RunID          runtime.RunID
	Definition     graph.DefinitionRef
	Inputs         map[string]any
	IdempotencyKey string
	Identity       IdentityRequest
	Confirmed      bool
	DryRun         bool
	Activation     *hoststate.ActivationBinding
}

type StartRunResult struct {
	Run         *runtime.RunSnapshot
	Bound       runtime.BoundRun
	Decision    hoststate.PolicyDecision
	Facts       hoststate.PolicyFacts
	Diagnostics []diagnostic.Diagnostic
	Outcome     runtime.IdempotencyOutcome
	Phase       hoststate.StartPhase
	DryRun      bool
}

type InspectRunResult struct {
	Run       runtime.RunSnapshot
	Binding   hoststate.StartSnapshot
	Nodes     []runtime.NodeInvocationSnapshot
	Events    []runtime.Event
	Decisions []hoststate.PolicyDecision
}

type CancelRunRequest struct {
	RunID          runtime.RunID
	IdempotencyKey string
	Reason         string
	At             time.Time
}

type ExplainRunResult struct {
	Run         runtime.RunSnapshot
	Facts       hoststate.PolicyFacts
	Decision    hoststate.PolicyDecision
	Decisions   []hoststate.PolicyDecision
	Nodes       []runtime.NodeInvocationSnapshot
	Blocked     []runtime.BlockedReason
	DryRunTruth string
}

type HealthStatus struct {
	Started           bool      `json:"started"`
	Ready             bool      `json:"ready"`
	Recovering        bool      `json:"recovering"`
	LastRecoveryAt    time.Time `json:"last_recovery_at,omitempty"`
	LastRecoveryError string    `json:"last_recovery_error,omitempty"`
	IncompleteStarts  int       `json:"incomplete_starts"`
}

// Options contains only host boundaries. Concrete transport startup remains
// outside New, making construction deterministic and testable.
type Options struct {
	State         runtime.StateStore
	Journal       hoststate.Journal
	Definitions   DefinitionProvider
	Identity      IdentityProvider
	Policy        PolicyEvaluator
	Kinds         []stepkind.StepKind
	RequiredKinds []KindRef
	// Verifiers is the exact host verification catalog. Nil adopts a catalog
	// exposed by Definitions when available, otherwise the deterministic core.
	// Construction freezes implementations and specs for the Host lifetime.
	Verifiers     verification.Registry
	DryRun        DryRunSupport
	Activations   workflowwait.ActivationScheduler
	Waits         *runtime.WaitCoordinator
	Cancellation  *runtime.CancellationCoordinator
	RecoveryHooks []RecoveryHook
	// RecoveryRepeatPolicy authorizes crash/replay repetition after the core
	// executor effect and idempotency floors have passed. Nil keeps the
	// extraction-safe low-effect-only default.
	RecoveryRepeatPolicy runtime.RepeatPolicy
	// RecoveryRetryAuthorizer may grant graph retry for consequential effects;
	// nil preserves RetryEvaluator's fail-closed default.
	RecoveryRetryAuthorizer runtime.RetryAuthorizer
	ChildRuns               ChildRunMaterializer
	Telemetry               TelemetrySink
	Artifacts               values.ArtifactStore
	Clock                   Clock
	RecoveryInterval        time.Duration
	RecoveryBatchLimit      int
}

func normalizeIdentity(binding hoststate.IdentityBinding) hoststate.IdentityBinding {
	binding.Grants = append([]string(nil), binding.Grants...)
	sort.Strings(binding.Grants)
	binding.Grants = uniqueStrings(binding.Grants)
	binding.Extension = cloneStringMap(binding.Extension)
	if len(binding.Extension) == 0 {
		binding.Extension = nil
	}
	binding.RunScope = binding.RunScope.Clone()
	if binding.ExecutionTarget != nil {
		target := binding.ExecutionTarget.Clone()
		sort.Strings(target.Capabilities)
		target.Capabilities = uniqueStrings(target.Capabilities)
		binding.ExecutionTarget = &target
	}
	return binding
}

func normalizeIdentityRequest(request IdentityRequest) IdentityRequest {
	request.Attributes = cloneStringMap(request.Attributes)
	if len(request.Attributes) == 0 {
		request.Attributes = nil
	}
	if request.RunScope != nil {
		scope := request.RunScope.Clone()
		request.RunScope = &scope
	}
	if request.ExecutionTarget != nil {
		target := request.ExecutionTarget.Clone()
		sort.Slice(target.Kinds, func(i, j int) bool { return target.Kinds[i] < target.Kinds[j] })
		target.Kinds = uniqueTargetKinds(target.Kinds)
		target.RequiredCapabilities = append([]string(nil), target.RequiredCapabilities...)
		sort.Strings(target.RequiredCapabilities)
		target.RequiredCapabilities = uniqueStrings(target.RequiredCapabilities)
		target.RequiredLabels = cloneStringMap(target.RequiredLabels)
		target.SandboxModes = append([]hoststate.SandboxMode(nil), target.SandboxModes...)
		sort.Slice(target.SandboxModes, func(i, j int) bool { return target.SandboxModes[i] < target.SandboxModes[j] })
		target.SandboxModes = uniqueSandboxModes(target.SandboxModes)
		request.ExecutionTarget = &target
	}
	return request
}

func uniqueStrings(input []string) []string {
	if len(input) == 0 {
		return nil
	}
	result := input[:0]
	for _, value := range input {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func uniqueTargetKinds(input []hoststate.ExecutionTargetKind) []hoststate.ExecutionTargetKind {
	if len(input) == 0 {
		return nil
	}
	result := input[:0]
	for _, value := range input {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func uniqueSandboxModes(input []hoststate.SandboxMode) []hoststate.SandboxMode {
	if len(input) == 0 {
		return nil
	}
	result := input[:0]
	for _, value := range input {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func validateIdentityRequest(request IdentityRequest) error {
	if err := hoststate.ValidatePublicText(request.PrincipalHint, 256, false); err != nil {
		return errors.New("identity principal hint is invalid")
	}
	if err := hoststate.ValidatePublicText(request.SourceAuthority, 128, true); err != nil {
		return errors.New("identity source authority is invalid")
	}
	if request.RunScope != nil {
		if err := request.RunScope.Validate(); err != nil {
			return fmt.Errorf("run scope selector: %w", err)
		}
	}
	if request.ExecutionTarget != nil {
		if err := request.ExecutionTarget.Validate(); err != nil {
			return fmt.Errorf("execution target selector: %w", err)
		}
	}
	if err := hoststate.ValidatePublicAttributes(request.Attributes); err != nil {
		return errors.New("identity attributes are invalid")
	}
	return nil
}
