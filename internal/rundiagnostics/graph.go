package rundiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

const (
	graphDiagnosticSchemaVersion = "1"
	defaultNodeLimit             = 500
	defaultAttemptLimit          = 100
	defaultEventLimit            = 1000
	defaultValueLimit            = 1000
	defaultResourceLimit         = 500
	defaultActivationLimit       = 200
	maximumNodeLimit             = 5000
	maximumAttemptLimit          = 1000
	maximumEventLimit            = 10000
	maximumValueLimit            = 10000
	maximumResourceLimit         = 5000
	maximumActivationLimit       = 5000
)

var (
	ErrInvalidGraphQuery = errors.New("invalid graph-native run diagnostics query")
	ErrCorruptRunState   = errors.New("corrupt graph-native run diagnostics state")
)

// StateReader is the persisted-state subset needed to explain a run. It is
// deliberately read-only even when the concrete runtime store also supports
// lifecycle mutations.
type StateReader interface {
	LoadRun(context.Context, workflowruntime.RunID) (workflowruntime.RunSnapshot, error)
	ListRunInvocations(context.Context, workflowruntime.RunID) ([]workflowruntime.NodeInvocationSnapshot, error)
	ListAttempts(context.Context, workflowruntime.NodeInvocationID) ([]workflowruntime.AttemptSnapshot, error)
	LoadWait(context.Context, workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error)
	LoadValues(context.Context, values.ValueSetRef) (values.ValueSet, error)
	ListEvents(context.Context, workflowruntime.EventQuery) ([]workflowruntime.Event, error)
	Recovery(context.Context, workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error)
}

// BoundedStateReader lets persistence adapters enforce node and attempt bounds
// in SQL rather than materializing an unbounded history before projection.
// Service falls back to StateReader for extraction-compatible stores.
type BoundedStateReader interface {
	ListRunInvocationsForDiagnostics(context.Context, workflowruntime.RunID, int) ([]workflowruntime.NodeInvocationSnapshot, bool, error)
	ListAttemptsForDiagnostics(context.Context, workflowruntime.NodeInvocationID, int) ([]workflowruntime.AttemptSnapshot, bool, error)
}

// ControlReader exposes immutable catch/switch/finalizer facts.
type ControlReader interface {
	LoadControlDecision(context.Context, workflowruntime.ControlDecisionID) (workflowruntime.ControlDecisionSnapshot, error)
	LoadTerminalIntent(context.Context, workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error)
}

// ReplayReader exposes immutable replay provenance without mutation methods.
type ReplayReader interface {
	LoadReplayProvenance(context.Context, workflowruntime.RunID) (workflowruntime.ReplayProvenance, error)
}

// PinReader exposes an already-authorized run-scoped output binding.
type PinReader interface {
	LoadPin(context.Context, workflowruntime.NodeInvocationID) (workflowruntime.PinBinding, error)
}

// ResourceReader exposes current durable scheduler capacity ownership.
type ResourceReader interface {
	InspectSchedulerResources(context.Context, workflowruntime.SchedulerResourceQuery) (workflowruntime.SchedulerResourceState, error)
}

// StartReader exposes the immutable Hadron root binding, including an
// activation identity when the run was trigger-created.
type StartReader interface {
	LoadStart(context.Context, workflowruntime.RunID) (hoststate.StartSnapshot, error)
}

// ActivationAttemptSource exposes bounded durable fire-attempt history without
// coupling diagnostic DTOs to the activation persistence implementation.
// Absence is represented in Capabilities rather than as an empty history.
type ActivationAttemptSource interface {
	ListRunActivationAttempts(context.Context, workflowruntime.RunID, int) ([]ActivationFireAttempt, bool, error)
}

// Service composes persisted graph-native readers. Optional readers are
// advertised in Result.Capabilities so callers can distinguish unavailable
// history from an available empty history.
type Service struct {
	State       StateReader
	Plans       workflowruntime.RecoveryPlanSource
	Control     ControlReader
	Replay      ReplayReader
	Pins        PinReader
	Resources   ResourceReader
	Starts      StartReader
	Activations ActivationAttemptSource
}

// Query bounds every externally visible collection. A zero limit selects a
// conservative default; values above the documented maxima fail closed.
type Query struct {
	RunID           workflowruntime.RunID `json:"run_id"`
	Now             time.Time             `json:"now"`
	Display         values.DisplayPolicy  `json:"display,omitempty"`
	NodeLimit       int                   `json:"node_limit,omitempty"`
	AttemptLimit    int                   `json:"attempt_limit,omitempty"`
	EventLimit      int                   `json:"event_limit,omitempty"`
	ValueLimit      int                   `json:"value_limit,omitempty"`
	ResourceLimit   int                   `json:"resource_limit,omitempty"`
	ActivationLimit int                   `json:"activation_limit,omitempty"`
}

// Capabilities states which optional durable histories were consulted.
type Capabilities struct {
	ControlDecisions   bool `json:"control_decisions"`
	ReplayProvenance   bool `json:"replay_provenance"`
	PinBindings        bool `json:"pin_bindings"`
	ConcurrencyState   bool `json:"concurrency_state"`
	StartBinding       bool `json:"start_binding"`
	ActivationAttempts bool `json:"activation_attempts"`
}

// Truncation reports deterministic query bounds rather than silently implying
// the returned projection is complete.
type Truncation struct {
	Nodes       bool `json:"nodes,omitempty"`
	Edges       bool `json:"edges,omitempty"`
	Attempts    bool `json:"attempts,omitempty"`
	Events      bool `json:"events,omitempty"`
	Values      bool `json:"values,omitempty"`
	Resources   bool `json:"resources,omitempty"`
	Activations bool `json:"activations,omitempty"`
}

// Result is the reusable graph-native diagnostic DTO shared by Hadron
// transports. It contains no claim lease token, resume credential, or raw
// secret; safe lease owner, generation, and expiry facts remain visible.
type Result struct {
	SchemaVersion   string                          `json:"schema_version"`
	Run             RunDiagnostic                   `json:"run"`
	Plan            PlanDiagnostic                  `json:"plan"`
	Nodes           []NodeDiagnostic                `json:"nodes"`
	Values          []ValueSetDiagnostic            `json:"values,omitempty"`
	Events          []workflowruntime.RenderedEvent `json:"events,omitempty"`
	Control         ControlDiagnostic               `json:"control"`
	Replay          *ReplayDiagnostic               `json:"replay,omitempty"`
	Resources       *ResourceDiagnostic             `json:"resources,omitempty"`
	StartActivation *StartActivationDiagnostic      `json:"start_activation,omitempty"`
	StartPolicy     *StartPolicyDiagnostic          `json:"start_policy,omitempty"`
	Activations     []ActivationFireAttempt         `json:"activation_attempts,omitempty"`
	Capabilities    Capabilities                    `json:"capabilities"`
	Omissions       []string                        `json:"omissions,omitempty"`
	Truncated       Truncation                      `json:"truncated"`
}

type RunDiagnostic struct {
	ID         workflowruntime.RunID     `json:"id"`
	Plan       workflowruntime.PlanRef   `json:"plan"`
	Status     workflowruntime.RunStatus `json:"status"`
	Inputs     *values.ValueSetRef       `json:"inputs,omitempty"`
	Outputs    *values.ValueSetRef       `json:"outputs,omitempty"`
	Generation uint64                    `json:"generation"`
	CreatedAt  time.Time                 `json:"created_at"`
	UpdatedAt  time.Time                 `json:"updated_at"`
}

type PlanDiagnostic struct {
	ID            string                     `json:"id"`
	Version       string                     `json:"version"`
	Digest        string                     `json:"digest"`
	SchemaVersion string                     `json:"schema_version"`
	GraphDigest   string                     `json:"graph_digest"`
	Definition    DefinitionDiagnostic       `json:"definition"`
	Provenance    ProvenanceDiagnostic       `json:"provenance"`
	SourceDigests []SourceDigestDiagnostic   `json:"source_digests"`
	Source        *SourceDiagnostic          `json:"source,omitempty"`
	Nodes         []PlanNodeDiagnostic       `json:"nodes"`
	Edges         []PlanEdgeDiagnostic       `json:"edges,omitempty"`
	Activations   []PlanActivationDiagnostic `json:"activations,omitempty"`
}

type DefinitionDiagnostic struct {
	Authority string `json:"authority,omitempty"`
	Kind      string `json:"kind,omitempty"`
	ID        string `json:"id,omitempty"`
	Locator   string `json:"locator,omitempty"`
	Version   string `json:"version,omitempty"`
	Digest    string `json:"digest,omitempty"`
}

type ProvenanceDiagnostic struct {
	Authority string                          `json:"authority,omitempty"`
	Origin    string                          `json:"origin,omitempty"`
	Locator   string                          `json:"locator,omitempty"`
	Revision  string                          `json:"revision,omitempty"`
	Digest    string                          `json:"digest,omitempty"`
	Parents   []ProvenanceReferenceDiagnostic `json:"parents,omitempty"`
}

type ProvenanceReferenceDiagnostic struct {
	Authority string `json:"authority,omitempty"`
	Locator   string `json:"locator,omitempty"`
	Digest    string `json:"digest,omitempty"`
}

type SourceDigestDiagnostic struct {
	Format graph.SourceFormat `json:"format"`
	Digest string             `json:"digest"`
}

type SourceDiagnostic struct {
	Format      graph.SourceFormat `json:"format"`
	Locator     string             `json:"locator"`
	StartLine   int                `json:"start_line,omitempty"`
	StartColumn int                `json:"start_column,omitempty"`
	EndLine     int                `json:"end_line,omitempty"`
	EndColumn   int                `json:"end_column,omitempty"`
	Section     string             `json:"section,omitempty"`
	StepName    string             `json:"step_name,omitempty"`
	StageName   string             `json:"stage_name,omitempty"`
	Path        []string           `json:"path,omitempty"`
}

type PlanNodeDiagnostic struct {
	ID            string              `json:"id"`
	DisplayName   string              `json:"display_name,omitempty"`
	Kind          string              `json:"kind"`
	KindVersion   string              `json:"kind_version,omitempty"`
	ReadyWhen     graph.ReadyRule     `json:"ready_when"`
	Needs         []string            `json:"needs,omitempty"`
	Effects       graph.EffectSet     `json:"declared_effects,omitempty"`
	Finally       bool                `json:"finally,omitempty"`
	CatchTargets  []string            `json:"catch_targets,omitempty"`
	SwitchTargets []string            `json:"switch_targets,omitempty"`
	Position      *PositionDiagnostic `json:"position,omitempty"`
	Retry         *RetryDiagnostic    `json:"retry,omitempty"`
	Source        *SourceDiagnostic   `json:"source,omitempty"`
}

// PositionDiagnostic is the only graph metadata projected to transports. It
// accepts canonical finite x/y coordinates and never forwards arbitrary node
// metadata into an operator surface.
type PositionDiagnostic struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// RetryDiagnostic contains declaration-only scheduling facts. Runtime retry
// state remains represented by NodeDiagnostic.Attempts.
type RetryDiagnostic struct {
	Attempts     int                   `json:"attempts"`
	Strategy     graph.BackoffStrategy `json:"strategy,omitempty"`
	InitialDelay graph.Duration        `json:"initial_delay,omitempty"`
	MaxDelay     graph.Duration        `json:"max_delay,omitempty"`
}

// PlanEdgeDiagnostic is the safe graph-IR edge projection used by every
// transport. Edge metadata is deliberately excluded.
type PlanEdgeDiagnostic struct {
	From      string                   `json:"from"`
	To        string                   `json:"to"`
	Kind      graph.EdgeKind           `json:"kind"`
	Source    *SourceDiagnostic        `json:"source,omitempty"`
	ValueFlow *EdgeValueFlowDiagnostic `json:"value_flow,omitempty"`
}

// EdgeValueFlowDiagnostic associates one data edge with the already-rendered,
// bounded value sets at its source and target invocations. Refs are emitted
// only when the corresponding ValueSetDiagnostic is present in Result.Values.
type EdgeValueFlowDiagnostic struct {
	SourceOutputs []InvocationValueDiagnostic `json:"source_outputs,omitempty"`
	TargetInputs  []InvocationValueDiagnostic `json:"target_inputs,omitempty"`
	ValuesOmitted bool                        `json:"values_omitted,omitempty"`
}

type InvocationValueDiagnostic struct {
	Invocation workflowruntime.NodeInvocationID `json:"invocation"`
	Values     values.ValueSetRef               `json:"values"`
}

type PlanActivationDiagnostic struct {
	ID     string            `json:"id"`
	Kind   string            `json:"kind"`
	Source *SourceDiagnostic `json:"source,omitempty"`
}

type NodeDiagnostic struct {
	ID                workflowruntime.NodeInvocationID `json:"id"`
	Status            workflowruntime.NodeStatus       `json:"status"`
	Origin            workflowruntime.InvocationOrigin `json:"origin,omitempty"`
	MemoKeyDigest     string                           `json:"memo_key_digest,omitempty"`
	Inputs            *values.ValueSetRef              `json:"inputs,omitempty"`
	Outputs           *values.ValueSetRef              `json:"outputs,omitempty"`
	LatestAttempt     int                              `json:"latest_attempt,omitempty"`
	Priority          int                              `json:"priority,omitempty"`
	ClaimGeneration   uint64                           `json:"claim_generation"`
	Lease             *LeaseDiagnostic                 `json:"lease,omitempty"`
	Generation        uint64                           `json:"generation"`
	CreatedAt         time.Time                        `json:"created_at"`
	UpdatedAt         time.Time                        `json:"updated_at"`
	Source            *SourceDiagnostic                `json:"source,omitempty"`
	Definition        PlanNodeDiagnostic               `json:"definition"`
	Attempts          []AttemptDiagnostic              `json:"attempts,omitempty"`
	Wait              *WaitDiagnostic                  `json:"wait,omitempty"`
	Explanation       NodeExplanation                  `json:"explanation"`
	Upstream          []DependencyDiagnostic           `json:"upstream,omitempty"`
	Downstream        []DownstreamDiagnostic           `json:"downstream,omitempty"`
	Pin               *PinDiagnostic                   `json:"pin,omitempty"`
	Resources         NodeResourceDiagnostic           `json:"resources"`
	AttemptsTruncated bool                             `json:"attempts_truncated,omitempty"`
}

type LeaseDiagnostic struct {
	Owner      string    `json:"owner"`
	Generation uint64    `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
	Masked     bool      `json:"masked,omitempty"`
}

type AttemptDiagnostic struct {
	Number     int                        `json:"number"`
	Status     workflowruntime.NodeStatus `json:"status"`
	Executor   ExecutorDiagnostic         `json:"executor"`
	Inputs     *values.ValueSetRef        `json:"inputs,omitempty"`
	Outputs    *values.ValueSetRef        `json:"outputs,omitempty"`
	Failure    *FailureDiagnostic         `json:"failure,omitempty"`
	StartedAt  time.Time                  `json:"started_at"`
	FinishedAt time.Time                  `json:"finished_at,omitempty"`
	Generation uint64                     `json:"generation"`
}

type ExecutorDiagnostic struct {
	Kind       string            `json:"kind"`
	Version    string            `json:"version"`
	Target     string            `json:"target,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Masked     bool              `json:"masked,omitempty"`
}

type FailureDiagnostic struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Retryable bool              `json:"retryable,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
	Masked    bool              `json:"masked,omitempty"`
}

type WaitDiagnostic struct {
	ID           workflowruntime.WaitID    `json:"id"`
	Kind         workflowwait.Kind         `json:"kind"`
	Status       workflowwait.Status       `json:"status"`
	WakeSource   workflowwait.WakeSource   `json:"wake_source"`
	Visibility   workflowwait.Visibility   `json:"visibility"`
	WakeAt       time.Time                 `json:"wake_at,omitempty"`
	Deadline     time.Time                 `json:"deadline,omitempty"`
	Payload      *values.ValueSetRef       `json:"payload,omitempty"`
	ResumeValues *values.ValueSetRef       `json:"resume_values,omitempty"`
	Resolution   *WaitResolutionDiagnostic `json:"resolution,omitempty"`
	Generation   uint64                    `json:"generation"`
	CreatedAt    time.Time                 `json:"created_at"`
	UpdatedAt    time.Time                 `json:"updated_at"`
	ResolvedAt   time.Time                 `json:"resolved_at,omitempty"`
}

type WaitResolutionDiagnostic struct {
	Source        workflowwait.WakeSource `json:"source"`
	ResponderKind string                  `json:"responder_kind"`
	PayloadDigest string                  `json:"payload_digest,omitempty"`
	ResolvedAt    time.Time               `json:"resolved_at"`
}

type NodeExplanation struct {
	Code         string                             `json:"code"`
	Message      string                             `json:"message"`
	Dependencies []workflowruntime.NodeInvocationID `json:"dependencies,omitempty"`
	Details      map[string]string                  `json:"details,omitempty"`
	Failure      *FailureDiagnostic                 `json:"failure,omitempty"`
	Masked       bool                               `json:"masked,omitempty"`
}

type DependencyDiagnostic struct {
	NodeID      string                           `json:"node_id"`
	Invocations []DependencyInvocationDiagnostic `json:"invocations,omitempty"`
}

type DependencyInvocationDiagnostic struct {
	ID     workflowruntime.NodeInvocationID `json:"id"`
	Status workflowruntime.NodeStatus       `json:"status"`
}

type DownstreamDiagnostic struct {
	NodeID  string            `json:"node_id"`
	Effects graph.EffectSet   `json:"declared_effects,omitempty"`
	Source  *SourceDiagnostic `json:"source,omitempty"`
}

type PinDiagnostic struct {
	Outputs            values.ValueSetRef               `json:"outputs"`
	Source             workflowruntime.NodeInvocationID `json:"source"`
	SourcePlanDigest   string                           `json:"source_plan_digest"`
	SourceOrigin       workflowruntime.InvocationOrigin `json:"source_origin"`
	OutputSchemaDigest string                           `json:"output_schema_digest"`
	PolicyCode         string                           `json:"policy_code"`
	PolicyReason       string                           `json:"policy_reason"`
	BoundAt            time.Time                        `json:"bound_at"`
}

type ValueSetDiagnostic struct {
	Ref    values.ValueSetRef      `json:"ref"`
	Roles  []string                `json:"roles"`
	Values values.RenderedValueSet `json:"values"`
}

type ControlDiagnostic struct {
	Decisions      []ControlDecisionDiagnostic `json:"decisions,omitempty"`
	TerminalIntent *TerminalIntentDiagnostic   `json:"terminal_intent,omitempty"`
}

type ControlDecisionDiagnostic struct {
	Source     workflowruntime.NodeInvocationID       `json:"source"`
	Kind       workflowruntime.ControlDecisionKind    `json:"kind"`
	Outcome    workflowruntime.ControlDecisionOutcome `json:"outcome"`
	RuleIndex  *int                                   `json:"rule_index,omitempty"`
	Targets    []workflowruntime.NodeInvocationID     `json:"targets,omitempty"`
	BindAs     string                                 `json:"bind_as,omitempty"`
	Error      *values.ValueSetRef                    `json:"error,omitempty"`
	Generation uint64                                 `json:"generation"`
	CreatedAt  time.Time                              `json:"created_at"`
}

type TerminalIntentDiagnostic struct {
	IntendedStatus         workflowruntime.RunStatus            `json:"intended_status"`
	SuccessOutputsRequired bool                                 `json:"success_outputs_required,omitempty"`
	Reason                 *FailureDiagnostic                   `json:"reason,omitempty"`
	Error                  *values.ValueSetRef                  `json:"error,omitempty"`
	Finalizers             []workflowruntime.FinalizerScope     `json:"finalizers,omitempty"`
	Status                 workflowruntime.TerminalIntentStatus `json:"status"`
	Generation             uint64                               `json:"generation"`
	CreatedAt              time.Time                            `json:"created_at"`
	UpdatedAt              time.Time                            `json:"updated_at"`
	CompletedAt            time.Time                            `json:"completed_at,omitempty"`
}

type ReplayDiagnostic struct {
	SourceRunID workflowruntime.RunID    `json:"source_run_id"`
	FromNodeID  string                   `json:"from_node_id"`
	PlanDigest  string                   `json:"plan_digest"`
	CreatedAt   time.Time                `json:"created_at"`
	Policy      []ReplayPolicyDiagnostic `json:"policy,omitempty"`
}

type ReplayPolicyDiagnostic struct {
	Invocation workflowruntime.NodeInvocationID `json:"invocation"`
	Attempt    *workflowruntime.AttemptID       `json:"attempt,omitempty"`
	Allow      bool                             `json:"allow"`
	Code       string                           `json:"code"`
	Reason     string                           `json:"reason"`
}

type ResourceDiagnostic struct {
	Holders []workflowruntime.SchedulerResourceHolder `json:"holders"`
	Waiters []workflowruntime.SchedulerResourceWaiter `json:"waiters"`
}

type NodeResourceDiagnostic struct {
	Holders []workflowruntime.SchedulerResourceHolder `json:"holders,omitempty"`
	Waiter  *workflowruntime.SchedulerResourceWaiter  `json:"waiter,omitempty"`
}

// StartActivationDiagnostic is the narrower immutable context available on a
// bound run. OccurredAt is not relabeled as a scheduled fire time.
type StartActivationDiagnostic struct {
	ActivationID       string    `json:"activation_id"`
	FireIdentityDigest string    `json:"fire_identity_digest"`
	OccurredAt         time.Time `json:"occurred_at"`
}

// StartPolicyDiagnostic is a deliberately narrow projection of immutable
// host start facts. Identity, grants, trust, target handles, policy attributes,
// and request material never cross this boundary.
type StartPolicyDiagnostic struct {
	Effects              graph.EffectSet         `json:"declared_effects"`
	RequiredCapabilities []string                `json:"required_capabilities,omitempty"`
	BlastRadius          map[string]int          `json:"blast_radius"`
	NodeCount            int                     `json:"node_count"`
	DryRunAvailable      bool                    `json:"dry_run_available"`
	ConfirmationAdvised  bool                    `json:"confirmation_advised"`
	Decision             hoststate.PolicyOutcome `json:"decision"`
	ExposureRef          string                  `json:"exposure_ref,omitempty"`
	ExposureMasked       bool                    `json:"exposure_masked,omitempty"`
}

// ActivationFireAttempt is a credential-free projection of a stable schedule
// firing. ScheduledAt and FiredAt remain intentionally distinct.
type ActivationFireAttempt struct {
	FireID       string                `json:"fire_id"`
	ActivationID string                `json:"activation_id"`
	RunID        workflowruntime.RunID `json:"run_id"`
	ScheduledAt  time.Time             `json:"scheduled_at"`
	FiredAt      time.Time             `json:"fired_at,omitempty"`
	Attempt      int                   `json:"attempt"`
	Status       string                `json:"status"`
	FailureCode  string                `json:"failure_code,omitempty"`
	Source       string                `json:"source"`
}

func (a ActivationFireAttempt) validate(runID workflowruntime.RunID) error {
	if strings.TrimSpace(a.FireID) == "" || strings.TrimSpace(a.ActivationID) == "" || a.RunID != runID || a.ScheduledAt.IsZero() || a.Attempt < 1 || strings.TrimSpace(a.Status) == "" || strings.TrimSpace(a.Source) == "" {
		return errors.New("activation attempt has invalid identity, time, attempt, status, or source")
	}
	if !a.FiredAt.IsZero() && a.FiredAt.Before(a.ScheduledAt) {
		return errors.New("activation fired_at precedes scheduled_at")
	}
	fields := []struct {
		value    string
		maximum  int
		required bool
	}{{a.FireID, 256, true}, {a.ActivationID, 256, true}, {a.Status, 64, true}, {a.FailureCode, 128, false}, {a.Source, 64, true}}
	for _, field := range fields {
		if hoststate.ValidatePublicText(field.value, field.maximum, field.required) != nil {
			return errors.New("activation attempt contains invalid or credential-shaped metadata")
		}
		if _, sensitive := safeDiagnosticText(field.value); sensitive {
			return errors.New("activation attempt contains credential-shaped metadata")
		}
	}
	return nil
}

type normalizedQuery struct {
	Query
	NodeLimit, AttemptLimit, EventLimit, ValueLimit, ResourceLimit, ActivationLimit int
}

func normalizeQuery(query Query) (normalizedQuery, error) {
	if strings.TrimSpace(string(query.RunID)) == "" || query.Now.IsZero() {
		return normalizedQuery{}, fmt.Errorf("%w: run_id and now are required", ErrInvalidGraphQuery)
	}
	if err := query.Display.Validate(); err != nil {
		return normalizedQuery{}, fmt.Errorf("%w: %w", ErrInvalidGraphQuery, err)
	}
	limits := []*int{&query.NodeLimit, &query.AttemptLimit, &query.EventLimit, &query.ValueLimit, &query.ResourceLimit, &query.ActivationLimit}
	defaults := []int{defaultNodeLimit, defaultAttemptLimit, defaultEventLimit, defaultValueLimit, defaultResourceLimit, defaultActivationLimit}
	maximums := []int{maximumNodeLimit, maximumAttemptLimit, maximumEventLimit, maximumValueLimit, maximumResourceLimit, maximumActivationLimit}
	for index, limit := range limits {
		if *limit < 0 || *limit > maximums[index] {
			return normalizedQuery{}, fmt.Errorf("%w: limit[%d] must be between 0 and %d", ErrInvalidGraphQuery, index, maximums[index])
		}
		if *limit == 0 {
			*limit = defaults[index]
		}
	}
	query.Now = query.Now.UTC()
	return normalizedQuery{Query: query, NodeLimit: query.NodeLimit, AttemptLimit: query.AttemptLimit, EventLimit: query.EventLimit, ValueLimit: query.ValueLimit, ResourceLimit: query.ResourceLimit, ActivationLimit: query.ActivationLimit}, nil
}

func typedNil(input any) bool {
	if input == nil {
		return true
	}
	value := reflect.ValueOf(input)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func corrupt(resource string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrCorruptRunState, resource, err)
}

func cloneRef(input *values.ValueSetRef) *values.ValueSetRef {
	if input == nil {
		return nil
	}
	result := *input
	return &result
}

func safeSource(input *graph.SourceRef) *SourceDiagnostic {
	if input == nil {
		return nil
	}
	return &SourceDiagnostic{Format: input.Format, Locator: safeLocator(input.Locator), StartLine: input.StartLine, StartColumn: input.StartColumn, EndLine: input.EndLine, EndColumn: input.EndColumn, Section: input.Section, StepName: input.StepName, StageName: input.StageName, Path: append([]string(nil), input.Path...)}
}

func safePosition(metadata graph.Metadata) *PositionDiagnostic {
	raw, exists := metadata["position"]
	if !exists {
		return nil
	}
	position, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	x, xOK := finiteCoordinate(position["x"])
	y, yOK := finiteCoordinate(position["y"])
	if !xOK || !yOK {
		return nil
	}
	return &PositionDiagnostic{X: x, Y: y}
}

func finiteCoordinate(input any) (float64, bool) {
	var value float64
	switch typed := input.(type) {
	case float64:
		value = typed
	case float32:
		value = float64(typed)
	case int:
		value = float64(typed)
	case int8:
		value = float64(typed)
	case int16:
		value = float64(typed)
	case int32:
		value = float64(typed)
	case int64:
		value = float64(typed)
	case uint:
		value = float64(typed)
	case uint8:
		value = float64(typed)
	case uint16:
		value = float64(typed)
	case uint32:
		value = float64(typed)
	case uint64:
		value = float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		value = parsed
	default:
		return 0, false
	}
	return value, !math.IsNaN(value) && !math.IsInf(value, 0)
}

func safeLocator(input string) string {
	return hoststate.SanitizeLocator(input)
}

func safeProvenance(input graph.Provenance) ProvenanceDiagnostic {
	result := ProvenanceDiagnostic{Authority: input.Authority, Origin: input.Origin, Locator: safeLocator(input.Locator), Revision: input.Revision, Digest: input.Digest}
	for _, parent := range input.Parents {
		result.Parents = append(result.Parents, ProvenanceReferenceDiagnostic{Authority: parent.Authority, Locator: safeLocator(parent.Locator), Digest: parent.Digest})
	}
	return result
}

func maskedMap(input map[string]string, policy values.DisplayPolicy) (map[string]string, bool) {
	if len(input) == 0 {
		return nil, false
	}
	result := make(map[string]string, len(input))
	masked := !policy.RevealsPrivate()
	if !masked {
		for _, value := range input {
			if _, sensitive := safeDiagnosticText(value); sensitive {
				masked = true
				break
			}
		}
	}
	for key, value := range input {
		if masked {
			value = values.RedactedMarker
		}
		result[key] = value
	}
	return result, masked
}

func failureDiagnostic(input *workflowruntime.Failure, policy values.DisplayPolicy) *FailureDiagnostic {
	if input == nil {
		return nil
	}
	details, masked := maskedMap(input.Details, policy)
	message, messageMasked := safeDiagnosticText(input.Message)
	return &FailureDiagnostic{Code: input.Code, Message: message, Retryable: input.Retryable, Details: details, Masked: masked || messageMasked}
}

func safeDiagnosticText(input string) (string, bool) {
	lower := strings.ToLower(input)
	for _, marker := range []string{"secret://", "bearer ", "basic ", "token=", "password=", "passwd=", "api_key=", "apikey=", "signature=", "set-cookie:"} {
		if strings.Contains(lower, marker) {
			return values.RedactedMarker, true
		}
	}
	if scheme := strings.Index(input, "://"); scheme >= 0 {
		remainder := input[scheme+3:]
		if strings.Contains(remainder, "@") || strings.Contains(remainder, "?") || strings.Contains(remainder, "#") {
			return values.RedactedMarker, true
		}
	}
	return input, false
}

func canonicalIDs(input []string) []string {
	seen := make(map[string]struct{}, len(input))
	result := make([]string, 0, len(input))
	for _, item := range input {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}
