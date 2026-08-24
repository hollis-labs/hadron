package call

import (
	"context"
	"errors"
	"fmt"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// KindName is the graph-native call executor name.
	KindName = "call"
	// KindVersion is the first immutable call executor contract.
	KindVersion = "v1"

	OutputRunID        = "run-id"
	OutputStatus       = "status"
	OutputEventsRef    = "events-ref"
	OutputCancellation = "cancellation"
	OutputOutputsRef   = "outputs-ref"

	CodeInvalidInvocation = "call_invalid_invocation"
	CodeResolutionFailed  = "call_resolution_failed"
	CodeResolutionInvalid = "call_resolution_invalid"
	CodeDepthExceeded     = "call_depth_exceeded"
	CodeCycle             = "call_cycle"
	CodeRecordFailed      = "call_resolution_record_failed"
	CodeInlineFailed      = "call_inline_failed"
	CodeChildRunFailed    = "call_child_run_failed"
	CodeResultInvalid     = "call_result_invalid"
)

const DefaultMaxDepth = workflowcompile.DefaultMaxCallDepth

var (
	ErrInvalidOptions     = errors.New("invalid call executor options")
	ErrInvalidCall        = errors.New("invalid workflow call")
	ErrResolutionConflict = errors.New("call resolution replay conflict")
)

// ResolutionOutcome distinguishes a newly persisted resolution from an exact
// durable replay. Stores must reject a changed record for an existing Key.
type ResolutionOutcome string

const (
	ResolutionApplied  ResolutionOutcome = "applied"
	ResolutionReplayed ResolutionOutcome = "replayed"
)

func (o ResolutionOutcome) valid() bool {
	return o == ResolutionApplied || o == ResolutionReplayed
}

// ResolutionRecord is the immutable durable explanation of one logical call
// site across execution attempts.
// Lineage includes the resolved child as its final element. InputDigest binds
// the record to the already typed effective child inputs. Stores must persist
// this record and its parent-invocation event atomically.
type ResolutionRecord struct {
	Key         string                `json:"key"`
	Invocation  CallSiteIdentity      `json:"invocation"`
	Requested   graph.DefinitionRef   `json:"requested"`
	Resolved    graph.DefinitionRef   `json:"resolved"`
	InputDigest string                `json:"input_digest"`
	Lineage     []graph.DefinitionRef `json:"lineage"`
}

// Validate reports malformed durable resolution metadata. It does not resolve
// definitions or read workflow values.
func (r ResolutionRecord) Validate() error {
	if err := validateStableString("resolution key", r.Key, true); err != nil {
		return err
	}
	if err := r.Invocation.Validate(); err != nil {
		return err
	}
	if err := validateRequestedDefinition(r.Requested); err != nil {
		return fmt.Errorf("requested definition: %w", err)
	}
	if err := validateRequestedDefinition(r.Resolved); err != nil {
		return fmt.Errorf("resolved definition: %w", err)
	}
	if err := values.ValidateDigest(r.Resolved.Digest); err != nil {
		return fmt.Errorf("resolved definition digest: %w", err)
	}
	if r.Resolved.Provenance == nil || r.Resolved.Provenance.Digest != r.Resolved.Digest {
		return fmt.Errorf("resolved definition requires matching provenance digest")
	}
	if err := values.ValidateDigest(r.InputDigest); err != nil {
		return fmt.Errorf("input digest: %w", err)
	}
	if len(r.Lineage) < 2 {
		return fmt.Errorf("resolution lineage requires parent and resolved child")
	}
	seen := make(map[string]int, len(r.Lineage))
	for index, definition := range r.Lineage {
		if err := validateRequestedDefinition(definition); err != nil {
			return fmt.Errorf("resolution lineage[%d]: %w", index, err)
		}
		if err := values.ValidateDigest(definition.Digest); err != nil {
			return fmt.Errorf("resolution lineage[%d]: %w", index, err)
		}
		if prior, duplicate := seen[definition.Digest]; duplicate {
			return fmt.Errorf("resolution lineage[%d] repeats lineage[%d] digest", index, prior)
		}
		seen[definition.Digest] = index
	}
	if r.Lineage[len(r.Lineage)-1].Digest != r.Resolved.Digest {
		return fmt.Errorf("resolution lineage must end with resolved definition")
	}
	return nil
}

// CallSiteIdentity excludes attempt number so retries of the same expanded
// parent node stay pinned to one resolution and one child-run identity.
type CallSiteIdentity struct {
	RunID     string `json:"run_id"`
	NodeID    string `json:"node_id"`
	Iteration string `json:"iteration,omitempty"`
}

// Validate reports malformed logical parent invocation identity.
func (i CallSiteIdentity) Validate() error {
	if err := validateStableString("call site run id", i.RunID, true); err != nil {
		return err
	}
	if err := graph.ValidateID(i.NodeID); err != nil {
		return fmt.Errorf("call site node id: %w", err)
	}
	return validateStableString("call site iteration", i.Iteration, false)
}

// RecordResolutionRequest asks the host store to persist an exact resolution
// before any child graph work begins. The store owns idempotency and event
// ordering; a process-local success without durable state is invalid.
type RecordResolutionRequest struct {
	Record ResolutionRecord
}

// ResolutionStore is the narrow durability boundary for parent call
// invocations. Implementations must be concurrency-safe, exact-replay safe,
// and must return ErrResolutionConflict (or an error wrapping it) when Key was
// used for a different record.
type ResolutionStore interface {
	RecordCallResolution(context.Context, RecordResolutionRequest) (ResolutionRecord, ResolutionOutcome, error)
}

// InlineRequest executes a resolved definition beneath the parent run
// identity. Lineage and IdempotencyKey must be propagated to nested calls.
// Inputs and Definition are defensive copies.
type InlineRequest struct {
	Parent         CallSiteIdentity                   `json:"parent"`
	Definition     workflowcompile.ResolvedDefinition `json:"definition"`
	Inputs         values.ValueSet                    `json:"inputs"`
	Lineage        []graph.DefinitionRef              `json:"lineage"`
	IdempotencyKey string                             `json:"idempotency_key"`
}

// InlineResult contains the finalized declared child outputs. Operational
// events remain attached to the parent run and are not data-plane fields.
type InlineResult struct {
	Outputs values.ValueSet `json:"outputs"`
}

// InlineExecutor runs a resolved child graph using the parent RunID and a
// nested, collision-free invocation namespace chosen by the host runtime.
type InlineExecutor interface {
	ExecuteInline(context.Context, InlineRequest) (InlineResult, error)
}

// CancellationHandle is a stable, non-secret reference for child-run control.
// It does not grant permission by itself; host policy authorizes its use.
type CancellationHandle struct {
	RunID  workflowruntime.RunID   `json:"run_id"`
	Policy graph.ParentClosePolicy `json:"policy"`
	Ref    string                  `json:"ref"`
}

// ChildRunRequest asks the host to atomically create/replay a child Run and
// its existing runtime.ChildRunLink. ChildRunID and IdempotencyKey are derived
// from immutable parent call-site and persisted resolution identity.
type ChildRunRequest struct {
	Parent         CallSiteIdentity                   `json:"parent"`
	ChildRunID     workflowruntime.RunID              `json:"child_run_id"`
	Definition     workflowcompile.ResolvedDefinition `json:"definition"`
	Plan           workflowruntime.PlanRef            `json:"plan"`
	Inputs         values.ValueSet                    `json:"inputs"`
	Lineage        []graph.DefinitionRef              `json:"lineage"`
	ParentClose    graph.ParentClosePolicy            `json:"parent_close"`
	IdempotencyKey string                             `json:"idempotency_key"`
}

// ChildRunResult is the durable child handle returned after atomic creation or
// exact replay. Run.Outputs may be nil while the child is active.
type ChildRunResult struct {
	Run          workflowruntime.RunSnapshot  `json:"run"`
	Link         workflowruntime.ChildRunLink `json:"link"`
	EventsRef    string                       `json:"events_ref"`
	Cancellation CancellationHandle           `json:"cancellation"`
}

// ChildRunExecutor creates/replays separately identified child runs. A
// successful return guarantees the Run, ChildRunLink, creation event, and
// input reference are durable as one semantic operation.
type ChildRunExecutor interface {
	StartChildRun(context.Context, ChildRunRequest) (ChildRunResult, error)
}

// ContextProvider reconstructs the parent-scoped expression context used to
// evaluate child declaration and resolver/import defaults. Invocation.Inputs
// already contains evaluated node-local with: values; those values are overlaid
// after defaults and remain authoritative on name collisions.
type ContextProvider interface {
	ExpressionContext(context.Context, stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error)
}

// ContextProviderFunc adapts a function to ContextProvider.
type ContextProviderFunc func(context.Context, stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error)

// ExpressionContext implements ContextProvider.
func (f ContextProviderFunc) ExpressionContext(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, values.ExpressionOptions, error) {
	return f(ctx, invocation)
}

// Options supplies every host boundary required by call@v1.
type Options struct {
	Resolver workflowcompile.DefinitionResolver
	State    ResolutionStore
	Context  ContextProvider
	Inline   InlineExecutor
	Runs     ChildRunExecutor
	MaxDepth int
}

// Executor implements the registered graph-native call step kind.
type Executor struct {
	resolver workflowcompile.DefinitionResolver
	state    ResolutionStore
	context  ContextProvider
	inline   InlineExecutor
	runs     ChildRunExecutor
	maxDepth int
}

func invalidOptions(reason string) error {
	return fmt.Errorf("%w: %s", ErrInvalidOptions, reason)
}
