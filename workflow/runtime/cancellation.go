package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
)

const (
	EventRunCancellationRequested = "run.cancellation_requested"
	EventCancellationResolved     = "cancellation.resolved"
)

var (
	ErrInvalidCancellation     = errors.New("invalid workflow cancellation")
	ErrCancellationUnsupported = errors.New("workflow cancellation is unsupported by executor")
)

// CancellationIntentKind describes durable work that cannot be completed
// inside the run-cancellation transaction because it requires adapter or host
// I/O.
type CancellationIntentKind string

const (
	CancellationRunningAttempt    CancellationIntentKind = "running_attempt"
	CancellationExternalOperation CancellationIntentKind = "external_operation"
	CancellationChildRun          CancellationIntentKind = "child_run"
)

func (k CancellationIntentKind) Valid() bool {
	switch k {
	case CancellationRunningAttempt, CancellationExternalOperation, CancellationChildRun:
		return true
	default:
		return false
	}
}

type CancellationIntentStatus string

const (
	CancellationPending  CancellationIntentStatus = "pending"
	CancellationResolved CancellationIntentStatus = "resolved"
)

func (s CancellationIntentStatus) Valid() bool {
	return s == CancellationPending || s == CancellationResolved
}

// CancellationIntentSnapshot is restart-durable and remains pending while an
// explicit or unsupported remote cancellation is unresolved. Attempt and
// ChildRunID are mutually exclusive according to Kind.
type CancellationIntentSnapshot struct {
	ID          string                   `json:"id"`
	RunID       RunID                    `json:"run_id"`
	Kind        CancellationIntentKind   `json:"kind"`
	Attempt     *AttemptID               `json:"attempt,omitempty"`
	ChildRunID  RunID                    `json:"child_run_id,omitempty"`
	ChildPolicy graph.ParentClosePolicy  `json:"child_policy,omitempty"`
	Status      CancellationIntentStatus `json:"status"`
	Generation  uint64                   `json:"generation"`
	RequestedAt time.Time                `json:"requested_at"`
	ResolvedAt  time.Time                `json:"resolved_at,omitempty"`
	UpdatedAt   time.Time                `json:"updated_at"`
}

func (s CancellationIntentSnapshot) Validate() error {
	if err := validateRequiredText("cancellation intent id", s.ID); err != nil {
		return err
	}
	if err := validateOpaqueID("cancellation run id", string(s.RunID)); err != nil {
		return err
	}
	if !s.Kind.Valid() || !s.Status.Valid() {
		return fmt.Errorf("unsupported cancellation intent kind or status")
	}
	if s.Generation == 0 || s.RequestedAt.IsZero() || s.UpdatedAt.Before(s.RequestedAt) {
		return fmt.Errorf("cancellation intent requires generation and ordered timestamps")
	}
	switch s.Kind {
	case CancellationRunningAttempt, CancellationExternalOperation:
		if s.Attempt == nil || s.ChildRunID != "" || s.ChildPolicy != "" {
			return fmt.Errorf("attempt cancellation intent requires only attempt identity")
		}
		if err := s.Attempt.Validate(); err != nil {
			return err
		}
		if s.Attempt.Invocation.RunID != s.RunID {
			return fmt.Errorf("cancellation attempt must belong to run")
		}
	case CancellationChildRun:
		if s.Attempt != nil || s.ChildRunID == "" || s.ChildRunID == s.RunID || s.ChildPolicy != graph.ParentCloseRequestCancel {
			return fmt.Errorf("child cancellation intent requires request_cancel child identity")
		}
	}
	if s.Status == CancellationPending && !s.ResolvedAt.IsZero() {
		return fmt.Errorf("pending cancellation intent must not contain resolved_at")
	}
	if s.Status == CancellationResolved && (s.ResolvedAt.IsZero() || s.ResolvedAt.Before(s.RequestedAt) || !s.ResolvedAt.Equal(s.UpdatedAt)) {
		return fmt.Errorf("resolved cancellation intent requires ordered resolved_at")
	}
	return nil
}

// ChildRunLink is the immutable relation used to propagate parent close.
type ChildRunLink struct {
	ParentRunID RunID                   `json:"parent_run_id"`
	Invocation  NodeInvocationID        `json:"invocation"`
	ChildRunID  RunID                   `json:"child_run_id"`
	Policy      graph.ParentClosePolicy `json:"policy"`
	CreatedAt   time.Time               `json:"created_at"`
}

func (l ChildRunLink) Validate() error {
	if err := validateOpaqueID("parent run id", string(l.ParentRunID)); err != nil {
		return err
	}
	if err := l.Invocation.Validate(); err != nil {
		return err
	}
	if l.Invocation.RunID != l.ParentRunID {
		return fmt.Errorf("child invocation must belong to parent run")
	}
	if err := validateOpaqueID("child run id", string(l.ChildRunID)); err != nil {
		return err
	}
	if l.ChildRunID == l.ParentRunID || !l.Policy.Valid() || l.CreatedAt.IsZero() {
		return fmt.Errorf("child run link requires distinct run, policy, and created_at")
	}
	return nil
}

// RequestRunCancellationRequest atomically begins cancellation and is
// idempotent by key. Reason is safe-to-persist structured metadata.
type RequestRunCancellationRequest struct {
	RunID              RunID
	ExpectedGeneration uint64
	IdempotencyKey     string
	Reason             Failure
	At                 time.Time
}

func (r RequestRunCancellationRequest) Validate() error {
	if err := validateOpaqueID("run id", string(r.RunID)); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("run cancellation requires generation and timestamp")
	}
	if err := validateRequiredText("run cancellation idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	return r.Reason.Validate()
}

type RequestRunCancellationResult struct {
	Outcome IdempotencyOutcome
	Run     RunSnapshot
	Nodes   []NodeInvocationSnapshot
	Intents []CancellationIntentSnapshot
	Events  []Event
}

type ResolveCancellationIntentRequest struct {
	IntentID           string
	ExpectedGeneration uint64
	At                 time.Time
}

func (r ResolveCancellationIntentRequest) Validate() error {
	if err := validateRequiredText("cancellation intent id", r.IntentID); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("cancellation resolution requires generation and timestamp")
	}
	return nil
}

type ResolveCancellationIntentResult struct {
	Intent  CancellationIntentSnapshot
	Node    *NodeInvocationSnapshot
	Attempt *AttemptSnapshot
	Event   *Event
}

type CancellationIntentQuery struct {
	RunID RunID
	Limit int
}

// CancellationStore is the atomic durable cancellation surface.
type CancellationStore interface {
	RecordChildRun(context.Context, ChildRunLink) error
	ListChildRuns(context.Context, RunID) ([]ChildRunLink, error)
	RequestRunCancellation(context.Context, RequestRunCancellationRequest) (RequestRunCancellationResult, error)
	ResolveCancellationIntent(context.Context, ResolveCancellationIntentRequest) (ResolveCancellationIntentResult, error)
	RecoverCancellationIntents(context.Context, CancellationIntentQuery) ([]CancellationIntentSnapshot, error)
}

// AttemptCanceler signals an in-process or host-managed context execution.
// Success means execution ownership has stopped and the durable attempt may be
// closed. Calls happen outside StateStore transactions.
type AttemptCanceler interface {
	CancelAttempt(context.Context, AttemptSnapshot) error
}

// ChildRunCanceler requests cancellation from a separately hosted child run.
// It is used only for request_cancel links; direct cancel links are propagated
// inside the owning StateStore.
type ChildRunCanceler interface {
	RequestChildRunCancellation(context.Context, ChildRunLink) error
}

// CancellationCoordinator replays durable cancellation work without keeping
// correctness-critical process state.
type CancellationCoordinator struct {
	Store    StateStore
	Registry stepkind.Registry
	External *ExternalOperationCoordinator
	Attempts AttemptCanceler
	Children ChildRunCanceler
	Now      func() time.Time
}

// Request commits run cancellation before performing any host or adapter I/O.
func (c CancellationCoordinator) Request(ctx context.Context, request RequestRunCancellationRequest) (RequestRunCancellationResult, []error, error) {
	if err := c.validate(ctx); err != nil {
		return RequestRunCancellationResult{}, nil, err
	}
	durableCtx := context.WithoutCancel(ctx)
	result, err := c.Store.RequestRunCancellation(durableCtx, request)
	if err != nil {
		return RequestRunCancellationResult{}, nil, err
	}
	errorsByIntent := c.reconcileIntents(ctx, result.Intents)
	return result, errorsByIntent, nil
}

// Recover replays pending I/O intents in deterministic store order.
func (c CancellationCoordinator) Recover(ctx context.Context, query CancellationIntentQuery) ([]CancellationIntentSnapshot, []error, error) {
	if err := c.validate(ctx); err != nil {
		return nil, nil, err
	}
	intents, err := c.Store.RecoverCancellationIntents(context.WithoutCancel(ctx), query)
	if err != nil {
		return nil, nil, err
	}
	return intents, c.reconcileIntents(ctx, intents), nil
}

func (c CancellationCoordinator) reconcileIntents(ctx context.Context, intents []CancellationIntentSnapshot) []error {
	ordered := append([]CancellationIntentSnapshot(nil), intents...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	result := make([]error, 0)
	for _, intent := range ordered {
		if intent.Status != CancellationPending {
			continue
		}
		if err := c.reconcileIntent(ctx, intent); err != nil {
			result = append(result, fmt.Errorf("cancellation intent %s: %w", intent.ID, err))
		}
	}
	return result
}

func (c CancellationCoordinator) reconcileIntent(ctx context.Context, intent CancellationIntentSnapshot) error {
	switch intent.Kind {
	case CancellationRunningAttempt:
		attempt, err := c.Store.LoadAttempt(context.WithoutCancel(ctx), *intent.Attempt)
		if err != nil {
			return err
		}
		_, spec, err := stepkind.Resolve(c.Registry, attempt.Executor.Kind, attempt.Executor.Version)
		if err != nil {
			return err
		}
		if spec.Cancellation.Mode != stepkind.CancellationContext || c.Attempts == nil {
			return ErrCancellationUnsupported
		}
		if err := c.Attempts.CancelAttempt(ctx, attempt); err != nil {
			return err
		}
	case CancellationExternalOperation:
		attempt, err := c.Store.LoadAttempt(context.WithoutCancel(ctx), *intent.Attempt)
		if err != nil {
			return err
		}
		_, spec, err := stepkind.Resolve(c.Registry, attempt.Executor.Kind, attempt.Executor.Version)
		if err != nil {
			return err
		}
		if spec.Cancellation.Mode != stepkind.CancellationExplicit || c.External == nil {
			return ErrCancellationUnsupported
		}
		outcome, cancelErr := c.External.RequestCancel(ctx, *intent.Attempt)
		if cancelErr != nil && outcome.Operation.Status == stepkind.ObservationPending {
			return cancelErr
		}
		if outcome.Operation.Status == stepkind.ObservationPending {
			return nil
		}
	case CancellationChildRun:
		if c.Children == nil {
			return ErrCancellationUnsupported
		}
		link := ChildRunLink{ParentRunID: intent.RunID, ChildRunID: intent.ChildRunID, Policy: intent.ChildPolicy}
		links, err := c.Store.ListChildRuns(context.WithoutCancel(ctx), intent.RunID)
		if err != nil {
			return err
		}
		found := false
		for _, candidate := range links {
			if candidate.ChildRunID == intent.ChildRunID {
				link, found = candidate, true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: child run link", ErrNotFound)
		}
		if err := c.Children.RequestChildRunCancellation(ctx, link); err != nil {
			return err
		}
	}
	_, err := c.Store.ResolveCancellationIntent(context.WithoutCancel(ctx), ResolveCancellationIntentRequest{
		IntentID: intent.ID, ExpectedGeneration: intent.Generation, At: c.atOrAfter(intent.UpdatedAt),
	})
	return err
}

func (c CancellationCoordinator) validate(ctx context.Context) error {
	if ctx == nil || nilStateStore(c.Store) || nilStepKindRegistry(c.Registry) {
		return fmt.Errorf("%w: coordinator requires context, store, and registry", ErrInvalidCancellation)
	}
	return nil
}

func (c CancellationCoordinator) atOrAfter(floor time.Time) time.Time {
	at := time.Now().UTC()
	if c.Now == nil {
		if at.Before(floor) {
			return floor
		}
		return at
	}
	at = c.Now().UTC()
	if at.Before(floor) {
		return floor
	}
	return at
}
