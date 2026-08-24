package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	ErrNotFound            = errors.New("workflow state not found")
	ErrAlreadyExists       = errors.New("workflow state already exists")
	ErrInvalidRecord       = errors.New("invalid workflow state record")
	ErrCASMismatch         = errors.New("workflow state compare-and-swap mismatch")
	ErrIdempotencyConflict = errors.New("workflow idempotency conflict")
	ErrClaimMismatch       = errors.New("workflow claim token or generation mismatch")
	ErrLeaseExpired        = errors.New("workflow claim lease expired")
)

// CASMismatchError reports the expected and current record generation.
type CASMismatchError struct {
	Resource string
	Expected uint64
	Actual   uint64
}

func (e *CASMismatchError) Error() string {
	return fmt.Sprintf("%s: %s: expected generation %d, actual %d", ErrCASMismatch, e.Resource, e.Expected, e.Actual)
}

// Unwrap supports errors.Is(err, ErrCASMismatch).
func (e *CASMismatchError) Unwrap() error { return ErrCASMismatch }

// IdempotencyConflictError reports reuse of a key for a different request.
type IdempotencyConflictError struct {
	Operation string
	Key       string
}

func (e *IdempotencyConflictError) Error() string {
	return fmt.Sprintf("%s: %s key %q was already used for a different request", ErrIdempotencyConflict, e.Operation, e.Key)
}

// Unwrap supports errors.Is(err, ErrIdempotencyConflict).
func (e *IdempotencyConflictError) Unwrap() error { return ErrIdempotencyConflict }

// IdempotencyOutcome distinguishes a new mutation from an exact replay.
type IdempotencyOutcome string

const (
	IdempotencyApplied  IdempotencyOutcome = "applied"
	IdempotencyReplayed IdempotencyOutcome = "replayed"
)

// CreateRunRequest starts persistence for one already validated pending run.
// It deliberately does not embed the W02-T03 BoundRun contract.
type CreateRunRequest struct {
	ID                  RunID
	Plan                PlanRef
	Status              RunStatus
	Inputs              *values.ValueSetRef
	StartIdempotencyKey string
	CreatedAt           time.Time
}

// SaveRunRequest stores non-lifecycle run fields when ExpectedGeneration
// matches. Status changes must use TransitionRun.
type SaveRunRequest struct {
	Snapshot           RunSnapshot
	ExpectedGeneration uint64
}

// CreateNodeInvocationRequest persists a new pending invocation snapshot. The
// store assigns generation one; attempt and claim state must initially be empty.
type CreateNodeInvocationRequest struct {
	Snapshot NodeInvocationSnapshot
}

// SaveNodeInvocationRequest stores non-lifecycle invocation fields under record
// CAS. Status, attempt, and claim fields have dedicated atomic methods.
type SaveNodeInvocationRequest struct {
	Snapshot           NodeInvocationSnapshot
	ExpectedGeneration uint64
}

// SaveValuesRequest persists a defensively copied value set for an owner.
type SaveValuesRequest struct {
	Owner  ValueOwner
	Values values.ValueSet
}

// AppendEventRequest supplies an event without a store-assigned sequence.
type AppendEventRequest struct {
	RunID      RunID
	Invocation *NodeInvocationID
	Attempt    *AttemptID
	Type       string
	OccurredAt time.Time
	Attributes map[string]string
	Values     *values.ValueSetRef
	Redaction  values.RedactionClass
	Retention  values.RetentionClass
}

// EventQuery reads immutable events after an exclusive per-run sequence.
type EventQuery struct {
	RunID         RunID
	AfterSequence uint64
	Limit         int
}

// ClaimNodeRequest atomically attempts to acquire a ready node only when
// ExpectedClaimGeneration matches. A matching request against a non-ready node
// or a live lease durably returns Acquired false; an expired ready lease may be
// replaced. Now is caller-supplied for deterministic storage adapters and
// tests; LeaseUntil must be later than Now.
type ClaimNodeRequest struct {
	InvocationID            NodeInvocationID
	ExpectedClaimGeneration uint64
	Owner                   string
	Token                   string
	IdempotencyKey          string
	Now                     time.Time
	LeaseUntil              time.Time
}

// ClaimResult never exposes another claimant's lease.
type ClaimResult struct {
	Acquired bool
	Replayed bool
	Lease    *ClaimLease
}

// RenewLeaseRequest extends the matching, unexpired lease without changing its
// claim generation.
type RenewLeaseRequest struct {
	InvocationID NodeInvocationID
	Owner        string
	Token        string
	Generation   uint64
	Now          time.Time
	LeaseUntil   time.Time
}

// ReleaseClaimRequest clears the matching lease at Now. The next claimant must
// use the released claim generation as its expected generation.
type ReleaseClaimRequest struct {
	InvocationID NodeInvocationID
	Owner        string
	Token        string
	Generation   uint64
	Now          time.Time
}

// ExternalActivationRequest records one externally initiated run request.
type ExternalActivationRequest struct {
	ActivationID   string
	IdempotencyKey string
	RequestedRunID RunID
	Plan           PlanRef
	Inputs         *values.ValueSetRef
	OccurredAt     time.Time
}

// ExternalActivationSnapshot is the immutable accepted activation request.
type ExternalActivationSnapshot struct {
	ActivationID   string              `json:"activation_id"`
	IdempotencyKey string              `json:"idempotency_key"`
	RequestedRunID RunID               `json:"requested_run_id"`
	Plan           PlanRef             `json:"plan"`
	Inputs         *values.ValueSetRef `json:"inputs,omitempty"`
	OccurredAt     time.Time           `json:"occurred_at"`
}

// RecoveryQuery selects deterministic recovery candidates. Limit applies
// independently to each result category; zero means unlimited.
type RecoveryQuery struct {
	RunID RunID
	Now   time.Time
	Limit int
}

// RecoverySnapshot contains only persisted candidates; category membership may
// overlap. Ready is a storage candidate set whose order is not a scheduling
// contract. Recovery does not schedule or transition records.
type RecoverySnapshot struct {
	ActiveRuns    []RunSnapshot
	Ready         []NodeInvocationSnapshot
	Running       []NodeInvocationSnapshot
	Waiting       []NodeInvocationSnapshot
	Leased        []NodeInvocationSnapshot
	ExpiredLeases []NodeInvocationSnapshot
	DueTimers     []WaitSnapshot
}

// StateStore is the high-level, extraction-ready runtime persistence contract.
// Implementations must defensively isolate mutable envelopes and preserve CAS,
// idempotency, append-only event, and recovery-query semantics.
type StateStore interface {
	ExternalOperationStore
	RetryStore
	FanOutStore
	CancellationStore

	CreateRun(context.Context, CreateRunRequest) (RunSnapshot, IdempotencyOutcome, error)
	LoadRun(context.Context, RunID) (RunSnapshot, error)
	SaveRun(context.Context, SaveRunRequest) (RunSnapshot, error)
	TransitionRun(context.Context, RunTransitionRequest) (RunTransitionResult, error)

	CreateNodeInvocation(context.Context, CreateNodeInvocationRequest) (NodeInvocationSnapshot, error)
	LoadNodeInvocation(context.Context, NodeInvocationID) (NodeInvocationSnapshot, error)
	SaveNodeInvocation(context.Context, SaveNodeInvocationRequest) (NodeInvocationSnapshot, error)
	TransitionNode(context.Context, NodeTransitionRequest) (NodeTransitionResult, error)

	StartNodeAttempt(context.Context, StartNodeAttemptRequest) (StartNodeAttemptResult, error)
	FinishNodeAttempt(context.Context, FinishNodeAttemptRequest) (FinishNodeAttemptResult, error)
	LoadAttempt(context.Context, AttemptID) (AttemptSnapshot, error)
	ListAttempts(context.Context, NodeInvocationID) ([]AttemptSnapshot, error)

	LoadWait(context.Context, WaitID) (WaitSnapshot, error)

	SaveValues(context.Context, SaveValuesRequest) (values.ValueSetRef, error)
	LoadValues(context.Context, values.ValueSetRef) (values.ValueSet, error)

	RecordPlan(context.Context, PlanRef) error
	LoadPlan(context.Context, string) (PlanRef, error)

	AppendEvent(context.Context, AppendEventRequest) (Event, error)
	ListEvents(context.Context, EventQuery) ([]Event, error)

	ClaimNode(context.Context, ClaimNodeRequest) (ClaimResult, error)
	RenewNodeLease(context.Context, RenewLeaseRequest) (ClaimLease, error)
	ReleaseNodeClaim(context.Context, ReleaseClaimRequest) error

	PutCacheEntry(context.Context, CacheEntry) error
	GetCacheEntry(context.Context, string, time.Time) (CacheEntry, bool, error)
	PutPinnedValue(context.Context, PinnedValue) error
	GetPinnedValue(context.Context, string, time.Time) (PinnedValue, bool, error)
	ListPinnedValues(context.Context, time.Time) ([]PinnedValue, error)

	RecordExternalActivation(context.Context, ExternalActivationRequest) (ExternalActivationSnapshot, IdempotencyOutcome, error)
	Recovery(context.Context, RecoveryQuery) (RecoverySnapshot, error)
}
