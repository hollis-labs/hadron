package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventCompensationEligible  = "compensation.entry_eligible"
	EventCompensationFrozen    = "compensation.frozen"
	EventCompensationReady     = "compensation.entry_ready"
	EventCompensationFinished  = "compensation.entry_finished"
	EventCompensationCompleted = "compensation.completed"
	EventCompensationCanceled  = "compensation.canceled"

	CompensationHandlerChildRunIDInput      = "compensation.child.run_id"
	CompensationHandlerChildResolutionInput = "compensation.child.resolution"
)

// CompensationHandlerInputNamespace identifies one typed, durable evidence
// envelope made available to a compensation handler.
type CompensationHandlerInputNamespace string

const (
	CompensationHandlerOriginalInputs  CompensationHandlerInputNamespace = "original.inputs"
	CompensationHandlerOriginalOutputs CompensationHandlerInputNamespace = "original.outputs"
	CompensationHandlerOriginalError   CompensationHandlerInputNamespace = "original.error"
	CompensationHandlerReceipt         CompensationHandlerInputNamespace = "receipt"
)

// CompensationHandlerInputName returns the stable injective property name a
// handler InputSchema uses for an exact source ValueSet name.
func CompensationHandlerInputName(namespace CompensationHandlerInputNamespace, sourceName string) (string, error) {
	switch namespace {
	case CompensationHandlerOriginalInputs, CompensationHandlerOriginalOutputs, CompensationHandlerOriginalError, CompensationHandlerReceipt:
	default:
		return "", fmt.Errorf("%w: unknown handler input namespace %q", ErrInvalidCompensation, namespace)
	}
	if sourceName == "" || sourceName != strings.TrimSpace(sourceName) || !utf8.ValidString(sourceName) {
		return "", fmt.Errorf("%w: invalid handler source value name", ErrInvalidCompensation)
	}
	return fmt.Sprintf("compensation.%s.%d:%s", namespace, len([]byte(sourceName)), sourceName), nil
}

var (
	ErrInvalidCompensation  = errors.New("invalid compensation request")
	ErrCompensationPending  = errors.New("compensation is pending")
	ErrCompensationConflict = errors.New("compensation state conflict")
)

type CompensationLedgerStatus string

const (
	CompensationCollecting CompensationLedgerStatus = "collecting"
	CompensationFrozen     CompensationLedgerStatus = "frozen"
	CompensationRunning    CompensationLedgerStatus = "running"
	CompensationTerminal   CompensationLedgerStatus = "terminal"
)

func (s CompensationLedgerStatus) Valid() bool {
	return s == CompensationCollecting || s == CompensationFrozen || s == CompensationRunning || s == CompensationTerminal
}

type CompensationOutcome string

const (
	CompensationOutcomeSucceeded CompensationOutcome = "succeeded"
	CompensationOutcomePartial   CompensationOutcome = "partial"
	CompensationOutcomeFailed    CompensationOutcome = "failed"
	CompensationOutcomeCanceled  CompensationOutcome = "canceled"
)

func (o CompensationOutcome) Valid() bool {
	return o == "" || o == CompensationOutcomeSucceeded || o == CompensationOutcomePartial || o == CompensationOutcomeFailed || o == CompensationOutcomeCanceled
}

type CompensationCycle struct {
	Number       int                 `json:"number"`
	Attestation  string              `json:"attestation,omitempty"`
	CancelReason string              `json:"cancel_reason,omitempty"`
	Outcome      CompensationOutcome `json:"outcome,omitempty"`
	StartedAt    time.Time           `json:"started_at"`
	CompletedAt  time.Time           `json:"completed_at,omitempty"`
}

type CompensationEntryHistory struct {
	Cycle           int                         `json:"cycle"`
	Handler         NodeInvocationID            `json:"handler"`
	Status          CompensationEntryStatus     `json:"status"`
	ChildResolution CompensationChildResolution `json:"child_resolution,omitempty"`
	Outputs         *values.ValueSetRef         `json:"outputs,omitempty"`
	Failure         *Failure                    `json:"failure,omitempty"`
	CompletedAt     time.Time                   `json:"completed_at"`
}

type CompensationEntryStatus string

const (
	CompensationEligible  CompensationEntryStatus = "eligible"
	CompensationPending   CompensationEntryStatus = "pending"
	CompensationActive    CompensationEntryStatus = "active"
	CompensationSucceeded CompensationEntryStatus = "succeeded"
	CompensationPartial   CompensationEntryStatus = "partial"
	CompensationFailed    CompensationEntryStatus = "failed"
	CompensationCanceled  CompensationEntryStatus = "canceled"
)

func (s CompensationEntryStatus) Valid() bool {
	switch s {
	case CompensationEligible, CompensationPending, CompensationActive, CompensationSucceeded, CompensationPartial, CompensationFailed, CompensationCanceled:
		return true
	default:
		return false
	}
}

func (s CompensationEntryStatus) Terminal() bool {
	return s == CompensationSucceeded || s == CompensationPartial || s == CompensationFailed || s == CompensationCanceled
}

// CompensationChildResolution is the immutable outcome observed before a
// parent-side handler is admitted. NoLedger is distinct from a child ledger
// that ran and succeeded.
type CompensationChildResolution string

const (
	CompensationChildNoLedger  CompensationChildResolution = "no_ledger"
	CompensationChildSucceeded CompensationChildResolution = "succeeded"
	CompensationChildPartial   CompensationChildResolution = "partial"
	CompensationChildFailed    CompensationChildResolution = "failed"
	CompensationChildCanceled  CompensationChildResolution = "canceled"
)

func (r CompensationChildResolution) Valid() bool {
	switch r {
	case "", CompensationChildNoLedger, CompensationChildSucceeded, CompensationChildPartial, CompensationChildFailed, CompensationChildCanceled:
		return true
	default:
		return false
	}
}

// CompensationLedgerSnapshot is a separate run-owned rollback projection.
// OriginalStatus/Failure preserve the forward terminal intent and are never
// rewritten by handler outcomes.
type CompensationLedgerSnapshot struct {
	RunID           RunID                     `json:"run_id"`
	PlanDigest      string                    `json:"plan_digest"`
	Status          CompensationLedgerStatus  `json:"status"`
	Outcome         CompensationOutcome       `json:"outcome,omitempty"`
	Trigger         graph.CompensationTrigger `json:"trigger,omitempty"`
	OriginalStatus  RunStatus                 `json:"original_status,omitempty"`
	OriginalFailure *values.ValueSetRef       `json:"original_failure,omitempty"`
	CancelReason    string                    `json:"cancel_reason,omitempty"`
	Cycles          []CompensationCycle       `json:"cycles,omitempty"`
	Generation      uint64                    `json:"generation"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
	CompletedAt     time.Time                 `json:"completed_at,omitempty"`
}

func (s CompensationLedgerSnapshot) Validate() error {
	if err := validateOpaqueID("compensation run id", string(s.RunID)); err != nil {
		return err
	}
	if err := values.ValidateDigest(s.PlanDigest); err != nil {
		return fmt.Errorf("compensation plan digest: %w", err)
	}
	if !s.Status.Valid() || !s.Outcome.Valid() {
		return fmt.Errorf("invalid compensation ledger status or outcome")
	}
	if err := validateOptionalValueSetRef(s.OriginalFailure); err != nil {
		return err
	}
	if s.Status == CompensationCollecting {
		if s.Trigger != "" || s.OriginalStatus != "" || s.OriginalFailure != nil || len(s.Cycles) != 0 {
			return fmt.Errorf("collecting compensation ledger cannot carry frozen forward intent")
		}
	} else {
		if !s.Trigger.Valid() || !s.OriginalStatus.Terminal() || len(s.Cycles) == 0 {
			return fmt.Errorf("frozen compensation ledger requires trigger, original terminal status, and a cycle")
		}
		triggerMatches := s.Trigger == graph.CompensationManual && s.OriginalStatus == RunSucceeded ||
			s.Trigger == graph.CompensationOnFailure && (s.OriginalStatus == RunFailed || s.OriginalStatus == RunCrashed) ||
			s.Trigger == graph.CompensationOnCancel && s.OriginalStatus == RunCanceled ||
			s.Trigger == graph.CompensationOnTimeout && s.OriginalStatus == RunTimedOut
		if !triggerMatches {
			return fmt.Errorf("compensation trigger differs from original terminal status")
		}
		if s.OriginalStatus == RunSucceeded && s.OriginalFailure != nil || s.OriginalStatus != RunSucceeded && s.OriginalFailure == nil {
			return fmt.Errorf("compensation original failure differs from original status")
		}
	}
	if s.Status == CompensationTerminal {
		if s.Outcome == "" || s.CompletedAt.IsZero() {
			return fmt.Errorf("terminal compensation ledger requires outcome and completion time")
		}
	} else if s.Outcome != "" || !s.CompletedAt.IsZero() {
		return fmt.Errorf("nonterminal compensation ledger cannot carry terminal outcome")
	}
	if !s.CompletedAt.IsZero() && (s.CompletedAt.Before(s.CreatedAt) || s.CompletedAt.After(s.UpdatedAt)) {
		return fmt.Errorf("compensation ledger completion time is outside its durable lifetime")
	}
	var priorCompleted time.Time
	for index, cycle := range s.Cycles {
		if cycle.Number != index+1 || cycle.StartedAt.IsZero() || cycle.StartedAt.Before(s.CreatedAt) || cycle.StartedAt.After(s.UpdatedAt) ||
			!priorCompleted.IsZero() && cycle.StartedAt.Before(priorCompleted) {
			return fmt.Errorf("invalid compensation cycle history")
		}
		if cycle.Outcome == "" {
			if !cycle.CompletedAt.IsZero() || index != len(s.Cycles)-1 || s.Status == CompensationTerminal {
				return fmt.Errorf("only the current nonterminal compensation cycle may be open")
			}
		} else {
			if !cycle.Outcome.Valid() || cycle.CompletedAt.IsZero() || cycle.CompletedAt.Before(cycle.StartedAt) || cycle.CompletedAt.After(s.UpdatedAt) {
				return fmt.Errorf("invalid compensation cycle completion")
			}
			priorCompleted = cycle.CompletedAt
		}
		if cycle.Attestation != "" {
			if err := values.ValidateDigest(cycle.Attestation); err != nil {
				return fmt.Errorf("compensation cycle attestation: %w", err)
			}
		}
		if len(cycle.CancelReason) > 512 || cycle.CancelReason != strings.TrimSpace(cycle.CancelReason) || cycle.CancelReason != "" && cycle.Outcome != CompensationOutcomeCanceled {
			return fmt.Errorf("invalid compensation cycle cancel reason")
		}
	}
	if s.Status != CompensationCollecting && s.Status != CompensationTerminal && s.Cycles[len(s.Cycles)-1].Outcome != "" {
		return fmt.Errorf("nonterminal compensation ledger requires an open current cycle")
	}
	if s.Status == CompensationTerminal {
		last := s.Cycles[len(s.Cycles)-1]
		if last.Outcome != s.Outcome || !last.CompletedAt.Equal(s.CompletedAt) {
			return fmt.Errorf("terminal compensation ledger differs from its final cycle")
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

// CompensationEntrySnapshot binds one actual applied forward effect to one
// dormant handler invocation and immutable typed evidence.
type CompensationEntrySnapshot struct {
	ID              string                      `json:"id"`
	RunID           RunID                       `json:"run_id"`
	PlanDigest      string                      `json:"plan_digest"`
	Source          NodeInvocationID            `json:"source"`
	SourceAttempt   AttemptID                   `json:"source_attempt"`
	Handler         NodeInvocationID            `json:"handler"`
	Status          CompensationEntryStatus     `json:"status"`
	Operation       string                      `json:"operation"`
	EvidenceDigest  string                      `json:"evidence_digest"`
	OriginalInputs  *values.ValueSetRef         `json:"original_inputs,omitempty"`
	OriginalOutputs *values.ValueSetRef         `json:"original_outputs,omitempty"`
	OriginalError   *values.ValueSetRef         `json:"original_error,omitempty"`
	Receipt         values.ValueSetRef          `json:"receipt"`
	Prerequisites   []string                    `json:"prerequisites,omitempty"`
	HandlerOutputs  *values.ValueSetRef         `json:"handler_outputs,omitempty"`
	HandlerFailure  *Failure                    `json:"handler_failure,omitempty"`
	ChildRunID      RunID                       `json:"child_run_id,omitempty"`
	ChildResolution CompensationChildResolution `json:"child_resolution,omitempty"`
	History         []CompensationEntryHistory  `json:"history,omitempty"`
	Generation      uint64                      `json:"generation"`
	CreatedAt       time.Time                   `json:"created_at"`
	UpdatedAt       time.Time                   `json:"updated_at"`
	CompletedAt     time.Time                   `json:"completed_at,omitempty"`
}

func (s CompensationEntrySnapshot) Validate() error {
	if err := validateRequiredText("compensation entry id", s.ID); err != nil {
		return err
	}
	if err := validateOpaqueID("compensation entry run id", string(s.RunID)); err != nil {
		return err
	}
	if err := values.ValidateDigest(s.PlanDigest); err != nil {
		return err
	}
	if err := s.Source.Validate(); err != nil {
		return err
	}
	if err := s.SourceAttempt.Validate(); err != nil || s.SourceAttempt.Invocation != s.Source {
		return fmt.Errorf("compensation source attempt does not match source")
	}
	if err := s.Handler.Validate(); err != nil || s.Source.RunID != s.RunID || s.Handler.RunID != s.RunID || s.Handler == s.Source {
		return fmt.Errorf("compensation invocation identities are invalid")
	}
	if !s.Status.Valid() {
		return fmt.Errorf("invalid compensation entry status %q", s.Status)
	}
	if err := validateRequiredText("compensation operation", s.Operation); err != nil {
		return err
	}
	if err := values.ValidateDigest(s.EvidenceDigest); err != nil {
		return err
	}
	for _, ref := range []*values.ValueSetRef{s.OriginalInputs, s.OriginalOutputs, s.OriginalError, s.HandlerOutputs} {
		if err := validateOptionalValueSetRef(ref); err != nil {
			return err
		}
	}
	if err := s.Receipt.Validate(); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(s.Prerequisites))
	for _, id := range s.Prerequisites {
		if err := validateRequiredText("compensation prerequisite", id); err != nil {
			return err
		}
		if id == s.ID {
			return fmt.Errorf("compensation entry cannot depend on itself")
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("duplicate compensation prerequisite")
		}
		seen[id] = struct{}{}
	}
	if s.HandlerFailure != nil {
		if err := s.HandlerFailure.Validate(); err != nil {
			return err
		}
	}
	if !s.ChildResolution.Valid() || s.ChildRunID == "" && s.ChildResolution != "" {
		return fmt.Errorf("invalid compensation child resolution")
	}
	if err := ValidateCompensationChildRunID(s.ChildRunID); err != nil {
		return err
	}
	if s.ChildRunID != "" && s.ChildRunID == s.RunID {
		return fmt.Errorf("compensation child run cannot be the owning run")
	}
	if s.ChildRunID != "" && (s.Status == CompensationActive || s.Status.Terminal()) && s.ChildResolution == "" {
		return fmt.Errorf("activated child compensation entry requires an explicit child resolution")
	}
	if s.Status.Terminal() {
		if s.CompletedAt.IsZero() {
			return fmt.Errorf("terminal compensation entry requires completion time")
		}
		if s.CompletedAt.Before(s.CreatedAt) || s.CompletedAt.After(s.UpdatedAt) {
			return fmt.Errorf("compensation entry completion time is outside its durable lifetime")
		}
	} else if !s.CompletedAt.IsZero() || s.HandlerOutputs != nil || s.HandlerFailure != nil {
		return fmt.Errorf("nonterminal compensation entry carries terminal result")
	}
	if s.Status == CompensationSucceeded && s.HandlerFailure != nil || s.Status == CompensationFailed && s.HandlerFailure == nil {
		return fmt.Errorf("compensation entry failure differs from terminal status")
	}
	var priorCompleted time.Time
	for index, history := range s.History {
		if history.Cycle != index+1 || !history.Status.Terminal() || history.CompletedAt.IsZero() || history.CompletedAt.Before(s.CreatedAt) ||
			history.CompletedAt.After(s.UpdatedAt) || !priorCompleted.IsZero() && history.CompletedAt.Before(priorCompleted) ||
			history.Handler.RunID != s.RunID || history.Handler.Validate() != nil {
			return fmt.Errorf("invalid compensation entry history")
		}
		if err := validateOptionalValueSetRef(history.Outputs); err != nil {
			return err
		}
		if history.Failure != nil {
			if err := history.Failure.Validate(); err != nil {
				return err
			}
		}
		if !history.ChildResolution.Valid() || s.ChildRunID == "" && history.ChildResolution != "" {
			return fmt.Errorf("invalid compensation entry history child resolution")
		}
		if history.Status == CompensationSucceeded && history.Failure != nil || history.Status == CompensationFailed && history.Failure == nil {
			return fmt.Errorf("compensation entry history failure differs from status")
		}
		priorCompleted = history.CompletedAt
	}
	if !priorCompleted.IsZero() && !s.CompletedAt.IsZero() && s.CompletedAt.Before(priorCompleted) {
		return fmt.Errorf("compensation entry completion precedes prior retry history")
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

type CompensationEligibility struct {
	PlanDigest      string
	HandlerNodeID   string
	Evidence        stepkind.ReversibilityEvidence
	Receipt         stepkind.CompensationReceipt
	OriginalOutputs values.ValueSet
	OriginalError   values.ValueSet
	ChildRunID      RunID
}

type FinishCompensableAttemptRequest struct {
	Finish      FinishNodeAttemptRequest
	Eligibility CompensationEligibility
}

type FinishCompensableAttemptResult struct {
	Finish FinishNodeAttemptResult
	Ledger CompensationLedgerSnapshot
	Entry  CompensationEntrySnapshot
}

// ValidateCompensationTerminalEvidence binds the original typed error
// evidence to the exact terminal result of the forward effect attempt.
func ValidateCompensationTerminalEvidence(status NodeStatus, originalError values.ValueSet) error {
	switch status {
	case NodeSucceeded:
		if len(originalError) != 0 {
			return fmt.Errorf("%w: succeeded compensable attempt cannot carry original error evidence", ErrInvalidCompensation)
		}
	case NodeFailed, NodeCanceled, NodeTimedOut, NodeCrashed:
		if len(originalError) == 0 {
			return fmt.Errorf("%w: unsuccessful compensable attempt requires typed original error evidence", ErrInvalidCompensation)
		}
		if err := values.ValidatePersistableSet(originalError); err != nil {
			return fmt.Errorf("%w: original error evidence is not persistable: %w", ErrInvalidCompensation, err)
		}
	default:
		return fmt.Errorf("%w: compensable attempt status %q cannot establish eligibility", ErrInvalidCompensation, status)
	}
	return nil
}

type FreezeCompensationRequest struct {
	RunID                    RunID
	PlanDigest               string
	ExpectedRunGeneration    uint64
	ExpectedIntentGeneration uint64
	Trigger                  graph.CompensationTrigger
	OriginalStatus           RunStatus
	OriginalFailure          *values.ValueSetRef
	Dependencies             map[string][]string
	IdempotencyKey           string
	At                       time.Time
}

type FreezeCompensationResult struct {
	Outcome IdempotencyOutcome
	Ledger  CompensationLedgerSnapshot
	Entries []CompensationEntrySnapshot
}

// BeginManualCompensationRequest freezes a collecting ledger after the
// original run has already reached an immutable terminal state. Authorization
// is a non-secret durable policy attestation supplied by the host.
type BeginManualCompensationRequest struct {
	RunID                 RunID
	PlanDigest            string
	ExpectedRunGeneration uint64
	OriginalStatus        RunStatus
	Dependencies          map[string][]string
	IdempotencyKey        string
	Authorization         string
	At                    time.Time
}

type ActivateCompensationEntryRequest struct {
	RunID                    RunID
	EntryID                  string
	ExpectedLedgerGeneration uint64
	ExpectedEntryGeneration  uint64
	Inputs                   values.ValueSet
	ChildResolution          CompensationChildResolution
	At                       time.Time
}

type ActivateCompensationEntryResult struct {
	Ledger CompensationLedgerSnapshot
	Entry  CompensationEntrySnapshot
	Node   NodeInvocationSnapshot
}

type SealCompensationEntryRequest struct {
	RunID                    RunID
	EntryID                  string
	ExpectedLedgerGeneration uint64
	ExpectedEntryGeneration  uint64
	ExpectedNodeGeneration   uint64
	At                       time.Time
}

type SealCompensationEntryResult struct {
	Ledger CompensationLedgerSnapshot
	Entry  CompensationEntrySnapshot
}

// FailCompensationEntryRequest durably terminalizes a pending entry when its
// pinned handler inputs cannot be constructed. No handler invocation is
// materialized because there is no schema-valid input envelope to execute.
type FailCompensationEntryRequest struct {
	RunID                    RunID
	EntryID                  string
	ExpectedLedgerGeneration uint64
	ExpectedEntryGeneration  uint64
	Failure                  Failure
	ChildResolution          CompensationChildResolution
	At                       time.Time
}

type CancelCompensationRequest struct {
	RunID                    RunID
	ExpectedLedgerGeneration uint64
	IdempotencyKey           string
	Reason                   string
	At                       time.Time
}

type RetryCompensationRequest struct {
	RunID                    RunID
	ExpectedLedgerGeneration uint64
	IdempotencyKey           string
	Attestation              string
	At                       time.Time
}

// CompensationRequestDigest binds idempotency to semantic intent while
// excluding ephemeral CAS generations and wall-clock timestamps. Those remain
// mandatory first-apply fences but cannot make a lost-response replay differ.
func CompensationRequestDigest(input any) (string, error) {
	var semantic any
	switch request := input.(type) {
	case FreezeCompensationRequest:
		semantic = struct {
			RunID           RunID
			PlanDigest      string
			Trigger         graph.CompensationTrigger
			OriginalStatus  RunStatus
			OriginalFailure *values.ValueSetRef
			Dependencies    map[string][]string
			IdempotencyKey  string
		}{request.RunID, request.PlanDigest, request.Trigger, request.OriginalStatus, request.OriginalFailure, request.Dependencies, request.IdempotencyKey}
	case BeginManualCompensationRequest:
		semantic = struct {
			RunID                         RunID
			PlanDigest                    string
			OriginalStatus                RunStatus
			Dependencies                  map[string][]string
			IdempotencyKey, Authorization string
		}{request.RunID, request.PlanDigest, request.OriginalStatus, request.Dependencies, request.IdempotencyKey, request.Authorization}
	case CancelCompensationRequest:
		semantic = struct {
			RunID                  RunID
			IdempotencyKey, Reason string
		}{request.RunID, request.IdempotencyKey, request.Reason}
	case RetryCompensationRequest:
		semantic = struct {
			RunID                       RunID
			IdempotencyKey, Attestation string
		}{request.RunID, request.IdempotencyKey, request.Attestation}
	default:
		return "", fmt.Errorf("%w: unsupported idempotency request %T", ErrInvalidCompensation, input)
	}
	encoded, err := json.Marshal(semantic)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

// CompensationStore is an optional extension: StateStore and CompleteHost are
// unchanged. Implementations own the atomic finish+eligibility and
// ledger+handler materialization transactions.
type CompensationStore interface {
	FinishCompensableAttempt(context.Context, FinishCompensableAttemptRequest) (FinishCompensableAttemptResult, error)
	FreezeCompensation(context.Context, FreezeCompensationRequest) (FreezeCompensationResult, error)
	BeginManualCompensation(context.Context, BeginManualCompensationRequest) (FreezeCompensationResult, error)
	LoadCompensationLedger(context.Context, RunID) (CompensationLedgerSnapshot, error)
	ListCompensationEntries(context.Context, RunID) ([]CompensationEntrySnapshot, error)
	LoadCompensationEntryByHandler(context.Context, NodeInvocationID) (CompensationEntrySnapshot, error)
	ActivateCompensationEntry(context.Context, ActivateCompensationEntryRequest) (ActivateCompensationEntryResult, error)
	FailCompensationEntry(context.Context, FailCompensationEntryRequest) (SealCompensationEntryResult, error)
	SealCompensationEntry(context.Context, SealCompensationEntryRequest) (SealCompensationEntryResult, error)
	CancelCompensation(context.Context, CancelCompensationRequest) (CompensationLedgerSnapshot, error)
	RetryCompensation(context.Context, RetryCompensationRequest) (CompensationLedgerSnapshot, error)
	RecoverCompensation(context.Context, int) ([]CompensationLedgerSnapshot, error)
}

// CompensationProgressResult reports one bounded scheduling pass. Activated
// handlers are ordinary durable node invocations; sealed entries retain their
// result separately from the immutable forward outcome.
type CompensationProgressResult struct {
	Ledger    CompensationLedgerSnapshot        `json:"ledger"`
	Activated []ActivateCompensationEntryResult `json:"activated,omitempty"`
	Sealed    []SealCompensationEntryResult     `json:"sealed,omitempty"`
}

// CompensationCoordinator reconstructs rollback scheduling solely from the
// durable ledger. It never executes handlers itself and therefore preserves
// the ordinary claim, retry, policy, timeout, and executor path.
type CompensationCoordinator struct {
	Store        StateStore
	Compensation CompensationStore
	Plans        RecoveryPlanSource
}

// Progress seals terminal handlers, then activates every reverse-dependency
// eligible entry. Independent entries become Ready in the same pass.
func (c CompensationCoordinator) Progress(ctx context.Context, runID RunID, at time.Time) (CompensationProgressResult, error) {
	return c.progress(ctx, runID, at, make(map[RunID]struct{}))
}

// progress carries one recursion path so a parent waiting on a child-owned
// ledger can advance that ledger without allowing a corrupt child-link cycle
// to recurse forever. The public store also rejects self-links, but the path
// fence remains required for multi-run cycles in independently imported data.
func (c CompensationCoordinator) progress(ctx context.Context, runID RunID, at time.Time, path map[RunID]struct{}) (CompensationProgressResult, error) {
	if ctx == nil || nilStateStore(c.Store) || c.Compensation == nil || at.IsZero() {
		return CompensationProgressResult{}, fmt.Errorf("%w: compensation progress requires context, stores, and time", ErrInvalidCompensation)
	}
	if _, exists := path[runID]; exists {
		return CompensationProgressResult{}, fmt.Errorf("%w: compensation child ledger cycle includes run %s", ErrCompensationConflict, runID)
	}
	path[runID] = struct{}{}
	defer delete(path, runID)
	ledger, err := c.Compensation.LoadCompensationLedger(ctx, runID)
	if err != nil {
		return CompensationProgressResult{}, err
	}
	result := CompensationProgressResult{Ledger: ledger}
	if ledger.Status == CompensationTerminal {
		return result, nil
	}
	if c.Plans == nil {
		return result, fmt.Errorf("%w: compensation progress requires exact plan source", ErrInvalidCompensation)
	}
	run, loadErr := c.Store.LoadRun(ctx, runID)
	if loadErr != nil {
		return result, loadErr
	}
	plan, planErr := c.Plans.LoadRecoveryPlan(ctx, run)
	if planErr != nil {
		return result, planErr
	}
	if validationErr := plan.Validate(); validationErr != nil {
		return result, fmt.Errorf("%w: invalid pinned compensation plan: %w", ErrCompensationConflict, validationErr)
	}
	if plan.Ref != run.Plan || plan.Ref.Digest != ledger.PlanDigest || plan.Plan.Digest != ledger.PlanDigest {
		return result, fmt.Errorf("%w: compensation plan source differs from ledger", ErrCompensationConflict)
	}
	handlerGraph := &plan.Plan.Graph
	entries, err := c.Compensation.ListCompensationEntries(ctx, runID)
	if err != nil {
		return result, err
	}
	if err := validateCompensationEntriesForPlan(ledger, entries, plan.Plan.Graph); err != nil {
		return result, err
	}
	for _, entry := range entries {
		if entry.Status != CompensationActive {
			continue
		}
		handler, loadErr := c.Store.LoadNodeInvocation(ctx, entry.Handler)
		if loadErr != nil {
			return result, loadErr
		}
		if !handler.Status.Terminal() {
			continue
		}
		sealed, sealErr := c.Compensation.SealCompensationEntry(context.WithoutCancel(ctx), SealCompensationEntryRequest{
			RunID: runID, EntryID: entry.ID, ExpectedLedgerGeneration: result.Ledger.Generation,
			ExpectedEntryGeneration: entry.Generation, ExpectedNodeGeneration: handler.Generation,
			At: maxRecoveryTime(maxRecoveryTime(maxRecoveryTime(at, handler.UpdatedAt), entry.UpdatedAt), result.Ledger.UpdatedAt),
		})
		if errors.Is(sealErr, ErrCASMismatch) || errors.Is(sealErr, ErrCompensationPending) {
			return result, sealErr
		}
		if sealErr != nil {
			return result, sealErr
		}
		result.Ledger = sealed.Ledger
		result.Sealed = append(result.Sealed, sealed)
	}
	if result.Ledger.Status == CompensationTerminal {
		return result, nil
	}
	entries, err = c.Compensation.ListCompensationEntries(ctx, runID)
	if err != nil {
		return result, err
	}
	byID := make(map[string]CompensationEntrySnapshot, len(entries))
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	for _, entry := range entries {
		if entry.Status != CompensationPending {
			continue
		}
		ready := true
		for _, prerequisite := range entry.Prerequisites {
			candidate, exists := byID[prerequisite]
			if !exists {
				return result, fmt.Errorf("%w: missing compensation prerequisite %q", ErrCompensationConflict, prerequisite)
			}
			ready = ready && candidate.Status.Terminal()
		}
		if !ready {
			continue
		}
		var childResolution CompensationChildResolution
		var childResolutionAt time.Time
		if entry.ChildRunID != "" {
			childLedger, childErr := c.Compensation.LoadCompensationLedger(ctx, entry.ChildRunID)
			if errors.Is(childErr, ErrNotFound) {
				child, runErr := c.Store.LoadRun(ctx, entry.ChildRunID)
				if runErr != nil {
					return result, runErr
				}
				if !child.Status.Terminal() {
					continue
				}
				childResolution = CompensationChildNoLedger
				childResolutionAt = child.UpdatedAt
			} else if childErr != nil {
				return result, childErr
			} else if childLedger.Status == CompensationCollecting {
				child, runErr := c.Store.LoadRun(ctx, entry.ChildRunID)
				if runErr != nil {
					return result, runErr
				}
				if !child.Status.Terminal() {
					continue
				}
				if child.Status != RunSucceeded {
					switch child.Status {
					case RunCanceled:
						childResolution = CompensationChildCanceled
					case RunFailed, RunTimedOut, RunCrashed:
						childResolution = CompensationChildFailed
					default:
						return result, fmt.Errorf("%w: terminal child %s has unsupported status %s", ErrCompensationConflict, entry.ChildRunID, child.Status)
					}
					childResolutionAt = maxRecoveryTime(child.UpdatedAt, childLedger.UpdatedAt)
				} else {
					plan, planErr := c.Plans.LoadRecoveryPlan(ctx, child)
					if planErr != nil {
						return result, planErr
					}
					manual := false
					if plan.Plan.Graph.Compensation != nil {
						for _, trigger := range plan.Plan.Graph.Compensation.Triggers {
							manual = manual || trigger == graph.CompensationManual
						}
					}
					if !manual {
						// The child cannot be rolled back through the parent trigger.
						// Preserve best-effort parent convergence as a durable child
						// failure instead of poisoning every recovery pass.
						childResolution = CompensationChildFailed
						childResolutionAt = maxRecoveryTime(child.UpdatedAt, childLedger.UpdatedAt)
					} else {
						key := "parent-compensation:" + entry.ID
						authorization := values.SHA256Digest([]byte(strings.Join([]string{"parent-compensation", string(runID), entry.ID, string(child.ID), child.Plan.Digest, fmt.Sprint(child.Generation), string(child.Status), key}, "\x00")))
						begun, beginErr := c.Compensation.BeginManualCompensation(context.WithoutCancel(ctx), BeginManualCompensationRequest{
							RunID: child.ID, PlanDigest: child.Plan.Digest, ExpectedRunGeneration: child.Generation,
							OriginalStatus: child.Status, Dependencies: compensationDependencies(plan.Plan.Graph),
							IdempotencyKey: key, Authorization: authorization,
							At: maxRecoveryTime(at, child.UpdatedAt),
						})
						if beginErr != nil {
							return result, beginErr
						}
						childLedger = begun.Ledger
					}
				}
			}
			if childLedger.Status == CompensationFrozen || childLedger.Status == CompensationRunning {
				childProgress, progressErr := c.progress(context.WithoutCancel(ctx), entry.ChildRunID, maxRecoveryTime(at, childLedger.UpdatedAt), path)
				if progressErr != nil {
					return result, progressErr
				}
				childLedger = childProgress.Ledger
			}
			if childLedger.Status != CompensationTerminal && childResolution == "" {
				continue
			}
			if childLedger.Status == CompensationTerminal {
				childResolution, err = compensationChildResolution(childLedger.Outcome)
				if err != nil {
					return result, err
				}
				childResolutionAt = childLedger.UpdatedAt
			}
		}
		entryForInputs := entry
		entryForInputs.ChildResolution = childResolution
		inputs, inputErr := c.handlerInputs(ctx, entryForInputs, handlerGraph)
		if inputErr != nil {
			failure := Failure{
				Code:      "compensation_handler_binding_invalid",
				Message:   "compensation handler input binding failed",
				Retryable: false,
				Details: map[string]string{
					"stage":                "binding",
					"retry_classification": string(stepkind.RetryPermanent),
				},
			}
			failed, failErr := c.Compensation.FailCompensationEntry(context.WithoutCancel(ctx), FailCompensationEntryRequest{
				RunID: runID, EntryID: entry.ID, ExpectedLedgerGeneration: result.Ledger.Generation,
				ExpectedEntryGeneration: entry.Generation, Failure: failure, ChildResolution: childResolution,
				At: maxRecoveryTime(maxRecoveryTime(maxRecoveryTime(at, entry.UpdatedAt), result.Ledger.UpdatedAt), childResolutionAt),
			})
			if failErr != nil {
				return result, errors.Join(inputErr, failErr)
			}
			result.Ledger = failed.Ledger
			result.Sealed = append(result.Sealed, failed)
			byID[entry.ID] = failed.Entry
			continue
		}
		activated, activateErr := c.Compensation.ActivateCompensationEntry(context.WithoutCancel(ctx), ActivateCompensationEntryRequest{
			RunID: runID, EntryID: entry.ID, ExpectedLedgerGeneration: result.Ledger.Generation,
			ExpectedEntryGeneration: entry.Generation, Inputs: inputs, ChildResolution: childResolution,
			At: maxRecoveryTime(maxRecoveryTime(maxRecoveryTime(at, entry.UpdatedAt), result.Ledger.UpdatedAt), childResolutionAt),
		})
		if activateErr != nil {
			return result, activateErr
		}
		result.Ledger = activated.Ledger
		result.Activated = append(result.Activated, activated)
	}
	return result, nil
}

func validateCompensationEntriesForPlan(ledger CompensationLedgerSnapshot, entries []CompensationEntrySnapshot, workflow graph.Graph) error {
	if err := ValidateCompensationEntryDependencies(entries); err != nil {
		return fmt.Errorf("%w: compensation entry dependency set: %w", ErrCompensationConflict, err)
	}
	for _, entry := range entries {
		if entry.RunID != ledger.RunID || entry.PlanDigest != ledger.PlanDigest {
			return fmt.Errorf("%w: compensation entry %q differs from ledger identity", ErrCompensationConflict, entry.ID)
		}
		source, sourceExists := graphNode(workflow, entry.Source.NodeID)
		handler, handlerExists := graphNode(workflow, entry.Handler.NodeID)
		if !sourceExists || !handlerExists || source.Compensation == nil || graph.NormalizeID(source.Compensation.Handler) != graph.NormalizeID(handler.ID) || graph.NormalizeID(handler.ID) != graph.NormalizeID(entry.Handler.NodeID) {
			return fmt.Errorf("%w: compensation entry %q is not bound to its exact pinned source and handler", ErrCompensationConflict, entry.ID)
		}
	}
	return nil
}

func compensationChildResolution(outcome CompensationOutcome) (CompensationChildResolution, error) {
	switch outcome {
	case CompensationOutcomeSucceeded:
		return CompensationChildSucceeded, nil
	case CompensationOutcomePartial:
		return CompensationChildPartial, nil
	case CompensationOutcomeFailed:
		return CompensationChildFailed, nil
	case CompensationOutcomeCanceled:
		return CompensationChildCanceled, nil
	default:
		return "", fmt.Errorf("%w: child compensation has no terminal outcome", ErrCompensationConflict)
	}
}

func (c CompensationCoordinator) handlerInputs(ctx context.Context, entry CompensationEntrySnapshot, workflow *graph.Graph) (values.ValueSet, error) {
	result := make(values.ValueSet)
	// Source names remain byte-for-byte intact after a length-delimited
	// namespace. This encoding is injective and cannot collide with the
	// reserved compensation.child.* metadata keys.
	add := func(namespace CompensationHandlerInputNamespace, ref *values.ValueSetRef) error {
		if ref == nil {
			return nil
		}
		set, err := c.Store.LoadValues(ctx, *ref)
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(set))
		for name := range set {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		for _, name := range keys {
			target, err := CompensationHandlerInputName(namespace, name)
			if err != nil {
				return err
			}
			if _, exists := result[target]; exists {
				return fmt.Errorf("%w: compensation handler input collision %q", ErrCompensationConflict, target)
			}
			result[target] = set[name]
		}
		return nil
	}
	if err := add(CompensationHandlerOriginalInputs, entry.OriginalInputs); err != nil {
		return nil, err
	}
	if err := add(CompensationHandlerOriginalOutputs, entry.OriginalOutputs); err != nil {
		return nil, err
	}
	if err := add(CompensationHandlerOriginalError, entry.OriginalError); err != nil {
		return nil, err
	}
	receipt := entry.Receipt
	if err := add(CompensationHandlerReceipt, &receipt); err != nil {
		return nil, err
	}
	if entry.ChildRunID != "" {
		metadata := values.Metadata{Producer: values.Producer{Kind: "compensation", Reference: entry.ID}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
		childRun, err := values.NewInline(string(entry.ChildRunID), metadata)
		if err != nil {
			return nil, err
		}
		childResolution, err := values.NewInline(string(entry.ChildResolution), metadata)
		if err != nil {
			return nil, err
		}
		result[CompensationHandlerChildRunIDInput] = childRun
		result[CompensationHandlerChildResolutionInput] = childResolution
	}
	if err := values.ValidatePersistableSet(result); err != nil {
		return nil, err
	}
	if workflow == nil {
		return result, nil
	}
	var handler *graph.Node
	for index := range workflow.Nodes {
		if graph.NormalizeID(workflow.Nodes[index].ID) == graph.NormalizeID(entry.Handler.NodeID) {
			handler = &workflow.Nodes[index]
			break
		}
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: exact plan does not contain compensation handler %q", ErrCompensationConflict, entry.Handler.NodeID)
	}
	if len(handler.InputBindings) == 0 {
		return result, nil
	}
	bound := make(values.ValueSet, len(handler.InputBindings))
	engine := values.NewExpressionEngine()
	for _, name := range sortedCompensationBindingNames(handler.InputBindings) {
		metadata := values.Metadata{Producer: values.Producer{Kind: "compensation-binding", Reference: entry.ID, Output: name}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
		value, bindErr := engine.EvaluateBinding(handler.InputBindings[name], values.ExpressionContext{Compensation: result}, values.ExpressionOptions{VisibleSteps: []string{}}, metadata)
		if bindErr != nil {
			return nil, fmt.Errorf("%w: compensation handler input %q: %w", ErrCompensationConflict, name, bindErr)
		}
		bound[name] = value
	}
	if err := values.ValidatePersistableSet(bound); err != nil {
		return nil, fmt.Errorf("%w: compensation handler inputs are not persistable: %w", ErrCompensationConflict, err)
	}
	return bound, nil
}

func sortedCompensationBindingNames(bindings map[string]graph.Binding) []string {
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func compensationEntryID(attempt AttemptID) string {
	material := fmt.Sprintf("%s\x00%s\x00%s\x00%d", attempt.Invocation.RunID, attempt.Invocation.NodeID, attempt.Invocation.Iteration, attempt.Number)
	return "ce-" + strings.TrimPrefix(values.SHA256Digest([]byte(material)), "sha256:")[:32]
}

func CompensationEntryID(attempt AttemptID) string { return compensationEntryID(attempt) }

func CompensationEvidenceDigest(evidence stepkind.ReversibilityEvidence) (string, error) {
	if strings.TrimSpace(evidence.Operation) == "" || evidence.Operation != strings.TrimSpace(evidence.Operation) {
		return "", fmt.Errorf("%w: reversibility operation is required", ErrInvalidCompensation)
	}
	if err := values.ValidateSchema(evidence.ReceiptSchema); err != nil {
		return "", fmt.Errorf("%w: receipt schema: %w", ErrInvalidCompensation, err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

func compensationHandlerID(runID RunID, nodeID, entryID string) NodeInvocationID {
	return NodeInvocationID{RunID: runID, NodeID: nodeID, Iteration: "comp:" + entryID}
}

func CompensationHandlerID(runID RunID, nodeID, entryID string) NodeInvocationID {
	return compensationHandlerID(runID, nodeID, entryID)
}

func ValidateCompensationHandlerNodeID(nodeID string) error { return graph.ValidateID(nodeID) }

func ValidateCompensationChildRunID(runID RunID) error {
	if runID == "" {
		return nil
	}
	return validateOpaqueID("compensation child run id", string(runID))
}

func CompensationHandlers(workflow graph.Graph) map[string]struct{} {
	result := make(map[string]struct{})
	for _, node := range workflow.Nodes {
		if node.Compensation != nil {
			result[graph.NormalizeID(node.Compensation.Handler)] = struct{}{}
		}
	}
	return result
}

// CompensationDependencies exposes the deterministic reverse dependency
// projection to production hosts that initiate an authorized manual rollback.
func CompensationDependencies(workflow graph.Graph) map[string][]string {
	return compensationDependencies(workflow)
}

// ValidateCompensationEntryDependencies rejects malformed durable prerequisite
// graphs supplied directly through CompensationStore, independently of graph
// compiler validation.
func ValidateCompensationEntryDependencies(entries []CompensationEntrySnapshot) error {
	byID := make(map[string]CompensationEntrySnapshot, len(entries))
	for _, entry := range entries {
		if err := entry.Validate(); err != nil {
			return err
		}
		if _, duplicate := byID[entry.ID]; duplicate {
			return fmt.Errorf("duplicate compensation entry %q", entry.ID)
		}
		byID[entry.ID] = entry
	}
	visiting, visited := make(map[string]bool), make(map[string]bool)
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return fmt.Errorf("compensation prerequisite cycle at %q", id)
		}
		if visited[id] {
			return nil
		}
		entry, exists := byID[id]
		if !exists {
			return fmt.Errorf("missing compensation prerequisite %q", id)
		}
		visiting[id] = true
		for _, prerequisite := range entry.Prerequisites {
			if _, exists := byID[prerequisite]; !exists {
				return fmt.Errorf("missing compensation prerequisite %q", prerequisite)
			}
			if err := visit(prerequisite); err != nil {
				return err
			}
		}
		visiting[id], visited[id] = false, true
		return nil
	}
	for id := range byID {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func isCompensationHandler(workflow graph.Graph, nodeID string) bool {
	_, ok := CompensationHandlers(workflow)[graph.NormalizeID(nodeID)]
	return ok
}

func compensationDependencies(workflow graph.Graph) map[string][]string {
	adjacent := make(map[string][]string)
	for _, node := range workflow.Nodes {
		for _, need := range node.Needs {
			adjacent[graph.NormalizeID(need.Node)] = append(adjacent[graph.NormalizeID(need.Node)], graph.NormalizeID(node.ID))
		}
	}
	for _, edge := range workflow.Edges {
		adjacent[graph.NormalizeID(edge.From)] = append(adjacent[graph.NormalizeID(edge.From)], graph.NormalizeID(edge.To))
	}
	result := make(map[string][]string)
	for _, source := range workflow.Nodes {
		if source.Compensation == nil {
			continue
		}
		seen := make(map[string]struct{})
		queue := append([]string(nil), adjacent[graph.NormalizeID(source.ID)]...)
		for len(queue) != 0 {
			current := queue[0]
			queue = queue[1:]
			if _, ok := seen[current]; ok {
				continue
			}
			seen[current] = struct{}{}
			queue = append(queue, adjacent[current]...)
		}
		for _, candidate := range workflow.Nodes {
			if candidate.Compensation != nil {
				if _, downstream := seen[graph.NormalizeID(candidate.ID)]; downstream {
					result[source.ID] = append(result[source.ID], candidate.ID)
				}
			}
		}
		sort.Strings(result[source.ID])
	}
	return result
}
