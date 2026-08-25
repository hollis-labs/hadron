package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const MaximumRunQueryLimit = 1000

var (
	ErrSignalNotOpen   = errors.New("named workflow signal is not open")
	ErrSignalAmbiguous = errors.New("named workflow signal is ambiguous")
)

// SignalSelector identifies one named signal wait without treating the
// caller-visible run or wait ID as authorization.
type SignalSelector struct {
	RunID       RunID  `json:"run_id"`
	Name        string `json:"name"`
	Correlation string `json:"correlation"`
}

func (s SignalSelector) Validate() error {
	if err := validateOpaqueID("signal run id", string(s.RunID)); err != nil {
		return err
	}
	if err := validateRequiredText("signal name", s.Name); err != nil {
		return err
	}
	return validateRequiredText("signal correlation", s.Correlation)
}

// RunStateQuery is a strictly bounded, read-only operational projection.
// Limit applies independently to node, attempt, wait, and event histories.
type RunStateQuery struct {
	RunID         RunID  `json:"run_id"`
	AfterSequence uint64 `json:"after_sequence,omitempty"`
	Limit         int    `json:"limit"`
}

func (q RunStateQuery) Validate() error {
	if err := validateOpaqueID("query run id", string(q.RunID)); err != nil {
		return err
	}
	if q.Limit < 1 || q.Limit > MaximumRunQueryLimit {
		return fmt.Errorf("run query limit must be between 1 and %d", MaximumRunQueryLimit)
	}
	return nil
}

type RunStateView struct {
	Run      RunSnapshot              `json:"run"`
	Nodes    []NodeInvocationSnapshot `json:"nodes,omitempty"`
	Attempts []AttemptSnapshot        `json:"attempts,omitempty"`
	Waits    []WaitSnapshot           `json:"waits,omitempty"`
	Events   []Event                  `json:"events,omitempty"`
}

// RunUpdateStatus is the durable tracked-operation lifecycle around the
// canonical wait-resume mutation.
type RunUpdateStatus string

const (
	RunUpdatePending RunUpdateStatus = "pending"
	RunUpdateApplied RunUpdateStatus = "applied"
	RunUpdateClosed  RunUpdateStatus = "closed"
)

func (s RunUpdateStatus) Valid() bool {
	return s == RunUpdatePending || s == RunUpdateApplied || s == RunUpdateClosed
}

type BeginRunUpdateRequest struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	Selector       SignalSelector         `json:"selector"`
	WaitID         WaitID                 `json:"wait_id"`
	Responder      workflowwait.Responder `json:"responder"`
	Payload        values.Value           `json:"payload"`
	ReceivedAt     time.Time              `json:"received_at"`
}

func (r BeginRunUpdateRequest) Validate() error {
	if err := validateRequiredText("run update idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if err := r.Selector.Validate(); err != nil {
		return err
	}
	if err := (WaitRef{ID: r.WaitID}).Validate(); err != nil {
		return err
	}
	if err := r.Responder.Validate(); err != nil {
		return err
	}
	if err := r.Payload.Validate(); err != nil {
		return err
	}
	if r.ReceivedAt.IsZero() {
		return fmt.Errorf("run update received_at is required")
	}
	return nil
}

// RunUpdateReceipt is intentionally compact: detailed node/attempt/event
// history remains available only through the bounded query path.
type RunUpdateReceipt struct {
	Outcome    ResumeOutcome       `json:"outcome"`
	WaitID     WaitID              `json:"wait_id"`
	WaitStatus WaitStatus          `json:"wait_status"`
	ResolvedAt time.Time           `json:"resolved_at,omitempty"`
	Values     *values.ValueSetRef `json:"values,omitempty"`
}

func (r RunUpdateReceipt) Validate() error {
	if r.Outcome != ResumeApplied && r.Outcome != ResumeReplayed && r.Outcome != ResumeAlreadyResumed && r.Outcome != ResumeClosed {
		return fmt.Errorf("unsupported run update outcome %q", r.Outcome)
	}
	if err := (WaitRef{ID: r.WaitID}).Validate(); err != nil {
		return err
	}
	if !r.WaitStatus.Valid() || r.WaitStatus == WaitOpen || r.ResolvedAt.IsZero() {
		return errors.New("run update receipt requires a terminal wait status and resolution time")
	}
	if r.Values != nil {
		return r.Values.Validate()
	}
	return nil
}

type RunUpdateSnapshot struct {
	Request    BeginRunUpdateRequest `json:"request"`
	Status     RunUpdateStatus       `json:"status"`
	Receipt    *RunUpdateReceipt     `json:"receipt,omitempty"`
	Generation uint64                `json:"generation"`
	CreatedAt  time.Time             `json:"created_at"`
	UpdatedAt  time.Time             `json:"updated_at"`
}

func (s RunUpdateSnapshot) Validate() error {
	if err := s.Request.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported run update status %q", s.Status)
	}
	if s.Status == RunUpdatePending && s.Receipt != nil || s.Status != RunUpdatePending && s.Receipt == nil {
		return fmt.Errorf("run update receipt does not match status")
	}
	if s.Receipt != nil {
		if err := s.Receipt.Validate(); err != nil {
			return err
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

type CompleteRunUpdateRequest struct {
	IdempotencyKey     string           `json:"idempotency_key"`
	ExpectedGeneration uint64           `json:"expected_generation"`
	Status             RunUpdateStatus  `json:"status"`
	Receipt            RunUpdateReceipt `json:"receipt"`
	At                 time.Time        `json:"at"`
}

// RunControlStore adds no alternate lifecycle mutation. Its only write
// records and seals tracked update intent around WaitStore.ResumeNodeWait.
type RunControlStore interface {
	QueryRunState(context.Context, RunStateQuery) (RunStateView, error)
	FindOpenSignalWait(context.Context, SignalSelector) (WaitSnapshot, error)
	FindSignalWait(context.Context, SignalSelector, string) (WaitSnapshot, error)
	BeginRunUpdate(context.Context, BeginRunUpdateRequest) (RunUpdateSnapshot, IdempotencyOutcome, error)
	CompleteRunUpdate(context.Context, CompleteRunUpdateRequest) (RunUpdateSnapshot, error)
	LoadRunUpdate(context.Context, string) (RunUpdateSnapshot, error)
	RecoverRunUpdates(context.Context, int) ([]RunUpdateSnapshot, error)
}
