package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const MaximumReactorRecoveryLimit = 1000

var (
	ErrReactorRolling  = errors.New("workflow reactor is rolling to a new generation")
	ErrReactorBusy     = errors.New("workflow reactor has an applying delivery")
	ErrReactorTerminal = errors.New("workflow reactor is terminal")
)

type ReactorStatus string

const (
	ReactorStarting ReactorStatus = "starting"
	ReactorWaiting  ReactorStatus = "waiting"
	ReactorRolling  ReactorStatus = "rolling"
	ReactorFailed   ReactorStatus = "failed"
	ReactorClosed   ReactorStatus = "closed"
)

func (s ReactorStatus) Valid() bool {
	return s == ReactorStarting || s == ReactorWaiting || s == ReactorRolling || s == ReactorFailed || s == ReactorClosed
}

// ReactorIdentity is derived by a host from one exact source-owned activation
// registration. Callers never choose its plan, provenance, or logical ID.
type ReactorIdentity struct {
	ID                     string              `json:"id"`
	RegistrationID         string              `json:"registration_id"`
	RegistrationGeneration uint64              `json:"registration_generation"`
	Correlation            string              `json:"correlation"`
	Definition             graph.DefinitionRef `json:"definition"`
	Plan                   PlanRef             `json:"plan"`
	Provenance             graph.Provenance    `json:"provenance"`
}

func (i ReactorIdentity) Validate() error {
	if err := validateOpaqueID("reactor id", i.ID); err != nil {
		return err
	}
	if err := validateRequiredText("reactor registration id", i.RegistrationID); err != nil {
		return err
	}
	if i.RegistrationGeneration == 0 {
		return errors.New("reactor registration generation must be positive")
	}
	if err := validateRequiredText("reactor correlation", i.Correlation); err != nil {
		return err
	}
	if err := i.Plan.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(i.Definition.Digest) == "" || i.Provenance.Digest == "" {
		return errors.New("reactor definition, plan, and provenance must be exact and digest-bound")
	}
	return nil
}

type ReactorSnapshot struct {
	Identity            ReactorIdentity `json:"identity"`
	CurrentGeneration   uint64          `json:"current_generation"`
	CurrentRunID        RunID           `json:"current_run_id"`
	ContinueAfterEvents uint64          `json:"continue_after_events"`
	Status              ReactorStatus   `json:"status"`
	Generation          uint64          `json:"generation"`
	EventCount          uint64          `json:"event_count"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

func (s ReactorSnapshot) Validate() error {
	if err := s.Identity.Validate(); err != nil {
		return err
	}
	if s.CurrentGeneration == 0 || s.CurrentRunID == "" || s.ContinueAfterEvents == 0 || s.ContinueAfterEvents > 1_000_000 || !s.Status.Valid() {
		return errors.New("reactor requires current generation, run, continuation threshold, and status")
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

type ReactorDeliveryStatus string

const (
	ReactorDeliveryPending  ReactorDeliveryStatus = "pending"
	ReactorDeliveryApplying ReactorDeliveryStatus = "applying"
	ReactorDeliveryApplied  ReactorDeliveryStatus = "applied"
	ReactorDeliveryClosed   ReactorDeliveryStatus = "closed"
)

func (s ReactorDeliveryStatus) Valid() bool {
	return s == ReactorDeliveryPending || s == ReactorDeliveryApplying || s == ReactorDeliveryApplied || s == ReactorDeliveryClosed
}

type ReactorDeliveryRequest struct {
	ReactorID      string                 `json:"reactor_id"`
	IdempotencyKey string                 `json:"idempotency_key"`
	SignalName     string                 `json:"signal_name"`
	Payload        values.Value           `json:"payload"`
	Responder      workflowwait.Responder `json:"responder"`
	OccurredAt     time.Time              `json:"occurred_at"`
	ReceivedAt     time.Time              `json:"received_at"`
}

func (r ReactorDeliveryRequest) Validate() error {
	if err := validateOpaqueID("delivery reactor id", r.ReactorID); err != nil {
		return err
	}
	if err := validateRequiredText("delivery idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if err := validateRequiredText("delivery signal name", r.SignalName); err != nil {
		return err
	}
	if err := r.Payload.Validate(); err != nil {
		return err
	}
	if err := r.Responder.Validate(); err != nil {
		return err
	}
	if r.OccurredAt.IsZero() || r.ReceivedAt.IsZero() || r.ReceivedAt.Before(r.OccurredAt) {
		return errors.New("delivery requires ordered occurred_at and received_at")
	}
	return nil
}

type ReactorDeliverySnapshot struct {
	Request           ReactorDeliveryRequest `json:"request"`
	ReactorGeneration uint64                 `json:"reactor_generation"`
	RunID             RunID                  `json:"run_id"`
	// StartsGeneration is store-derived only for the delivery that created
	// generation one. Its payload is consumed by ordinary Host.StartRun and
	// must not also be resumed into a wait.
	StartsGeneration bool                    `json:"starts_generation,omitempty"`
	ClaimedWaitID    WaitID                  `json:"claimed_wait_id,omitempty"`
	Status           ReactorDeliveryStatus   `json:"status"`
	Generation       uint64                  `json:"generation"`
	Receipt          *ReactorDeliveryReceipt `json:"receipt,omitempty"`
	CreatedAt        time.Time               `json:"created_at"`
	UpdatedAt        time.Time               `json:"updated_at"`
}

func (s ReactorDeliverySnapshot) Validate() error {
	if err := s.Request.Validate(); err != nil {
		return err
	}
	if s.ReactorGeneration == 0 || s.RunID == "" || !s.Status.Valid() {
		return errors.New("delivery requires reactor generation, run, and status")
	}
	if (s.Status == ReactorDeliveryApplied || s.Status == ReactorDeliveryClosed) != (s.Receipt != nil) {
		return errors.New("delivery receipt does not match terminal status")
	}
	if s.Status == ReactorDeliveryPending && s.ClaimedWaitID != "" || s.Status == ReactorDeliveryApplying && !s.StartsGeneration && s.ClaimedWaitID == "" {
		return errors.New("delivery claim target does not match status")
	}
	if s.StartsGeneration && s.ReactorGeneration != 1 || s.StartsGeneration && s.ClaimedWaitID != "" {
		return errors.New("generation-start delivery must be the credential-free first generation")
	}
	if s.Receipt != nil {
		if err := s.Receipt.Validate(); err != nil {
			return err
		}
		if s.Receipt.RunID != s.RunID {
			return errors.New("delivery receipt consumption does not match its immutable target")
		}
		if s.Receipt.Kind == ReactorDeliveryTerminalRun {
			if s.Status != ReactorDeliveryClosed || s.StartsGeneration || s.ClaimedWaitID != "" {
				return errors.New("terminal-run receipt must close an unclaimed later delivery")
			}
		} else if s.Status != ReactorDeliveryApplied || s.StartsGeneration != (s.Receipt.Kind == ReactorDeliveryStartedRun) {
			return errors.New("delivery receipt consumption does not match its immutable target")
		}
		if s.Receipt.Update != nil && s.Receipt.Update.WaitID != s.ClaimedWaitID {
			return errors.New("delivery receipt does not match its claimed wait")
		}
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

type BeginReactorDeliveryRequest struct {
	Identity            ReactorIdentity        `json:"identity"`
	InitialRunID        RunID                  `json:"initial_run_id"`
	ContinueAfterEvents uint64                 `json:"continue_after_events"`
	Delivery            ReactorDeliveryRequest `json:"delivery"`
	At                  time.Time              `json:"at"`
}

type ClaimReactorDeliveryRequest struct {
	ReactorID          string    `json:"reactor_id"`
	IdempotencyKey     string    `json:"idempotency_key"`
	ExpectedGeneration uint64    `json:"expected_generation"`
	WaitID             WaitID    `json:"wait_id,omitempty"`
	At                 time.Time `json:"at"`
}

type ReleaseReactorDeliveryRequest struct {
	ReactorID          string    `json:"reactor_id"`
	IdempotencyKey     string    `json:"idempotency_key"`
	ExpectedGeneration uint64    `json:"expected_generation"`
	At                 time.Time `json:"at"`
}

type ReactorDeliveryReceiptKind string

const (
	ReactorDeliveryStartedRun  ReactorDeliveryReceiptKind = "started_run"
	ReactorDeliveryResumedWait ReactorDeliveryReceiptKind = "resumed_wait"
	ReactorDeliveryTerminalRun ReactorDeliveryReceiptKind = "terminal_run"
)

// ReactorDeliveryReceipt records the one canonical consumption path. The
// first delivery is consumed by Host.StartRun; later deliveries are consumed
// only through the canonical tracked wait update.
type ReactorDeliveryReceipt struct {
	Kind        ReactorDeliveryReceiptKind `json:"kind"`
	RunID       RunID                      `json:"run_id"`
	Update      *RunUpdateReceipt          `json:"update,omitempty"`
	RunStatus   RunStatus                  `json:"run_status,omitempty"`
	ProcessedAt time.Time                  `json:"processed_at"`
}

func (r ReactorDeliveryReceipt) Validate() error {
	if r.RunID == "" || r.ProcessedAt.IsZero() {
		return errors.New("reactor delivery receipt requires run and processed_at")
	}
	switch r.Kind {
	case ReactorDeliveryStartedRun:
		if r.Update != nil || r.RunStatus != "" {
			return errors.New("started-run delivery receipt cannot contain a wait update")
		}
	case ReactorDeliveryResumedWait:
		if r.Update == nil || r.RunStatus != "" {
			return errors.New("resumed-wait delivery receipt requires an update")
		}
		if err := r.Update.Validate(); err != nil {
			return err
		}
		if r.Update.Outcome == ResumeClosed {
			return errors.New("closed wait did not consume a reactor delivery")
		}
	case ReactorDeliveryTerminalRun:
		if r.Update != nil || !r.RunStatus.Terminal() || r.RunStatus == RunSucceeded {
			return errors.New("terminal-run delivery receipt requires an unsuccessful terminal run")
		}
	default:
		return fmt.Errorf("unsupported reactor delivery receipt kind %q", r.Kind)
	}
	return nil
}

// FailReactorRequest durably fences a reactor whose current run ended without
// a successful continuation boundary. Pending deliveries are closed against
// that exact terminal run in the same atomic transition.
type FailReactorRequest struct {
	ReactorID          string    `json:"reactor_id"`
	ExpectedGeneration uint64    `json:"expected_generation"`
	RunID              RunID     `json:"run_id"`
	RunStatus          RunStatus `json:"run_status"`
	At                 time.Time `json:"at"`
}

func (r FailReactorRequest) Validate() error {
	if err := validateOpaqueID("reactor id", r.ReactorID); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.RunID == "" || !r.RunStatus.Terminal() || r.RunStatus == RunSucceeded || r.At.IsZero() {
		return errors.New("reactor failure requires generation and an unsuccessful terminal run")
	}
	return nil
}

type CompleteReactorDeliveryRequest struct {
	ReactorID          string                 `json:"reactor_id"`
	IdempotencyKey     string                 `json:"idempotency_key"`
	ExpectedGeneration uint64                 `json:"expected_generation"`
	Status             ReactorDeliveryStatus  `json:"status"`
	Receipt            ReactorDeliveryReceipt `json:"receipt"`
	At                 time.Time              `json:"at"`
}

type ReactorContinuationStatus string

const (
	ReactorContinuationPending   ReactorContinuationStatus = "pending"
	ReactorContinuationStarted   ReactorContinuationStatus = "started"
	ReactorContinuationCompleted ReactorContinuationStatus = "completed"
)

func (s ReactorContinuationStatus) Valid() bool {
	return s == ReactorContinuationPending || s == ReactorContinuationStarted || s == ReactorContinuationCompleted
}

type ReactorContinuationRequest struct {
	IdempotencyKey     string          `json:"idempotency_key"`
	ReactorID          string          `json:"reactor_id"`
	ExpectedGeneration uint64          `json:"expected_generation"`
	FromGeneration     uint64          `json:"from_generation"`
	FromRunID          RunID           `json:"from_run_id"`
	ToRunID            RunID           `json:"to_run_id"`
	State              values.ValueSet `json:"state,omitempty"`
	At                 time.Time       `json:"at"`
}

func (r ReactorContinuationRequest) Validate() error {
	if err := validateRequiredText("continuation key", r.IdempotencyKey); err != nil {
		return err
	}
	if err := validateOpaqueID("continuation reactor id", r.ReactorID); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.FromGeneration == 0 || r.FromRunID == "" || r.ToRunID == "" || r.FromRunID == r.ToRunID || r.At.IsZero() {
		return errors.New("continuation generations, distinct runs, and time are required")
	}
	return r.State.Validate()
}

type ReactorContinuationSnapshot struct {
	Request    ReactorContinuationRequest `json:"request"`
	Status     ReactorContinuationStatus  `json:"status"`
	Generation uint64                     `json:"generation"`
	CreatedAt  time.Time                  `json:"created_at"`
	UpdatedAt  time.Time                  `json:"updated_at"`
}

func (s ReactorContinuationSnapshot) Validate() error {
	if err := s.Request.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() {
		return fmt.Errorf("unsupported continuation status %q", s.Status)
	}
	return validateSnapshotTimes(s.Generation, s.CreatedAt, s.UpdatedAt)
}

type ReactorStore interface {
	BeginReactorDelivery(context.Context, BeginReactorDeliveryRequest) (ReactorSnapshot, ReactorDeliverySnapshot, IdempotencyOutcome, error)
	LoadReactor(context.Context, string) (ReactorSnapshot, error)
	MarkReactorWaiting(context.Context, string, uint64, time.Time) (ReactorSnapshot, error)
	LoadReactorDelivery(context.Context, string, string) (ReactorDeliverySnapshot, error)
	ClaimReactorDelivery(context.Context, ClaimReactorDeliveryRequest) (ReactorDeliverySnapshot, error)
	ReleaseReactorDelivery(context.Context, ReleaseReactorDeliveryRequest) (ReactorDeliverySnapshot, error)
	CompleteReactorDelivery(context.Context, CompleteReactorDeliveryRequest) (ReactorSnapshot, ReactorDeliverySnapshot, error)
	FailReactor(context.Context, FailReactorRequest) (ReactorSnapshot, error)
	RecoverReactorDeliveries(context.Context, int) ([]ReactorDeliverySnapshot, error)
	RecoverReactors(context.Context, int) ([]ReactorSnapshot, error)
	BeginReactorContinuation(context.Context, ReactorContinuationRequest) (ReactorSnapshot, ReactorContinuationSnapshot, IdempotencyOutcome, error)
	CompleteReactorContinuation(context.Context, string, uint64, time.Time) (ReactorSnapshot, ReactorContinuationSnapshot, error)
	RecoverReactorContinuations(context.Context, int) ([]ReactorContinuationSnapshot, error)
}
