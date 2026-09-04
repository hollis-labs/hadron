package hoststate

import (
	"context"
	"errors"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/go-workflow/runtime"
)

var (
	ErrCallbackExpired    = errors.New("workflow callback expired")
	ErrCallbackCredential = errors.New("workflow callback credential rejected")
)

type ActivationReconcileRequest struct {
	SourceOwnerKey            string                   `json:"source_owner_key"`
	ExpectedCurrentPlanDigest string                   `json:"expected_current_plan_digest,omitempty"`
	PlanDigest                string                   `json:"plan_digest,omitempty"`
	Registrations             []ActivationRegistration `json:"registrations,omitempty"`
	At                        time.Time                `json:"at"`
}

type ActivationReconcileResult struct {
	SourceOwnerKey    string                     `json:"source_owner_key"`
	CurrentPlanDigest string                     `json:"current_plan_digest,omitempty"`
	SourceGeneration  uint64                     `json:"source_generation"`
	Registrations     []ActivationRegistration   `json:"registrations,omitempty"`
	Outcome           runtime.IdempotencyOutcome `json:"outcome"`
}

type ActivationPrepareRequest struct {
	RegistrationID                 string
	ExpectedRegistrationGeneration uint64
	FireID                         string
	Attempt                        int
	ScheduledAt                    time.Time
	ObservedAt                     time.Time
	LogicalRunID                   string
}

type ActivationPrepareResult struct {
	Registration ActivationRegistration
	Dispatch     ActivationDispatch
	ReplaceRuns  []runtime.RunID
	Outcome      runtime.IdempotencyOutcome
}

type ActivationCompleteRequest struct {
	FireID             string
	ExpectedGeneration uint64
	Attempt            int
	Status             ActivationDispatchStatus
	ReasonCode         string
	At                 time.Time
}

type CallbackBeginRequest struct {
	CallbackID       string
	IdempotencyKey   string
	CredentialDigest string
	PayloadDigest    string
	ReceivedAt       time.Time
}

type CallbackDelivery struct {
	Registration   CallbackRegistration       `json:"registration"`
	IdempotencyKey string                     `json:"idempotency_key"`
	ReceivedAt     time.Time                  `json:"received_at"`
	CompletedAt    time.Time                  `json:"completed_at,omitempty"`
	Outcome        runtime.IdempotencyOutcome `json:"outcome"`
}

type ActivationStore interface {
	gosched.Store
	RegisterActivation(context.Context, ActivationRegistration) (ActivationRegistration, runtime.IdempotencyOutcome, error)
	ReconcileDerivedActivations(context.Context, ActivationReconcileRequest) (ActivationReconcileResult, error)
	ListDerivedActivations(context.Context, string) ([]ActivationRegistration, error)
	LoadActivation(context.Context, string) (ActivationRegistration, error)
	RecordActivationEvent(context.Context, ActivationEvent) (gosched.Fire, runtime.IdempotencyOutcome, error)
	PrepareActivation(context.Context, ActivationPrepareRequest) (ActivationPrepareResult, error)
	LoadActivationDispatch(context.Context, string) (ActivationDispatch, error)
	CompleteActivation(context.Context, ActivationCompleteRequest) (ActivationDispatch, error)
	RecordActivationObserver(context.Context, gosched.ObserverEvent) error
	CreateCallback(context.Context, CallbackRegistration) (CallbackRegistration, runtime.IdempotencyOutcome, error)
	LoadCallback(context.Context, string) (CallbackRegistration, error)
	BeginCallback(context.Context, CallbackBeginRequest) (CallbackDelivery, error)
	CompleteCallback(context.Context, string, string, time.Time) (CallbackDelivery, error)
}
