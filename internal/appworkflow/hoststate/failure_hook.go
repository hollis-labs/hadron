package hoststate

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
)

type FailureHookStatus string

const (
	FailureHookPending    FailureHookStatus = "pending"
	FailureHookStarting   FailureHookStatus = "starting"
	FailureHookStarted    FailureHookStatus = "started"
	FailureHookSuppressed FailureHookStatus = "suppressed"
	FailureHookFailed     FailureHookStatus = "failed"
)

func (s FailureHookStatus) Valid() bool {
	return s == FailureHookPending || s == FailureHookStarting || s == FailureHookStarted || s == FailureHookSuppressed || s == FailureHookFailed
}

type FailureHookBinding struct {
	SourceRunID  runtime.RunID       `json:"source_run_id"`
	SourcePlan   runtime.PlanRef     `json:"source_plan"`
	HandlerRunID runtime.RunID       `json:"handler_run_id"`
	Handler      graph.DefinitionRef `json:"handler"`
	Identity     IdentityBinding     `json:"identity"`
	Depth        int                 `json:"depth"`
	MaximumDepth int                 `json:"maximum_depth"`
	BoundAt      time.Time           `json:"bound_at"`
}

func (b FailureHookBinding) Validate() error {
	if b.SourceRunID == "" || b.HandlerRunID == "" || b.SourceRunID == b.HandlerRunID || strings.TrimSpace(b.Handler.Digest) == "" || b.Depth < 0 || b.MaximumDepth < 1 || b.MaximumDepth > 16 || b.BoundAt.IsZero() {
		return errors.New("failure hook requires source, distinct handler, exact definition, bounded depth, and time")
	}
	if err := b.SourcePlan.Validate(); err != nil {
		return err
	}
	return b.Identity.Validate()
}

type FailureHookSnapshot struct {
	Binding    FailureHookBinding `json:"binding"`
	Status     FailureHookStatus  `json:"status"`
	Generation uint64             `json:"generation"`
	Error      string             `json:"error,omitempty"`
	CreatedAt  time.Time          `json:"created_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
}

func (s FailureHookSnapshot) Validate() error {
	if err := s.Binding.Validate(); err != nil {
		return err
	}
	if !s.Status.Valid() || s.Generation == 0 || s.CreatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return errors.New("failure hook snapshot is malformed")
	}
	if (s.Status == FailureHookFailed) != (s.Error != "") {
		return errors.New("failure hook error does not match status")
	}
	return nil
}

type BindFailureHookRequest struct {
	SourceRunID  runtime.RunID
	SourcePlan   runtime.PlanRef
	HandlerRunID runtime.RunID
	Handler      graph.DefinitionRef
	Identity     IdentityBinding
	MaximumDepth int
	At           time.Time
}

type FailureHookJournal interface {
	ListUnhandledFailedRuns(context.Context, int) ([]runtime.RunID, error)
	BindFailureHook(context.Context, BindFailureHookRequest) (FailureHookSnapshot, runtime.IdempotencyOutcome, error)
	CompleteFailureHook(context.Context, runtime.RunID, uint64, FailureHookStatus, string, time.Time) (FailureHookSnapshot, error)
	RecoverFailureHooks(context.Context, int) ([]FailureHookSnapshot, error)
}
