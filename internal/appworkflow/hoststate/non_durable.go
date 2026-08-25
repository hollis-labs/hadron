package hoststate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// NonDurableStartRecord is the bounded host audit for durability:none. It
// deliberately contains no node, attempt, wait, or event progress history.
type NonDurableStartRecord struct {
	RunID         runtime.RunID       `json:"run_id"`
	StartKey      string              `json:"start_key"`
	RequestDigest string              `json:"request_digest"`
	Plan          runtime.PlanRef     `json:"plan"`
	Identity      IdentityBinding     `json:"identity"`
	Facts         PolicyFacts         `json:"facts"`
	Decision      PolicyDecision      `json:"decision"`
	Run           runtime.RunSnapshot `json:"run"`
	Outputs       values.ValueSet     `json:"outputs,omitempty"`
	Failure       *runtime.Failure    `json:"failure,omitempty"`
	CompletedAt   time.Time           `json:"completed_at"`
}

func (r NonDurableStartRecord) Validate() error {
	if strings.TrimSpace(r.StartKey) == "" || r.CompletedAt.IsZero() || !r.Run.Status.Terminal() || r.Run.ID != r.RunID || r.Run.Plan != r.Plan {
		return errors.New("non-durable audit requires exact terminal run, key, plan, and completion time")
	}
	if err := values.ValidateDigest(r.RequestDigest); err != nil {
		return err
	}
	if err := r.Plan.Validate(); err != nil {
		return err
	}
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	if err := r.Facts.Validate(); err != nil {
		return err
	}
	if err := r.Decision.Validate(); err != nil {
		return err
	}
	if r.Facts.Operation != "start" || r.Facts.RunID != r.RunID || r.Decision.RunID != r.RunID || r.Facts.Plan != r.Plan || r.Decision.Operation != "start" {
		return errors.New("non-durable audit policy binding does not match the run")
	}
	if !sameIdentityBinding(r.Identity, r.Facts.Identity) {
		return errors.New("non-durable audit identity differs from policy identity")
	}
	if err := r.Outputs.Validate(); err != nil {
		return err
	}
	if r.Run.Status == runtime.RunSucceeded && r.Failure != nil || r.Run.Status != runtime.RunSucceeded && r.Failure == nil {
		return errors.New("non-durable audit failure does not match terminal status")
	}
	if r.Failure != nil {
		return r.Failure.Validate()
	}
	return nil
}

// NonDurableJournal is intentionally separate from Journal: durable hosts
// without embedded execution support keep their existing contract unchanged.
type NonDurableJournal interface {
	RecordNonDurableStart(context.Context, NonDurableStartRecord) (NonDurableStartRecord, runtime.IdempotencyOutcome, error)
	LoadNonDurableStart(context.Context, runtime.RunID) (NonDurableStartRecord, error)
	LoadNonDurableStartByKey(context.Context, string) (NonDurableStartRecord, error)
}

func sameIdentityBinding(left, right IdentityBinding) bool {
	return reflect.DeepEqual(left, right)
}
