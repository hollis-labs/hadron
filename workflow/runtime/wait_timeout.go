package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// EventWaitTimedOut records the wait identity and persisted deadline that
// caused an invocation attempt to time out.
const EventWaitTimedOut = "wait.timed_out"

// ErrWaitTimeoutNotDue identifies a deadline that has not arrived.
var ErrWaitTimeoutNotDue = errors.New("workflow wait timeout is not due")

// WaitTimeoutNotDueError reports a persisted deadline that has not arrived.
type WaitTimeoutNotDueError struct {
	Now      time.Time
	Deadline time.Time
}

func (e *WaitTimeoutNotDueError) Error() string {
	return fmt.Sprintf("%s: now %s precedes deadline %s", ErrWaitTimeoutNotDue, e.Now.Format(time.RFC3339Nano), e.Deadline.Format(time.RFC3339Nano))
}

func (e *WaitTimeoutNotDueError) Unwrap() error { return ErrWaitTimeoutNotDue }

// TimeoutWaitRequest atomically closes one open wait and its waiting node
// attempt under wait/node CAS. Deadline must exactly match the persisted wait.
// An already-terminal wait returns its current durable wait and node without
// applying a mutation; expected generations fence only the open mutation.
type TimeoutWaitRequest struct {
	WaitID                 WaitID
	ExpectedWaitGeneration uint64
	ExpectedNodeGeneration uint64
	IdempotencyKey         string
	Deadline               time.Time
	Now                    time.Time
}

// Validate reports malformed timeout input. A valid request may still be not
// due when Now precedes Deadline.
func (r TimeoutWaitRequest) Validate() error {
	if err := (WaitRef{ID: r.WaitID}).Validate(); err != nil {
		return err
	}
	if r.ExpectedWaitGeneration == 0 || r.ExpectedNodeGeneration == 0 {
		return fmt.Errorf("timeout requires positive wait and node generations")
	}
	if err := validateRequiredText("timeout idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if r.Deadline.IsZero() || r.Now.IsZero() {
		return fmt.Errorf("timeout deadline and now are required")
	}
	return nil
}

// WaitTimeoutResult is the store-neutral atomic outcome. Applied is false only
// when the wait was already resolved. Replayed marks an exact durable retry.
// Events contains the attempt-finished, node-status, and wait-timeout facts in
// durable sequence order for an applied timeout.
type WaitTimeoutResult struct {
	Applied  bool
	Replayed bool
	Wait     WaitSnapshot
	Node     NodeInvocationSnapshot
	Attempt  AttemptSnapshot
	Events   []Event
}

// WaitTimeoutStore is the timeout portion of the atomic WaitStore contract.
type WaitTimeoutStore interface {
	TimeoutWait(context.Context, TimeoutWaitRequest) (WaitTimeoutResult, error)
}

// WaitTimeoutFailure returns the canonical attempt failure for a deadline.
func WaitTimeoutFailure(deadline time.Time) Failure {
	return Failure{
		Code: "wait_timeout", Message: "wait deadline reached",
		Details: map[string]string{"deadline": deadline.UTC().Format(time.RFC3339Nano)},
	}
}
