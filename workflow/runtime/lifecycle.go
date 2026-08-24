package runtime

import (
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventRunStatusChanged    = "run.status_changed"
	EventNodeStatusChanged   = "node.status_changed"
	EventNodeAttemptStarted  = "node.attempt_started"
	EventNodeAttemptFinished = "node.attempt_finished"
)

var (
	ErrInvalidTransition  = errors.New("invalid workflow lifecycle transition")
	ErrTransitionConflict = errors.New("workflow lifecycle transition conflict")
	ErrAttemptConflict    = errors.New("workflow attempt lifecycle conflict")
)

// TransitionError describes a disallowed status edge without requiring callers
// to parse the presentation string.
type TransitionError struct {
	Entity string
	ID     string
	From   string
	To     string
	Reason string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: %s %q cannot transition from %q to %q: %s", ErrInvalidTransition, e.Entity, e.ID, e.From, e.To, e.Reason)
}

// Unwrap supports errors.Is(err, ErrInvalidTransition).
func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }

// TransitionConflictError describes a same-status request whose state data is
// not an exact semantic replay.
type TransitionConflictError struct {
	Entity string
	ID     string
	Status string
	Reason string
}

func (e *TransitionConflictError) Error() string {
	return fmt.Sprintf("%s: %s %q at %q: %s", ErrTransitionConflict, e.Entity, e.ID, e.Status, e.Reason)
}

// Unwrap supports errors.Is(err, ErrTransitionConflict).
func (e *TransitionConflictError) Unwrap() error { return ErrTransitionConflict }

// AttemptConflictError describes an incoherent attempt history or operation.
type AttemptConflictError struct {
	Invocation NodeInvocationID
	Attempt    int
	Reason     string
}

func (e *AttemptConflictError) Error() string {
	return fmt.Sprintf("%s: node %q attempt %d: %s", ErrAttemptConflict, e.Invocation.NodeID, e.Attempt, e.Reason)
}

// Unwrap supports errors.Is(err, ErrAttemptConflict).
func (e *AttemptConflictError) Unwrap() error { return ErrAttemptConflict }

// TransitionOutcome distinguishes a persisted state change from an exact
// same-state semantic replay.
type TransitionOutcome string

const (
	TransitionApplied TransitionOutcome = "applied"
	TransitionNoOp    TransitionOutcome = "no_op"
)

// ClaimProof fences lifecycle mutations to the current lease owner.
type ClaimProof struct {
	Owner      string
	Token      string
	Generation uint64
}

// Validate reports incomplete claim proof.
func (p ClaimProof) Validate() error {
	if err := validateRequiredText("claim owner", p.Owner); err != nil {
		return err
	}
	if err := validateRequiredText("claim token", p.Token); err != nil {
		return err
	}
	if p.Generation == 0 {
		return fmt.Errorf("claim generation must be positive")
	}
	return nil
}

// RunTransitionRequest changes only lifecycle-owned run fields under CAS.
type RunTransitionRequest struct {
	RunID              RunID
	ExpectedGeneration uint64
	To                 RunStatus
	Outputs            *values.ValueSetRef
	At                 time.Time
}

// RunTransitionResult is the atomic run snapshot and event outcome. Event is
// nil only for an exact no-op.
type RunTransitionResult struct {
	Snapshot RunSnapshot
	Outcome  TransitionOutcome
	Event    *Event
}

// NodeTransitionRequest changes a non-attempt node lifecycle edge under CAS.
// Claim is required when the node currently has a lease.
type NodeTransitionRequest struct {
	InvocationID       NodeInvocationID
	ExpectedGeneration uint64
	To                 NodeStatus
	Blocked            *BlockedReason
	Claim              *ClaimProof
	At                 time.Time
}

// NodeTransitionResult is the atomic node snapshot and event outcome. Event is
// nil only for an exact no-op.
type NodeTransitionResult struct {
	Snapshot NodeInvocationSnapshot
	Outcome  TransitionOutcome
	Event    *Event
}

// StartNodeAttemptRequest atomically creates LatestAttempt+1 and moves a ready
// node to running. Attempt numbers are assigned by the store.
type StartNodeAttemptRequest struct {
	InvocationID           NodeInvocationID
	ExpectedNodeGeneration uint64
	Claim                  ClaimProof
	Executor               ExecutorMetadata
	Inputs                 *values.ValueSetRef
	At                     time.Time
}

// StartNodeAttemptResult contains the atomically persisted node, attempt, and
// attempt-started event.
type StartNodeAttemptResult struct {
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Event   Event
}

// FinishNodeAttemptRequest atomically closes the latest unfinished running
// attempt. AttemptStatus is the durable attempt outcome. NextNodeStatus is
// either that terminal outcome or ready for a future retry/recovery decision.
type FinishNodeAttemptRequest struct {
	InvocationID              NodeInvocationID
	AttemptNumber             int
	ExpectedNodeGeneration    uint64
	ExpectedAttemptGeneration uint64
	Claim                     ClaimProof
	AttemptStatus             NodeStatus
	NextNodeStatus            NodeStatus
	Outputs                   *values.ValueSetRef
	Failure                   *Failure
	At                        time.Time
}

// FinishNodeAttemptResult contains the atomically persisted aggregate node,
// finished attempt, and attempt-finished event.
type FinishNodeAttemptResult struct {
	Node    NodeInvocationSnapshot
	Attempt AttemptSnapshot
	Event   Event
}

// ValidateRunStatusTransition validates an applied run status edge. Same-state
// semantic replay is evaluated by the store after CAS.
func ValidateRunStatusTransition(from, to RunStatus) error {
	if !from.Valid() || !to.Valid() {
		return &TransitionError{Entity: "run", From: string(from), To: string(to), Reason: "status is not supported"}
	}
	if from == to || from.CanTransitionTo(to) {
		return nil
	}
	reason := "edge is not legal"
	if from.Terminal() {
		reason = "terminal status cannot be reopened"
	}
	return &TransitionError{Entity: "run", From: string(from), To: string(to), Reason: reason}
}

// ValidateNodeStatusTransition validates an applied generic node status edge.
// Execution-only edges are owned by StartNodeAttempt and FinishNodeAttempt.
func ValidateNodeStatusTransition(from, to NodeStatus) error {
	if !from.Valid() || !to.Valid() {
		return &TransitionError{Entity: "node", From: string(from), To: string(to), Reason: "status is not supported"}
	}
	if from == to || from.CanTransitionTo(to) {
		return nil
	}
	reason := "edge is not legal for a generic node transition"
	if from.Terminal() {
		reason = "terminal status cannot be reopened"
	}
	return &TransitionError{Entity: "node", From: string(from), To: string(to), Reason: reason}
}
