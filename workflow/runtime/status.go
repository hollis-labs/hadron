package runtime

import workflowwait "github.com/hollis-labs/hadron/workflow/wait"

// RunStatus is the persisted state of a workflow run.
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunWaiting   RunStatus = "waiting"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
	RunTimedOut  RunStatus = "timed_out"
	RunCrashed   RunStatus = "crashed"
)

// Valid reports whether s is a supported persisted run status.
func (s RunStatus) Valid() bool {
	switch s {
	case RunPending, RunRunning, RunWaiting, RunSucceeded, RunFailed,
		RunCanceled, RunTimedOut, RunCrashed:
		return true
	default:
		return false
	}
}

// Active reports whether a run belongs in active-run recovery queries.
func (s RunStatus) Active() bool {
	switch s {
	case RunPending, RunRunning, RunWaiting:
		return true
	default:
		return false
	}
}

// Terminal reports whether a run status cannot transition to another status.
func (s RunStatus) Terminal() bool {
	switch s {
	case RunSucceeded, RunFailed, RunCanceled, RunTimedOut, RunCrashed:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether the run lifecycle permits an applied status
// change. Same-state idempotency is evaluated separately against state data.
func (s RunStatus) CanTransitionTo(next RunStatus) bool {
	switch s {
	case RunPending:
		return next == RunRunning || next == RunCanceled || next == RunTimedOut
	case RunRunning:
		return next == RunWaiting || next == RunSucceeded || next == RunFailed ||
			next == RunCanceled || next == RunTimedOut || next == RunCrashed
	case RunWaiting:
		return next == RunRunning || next == RunSucceeded || next == RunFailed ||
			next == RunCanceled || next == RunTimedOut || next == RunCrashed
	default:
		return false
	}
}

// NodeStatus is the persisted state of one node invocation.
type NodeStatus string

const (
	NodePending   NodeStatus = "pending"
	NodeReady     NodeStatus = "ready"
	NodeRunning   NodeStatus = "running"
	NodeWaiting   NodeStatus = "waiting"
	NodeSucceeded NodeStatus = "succeeded"
	NodeFailed    NodeStatus = "failed"
	NodeSkipped   NodeStatus = "skipped"
	NodeCanceled  NodeStatus = "canceled"
	NodeTimedOut  NodeStatus = "timed_out"
	NodeCrashed   NodeStatus = "crashed"
	NodeBlocked   NodeStatus = "blocked"
)

// Valid reports whether s is a supported persisted node status.
func (s NodeStatus) Valid() bool {
	switch s {
	case NodePending, NodeReady, NodeRunning, NodeWaiting, NodeSucceeded,
		NodeFailed, NodeSkipped, NodeCanceled, NodeTimedOut, NodeCrashed, NodeBlocked:
		return true
	default:
		return false
	}
}

// Terminal reports whether an aggregate node status cannot be reopened.
func (s NodeStatus) Terminal() bool {
	switch s {
	case NodeSucceeded, NodeFailed, NodeSkipped, NodeCanceled, NodeTimedOut, NodeCrashed:
		return true
	default:
		return false
	}
}

// CanTransitionTo reports whether the generic node lifecycle permits an
// applied status change. Attempt start/finish operations own execution-only
// transitions, including ready-to-running and running-to-terminal/ready.
func (s NodeStatus) CanTransitionTo(next NodeStatus) bool {
	switch s {
	case NodePending:
		return next == NodeReady || next == NodeSkipped || next == NodeCanceled ||
			next == NodeTimedOut || next == NodeBlocked
	case NodeReady:
		return next == NodeSkipped || next == NodeCanceled || next == NodeTimedOut || next == NodeBlocked
	case NodeRunning:
		return next == NodeWaiting
	case NodeWaiting:
		return next == NodeReady
	case NodeBlocked:
		return next == NodePending || next == NodeReady || next == NodeSkipped ||
			next == NodeCanceled || next == NodeTimedOut
	default:
		return false
	}
}

// WaitStatus is retained as a source-compatible alias. workflow/wait owns the
// generic wait state model.
type WaitStatus = workflowwait.Status

const (
	WaitOpen     = workflowwait.StatusOpen
	WaitResumed  = workflowwait.StatusResumed
	WaitTimedOut = workflowwait.StatusTimedOut
	WaitCanceled = workflowwait.StatusCanceled
)
