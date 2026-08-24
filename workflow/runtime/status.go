package runtime

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

// WaitStatus is the minimal persistence state of a suspended invocation. Wake
// source and correlation semantics belong to W03-T05.
type WaitStatus string

const (
	WaitOpen     WaitStatus = "open"
	WaitResumed  WaitStatus = "resumed"
	WaitTimedOut WaitStatus = "timed_out"
	WaitCanceled WaitStatus = "canceled"
)

// Valid reports whether s is supported by the persistence snapshot.
func (s WaitStatus) Valid() bool {
	switch s {
	case WaitOpen, WaitResumed, WaitTimedOut, WaitCanceled:
		return true
	default:
		return false
	}
}
