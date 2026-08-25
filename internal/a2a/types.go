package a2a

import (
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// TaskRequest carries execution intent only. Authenticated principal facts are
// bound from the request context at the appworkflow boundary.
type TaskRequest struct {
	ID              string                             `json:"id,omitempty"`
	Skill           graph.DefinitionRef                `json:"skill"`
	Input           map[string]any                     `json:"input,omitempty"`
	IdempotencyKey  string                             `json:"idempotency_key"`
	RunScope        *hoststate.RunScopeSelector        `json:"run_scope,omitempty"`
	ExecutionTarget *hoststate.ExecutionTargetSelector `json:"execution_target,omitempty"`
	Confirmed       bool                               `json:"confirmed,omitempty"`
}

type CancelTaskRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Reason         string `json:"reason,omitempty"`
}

type ResumeTaskRequest struct {
	WaitID         appworkflow.WaitID             `json:"wait_id"`
	Correlation    string                         `json:"correlation"`
	Token          string                         `json:"token,omitempty"`
	WakeSource     appworkflow.WorkflowWakeSource `json:"wake_source"`
	Payload        values.Value                   `json:"payload"`
	IdempotencyKey string                         `json:"idempotency_key"`
}

// TaskResponse is a transport-safe projection assembled exclusively from
// authorized appworkflow operations.
type TaskResponse struct {
	ID         string                              `json:"id"`
	RunID      appworkflow.RunID                   `json:"run_id"`
	Definition graph.DefinitionRef                 `json:"definition"`
	Outcome    workflowruntime.IdempotencyOutcome  `json:"outcome,omitempty"`
	Available  bool                                `json:"available"`
	Status     TaskStatus                          `json:"status"`
	Waits      []appworkflow.WorkflowWaitListItem  `json:"waits,omitempty"`
	Values     []rundiagnostics.ValueSetDiagnostic `json:"values,omitempty"`
	Events     []workflowruntime.RenderedEvent     `json:"events,omitempty"`
	Result     *TaskResult                         `json:"result,omitempty"`
	Omissions  []string                            `json:"omissions,omitempty"`
	Truncated  TaskTruncation                      `json:"truncated"`
}

type TaskStatus struct {
	State   string `json:"state"`
	Message string `json:"message,omitempty"`
}

type TaskResult struct {
	OutputType string                  `json:"outputType"`
	Output     values.RenderedValueSet `json:"output"`
}

type TaskTruncation struct {
	Waits  bool `json:"waits,omitempty"`
	Values bool `json:"values,omitempty"`
	Events bool `json:"events,omitempty"`
}

type ResumeTaskResponse struct {
	Resume appworkflow.ResumeWorkflowRunResult `json:"resume"`
}
