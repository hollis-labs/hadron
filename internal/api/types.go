package api

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/trigger"
)

// RunStore is the run persistence surface required by the HTTP API.
type RunStore interface {
	CreateRun(ctx context.Context, rec persistence.RunRecord) error
	GetRun(ctx context.Context, id string) (persistence.RunRecord, error)
	ListRunsByWorkspaceFiltered(ctx context.Context, workspaceID string, limit int, cursorID string, createdAfter, createdBefore *time.Time) ([]persistence.RunRecord, error)
	ListRunEvents(ctx context.Context, runID string, limit int) ([]persistence.RunEventRecord, error)
	ListRunEventsFiltered(ctx context.Context, runID string, limit int, cursorID int64, createdAfter, createdBefore *time.Time) ([]persistence.RunEventRecord, error)
}

type ScheduleStore interface {
	GetSchedule(ctx context.Context, id string) (persistence.ScheduleRecord, error)
	ListSchedulesByWorkspace(ctx context.Context, workspaceID string) ([]persistence.ScheduleRecord, error)
	CreateSchedule(ctx context.Context, rec persistence.ScheduleRecord) error
	UpdateScheduleEnabledAndNext(ctx context.Context, id string, enabled bool, nextRun *time.Time) error
	UpdateScheduleFields(ctx context.Context, id string, name, cronExpr, blueprintPath string, enabled bool, nextRun *time.Time) error
	DeleteSchedule(ctx context.Context, id string) error
}

type PipelineStore interface {
	GetPipelineRun(ctx context.Context, id string) (persistence.PipelineRunRecord, error)
	ListPipelineRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]persistence.PipelineRunRecord, error)
	ListPipelineStageRuns(ctx context.Context, pipelineRunID string) ([]persistence.PipelineStageRunRecord, error)
}

type WorkspaceStore interface {
	CreateWorkspace(ctx context.Context, id, name string) error
	GetWorkspace(ctx context.Context, id string) (persistence.WorkspaceRecord, error)
	ListWorkspaces(ctx context.Context) ([]persistence.WorkspaceRecord, error)
}

type TriggerStore interface {
	CreateTrigger(ctx context.Context, rec persistence.TriggerRecord) error
	GetTrigger(ctx context.Context, id string) (persistence.TriggerRecord, error)
	ListTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	DeleteTrigger(ctx context.Context, id string) error
	GetTriggerByPath(ctx context.Context, path string) (persistence.TriggerRecord, error)
	UpdateTriggerFired(ctx context.Context, id string) error
	ListWebhookTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	ListFileWatchTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	DeleteExpiredTriggers(ctx context.Context, now time.Time) (int64, error)
}

type HumanGateStore interface {
	GetHumanGate(ctx context.Context, id string) (persistence.HumanGateRecord, error)
	SubmitHumanGateDecision(ctx context.Context, id, decision string, decidedAt time.Time) error
}

type MessageStore interface {
	Send(ctx context.Context, substrate string, env messaging.Envelope) (messaging.Envelope, error)
	Get(ctx context.Context, substrate, id string) (messaging.Envelope, error)
	Inbox(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]messaging.Envelope, error)
	List(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]messaging.Envelope, error)
	Thread(ctx context.Context, substrate, threadID string, limit int) ([]messaging.Envelope, error)
	Consume(ctx context.Context, substrate, id string) error
}

type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
	Cancel(runID string) bool
}

type Scheduler interface {
	Start()
	Stop()
	Status() scheduler.Status
	TickNow(ctx context.Context) error
}

type PipelineRunner interface {
	Start(ctx context.Context, pipelineRunID, pipelinePath, workspaceID string) error
}

// WorkflowRequestAuthenticator authenticates the HTTP request and applies the
// caller's exposure profile to the requested operation. The returned context
// carries authenticated facts consumed again by appworkflow.IdentityProvider;
// handlers never synthesize a principal from request JSON.
type WorkflowRequestAuthenticator interface {
	AuthenticateWorkflowRequest(*http.Request, appworkflow.WorkflowAccessIntent) (context.Context, error)
}

type WorkflowRequestAuthenticatorFunc func(*http.Request, appworkflow.WorkflowAccessIntent) (context.Context, error)

func (f WorkflowRequestAuthenticatorFunc) AuthenticateWorkflowRequest(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
	return f(request, intent)
}

type A2ATaskService interface {
	SubmitTask(context.Context, a2a.TaskRequest) (*a2a.TaskResponse, error)
	GetTask(context.Context, string) (*a2a.TaskResponse, error)
	CancelTask(context.Context, string, a2a.CancelTaskRequest) (*a2a.TaskResponse, error)
	ResumeTask(context.Context, string, a2a.ResumeTaskRequest) (*a2a.ResumeTaskResponse, error)
}

type AgentCardProvider interface {
	Card(context.Context, string) (*agentcard.AgentCard, error)
}

// Dependencies groups daemon services used by the API handlers.
type Dependencies struct {
	Runs          RunStore
	Schedules     ScheduleStore
	Pipelines     PipelineStore
	Workspaces    WorkspaceStore
	Triggers      TriggerStore
	HumanGates    HumanGateStore
	Messages      MessageStore
	Runner        Runner
	Scheduler     Scheduler
	Pipeline      PipelineRunner
	Workflows     appworkflow.WorkflowOperations
	WorkflowReads appworkflow.WorkflowRunReadOperations
	WorkflowAuth  WorkflowRequestAuthenticator
	A2ATasks      A2ATaskService
	AgentCard     AgentCardProvider
	BlueprintDir  string
	// WebUI is the optional transport-neutral operator SPA. The daemon owns
	// delivery; the handler owns only static browser assets and never workflow
	// semantics.
	WebUI http.Handler
}

type Server struct {
	httpServer     *http.Server
	handler        http.Handler
	deps           Dependencies
	runSeq         atomic.Uint64
	scheduleSeq    atomic.Uint64
	pipelineSeq    atomic.Uint64
	triggerSeq     atomic.Uint64
	triggerManager *trigger.Manager
}
