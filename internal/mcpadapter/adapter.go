package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type Store interface {
	CreateWorkspace(ctx context.Context, id, name string) error
	ListWorkspaces(ctx context.Context) ([]persistence.WorkspaceRecord, error)
	GetWorkspace(ctx context.Context, id string) (persistence.WorkspaceRecord, error)

	ListRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]persistence.RunRecord, error)
	GetRun(ctx context.Context, id string) (persistence.RunRecord, error)
	ListRunEvents(ctx context.Context, runID string, limit int) ([]persistence.RunEventRecord, error)

	ListPipelineRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]persistence.PipelineRunRecord, error)
	GetPipelineRun(ctx context.Context, id string) (persistence.PipelineRunRecord, error)
	ListPipelineStageRuns(ctx context.Context, pipelineRunID string) ([]persistence.PipelineStageRunRecord, error)

	ListSchedulesByWorkspace(ctx context.Context, workspaceID string) ([]persistence.ScheduleRecord, error)
	CreateSchedule(ctx context.Context, rec persistence.ScheduleRecord) error
	UpdateScheduleEnabledAndNext(ctx context.Context, id string, enabled bool, nextRun *time.Time) error
	DeleteSchedule(ctx context.Context, id string) error

	CreateTrigger(ctx context.Context, rec persistence.TriggerRecord) error
	GetTrigger(ctx context.Context, id string) (persistence.TriggerRecord, error)
	ListTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	DeleteTrigger(ctx context.Context, id string) error
	ListTriggersByCreatedBy(ctx context.Context, createdBy string) ([]persistence.TriggerRecord, error)

	GetHumanGate(ctx context.Context, id string) (persistence.HumanGateRecord, error)
	SubmitHumanGateDecision(ctx context.Context, id, decision string, decidedAt time.Time) error
	CreateMessage(ctx context.Context, rec persistence.MessageRecord) error
	GetMessage(ctx context.Context, id string) (persistence.MessageRecord, error)
	ListMessagesByRecipient(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]persistence.MessageRecord, error)
	ListMessagesByRecipientNonDestructive(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]persistence.MessageRecord, error)
	ListMessagesByThread(ctx context.Context, substrate, threadID string, limit int) ([]persistence.MessageRecord, error)
	ConsumeMessage(ctx context.Context, id string, consumedAt time.Time) error
}

type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
	Cancel(runID string) bool
}

type PipelineRunner interface {
	Start(ctx context.Context, pipelineRunID, pipelinePath, workspaceID string) error
}

type SchedulerControl interface {
	Start()
	Stop()
	TickNow(ctx context.Context) error
	Status() scheduler.Status
}

const (
	ScopeRunWrite         = "run.write"
	ScopeRunCancel        = "run.cancel"
	ScopePipelineWrite    = "pipeline.write"
	ScopeSchedulerControl = "scheduler.control"
	ScopeWorkspaceWrite   = "workspace.write"
	ScopeScheduleWrite    = "schedule.write"
	ScopeTriggerWrite     = "trigger.write"
	ScopeHumanGateWrite   = "human_gate.write"
	ScopeMessageWrite     = "message.write"
)

func AllScopes() []string {
	return []string{
		ScopeRunWrite,
		ScopeRunCancel,
		ScopePipelineWrite,
		ScopeSchedulerControl,
		ScopeWorkspaceWrite,
		ScopeScheduleWrite,
		ScopeTriggerWrite,
		ScopeHumanGateWrite,
		ScopeMessageWrite,
	}
}

func messageRecordEnvelope(rec persistence.MessageRecord) (map[string]any, error) {
	from, err := messaging.ParseURN(rec.FromURN)
	if err != nil {
		return nil, err
	}
	to, err := messaging.ParseURN(rec.ToURN)
	if err != nil {
		return nil, err
	}
	var payload any
	if strings.TrimSpace(rec.PayloadJSON) != "" {
		_ = json.Unmarshal([]byte(rec.PayloadJSON), &payload)
	}
	var metadata map[string]string
	if strings.TrimSpace(rec.MetadataJSON) != "" {
		_ = json.Unmarshal([]byte(rec.MetadataJSON), &metadata)
	}
	out := map[string]any{
		"id":             rec.ID,
		"substrate":      rec.Substrate,
		"kind":           rec.Kind,
		"channel":        rec.Channel,
		"from":           from.URN(),
		"to":             to.URN(),
		"thread_id":      rec.ThreadID,
		"in_reply_to":    rec.InReplyTo,
		"correlation_id": rec.CorrelationID,
		"payload":        payload,
		"content_type":   rec.ContentType,
		"metadata":       metadata,
		"created_at":     rec.CreatedAt.Format(time.RFC3339Nano),
	}
	if rec.DeliveredAt.Valid {
		out["delivered_at"] = rec.DeliveredAt.String
	}
	if rec.ConsumedAt.Valid {
		out["consumed_at"] = rec.ConsumedAt.String
	}
	if rec.CanceledAt.Valid {
		out["canceled_at"] = rec.CanceledAt.String
	}
	return out, nil
}

// Adapter exposes Hadron read/query surfaces as MCP tools over stdio.
type Adapter struct {
	store        Store
	runner       Runner
	sched        SchedulerControl
	pipeline     PipelineRunner
	registry     *registry.Registry
	token        string
	scopes       map[string]struct{}
	blueprintDir string
	sessionID    string // unique ID for this MCP session, used for trigger ownership
}

func New(store Store, runner Runner, sched SchedulerControl, pipeline PipelineRunner, token string, scopes []string, opts ...Option) *Adapter {
	scopeSet := map[string]struct{}{}
	for _, s := range scopes {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		scopeSet[s] = struct{}{}
	}
	a := &Adapter{
		store:        store,
		runner:       runner,
		sched:        sched,
		pipeline:     pipeline,
		token:        strings.TrimSpace(token),
		scopes:       scopeSet,
		blueprintDir: settings.DefaultBlueprintDir(),
		sessionID:    fmt.Sprintf("mcp-%s", time.Now().UTC().Format("20060102-150405")),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Option configures optional Adapter fields.
type Option func(*Adapter)

// WithBlueprintDir sets the directory used for blueprint listing and reading.
func WithBlueprintDir(dir string) Option {
	return func(a *Adapter) {
		if dir != "" {
			a.blueprintDir = dir
		}
	}
}

// WithRegistry sets the blueprint registry for registry MCP tools.
func WithRegistry(reg *registry.Registry) Option {
	return func(a *Adapter) {
		a.registry = reg
	}
}

// CallTool invokes a registered tool by name and returns its result.
// Primarily used in tests.
func (a *Adapter) CallTool(ctx context.Context, toolName string, args map[string]any) *mcp.CallToolResult {
	req := mcp.CallToolRequest{}
	req.Params.Name = toolName
	if args != nil {
		req.Params.Arguments = args
	}

	handlers := a.buildHandlerMap()
	handler, ok := handlers[toolName]
	if !ok {
		return toolError("not_found", "unknown tool: "+toolName)
	}
	result, err := handler(ctx, req)
	if err != nil {
		return toolError("internal_error", err.Error())
	}
	return result
}

// buildHandlerMap returns a map of tool name → handler function for direct invocation.
func (a *Adapter) buildHandlerMap() map[string]func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return map[string]func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error){
		"hadron_skills":             a.handleHadronSkills,
		"hadron_health":             a.handleHealth,
		"hadron_workspaces_list":    a.handleWorkspacesList,
		"hadron_workspace_get":      a.handleWorkspaceGet,
		"hadron_workspace_create":   a.handleWorkspaceCreate,
		"hadron_runs_list":          a.handleRunsList,
		"hadron_run_get":            a.handleRunGet,
		"hadron_run_enqueue":        a.handleRunEnqueue,
		"hadron_run_cancel":         a.handleRunCancel,
		"hadron_run_events":         a.handleRunEvents,
		"hadron_run_operations":     a.handleRunOperations,
		"hadron_run_mcp_calls":      a.handleRunMCPCalls,
		"hadron_schedules_list":     a.handleSchedulesList,
		"hadron_schedule_create":    a.handleScheduleCreate,
		"hadron_schedule_update":    a.handleScheduleUpdate,
		"hadron_pipelines_list":     a.handlePipelinesList,
		"hadron_pipeline_enqueue":   a.handlePipelineEnqueue,
		"hadron_pipeline_stages":    a.handlePipelineStages,
		"hadron_pipeline_graph":     a.handlePipelineGraph,
		"hadron_blueprint_validate": a.handleBlueprintValidate,
		"hadron_blueprint_lint":     a.handleBlueprintLint,
		"hadron_blueprints_list":    a.handleBlueprintsList,
		"hadron_blueprint_get":      a.handleBlueprintGet,
		"hadron_blueprint_discover": a.handleBlueprintDiscover,
		"hadron_blueprint_broker":   a.handleBlueprintBroker,
		"hadron_blueprint_search":   a.handleBlueprintSearch,
		"hadron_blueprint_schema":   a.handleBlueprintSchema,
		"hadron_schedule_delete":    a.handleScheduleDelete,
		"hadron_triggers_list":      a.handleTriggersList,
		"hadron_trigger_create":     a.handleTriggerCreate,
		"hadron_trigger_delete":     a.handleTriggerDelete,
		"hadron_trigger_watch":      a.handleTriggerWatch,
		"hadron_trigger_list_mine":  a.handleTriggerListMine,
		"hadron_human_gate_get":     a.handleHumanGateGet,
		"hadron_human_gate_submit":  a.handleHumanGateSubmit,
		"hadron_message_send":       a.handleMessageSend,
		"hadron_messages_inbox":     a.handleMessagesInbox,
		"hadron_message_get":        a.handleMessageGet,
		"hadron_message_consume":    a.handleMessageConsume,
	}
}

func (a *Adapter) Run(ctx context.Context) error {
	s := a.newServer()
	ctxFunc := func(_ context.Context) context.Context { return ctx }
	return server.ServeStdio(s, server.WithStdioContextFunc(ctxFunc))
}

func toolJSON(v any) *mcp.CallToolResult {
	body, _ := json.Marshal(v)
	return mcp.NewToolResultText(string(body))
}

func toolError(code, message string) *mcp.CallToolResult {
	body, _ := json.Marshal(map[string]string{"code": code, "message": message})
	return mcp.NewToolResultText(string(body))
}

func (a *Adapter) checkScope(scope string) *mcp.CallToolResult {
	if strings.TrimSpace(a.token) == "" {
		return toolError("auth_required", "no token configured for mutating tools")
	}
	if _, ok := a.scopes[scope]; !ok {
		return toolError("insufficient_scope", "token missing scope: "+scope)
	}
	return nil
}
