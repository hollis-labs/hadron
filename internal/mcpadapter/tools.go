package mcpadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/lint"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var runSeq uint64
var pipelineSeq uint64
var messageSeq uint64

func (a *Adapter) registerTools(s *server.MCPServer) {
	a.registerSkillsTool(s)

	s.AddTool(mcp.NewTool("hadron_health",
		mcp.WithDescription("Read Hadron MCP adapter health/status."),
	), a.handleHealth)

	s.AddTool(mcp.NewTool("hadron_workspaces_list",
		mcp.WithDescription("List all Hadron workspaces."),
	), a.handleWorkspacesList)

	s.AddTool(mcp.NewTool("hadron_workspace_get",
		mcp.WithDescription("Get a workspace by id."),
		mcp.WithString("workspace_id", mcp.Required(), mcp.Description("Workspace id")),
	), a.handleWorkspaceGet)

	s.AddTool(mcp.NewTool("hadron_workspace_create",
		mcp.WithDescription("Create a workspace (requires scope workspace.write)."),
		mcp.WithString("workspace_id", mcp.Required(), mcp.Description("Workspace id")),
		mcp.WithString("name", mcp.Description("Workspace display name")),
	), a.handleWorkspaceCreate)

	s.AddTool(mcp.NewTool("hadron_runs_list",
		mcp.WithDescription("List runs for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handleRunsList)

	s.AddTool(mcp.NewTool("hadron_run_get",
		mcp.WithDescription("Get a run by id."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handleRunGet)

	s.AddTool(mcp.NewTool("hadron_run_enqueue",
		mcp.WithDescription("Enqueue a blueprint run (requires scope run.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint path")),
		mcp.WithString("inputs_json", mcp.Description("JSON object string for inputs")),
	), a.handleRunEnqueue)

	s.AddTool(mcp.NewTool("hadron_run_cancel",
		mcp.WithDescription("Cancel a running run (requires scope run.cancel)."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
	), a.handleRunCancel)

	s.AddTool(mcp.NewTool("hadron_run_events",
		mcp.WithDescription("List run events for a run id."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handleRunEvents)

	s.AddTool(mcp.NewTool("hadron_run_operations",
		mcp.WithDescription("Summarize operation diagnostics for a run across MCP, HTTP, agent launch, and message wait primitives."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
		mcp.WithString("kind", mcp.Description("Optional operation kind filter: mcp_call, http_call, message_wait, or agent_launch")),
		mcp.WithString("cursor", mcp.Description("Optional pagination cursor returned by a previous call")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handleRunOperations)

	s.AddTool(mcp.NewTool("hadron_run_mcp_calls",
		mcp.WithDescription("Summarize MCP call diagnostics for a run."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handleRunMCPCalls)

	s.AddTool(mcp.NewTool("hadron_schedules_list",
		mcp.WithDescription("List schedules for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handleSchedulesList)

	s.AddTool(mcp.NewTool("hadron_schedule_create",
		mcp.WithDescription("Create a schedule (requires scope schedule.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("name", mcp.Description("Schedule display name")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint path")),
		mcp.WithString("cron_expr", mcp.Required(), mcp.Description("Cron expression (standard 5-field)")),
		mcp.WithBoolean("enabled", mcp.Description("Whether schedule is enabled (default true)")),
	), a.handleScheduleCreate)

	s.AddTool(mcp.NewTool("hadron_schedule_update",
		mcp.WithDescription("Update schedule enabled state (requires scope schedule.write)."),
		mcp.WithString("schedule_id", mcp.Required(), mcp.Description("Schedule id")),
		mcp.WithBoolean("enabled", mcp.Required(), mcp.Description("Enable or disable the schedule")),
	), a.handleScheduleUpdate)

	s.AddTool(mcp.NewTool("hadron_pipelines_list",
		mcp.WithDescription("List pipeline runs for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handlePipelinesList)

	s.AddTool(mcp.NewTool("hadron_pipeline_enqueue",
		mcp.WithDescription("Start a pipeline run (requires scope pipeline.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("pipeline_path", mcp.Required(), mcp.Description("Pipeline spec path")),
	), a.handlePipelineEnqueue)

	s.AddTool(mcp.NewTool("hadron_pipeline_stages",
		mcp.WithDescription("List stages for a pipeline run."),
		mcp.WithString("pipeline_run_id", mcp.Required(), mcp.Description("Pipeline run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handlePipelineStages)

	s.AddTool(mcp.NewTool("hadron_pipeline_graph",
		mcp.WithDescription("Get the DAG graph (nodes + edges) for a pipeline run. Includes stage positions, status, and dependency edges."),
		mcp.WithString("pipeline_run_id", mcp.Required(), mcp.Description("Pipeline run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handlePipelineGraph)

	s.AddTool(mcp.NewTool("hadron_blueprint_validate",
		mcp.WithDescription("Validate blueprint YAML/JSON content."),
		mcp.WithString("content", mcp.Required(), mcp.Description("Blueprint YAML or JSON content")),
	), a.handleBlueprintValidate)

	s.AddTool(mcp.NewTool("hadron_blueprints_list",
		mcp.WithDescription("List available blueprint files (recursive). Optionally filter by tag."),
		mcp.WithString("tag", mcp.Description("Filter by blueprint tag (e.g. 'audit', 'build')")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 25)")),
	), a.handleBlueprintsList)

	s.AddTool(mcp.NewTool("hadron_blueprint_get",
		mcp.WithDescription("Read a blueprint file's YAML content by path."),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Path to the blueprint file")),
	), a.handleBlueprintGet)

	a.registerBlueprintDiscoveryTools(s)

	s.AddTool(mcp.NewTool("hadron_schedule_delete",
		mcp.WithDescription("Delete a schedule by id (requires scope schedule.write)."),
		mcp.WithString("schedule_id", mcp.Required(), mcp.Description("Schedule id")),
	), a.handleScheduleDelete)

	s.AddTool(mcp.NewTool("hadron_blueprint_lint",
		mcp.WithDescription("Lint a blueprint or pipeline file for best-practice issues (unused inputs, missing timeouts, duplicate step names, template syntax errors, etc)."),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Path to the blueprint or pipeline file to lint")),
	), a.handleBlueprintLint)

	s.AddTool(mcp.NewTool("hadron_agent_card",
		mcp.WithDescription("Generate an A2A Agent Card JSON document. Returns card for a single blueprint or all blueprints in the configured directory."),
		mcp.WithString("blueprint_path", mcp.Description("Path to a single blueprint file. Omit to generate a composite card from all blueprints.")),
		mcp.WithString("url", mcp.Description("Base URL for the agent (default: http://localhost:8095)")),
	), a.handleAgentCard)

	// Triggers
	s.AddTool(mcp.NewTool("hadron_triggers_list",
		mcp.WithDescription("List all webhook triggers."),
	), a.handleTriggersList)

	s.AddTool(mcp.NewTool("hadron_trigger_create",
		mcp.WithDescription("Create a webhook trigger (requires scope trigger.write)."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Trigger display name")),
		mcp.WithString("path", mcp.Required(), mcp.Description("Webhook path (e.g. 'deploy-prod')")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint file path")),
		mcp.WithString("secret", mcp.Description("HMAC-SHA256 secret for signature validation")),
		mcp.WithString("extract_inputs", mcp.Description("JSON config mapping input names to body/header/query paths")),
		mcp.WithBoolean("one_shot", mcp.Description("Delete trigger after first firing (default false)")),
		mcp.WithNumber("ttl_minutes", mcp.Description("Auto-expire trigger after N minutes")),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
	), a.handleTriggerCreate)

	s.AddTool(mcp.NewTool("hadron_trigger_delete",
		mcp.WithDescription("Delete a webhook trigger by ID (requires scope trigger.write)."),
		mcp.WithString("trigger_id", mcp.Required(), mcp.Description("Trigger id")),
	), a.handleTriggerDelete)

	s.AddTool(mcp.NewTool("hadron_trigger_watch",
		mcp.WithDescription("Create a temporary trigger with TTL for agent use. Supports webhook and file_watch types (requires scope trigger.write)."),
		mcp.WithString("type", mcp.Required(), mcp.Description("Trigger type: 'webhook' or 'file_watch'")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint file path to run when triggered")),
		mcp.WithString("config", mcp.Required(), mcp.Description("JSON config: for webhook {\"path\":\"my-hook\",\"name\":\"My Hook\"}, for file_watch {\"paths\":[\"/dir\"],\"name\":\"Watch\",\"events\":\"create,modify\",\"debounce\":5}")),
		mcp.WithNumber("ttl_minutes", mcp.Required(), mcp.Description("TTL in minutes (max 1440 = 24h)")),
		mcp.WithBoolean("one_shot", mcp.Description("Delete trigger after first firing (default true)")),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
	), a.handleTriggerWatch)

	s.AddTool(mcp.NewTool("hadron_trigger_list_mine",
		mcp.WithDescription("List triggers created by the current MCP session."),
	), a.handleTriggerListMine)

	s.AddTool(mcp.NewTool("hadron_human_gate_get",
		mcp.WithDescription("Get a human gate by id."),
		mcp.WithString("gate_id", mcp.Required(), mcp.Description("Human gate id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handleHumanGateGet)

	s.AddTool(mcp.NewTool("hadron_human_gate_submit",
		mcp.WithDescription("Submit a decision for a waiting human gate (requires scope human_gate.write)."),
		mcp.WithString("gate_id", mcp.Required(), mcp.Description("Human gate id")),
		mcp.WithString("decision", mcp.Required(), mcp.Description("Decision option id to submit")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handleHumanGateSubmit)

	s.AddTool(mcp.NewTool("hadron_message_send",
		mcp.WithDescription("Store a message envelope for a configured message substrate (requires scope message.write)."),
		mcp.WithString("substrate", mcp.Required(), mcp.Description("Message substrate id from settings.json")),
		mcp.WithString("kind", mcp.Required(), mcp.Description("Message kind: request, response, notice, status_update, handoff, or escalation")),
		mcp.WithString("from", mcp.Required(), mcp.Description("Sender URN, e.g. msg://agent/local/reviewer")),
		mcp.WithString("to", mcp.Required(), mcp.Description("Recipient URN, e.g. msg://agent/local/reviewer")),
		mcp.WithString("thread_id", mcp.Description("Optional thread id")),
		mcp.WithString("in_reply_to", mcp.Description("Optional parent message id")),
		mcp.WithString("payload_json", mcp.Description("Optional JSON payload string")),
		mcp.WithString("content_type", mcp.Description("Optional content type (default application/json)")),
		mcp.WithString("metadata_json", mcp.Description("Optional JSON object string for metadata")),
	), a.handleMessageSend)

	s.AddTool(mcp.NewTool("hadron_messages_inbox",
		mcp.WithDescription("List messages for a recipient and optional correlation filter."),
		mcp.WithString("substrate", mcp.Required(), mcp.Description("Message substrate id from settings.json")),
		mcp.WithString("to", mcp.Required(), mcp.Description("Recipient URN")),
		mcp.WithString("correlation_id", mcp.Description("Optional thread / reply / correlation filter")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 100)")),
	), a.handleMessagesInbox)

	s.AddTool(mcp.NewTool("hadron_messages_list",
		mcp.WithDescription("List messages non-destructively for a recipient and optional correlation filter."),
		mcp.WithString("substrate", mcp.Required(), mcp.Description("Message substrate id from settings.json")),
		mcp.WithString("to", mcp.Required(), mcp.Description("Recipient URN")),
		mcp.WithString("correlation_id", mcp.Description("Optional thread / reply / correlation filter")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 100)")),
	), a.handleMessagesList)

	s.AddTool(mcp.NewTool("hadron_messages_thread",
		mcp.WithDescription("List messages in a thread or correlation group."),
		mcp.WithString("substrate", mcp.Required(), mcp.Description("Message substrate id from settings.json")),
		mcp.WithString("thread_id", mcp.Required(), mcp.Description("Thread id or correlation id")),
		mcp.WithNumber("limit", mcp.Description("Max items to return (default 10, max 100)")),
	), a.handleMessagesThread)

	s.AddTool(mcp.NewTool("hadron_message_get",
		mcp.WithDescription("Get a single stored message by id."),
		mcp.WithString("substrate", mcp.Description("Optional message substrate id to scope lookup")),
		mcp.WithString("message_id", mcp.Required(), mcp.Description("Message id")),
	), a.handleMessageGet)

	s.AddTool(mcp.NewTool("hadron_message_consume",
		mcp.WithDescription("Mark a stored message consumed (requires scope message.write)."),
		mcp.WithString("substrate", mcp.Description("Optional message substrate id to scope lookup")),
		mcp.WithString("message_id", mcp.Required(), mcp.Description("Message id")),
	), a.handleMessageConsume)

	// Register blueprint registry tools
	registerRegistryTools(s, a.registry)
}

func (a *Adapter) handleHealth(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return toolJSON(map[string]any{
		"status":    "ok",
		"service":   "hadron-mcp",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleWorkspacesList(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	items, err := a.store.ListWorkspaces(ctx)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, w := range items {
		out = append(out, map[string]any{
			"id":         w.ID,
			"name":       w.Name,
			"created_at": w.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at": w.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleWorkspaceGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := strings.TrimSpace(req.GetString("workspace_id", ""))
	if id == "" {
		return toolError("validation_error", "workspace_id is required"), nil
	}
	w, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":         w.ID,
		"name":       w.Name,
		"created_at": w.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": w.UpdatedAt.UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleWorkspaceCreate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeWorkspaceWrite); deny != nil {
		return deny, nil
	}
	id := strings.TrimSpace(req.GetString("workspace_id", ""))
	if id == "" {
		return toolError("validation_error", "workspace_id is required"), nil
	}
	name := strings.TrimSpace(req.GetString("name", ""))
	if name == "" {
		name = id
	}
	if err := a.store.CreateWorkspace(ctx, id, name); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	rec, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":         rec.ID,
		"name":       rec.Name,
		"created_at": rec.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": rec.UpdatedAt.UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleRunsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, r := range items {
		out = append(out, map[string]any{
			"id":         r.ID,
			"blueprint":  r.BlueprintPath,
			"status":     r.Status,
			"created_at": r.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d runs found. Use hadron_run_get with a specific run_id for full details including error messages.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleRunGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	rec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	var inputs map[string]any
	if rec.InputJSON != "" {
		_ = json.Unmarshal([]byte(rec.InputJSON), &inputs)
	}
	return toolJSON(map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"blueprint_path": rec.BlueprintPath,
		"status":         rec.Status,
		"inputs":         inputs,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":     nullString(rec.StartedAt),
		"ended_at":       nullString(rec.EndedAt),
		"error_message":  nullString(rec.ErrorMessage),
	}), nil
}

func (a *Adapter) handleRunEnqueue(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeRunWrite); deny != nil {
		return deny, nil
	}
	if a.runner == nil {
		return toolError("unavailable", "runner unavailable"), nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	if _, err := a.store.GetWorkspace(ctx, workspaceID); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	bp, err := blueprint.ParseFile(blueprintPath)
	if err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	inputsRaw := strings.TrimSpace(req.GetString("inputs_json", ""))
	inputs := map[string]any{}
	if inputsRaw != "" {
		if unmarshalErr := json.Unmarshal([]byte(inputsRaw), &inputs); unmarshalErr != nil {
			return toolError("validation_error", "inputs_json must be a JSON object"), nil //nolint:nilerr
		}
	}
	normalized, err := blueprint.NormalizeInputs(bp, inputs)
	if err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	runID := fmt.Sprintf("mcp-run-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	if err := a.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   workspaceID,
		BlueprintPath: blueprintPath,
		Inputs:        normalized,
	}); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"run_id": runID, "status": "queued", "workspace_id": workspaceID}), nil
}

func (a *Adapter) handleRunCancel(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeRunCancel); deny != nil {
		return deny, nil
	}
	if a.runner == nil {
		return toolError("unavailable", "runner unavailable"), nil
	}
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	if ok := a.runner.Cancel(runID); !ok {
		return toolError("not_found", "run not running"), nil
	}
	return toolJSON(map[string]any{"run_id": runID, "status": "cancellation_requested"}), nil
}

func (a *Adapter) handleRunEvents(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	runRec, getRunErr := a.store.GetRun(ctx, runID)
	if getRunErr != nil {
		if isNotFound(getRunErr) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", getRunErr.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListRunEvents(ctx, runID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, ev := range items {
		out = append(out, map[string]any{
			"id":         ev.ID,
			"run_id":     ev.RunID,
			"step_name":  nullString(ev.StepName),
			"event_type": ev.EventType,
			"message":    nullString(ev.Message),
			"created_at": ev.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d events found. Increase limit parameter for more events.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleRunMCPCalls(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	runRec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	events, listEventsErr := a.store.ListRunEvents(ctx, runID, 1000)
	if listEventsErr != nil {
		return toolError("internal_error", listEventsErr.Error()), nil
	}
	items := rundiagnostics.SummarizeMCPCalls(events)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"sequence":      item.Sequence,
			"step_name":     item.StepName,
			"server":        item.Server,
			"tool":          item.Tool,
			"transport":     item.Transport,
			"status":        item.Status,
			"retry_count":   item.RetryCount,
			"attempt_count": item.AttemptCount,
			"reused_client": item.ReusedClient,
			"health_probe":  item.HealthProbe,
			"reconnected":   item.Reconnected,
			"truncated":     item.Truncated,
			"result_json":   item.ResultJSON,
			"error_message": item.ErrorMessage,
			"started_at":    item.StartedAt,
			"finished_at":   item.FinishedAt,
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleRunOperations(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	runRec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	events, err := a.store.ListRunEvents(ctx, runID, 1000)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := rundiagnostics.SummarizeOperations(events)
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	kind := strings.TrimSpace(req.GetString("kind", ""))
	cursor := strings.TrimSpace(req.GetString("cursor", ""))
	page, nextCursor, totalCount, ok := filterPagedOperations(items, kind, limit, cursor)
	if !ok {
		return toolError("validation_error", "invalid cursor"), nil
	}
	out := make([]map[string]any, 0, len(page))
	for _, item := range page {
		out = append(out, map[string]any{
			"sequence":         item.Sequence,
			"kind":             item.Kind,
			"step_name":        item.StepName,
			"status":           item.Status,
			"started_at":       item.StartedAt,
			"finished_at":      item.FinishedAt,
			"error_message":    item.ErrorMessage,
			"truncated":        item.Truncated,
			"result_json":      item.ResultJSON,
			"server":           item.Server,
			"tool":             item.Tool,
			"transport":        item.Transport,
			"retry_count":      item.RetryCount,
			"attempt_count":    item.AttemptCount,
			"reused_client":    item.ReusedClient,
			"health_probe":     item.HealthProbe,
			"reconnected":      item.Reconnected,
			"method":           item.Method,
			"url":              item.URL,
			"status_code":      item.StatusCode,
			"duration_ms":      item.DurationMS,
			"substrate":        item.Substrate,
			"to":               item.To,
			"correlation_id":   item.CorrelationID,
			"timeout_ms":       item.TimeoutMS,
			"poll_count":       item.PollCount,
			"message_id":       item.MessageID,
			"logical_agent_id": item.LogicalAgentID,
			"launch_id":        item.LaunchID,
			"gate_id":          item.GateID,
			"decision":         item.Decision,
			"prompt":           item.Prompt,
		})
	}
	var next any
	if nextCursor != "" {
		next = nextCursor
	}
	return toolJSON(map[string]any{
		"items":       out,
		"count":       len(out),
		"total_count": totalCount,
		"next_cursor": next,
	}), nil
}

func (a *Adapter) handleSchedulesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListSchedulesByWorkspace(ctx, workspaceID)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, sc := range items {
		out = append(out, map[string]any{
			"id":             sc.ID,
			"workspace_id":   sc.WorkspaceID,
			"name":           sc.Name,
			"blueprint_path": sc.BlueprintPath,
			"cron_expr":      sc.CronExpr,
			"enabled":        sc.Enabled,
			"created_at":     sc.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at":     sc.UpdatedAt.UTC().Format(time.RFC3339),
			"last_run_at":    nullString(sc.LastRunAt),
			"next_run_at":    nullString(sc.NextRunAt),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d schedules found. Use schedule IDs to manage individual schedules.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleScheduleCreate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	cronExpr := strings.TrimSpace(req.GetString("cron_expr", ""))
	if cronExpr == "" {
		return toolError("validation_error", "cron_expr is required"), nil
	}
	if err := scheduler.ValidateCron(cronExpr); err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	name := strings.TrimSpace(req.GetString("name", ""))
	if name == "" {
		name = blueprintPath
	}
	enabled := req.GetBool("enabled", true)

	nextRun, err := scheduler.NextRun(cronExpr, time.Now())
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}

	now := time.Now().UTC()
	schedID := fmt.Sprintf("mcp-sched-%s", time.Now().UTC().Format("20060102-150405"))
	rec := persistence.ScheduleRecord{
		ID:            schedID,
		WorkspaceID:   workspaceID,
		Name:          name,
		BlueprintPath: blueprintPath,
		CronExpr:      cronExpr,
		Enabled:       enabled,
		CreatedAt:     now,
		UpdatedAt:     now,
		NextRunAt: sql.NullString{
			String: nextRun.UTC().Format(time.RFC3339),
			Valid:  true,
		},
	}
	if err := a.store.CreateSchedule(ctx, rec); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"name":           rec.Name,
		"blueprint_path": rec.BlueprintPath,
		"cron_expr":      rec.CronExpr,
		"enabled":        rec.Enabled,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleScheduleUpdate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	scheduleID := strings.TrimSpace(req.GetString("schedule_id", ""))
	if scheduleID == "" {
		return toolError("validation_error", "schedule_id is required"), nil
	}
	enabled := req.GetBool("enabled", true)
	if err := a.store.UpdateScheduleEnabledAndNext(ctx, scheduleID, enabled, nil); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"schedule_id": scheduleID, "enabled": enabled}), nil
}

func (a *Adapter) handlePipelinesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListPipelineRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, map[string]any{
			"id":            p.ID,
			"pipeline_path": p.PipelinePath,
			"status":        p.Status,
			"created_at":    p.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d pipeline runs found. Use hadron_pipeline_stages with a specific pipeline_run_id for full details.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handlePipelineEnqueue(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopePipelineWrite); deny != nil {
		return deny, nil
	}
	if a.pipeline == nil {
		return toolError("unavailable", "pipeline runner unavailable"), nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	if _, err := a.store.GetWorkspace(ctx, workspaceID); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	pipelinePath := strings.TrimSpace(req.GetString("pipeline_path", ""))
	if pipelinePath == "" {
		return toolError("validation_error", "pipeline_path is required"), nil
	}
	if _, err := pipeline.ParseFile(pipelinePath); err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	id := fmt.Sprintf("mcp-pl-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&pipelineSeq, 1))
	if err := a.pipeline.Start(ctx, id, pipelinePath, workspaceID); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"pipeline_run_id": id, "status": "queued", "workspace_id": workspaceID}), nil
}

func (a *Adapter) handlePipelineStages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := strings.TrimSpace(req.GetString("pipeline_run_id", ""))
	if id == "" {
		return toolError("validation_error", "pipeline_run_id is required"), nil
	}
	runRec, err := a.store.GetPipelineRun(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "pipeline run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "pipeline run not found in workspace"), nil
	}
	items, err := a.store.ListPipelineStageRuns(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}

	// Try to load pipeline spec for DAG metadata enrichment.
	specStages := parsePipelineSpecStages(runRec.PipelinePath)

	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		entry := map[string]any{
			"id":              st.ID,
			"workspace_id":    st.WorkspaceID,
			"pipeline_run_id": st.PipelineRunID,
			"stage_index":     st.StageIndex,
			"stage_name":      st.StageName,
			"run_id":          st.RunID,
			"status":          st.Status,
			"created_at":      st.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at":      st.UpdatedAt.UTC().Format(time.RFC3339),
		}
		if spec, ok := specStages[st.StageName]; ok {
			entry["depends_on"] = spec.DependsOn
			if spec.Position != nil {
				entry["position"] = map[string]any{"x": spec.Position.X, "y": spec.Position.Y}
			} else {
				entry["position"] = nil
			}
			entry["outputs"] = spec.Outputs
		} else {
			entry["depends_on"] = nil
			entry["position"] = nil
			entry["outputs"] = nil
		}
		out = append(out, entry)
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handlePipelineGraph(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := strings.TrimSpace(req.GetString("pipeline_run_id", ""))
	if id == "" {
		return toolError("validation_error", "pipeline_run_id is required"), nil
	}
	runRec, err := a.store.GetPipelineRun(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "pipeline run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "pipeline run not found in workspace"), nil
	}

	// Load stage run records for status.
	stageRuns, err := a.store.ListPipelineStageRuns(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	statusMap := make(map[string]string, len(stageRuns))
	for _, sr := range stageRuns {
		statusMap[sr.StageName] = sr.Status
	}

	// Parse pipeline spec for DAG structure.
	spec, parseErr := pipeline.ParseFile(runRec.PipelinePath)
	if parseErr != nil {
		return toolError("internal_error", "cannot parse pipeline spec: "+parseErr.Error()), nil //nolint:nilerr
	}

	nodes := make([]map[string]any, 0, len(spec.Stages))
	edges := make([]map[string]any, 0)

	for _, stage := range spec.Stages {
		status := statusMap[stage.Name]
		if status == "" {
			status = "pending"
		}

		var pos map[string]any
		if stage.Position != nil {
			pos = map[string]any{"x": stage.Position.X, "y": stage.Position.Y}
		}

		outputs := map[string]string{}
		if stage.Outputs != nil {
			outputs = stage.Outputs
		}

		nodes = append(nodes, map[string]any{
			"id":             stage.Name,
			"name":           stage.Name,
			"blueprint_path": stage.BlueprintPath,
			"position":       pos,
			"status":         status,
			"outputs":        outputs,
		})

		for _, dep := range stage.DependsOn {
			edges = append(edges, map[string]any{
				"source":    dep,
				"target":    stage.Name,
				"condition": stage.If,
			})
		}
	}

	return toolJSON(map[string]any{
		"nodes": nodes,
		"edges": edges,
	}), nil
}

// parsePipelineSpecStages attempts to parse a pipeline spec file and returns
// a map of stage name → Stage for DAG metadata enrichment. Returns nil on
// any parse error so callers can degrade gracefully.
func parsePipelineSpecStages(pipelinePath string) map[string]pipeline.Stage {
	if pipelinePath == "" {
		return nil
	}
	spec, err := pipeline.ParseFile(pipelinePath)
	if err != nil {
		return nil
	}
	m := make(map[string]pipeline.Stage, len(spec.Stages))
	for _, st := range spec.Stages {
		m[st.Name] = st
	}
	return m
}

func (a *Adapter) handleBlueprintLint(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bpPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if bpPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}

	rawContent, err := os.ReadFile(bpPath) // #nosec G304 -- MCP validate intentionally reads a caller-provided blueprint path.
	if err != nil {
		if os.IsNotExist(err) {
			return toolError("not_found", "file not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}

	var issues []lint.Issue

	// Try blueprint first, then pipeline.
	bp, bpErr := blueprint.ParseFile(bpPath)
	if bpErr == nil {
		issues = lint.LintBlueprint(bp, bpPath, rawContent)
	} else {
		spec, pipeErr := pipeline.ParseFile(bpPath)
		if pipeErr == nil {
			issues = lint.LintPipeline(spec, bpPath, rawContent)
		} else {
			return toolJSON(map[string]any{
				"path":   bpPath,
				"valid":  false,
				"error":  bpErr.Error(),
				"issues": []lint.Issue{},
			}), nil
		}
	}

	hasErrors := false
	for _, issue := range issues {
		if issue.Severity == lint.SeverityError {
			hasErrors = true
			break
		}
	}

	return toolJSON(map[string]any{
		"path":        bpPath,
		"valid":       !hasErrors,
		"issue_count": len(issues),
		"issues":      issues,
	}), nil
}

func (a *Adapter) handleBlueprintValidate(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	content := req.GetString("content", "")
	if strings.TrimSpace(content) == "" {
		return toolError("validation_error", "content is required"), nil
	}
	_, err := blueprint.ParseBytes([]byte(content))
	if err != nil {
		return toolJSON(map[string]any{"valid": false, "error": err.Error()}), nil //nolint:nilerr
	}
	return toolJSON(map[string]any{"valid": true}), nil
}

func (a *Adapter) handleBlueprintsList(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dir := a.blueprintDir
	tagFilter := strings.TrimSpace(strings.ToLower(req.GetString("tag", "")))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)

	var items []map[string]any
	walkErr := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// Listing is best-effort; skip inaccessible entries.
			return nil //nolint:nilerr
		}
		if d.IsDir() {
			return nil
		}
		ext := filepath.Ext(d.Name())
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		entry := map[string]any{
			"name": d.Name(),
			"path": path,
		}

		// Try parsing for metadata (name, tags only — summary view)
		bp, parseErr := blueprint.ParseFile(path)
		if parseErr == nil {
			entry["blueprint_name"] = bp.Spec.Name
			if len(bp.Spec.Tags) > 0 {
				entry["tags"] = bp.Spec.Tags
			}

			// Apply tag filter
			if tagFilter != "" {
				found := false
				for _, t := range bp.Spec.Tags {
					if strings.ToLower(t) == tagFilter {
						found = true
						break
					}
				}
				if !found {
					return nil // skip — tag doesn't match
				}
			}
		} else if tagFilter != "" {
			// If tag filter is set and we can't parse, skip
			return nil
		}

		items = append(items, entry)
		return nil
	})

	if walkErr != nil {
		if os.IsNotExist(walkErr) {
			env := budget.Apply([]map[string]any{}, budget.Config{Limit: limit}, "")
			return mcp.NewToolResultText(budget.ToolJSON(env)), nil
		}
		return toolError("internal_error", walkErr.Error()), nil
	}
	if items == nil {
		items = []map[string]any{}
	}
	env := budget.Apply(items, budget.Config{Limit: limit},
		"%d blueprints found. Use hadron_blueprint_get with a specific path for full details. Add tag filter to narrow results.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleBlueprintGet(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bpPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if bpPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	absPath, err := a.resolveBlueprintPath(bpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return toolError("not_found", "blueprint file not found"), nil
		}
		if strings.Contains(err.Error(), "outside") {
			return toolError("validation_error", err.Error()), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	data, err := os.ReadFile(absPath) // #nosec G304 -- path was validated to stay within the blueprint directory.
	if err != nil {
		if os.IsNotExist(err) {
			return toolError("not_found", "blueprint file not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"path":    absPath,
		"content": string(data),
	}), nil
}

func (a *Adapter) handleScheduleDelete(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	scheduleID := strings.TrimSpace(req.GetString("schedule_id", ""))
	if scheduleID == "" {
		return toolError("validation_error", "schedule_id is required"), nil
	}
	if err := a.store.DeleteSchedule(ctx, scheduleID); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"schedule_id": scheduleID, "deleted": true}), nil
}

func (a *Adapter) handleAgentCard(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bpPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	baseURL := strings.TrimSpace(req.GetString("url", ""))
	if baseURL == "" {
		baseURL = "http://localhost:8095"
	}

	if bpPath != "" {
		// Single blueprint mode
		bp, err := blueprint.ParseFile(bpPath)
		if err != nil {
			return toolError("validation_error", err.Error()), nil
		}
		skill := agentcard.SkillFromBlueprint(bp, bpPath)
		card := &agentcard.AgentCard{
			Name:               skill.Name,
			Description:        skill.Description,
			URL:                baseURL,
			Provider:           agentcard.Provider{Organization: "Hadron"},
			Version:            a.serverVersion,
			Capabilities:       agentcard.Capabilities{Streaming: false, PushNotifications: false},
			DefaultInputModes:  []string{"application/json"},
			DefaultOutputModes: []string{"application/json"},
			Skills:             []agentcard.Skill{skill},
		}
		data, err := card.JSON()
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		return mcp.NewToolResultText(string(data)), nil
	}

	// All blueprints mode
	dir := a.blueprintDir
	if dir == "" {
		return toolError("validation_error", "no blueprint directory configured"), nil
	}
	card, err := agentcard.FromDirectory(dir, baseURL)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	data, err := card.JSON()
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}

// ── Triggers ──────────────────────────────────────────────────────────────────

func (a *Adapter) handleTriggersList(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	items, err := a.store.ListTriggers(ctx)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, map[string]any{
			"id":             t.ID,
			"type":           t.Type,
			"name":           t.Name,
			"path":           t.Path,
			"blueprint_path": t.BlueprintPath,
			"workspace_id":   t.WorkspaceID,
			"enabled":        t.Enabled,
			"one_shot":       t.OneShot,
			"fired_count":    t.FiredCount,
			"created_at":     t.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleTriggerCreate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeTriggerWrite); deny != nil {
		return deny, nil
	}
	name := strings.TrimSpace(req.GetString("name", ""))
	if name == "" {
		return toolError("validation_error", "name is required"), nil
	}
	path := strings.TrimSpace(req.GetString("path", ""))
	if path == "" {
		return toolError("validation_error", "path is required"), nil
	}
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))

	triggerID := fmt.Sprintf("mcp-trig-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	rec := persistence.TriggerRecord{
		ID:            triggerID,
		Type:          "webhook",
		Name:          name,
		Path:          path,
		BlueprintPath: blueprintPath,
		WorkspaceID:   workspaceID,
		Enabled:       true,
		OneShot:       req.GetBool("one_shot", false),
	}
	if secret := strings.TrimSpace(req.GetString("secret", "")); secret != "" {
		rec.SecretHash = sql.NullString{String: secret, Valid: true}
	}
	if ei := strings.TrimSpace(req.GetString("extract_inputs", "")); ei != "" {
		rec.ExtractInputs = sql.NullString{String: ei, Valid: true}
	}
	ttlMinutes := req.GetFloat("ttl_minutes", 0)
	if ttlMinutes > 0 {
		expires := time.Now().UTC().Add(time.Duration(ttlMinutes) * time.Minute)
		rec.TTLExpiresAt = sql.NullString{String: expires.Format(time.RFC3339), Valid: true}
	}

	if err := a.store.CreateTrigger(ctx, rec); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"trigger_id":     triggerID,
		"name":           name,
		"path":           path,
		"blueprint_path": blueprintPath,
		"webhook_url":    "/hooks/" + path,
	}), nil
}

func (a *Adapter) handleTriggerWatch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeTriggerWrite); deny != nil {
		return deny, nil
	}

	trigType := strings.TrimSpace(req.GetString("type", ""))
	if trigType != "webhook" && trigType != "file_watch" {
		return toolError("validation_error", "type must be 'webhook' or 'file_watch'"), nil
	}
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	configJSON := strings.TrimSpace(req.GetString("config", ""))
	if configJSON == "" {
		return toolError("validation_error", "config is required"), nil
	}
	ttlMinutes := req.GetFloat("ttl_minutes", 0)
	if ttlMinutes <= 0 {
		return toolError("validation_error", "ttl_minutes is required and must be > 0"), nil
	}
	if ttlMinutes > 1440 {
		return toolError("validation_error", "ttl_minutes max is 1440 (24 hours)"), nil
	}
	oneShot := req.GetBool("one_shot", true)
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))

	// Parse config
	var cfg map[string]any
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return toolError("validation_error", "config must be valid JSON: "+err.Error()), nil //nolint:nilerr
	}

	triggerID := fmt.Sprintf("mcp-trig-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	expires := time.Now().UTC().Add(time.Duration(ttlMinutes) * time.Minute)

	rec := persistence.TriggerRecord{
		ID:            triggerID,
		Type:          trigType,
		BlueprintPath: blueprintPath,
		WorkspaceID:   workspaceID,
		Enabled:       true,
		OneShot:       oneShot,
		TTLExpiresAt:  sql.NullString{String: expires.Format(time.RFC3339), Valid: true},
		CreatedBy:     a.sessionID,
	}

	// Extract type-specific fields from config
	name, _ := cfg["name"].(string)
	if name == "" {
		name = trigType + "-watch"
	}
	rec.Name = name

	switch trigType {
	case "webhook":
		path, _ := cfg["path"].(string)
		if path == "" {
			return toolError("validation_error", "config.path is required for webhook triggers"), nil
		}
		rec.Path = path
	case "file_watch":
		// paths can be a JSON array or a single string
		switch v := cfg["paths"].(type) {
		case []any:
			pathsJSON, _ := json.Marshal(v)
			rec.Path = string(pathsJSON)
		case string:
			rec.Path = v
		default:
			return toolError("validation_error", "config.paths is required for file_watch triggers"), nil
		}

		// Optional debounce
		if d, ok := cfg["debounce"].(float64); ok && d > 0 {
			rec.DebounceSeconds = int(d)
		}

		// Store events filter in extract_inputs
		if events, ok := cfg["events"].(string); ok && events != "" {
			ei, _ := json.Marshal(map[string]string{"events": events})
			rec.ExtractInputs = sql.NullString{String: string(ei), Valid: true}
		}
	}

	if err := a.store.CreateTrigger(ctx, rec); err != nil {
		return toolError("internal_error", err.Error()), nil
	}

	result := map[string]any{
		"trigger_id":     triggerID,
		"type":           trigType,
		"name":           rec.Name,
		"blueprint_path": blueprintPath,
		"one_shot":       oneShot,
		"ttl_expires_at": expires.Format(time.RFC3339),
	}
	if trigType == "webhook" {
		result["webhook_url"] = "/hooks/" + rec.Path
	}
	return toolJSON(result), nil
}

func (a *Adapter) handleTriggerListMine(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	items, err := a.store.ListTriggersByCreatedBy(ctx, a.sessionID)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, map[string]any{
			"id":             t.ID,
			"type":           t.Type,
			"name":           t.Name,
			"path":           t.Path,
			"blueprint_path": t.BlueprintPath,
			"workspace_id":   t.WorkspaceID,
			"enabled":        t.Enabled,
			"one_shot":       t.OneShot,
			"fired_count":    t.FiredCount,
			"ttl_expires_at": nullString(t.TTLExpiresAt),
			"created_at":     t.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out), "session_id": a.sessionID}), nil
}

func (a *Adapter) handleTriggerDelete(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeTriggerWrite); deny != nil {
		return deny, nil
	}
	triggerID := strings.TrimSpace(req.GetString("trigger_id", ""))
	if triggerID == "" {
		return toolError("validation_error", "trigger_id is required"), nil
	}
	if err := a.store.DeleteTrigger(ctx, triggerID); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"trigger_id": triggerID, "deleted": true}), nil
}

func (a *Adapter) handleHumanGateGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	gateID := strings.TrimSpace(req.GetString("gate_id", ""))
	if gateID == "" {
		return toolError("validation_error", "gate_id is required"), nil
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "human gate not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "human gate not found in workspace"), nil
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(humanGateEnvelope(rec, options)), nil
}

func (a *Adapter) handleHumanGateSubmit(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeHumanGateWrite); deny != nil {
		return deny, nil
	}
	gateID := strings.TrimSpace(req.GetString("gate_id", ""))
	if gateID == "" {
		return toolError("validation_error", "gate_id is required"), nil
	}
	decision := strings.TrimSpace(req.GetString("decision", ""))
	if decision == "" {
		return toolError("validation_error", "decision is required"), nil
	}
	rec, err := a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "human gate not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "human gate not found in workspace"), nil
	}
	if rec.Status != "waiting" {
		return toolError("conflict", "human gate is not waiting"), nil
	}
	options, err := parseHumanGateOptions(rec.OptionsJSON)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	if !humanGateDecisionAllowed(decision, options) {
		return toolError("validation_error", "decision is not an allowed option"), nil
	}
	if submitErr := a.store.SubmitHumanGateDecision(ctx, gateID, decision, time.Now().UTC()); submitErr != nil {
		if isNotFound(submitErr) {
			return toolError("conflict", "human gate is not waiting or was not found"), nil
		}
		return toolError("internal_error", submitErr.Error()), nil
	}
	rec, err = a.store.GetHumanGate(ctx, gateID)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(humanGateEnvelope(rec, options)), nil
}

func (a *Adapter) handleMessageSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeMessageWrite); deny != nil {
		return deny, nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate == "" {
		return toolError("validation_error", "substrate is required"), nil
	}
	kind := messaging.Kind(strings.TrimSpace(req.GetString("kind", "")))
	if _, ok := validMCPMessageKinds[kind]; !ok {
		return toolError("validation_error", "invalid message kind"), nil
	}
	fromRaw := strings.TrimSpace(req.GetString("from", ""))
	toRaw := strings.TrimSpace(req.GetString("to", ""))
	from, ok := parseURNOK(fromRaw)
	if !ok {
		return toolError("validation_error", "invalid from URN"), nil
	}
	to, ok := parseURNOK(toRaw)
	if !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	payloadJSON := strings.TrimSpace(req.GetString("payload_json", ""))
	if payloadJSON == "" {
		payloadJSON = "null"
	}
	if !json.Valid([]byte(payloadJSON)) {
		return toolError("validation_error", "payload_json must be valid JSON"), nil
	}
	metadataJSON := strings.TrimSpace(req.GetString("metadata_json", ""))
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	metadata, ok := parseMetadataJSONMap(metadataJSON)
	if !ok {
		return toolError("validation_error", "metadata_json must be a JSON object"), nil
	}
	threadID := strings.TrimSpace(req.GetString("thread_id", ""))
	inReplyTo := strings.TrimSpace(req.GetString("in_reply_to", ""))
	correlationID := firstNonEmpty(metadata["correlation_id"], threadID, inReplyTo)
	messageID := fmt.Sprintf("msg-%s-%04d", time.Now().UTC().Format("20060102-150405"), messageSeqAdd())
	rec := persistence.MessageRecord{
		ID:            messageID,
		Substrate:     substrate,
		Kind:          string(kind),
		Channel:       strings.TrimSpace(req.GetString("channel", "")),
		FromURN:       from.URN(),
		ToURN:         to.URN(),
		ThreadID:      threadID,
		InReplyTo:     inReplyTo,
		CorrelationID: correlationID,
		PayloadJSON:   payloadJSON,
		ContentType:   defaultString(strings.TrimSpace(req.GetString("content_type", "")), "application/json"),
		MetadataJSON:  metadataJSON,
		CreatedAt:     time.Now().UTC(),
	}
	if createErr := a.store.CreateMessage(ctx, rec); createErr != nil {
		return toolError("internal_error", createErr.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
}

func (a *Adapter) handleMessagesInbox(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	toURN := strings.TrimSpace(req.GetString("to", ""))
	if substrate == "" || toURN == "" {
		return toolError("validation_error", "substrate and to are required"), nil
	}
	if _, ok := parseURNOK(toURN); !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipient(ctx, substrate, toURN, strings.TrimSpace(req.GetString("correlation_id", "")), limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessagesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	toURN := strings.TrimSpace(req.GetString("to", ""))
	if substrate == "" || toURN == "" {
		return toolError("validation_error", "substrate and to are required"), nil
	}
	if _, ok := parseURNOK(toURN); !ok {
		return toolError("validation_error", "invalid to URN"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByRecipientNonDestructive(ctx, substrate, toURN, strings.TrimSpace(req.GetString("correlation_id", "")), limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessagesThread(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	threadID := strings.TrimSpace(req.GetString("thread_id", ""))
	if substrate == "" || threadID == "" {
		return toolError("validation_error", "substrate and thread_id are required"), nil
	}
	limit := int(req.GetFloat("limit", 10))
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	recs, err := a.store.ListMessagesByThread(ctx, substrate, threadID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := make([]map[string]any, 0, len(recs))
	for _, rec := range recs {
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		items = append(items, env)
	}
	return toolJSON(map[string]any{"messages": items, "count": len(items)}), nil
}

func (a *Adapter) handleMessageGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	messageID := strings.TrimSpace(req.GetString("message_id", ""))
	if messageID == "" {
		return toolError("validation_error", "message_id is required"), nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return toolError("not_found", "message not found"), nil
		}
		if err != nil {
			if isNotFound(err) {
				return toolError("not_found", "message not found"), nil
			}
			return toolError("internal_error", err.Error()), nil
		}
		env, err := messageRecordEnvelope(rec)
		if err != nil {
			return toolError("internal_error", err.Error()), nil
		}
		return toolJSON(env), nil
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "message not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
}

func (a *Adapter) handleMessageConsume(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeMessageWrite); deny != nil {
		return deny, nil
	}
	messageID := strings.TrimSpace(req.GetString("message_id", ""))
	if messageID == "" {
		return toolError("validation_error", "message_id is required"), nil
	}
	substrate := strings.TrimSpace(req.GetString("substrate", ""))
	if substrate != "" {
		rec, err := a.store.GetMessage(ctx, messageID)
		if err == nil && rec.Substrate != substrate {
			return toolError("not_found", "message not found"), nil
		}
		if err != nil {
			if isNotFound(err) {
				return toolError("not_found", "message not found"), nil
			}
			return toolError("internal_error", err.Error()), nil
		}
	}
	if err := a.store.ConsumeMessage(ctx, messageID, time.Now().UTC()); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "message not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	rec, err := a.store.GetMessage(ctx, messageID)
	if err != nil {
		if isNotFound(err) {
			return toolJSON(map[string]any{"message_id": messageID, "status": "consumed"}), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	env, err := messageRecordEnvelope(rec)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(env), nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

var validMCPMessageKinds = map[messaging.Kind]struct{}{
	messaging.MsgKindRequest:      {},
	messaging.MsgKindResponse:     {},
	messaging.MsgKindNotice:       {},
	messaging.MsgKindStatusUpdate: {},
	messaging.MsgKindHandoff:      {},
	messaging.MsgKindEscalation:   {},
}

func workspaceDefault(workspaceID string) string {
	if strings.TrimSpace(workspaceID) == "" {
		return "default"
	}
	return strings.TrimSpace(workspaceID)
}

func messageSeqAdd() uint64 {
	return atomic.AddUint64(&messageSeq, 1)
}

func defaultString(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func nullString(s sql.NullString) any {
	if s.Valid {
		return s.String
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || strings.Contains(strings.ToLower(err.Error()), "no rows")
}

func filterPagedOperations(items []rundiagnostics.OperationDiagnostic, kind string, limit int, cursor string) ([]rundiagnostics.OperationDiagnostic, string, int, bool) {
	page, nextCursor, totalCount, err := rundiagnostics.FilterAndPageOperations(items, kind, limit, cursor)
	if err != nil {
		return nil, "", 0, false
	}
	return page, nextCursor, totalCount, true
}

func parseURNOK(raw string) (messaging.Address, bool) {
	addr, err := messaging.ParseURN(raw)
	if err != nil {
		return messaging.Address{}, false
	}
	return addr, true
}

func parseMetadataJSONMap(raw string) (map[string]string, bool) {
	metadata := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
		return nil, false
	}
	return metadata, true
}

type humanGateOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

func parseHumanGateOptions(raw string) ([]humanGateOption, error) {
	if strings.TrimSpace(raw) == "" {
		return []humanGateOption{}, nil
	}
	var options []humanGateOption
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil, fmt.Errorf("parse human gate options_json: %w", err)
	}
	return options, nil
}

func humanGateDecisionAllowed(decision string, options []humanGateOption) bool {
	for _, option := range options {
		if option.ID == decision {
			return true
		}
	}
	return false
}

func humanGateEnvelope(rec persistence.HumanGateRecord, options []humanGateOption) map[string]any {
	return map[string]any{
		"id":           rec.ID,
		"workspace_id": rec.WorkspaceID,
		"run_id":       rec.RunID,
		"step_name":    rec.StepName,
		"prompt":       rec.Prompt,
		"options":      options,
		"options_json": rec.OptionsJSON,
		"status":       rec.Status,
		"decision":     nullString(rec.Decision),
		"created_at":   rec.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":   rec.UpdatedAt.UTC().Format(time.RFC3339),
		"expires_at":   nullString(rec.ExpiresAt),
	}
}
