package mcpadapter

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var runSeq uint64
var pipelineSeq uint64
var messageSeq uint64

func (a *Adapter) registerTools(s *server.MCPServer) {
	a.registerSkillsTool(s)
	if a.workflowOnly {
		return
	}

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

func workspaceDefault(workspaceID string) string {
	if strings.TrimSpace(workspaceID) == "" {
		return "default"
	}
	return strings.TrimSpace(workspaceID)
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
