package mcpadapter

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	gomcp "github.com/hollis-labs/go-mcp/server"
)

var runSeq uint64
var pipelineSeq uint64
var messageSeq uint64

func (a *Adapter) registerTools(s *gomcp.Server) {
	a.registerSkillsTool(s)
	if a.workflowOnly {
		return
	}

	registerTool(s, "hadron_health", "Read Hadron MCP adapter health/status.",
		gomcp.EmptyObjectSchema(), a.handleHealth)

	registerTool(s, "hadron_workspaces_list", "List all Hadron workspaces.",
		gomcp.EmptyObjectSchema(), a.handleWorkspacesList)

	registerTool(s, "hadron_workspace_get", "Get a workspace by id.",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id": strProp("Workspace id"),
		}, "workspace_id"), a.handleWorkspaceGet)

	registerTool(s, "hadron_workspace_create", "Create a workspace (requires scope workspace.write).",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id": strProp("Workspace id"),
			"name":         strProp("Workspace display name"),
		}, "workspace_id"), a.handleWorkspaceCreate)

	registerTool(s, "hadron_runs_list", "List runs for a workspace.",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id": strProp("Workspace id (default: default)"),
			"limit":        numProp("Max items to return (default 10, max 25)"),
		}), a.handleRunsList)

	registerTool(s, "hadron_run_get", "Get a run by id.",
		gomcp.ObjectSchema(map[string]any{
			"run_id":       strProp("Run id"),
			"workspace_id": strProp("Optional workspace scope check"),
		}, "run_id"), a.handleRunGet)

	registerTool(s, "hadron_run_enqueue", "Enqueue a blueprint run (requires scope run.write).",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id":   strProp("Workspace id (default: default)"),
			"blueprint_path": strProp("Blueprint path"),
			"inputs_json":    strProp("JSON object string for inputs"),
		}, "blueprint_path"), a.handleRunEnqueue)

	registerTool(s, "hadron_run_cancel", "Cancel a running run (requires scope run.cancel).",
		gomcp.ObjectSchema(map[string]any{
			"run_id": strProp("Run id"),
		}, "run_id"), a.handleRunCancel)

	registerTool(s, "hadron_run_events", "List run events for a run id.",
		gomcp.ObjectSchema(map[string]any{
			"run_id":       strProp("Run id"),
			"workspace_id": strProp("Optional workspace scope check"),
			"limit":        numProp("Max items to return (default 10, max 25)"),
		}, "run_id"), a.handleRunEvents)

	registerTool(s, "hadron_run_operations", "Summarize operation diagnostics for a run across MCP, HTTP, agent launch, and message wait primitives.",
		gomcp.ObjectSchema(map[string]any{
			"run_id":       strProp("Run id"),
			"workspace_id": strProp("Optional workspace scope check"),
			"kind":         strProp("Optional operation kind filter: mcp_call, http_call, message_wait, or agent_launch"),
			"cursor":       strProp("Optional pagination cursor returned by a previous call"),
			"limit":        numProp("Max items to return (default 10, max 25)"),
		}, "run_id"), a.handleRunOperations)

	registerTool(s, "hadron_run_mcp_calls", "Summarize MCP call diagnostics for a run.",
		gomcp.ObjectSchema(map[string]any{
			"run_id":       strProp("Run id"),
			"workspace_id": strProp("Optional workspace scope check"),
		}, "run_id"), a.handleRunMCPCalls)

	registerTool(s, "hadron_schedules_list", "List schedules for a workspace.",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id": strProp("Workspace id (default: default)"),
			"limit":        numProp("Max items to return (default 10, max 25)"),
		}), a.handleSchedulesList)

	registerTool(s, "hadron_schedule_create", "Create a schedule (requires scope schedule.write).",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id":   strProp("Workspace id (default: default)"),
			"name":           strProp("Schedule display name"),
			"blueprint_path": strProp("Blueprint path"),
			"cron_expr":      strProp("Cron expression (standard 5-field)"),
			"enabled":        boolProp("Whether schedule is enabled (default true)"),
		}, "blueprint_path", "cron_expr"), a.handleScheduleCreate)

	registerTool(s, "hadron_schedule_update", "Update schedule enabled state (requires scope schedule.write).",
		gomcp.ObjectSchema(map[string]any{
			"schedule_id": strProp("Schedule id"),
			"enabled":     boolProp("Enable or disable the schedule"),
		}, "schedule_id", "enabled"), a.handleScheduleUpdate)

	registerTool(s, "hadron_pipelines_list", "List pipeline runs for a workspace.",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id": strProp("Workspace id (default: default)"),
			"limit":        numProp("Max items to return (default 10, max 25)"),
		}), a.handlePipelinesList)

	registerTool(s, "hadron_pipeline_enqueue", "Start a pipeline run (requires scope pipeline.write).",
		gomcp.ObjectSchema(map[string]any{
			"workspace_id":  strProp("Workspace id (default: default)"),
			"pipeline_path": strProp("Pipeline spec path"),
		}, "pipeline_path"), a.handlePipelineEnqueue)

	registerTool(s, "hadron_pipeline_stages", "List stages for a pipeline run.",
		gomcp.ObjectSchema(map[string]any{
			"pipeline_run_id": strProp("Pipeline run id"),
			"workspace_id":    strProp("Optional workspace scope check"),
		}, "pipeline_run_id"), a.handlePipelineStages)

	registerTool(s, "hadron_pipeline_graph", "Get the DAG graph (nodes + edges) for a pipeline run. Includes stage positions, status, and dependency edges.",
		gomcp.ObjectSchema(map[string]any{
			"pipeline_run_id": strProp("Pipeline run id"),
			"workspace_id":    strProp("Optional workspace scope check"),
		}, "pipeline_run_id"), a.handlePipelineGraph)

	registerTool(s, "hadron_blueprint_validate", "Validate blueprint YAML/JSON content.",
		gomcp.ObjectSchema(map[string]any{
			"content": strProp("Blueprint YAML or JSON content"),
		}, "content"), a.handleBlueprintValidate)

	registerTool(s, "hadron_blueprints_list", "List available blueprint files (recursive). Optionally filter by tag.",
		gomcp.ObjectSchema(map[string]any{
			"tag":   strProp("Filter by blueprint tag (e.g. 'audit', 'build')"),
			"limit": numProp("Max items to return (default 10, max 25)"),
		}), a.handleBlueprintsList)

	registerTool(s, "hadron_blueprint_get", "Read a blueprint file's YAML content by path.",
		gomcp.ObjectSchema(map[string]any{
			"blueprint_path": strProp("Path to the blueprint file"),
		}, "blueprint_path"), a.handleBlueprintGet)

	a.registerBlueprintDiscoveryTools(s)

	registerTool(s, "hadron_schedule_delete", "Delete a schedule by id (requires scope schedule.write).",
		gomcp.ObjectSchema(map[string]any{
			"schedule_id": strProp("Schedule id"),
		}, "schedule_id"), a.handleScheduleDelete)

	registerTool(s, "hadron_blueprint_lint", "Lint a blueprint or pipeline file for best-practice issues (unused inputs, missing timeouts, duplicate step names, template syntax errors, etc).",
		gomcp.ObjectSchema(map[string]any{
			"blueprint_path": strProp("Path to the blueprint or pipeline file to lint"),
		}, "blueprint_path"), a.handleBlueprintLint)

	registerTool(s, "hadron_agent_card", "Generate an A2A Agent Card JSON document. Returns card for a single blueprint or all blueprints in the configured directory.",
		gomcp.ObjectSchema(map[string]any{
			"blueprint_path": strProp("Path to a single blueprint file. Omit to generate a composite card from all blueprints."),
			"url":            strProp("Base URL for the agent (default: http://localhost:8095)"),
		}), a.handleAgentCard)

	// Triggers
	registerTool(s, "hadron_triggers_list", "List all webhook triggers.",
		gomcp.EmptyObjectSchema(), a.handleTriggersList)

	registerTool(s, "hadron_trigger_create", "Create a webhook trigger (requires scope trigger.write).",
		gomcp.ObjectSchema(map[string]any{
			"name":           strProp("Trigger display name"),
			"path":           strProp("Webhook path (e.g. 'deploy-prod')"),
			"blueprint_path": strProp("Blueprint file path"),
			"secret":         strProp("HMAC-SHA256 secret for signature validation"),
			"extract_inputs": strProp("JSON config mapping input names to body/header/query paths"),
			"one_shot":       boolProp("Delete trigger after first firing (default false)"),
			"ttl_minutes":    numProp("Auto-expire trigger after N minutes"),
			"workspace_id":   strProp("Workspace id (default: default)"),
		}, "name", "path", "blueprint_path"), a.handleTriggerCreate)

	registerTool(s, "hadron_trigger_delete", "Delete a webhook trigger by ID (requires scope trigger.write).",
		gomcp.ObjectSchema(map[string]any{
			"trigger_id": strProp("Trigger id"),
		}, "trigger_id"), a.handleTriggerDelete)

	registerTool(s, "hadron_trigger_watch", "Create a temporary trigger with TTL for agent use. Supports webhook and file_watch types (requires scope trigger.write).",
		gomcp.ObjectSchema(map[string]any{
			"type":           strProp("Trigger type: 'webhook' or 'file_watch'"),
			"blueprint_path": strProp("Blueprint file path to run when triggered"),
			"config":         strProp("JSON config: for webhook {\"path\":\"my-hook\",\"name\":\"My Hook\"}, for file_watch {\"paths\":[\"/dir\"],\"name\":\"Watch\",\"events\":\"create,modify\",\"debounce\":5}"),
			"ttl_minutes":    numProp("TTL in minutes (max 1440 = 24h)"),
			"one_shot":       boolProp("Delete trigger after first firing (default true)"),
			"workspace_id":   strProp("Workspace id (default: default)"),
		}, "type", "blueprint_path", "config", "ttl_minutes"), a.handleTriggerWatch)

	registerTool(s, "hadron_trigger_list_mine", "List triggers created by the current MCP session.",
		gomcp.EmptyObjectSchema(), a.handleTriggerListMine)

	registerTool(s, "hadron_human_gate_get", "Get a human gate by id.",
		gomcp.ObjectSchema(map[string]any{
			"gate_id":      strProp("Human gate id"),
			"workspace_id": strProp("Optional workspace scope check"),
		}, "gate_id"), a.handleHumanGateGet)

	registerTool(s, "hadron_human_gate_submit", "Submit a decision for a waiting human gate (requires scope human_gate.write).",
		gomcp.ObjectSchema(map[string]any{
			"gate_id":      strProp("Human gate id"),
			"decision":     strProp("Decision option id to submit"),
			"workspace_id": strProp("Optional workspace scope check"),
		}, "gate_id", "decision"), a.handleHumanGateSubmit)

	registerTool(s, "hadron_message_send", "Store a message envelope for a configured message substrate (requires scope message.write).",
		gomcp.ObjectSchema(map[string]any{
			"substrate":     strProp("Message substrate id from settings.json"),
			"kind":          strProp("Message kind: request, response, notice, status_update, handoff, or escalation"),
			"from":          strProp("Sender URN, e.g. msg://agent/local/reviewer"),
			"to":            strProp("Recipient URN, e.g. msg://agent/local/reviewer"),
			"thread_id":     strProp("Optional thread id"),
			"in_reply_to":   strProp("Optional parent message id"),
			"payload_json":  strProp("Optional JSON payload string"),
			"content_type":  strProp("Optional content type (default application/json)"),
			"metadata_json": strProp("Optional JSON object string for metadata"),
		}, "substrate", "kind", "from", "to"), a.handleMessageSend)

	registerTool(s, "hadron_messages_inbox", "List messages for a recipient and optional correlation filter.",
		gomcp.ObjectSchema(map[string]any{
			"substrate":      strProp("Message substrate id from settings.json"),
			"to":             strProp("Recipient URN"),
			"correlation_id": strProp("Optional thread / reply / correlation filter"),
			"limit":          numProp("Max items to return (default 10, max 100)"),
		}, "substrate", "to"), a.handleMessagesInbox)

	registerTool(s, "hadron_messages_list", "List messages non-destructively for a recipient and optional correlation filter.",
		gomcp.ObjectSchema(map[string]any{
			"substrate":      strProp("Message substrate id from settings.json"),
			"to":             strProp("Recipient URN"),
			"correlation_id": strProp("Optional thread / reply / correlation filter"),
			"limit":          numProp("Max items to return (default 10, max 100)"),
		}, "substrate", "to"), a.handleMessagesList)

	registerTool(s, "hadron_messages_thread", "List messages in a thread or correlation group.",
		gomcp.ObjectSchema(map[string]any{
			"substrate": strProp("Message substrate id from settings.json"),
			"thread_id": strProp("Thread id or correlation id"),
			"limit":     numProp("Max items to return (default 10, max 100)"),
		}, "substrate", "thread_id"), a.handleMessagesThread)

	registerTool(s, "hadron_message_get", "Get a single stored message by id.",
		gomcp.ObjectSchema(map[string]any{
			"substrate":  strProp("Optional message substrate id to scope lookup"),
			"message_id": strProp("Message id"),
		}, "message_id"), a.handleMessageGet)

	registerTool(s, "hadron_message_consume", "Mark a stored message consumed (requires scope message.write).",
		gomcp.ObjectSchema(map[string]any{
			"substrate":  strProp("Optional message substrate id to scope lookup"),
			"message_id": strProp("Message id"),
		}, "message_id"), a.handleMessageConsume)

	// Register blueprint registry tools
	registerRegistryTools(s, a.registry)
}

func (a *Adapter) handleHealth(_ context.Context, _ map[string]any) (any, error) {
	return map[string]any{
		"status":    "ok",
		"service":   "hadron-mcp",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}, nil
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
