package mcpadapter

import (
	"context"
	"strings"

	gomcp "github.com/hollis-labs/go-mcp/server"
)

const serverInstructions = "Hadron by Hollis Labs is an agent-first blueprint automation runner. Prefer hadron_skills for orientation, hadron_blueprint_broker or hadron_blueprint_discover to choose workflows, hadron_blueprint_schema before hadron_run_enqueue, and hadron_run_operations before scraping raw run events. This server is in active public beta."

const workflowServerInstructions = "Hadron exposes qualified graph-native workflows through profile-authorized meta-tools and session-scoped generated tools. Use hadron_workflow_catalog_search for ranked catalog recommendations and authoring next steps; use hadron_workflows_search only for the current session's exposure discovery. Validate, scaffold, contract-test, register, qualify, publish, and profile-pin exact versions before mounting with hadron_workflows_load. Hidden workflows, raw source, credentials, and secret values are never returned."

type toolBehavior struct {
	readOnly    bool
	destructive bool
	idempotent  bool
	openWorld   bool
}

func (a *Adapter) newServer() *gomcp.Server {
	instructions := serverInstructions
	if a.workflow != nil || a.workflowOnly {
		instructions = workflowServerInstructions
	}
	s := gomcp.NewServer(
		"Hadron by Hollis Labs",
		a.serverVersion,
		gomcp.WithInstructions(instructions),
		gomcp.WithCompletionHandler(a.handleCompletion),
	)
	a.registerTools(s)
	if a.workflow != nil {
		a.workflow.registerTools(s)
	}
	if !a.workflowOnly {
		a.registerPrompts(s)
		a.registerResources(s)
	}
	if a.workflow != nil {
		a.workflow.registerResources(s)
		a.workflow.bindServer(s)
		// Compute the initial mount synchronously, here, rather than
		// deferring it to whatever eventually serves this *gomcp.Server
		// (Adapter.Run's stdio, an HTTP handler wrapping it directly in a
		// test, or anything else): newServer must return a fully-mounted
		// server on its own, since it has no way to know how its result
		// will be exposed. See workflowSurface's doc comment for why
		// there is exactly one mount to compute, not one per caller.
		_, _, _ = a.workflow.current(context.Background(), a.token)
	}
	return s
}

// registerTool registers a Hadron-owned tool with its behavior annotations
// looked up by name, so every tools.go-family file states a name/description
// /schema/handler and nothing else -- the required annotation contract is
// centralized here rather than repeated per call site.
func registerTool(s *gomcp.Server, name, description string, inputSchema any, handler gomcp.ToolHandler) {
	b := hadronToolBehavior(name)
	s.RegisterTool(gomcp.Tool{
		Name:            name,
		Description:     description,
		InputSchema:     inputSchema,
		Handler:         handler,
		ReadOnlyHint:    b.readOnly,
		DestructiveHint: b.destructive,
		IdempotentHint:  b.idempotent,
		OpenWorldHint:   b.openWorld,
	})
}

func hadronToolBehavior(name string) toolBehavior {
	switch name {
	case "hadron_health",
		"hadron_workspaces_list",
		"hadron_workspace_get",
		"hadron_runs_list",
		"hadron_run_get",
		"hadron_run_events",
		"hadron_run_operations",
		"hadron_run_mcp_calls",
		"hadron_schedules_list",
		"hadron_pipelines_list",
		"hadron_pipeline_stages",
		"hadron_pipeline_graph",
		"hadron_blueprint_validate",
		"hadron_blueprints_list",
		"hadron_blueprint_get",
		"hadron_blueprint_discover",
		"hadron_blueprint_search",
		"hadron_blueprint_broker",
		"hadron_blueprint_schema",
		"hadron_blueprint_lint",
		"hadron_agent_card",
		"hadron_triggers_list",
		"hadron_trigger_list_mine",
		"hadron_human_gate_get",
		"hadron_messages_inbox",
		"hadron_messages_list",
		"hadron_messages_thread",
		"hadron_message_get",
		"hadron_registry_search",
		"hadron_registry_show",
		"hadron_registry_list",
		"hadron_skills":
		return toolBehavior{readOnly: true, destructive: false, idempotent: true, openWorld: false}
	case "hadron_schedule_update",
		"hadron_message_consume":
		return toolBehavior{readOnly: false, destructive: false, idempotent: true, openWorld: false}
	case "hadron_run_cancel",
		"hadron_schedule_delete",
		"hadron_trigger_delete":
		return toolBehavior{readOnly: false, destructive: true, idempotent: false, openWorld: false}
	case "hadron_workspace_create",
		"hadron_run_enqueue",
		"hadron_schedule_create",
		"hadron_pipeline_enqueue",
		"hadron_trigger_create",
		"hadron_trigger_watch",
		"hadron_human_gate_submit",
		"hadron_message_send",
		"hadron_registry_index":
		return toolBehavior{readOnly: false, destructive: false, idempotent: false, openWorld: false}
	default:
		if strings.HasPrefix(name, "hadron_registry_") {
			return toolBehavior{readOnly: true, destructive: false, idempotent: true, openWorld: false}
		}
		return toolBehavior{readOnly: false, destructive: false, idempotent: false, openWorld: false}
	}
}
