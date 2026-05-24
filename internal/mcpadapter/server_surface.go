package mcpadapter

import (
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const serverInstructions = "Hadron by Hollis Labs is an agent-first blueprint automation runner. Prefer hadron_skills for orientation, hadron_blueprint_broker or hadron_blueprint_discover to choose workflows, hadron_blueprint_schema before hadron_run_enqueue, and hadron_run_operations before scraping raw run events. This server is in active public beta."

type toolBehavior struct {
	readOnly    bool
	destructive bool
	idempotent  bool
	openWorld   bool
}

func (a *Adapter) newServer() *server.MCPServer {
	s := server.NewMCPServer(
		"Hadron by Hollis Labs",
		"0.4.0",
		server.WithToolCapabilities(true),
		server.WithPromptCapabilities(false),
		server.WithResourceCapabilities(false, false),
		server.WithPromptCompletionProvider(a),
		server.WithResourceCompletionProvider(a),
		server.WithCompletions(),
		server.WithInstructions(serverInstructions),
	)
	a.registerTools(s)
	a.registerPrompts(s)
	a.registerResources(s)
	a.finalizeToolSurface(s)
	return s
}

func (a *Adapter) finalizeToolSurface(s *server.MCPServer) {
	registered := s.ListTools()
	names := make([]string, 0, len(registered))
	for name := range registered {
		names = append(names, name)
	}
	sort.Strings(names)

	tools := make([]server.ServerTool, 0, len(names))
	for _, name := range names {
		entry := registered[name]
		tool := entry.Tool
		tool = applyToolBehavior(tool, hadronToolBehavior(name))
		tools = append(tools, server.ServerTool{
			Tool:    tool,
			Handler: entry.Handler,
		})
	}
	s.SetTools(tools...)
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

func applyToolBehavior(tool mcp.Tool, behavior toolBehavior) mcp.Tool {
	mcp.WithReadOnlyHintAnnotation(behavior.readOnly)(&tool)
	mcp.WithDestructiveHintAnnotation(behavior.destructive)(&tool)
	mcp.WithIdempotentHintAnnotation(behavior.idempotent)(&tool)
	mcp.WithOpenWorldHintAnnotation(behavior.openWorld)(&tool)
	return tool
}
