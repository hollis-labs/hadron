package mcpadapter

import (
	"context"
	"errors"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type hadronSkillDoc struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

var errHadronSkillNotFound = errors.New("skill not found")

var hadronSkillBodies = map[string]string{
	"start-here": `# Hadron MCP Start Here

Hadron is an agent-first graph-native workflow host. The preferred MCP flow is:
1. Use ` + "`hadron_workflow_catalog_search`" + ` for ranked qualified versions and an explicit next authoring step.
2. When no fit exists, validate a bounded draft, generate its scaffold, and run deterministic contract tests before registration.
3. Qualify and publish one exact digest, then profile-pin it without conflating registry current, registry qualification pin, or exposure pin.
4. Use ` + "`hadron_workflows_search`" + ` for the current profile's discoverable set and ` + "`hadron_workflows_load`" + ` to mount an exact generated tool.
5. Invoke the generated tool and follow its asynchronous durable run through typed inspect, events, waits, and values operations.

Never guess mutable aliases, source paths, effects, credentials, or schemas when an exact qualified descriptor is available.`,
	"workflow-lifecycle": `# Workflow Lifecycle

` + "`hadron_workflow_catalog_search`" + ` ranks authorized catalog records and returns either ` + "`inspect_exact`" + ` or ` + "`draft_validate`" + ` as the next step.

For a new workflow, use the author validate, scaffold, test, and register tools in order. A test call never registers. Registration may move the separate current alias only when ` + "`make_current`" + ` is explicitly supplied.

Registry version pinning qualifies one exact version for publication. Exposure pinning is a separate profile-generation CAS that controls direct tool visibility. Use the exact name, version, and digest throughout.`,
	"blueprint-discovery": `# Blueprint Discovery

Use ` + "`hadron_blueprint_broker`" + ` when you want ranked blueprint recommendations with explicit reasons and next steps.

Use ` + "`hadron_blueprint_discover`" + ` when you have a task and want likely-fit blueprints.
Use ` + "`hadron_blueprint_search`" + ` when you need deterministic keyword matching.
Use ` + "`hadron_blueprint_schema`" + ` after choosing a blueprint so you can construct valid inputs for ` + "`hadron_run_enqueue`" + `.

Avoid relying on registry-only tools for first-pass agent discovery. Those are still useful operationally, but the blueprint discovery tools work directly from the configured blueprint directory.`,
	"run-inspection": `# Run Inspection

Use ` + "`hadron_run_get`" + ` for the current run summary.
Use ` + "`hadron_run_operations`" + ` for structured diagnostics across MCP, HTTP, message waits, and agent launches.
Use ` + "`hadron_run_events`" + ` when you need the append-only raw event history.

Prefer ` + "`hadron_run_operations`" + ` before scraping event text when you need to understand a failed step.`,
	"message-workflows": `# Message Workflows

For local agent-to-agent workflows:
- ` + "`hadron_message_send`" + ` stores an envelope.
- ` + "`hadron_messages_inbox`" + ` destructively reads a recipient inbox.
- ` + "`hadron_messages_list`" + ` is the non-destructive list surface.
- ` + "`hadron_messages_thread`" + ` loads a full thread or correlation group.
- ` + "`hadron_message_get`" + ` and ` + "`hadron_message_consume`" + ` target a single message.

Prefer recipient and thread based reads over id-only polling when the workflow already has a stable thread or correlation id.`,
}

func hadronSkillIndex() []hadronSkillDoc {
	return []hadronSkillDoc{
		{Name: "start-here", Description: "Orientation for the Hadron MCP surface and recommended tool flow."},
		{Name: "workflow-lifecycle", Description: "How to discover, author, qualify, publish, and expose graph-native workflows."},
		{Name: "blueprint-discovery", Description: "How agents should discover blueprints and derive input schemas."},
		{Name: "run-inspection", Description: "How to inspect run status, structured operation diagnostics, and raw events."},
		{Name: "message-workflows", Description: "How to use Hadron's local message tools for agent workflows."},
	}
}

func (a *Adapter) registerSkillsTool(s *server.MCPServer) {
	s.AddTool(mcp.NewTool("hadron_skills",
		mcp.WithDescription("Hadron MCP orientation and skill index. Call with no args for the catalog; call with `name` to read one skill in full."),
		mcp.WithString("name", mcp.Description("Skill name to read in full. Omit to list available skills.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(false),
	), a.handleHadronSkills)
}

func (a *Adapter) handleHadronSkills(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name := req.GetString("name", "")
	if name == "" {
		items := hadronSkillIndex()
		return toolJSON(map[string]any{
			"items": items,
			"meta": map[string]any{
				"count":                 len(items),
				"progressive_discovery": true,
				"next":                  "hadron_skills",
			},
		}), nil
	}
	body, err := getHadronSkill(name)
	if err != nil {
		if errors.Is(err, errHadronSkillNotFound) {
			return toolError("skill_not_found", err.Error()), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	return mcp.NewToolResultText(body), nil
}

func getHadronSkill(name string) (string, error) {
	body, ok := hadronSkillBodies[name]
	if !ok {
		return "", errHadronSkillNotFound
	}
	return body, nil
}
