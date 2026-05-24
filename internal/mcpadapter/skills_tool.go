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

Hadron is an agent-first blueprint runner. The best MCP flow is:
1. Use ` + "`hadron_skills`" + ` for orientation when the surface is unfamiliar.
2. Use ` + "`hadron_blueprint_broker`" + ` or ` + "`hadron_blueprint_discover`" + ` to find the right blueprint.
3. Use ` + "`hadron_blueprint_schema`" + ` to inspect required inputs before enqueueing.
4. Use ` + "`hadron_run_enqueue`" + ` to start work.
5. Use ` + "`hadron_run_operations`" + ` for structured diagnostics and ` + "`hadron_run_events`" + ` for the raw audit trail.

Prefer discovery and schema tools before guessing blueprint paths or inputs.`,
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
