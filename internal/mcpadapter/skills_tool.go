package mcpadapter

import (
	"context"
	"errors"

	"github.com/hollis-labs/go-mcp/budget"
	gomcp "github.com/hollis-labs/go-mcp/server"
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

Use ` + "`hadron_workflow_run_inspect`" + ` for the redacted durable run projection.
Use ` + "`hadron_workflow_run_events`" + ` for bounded rendered events and ` + "`hadron_workflow_run_subscribe`" + ` for bounded progress polling. Typed waits and values are projected by inspect; resume through ` + "`hadron_workflow_run_resume`" + ` when the wait contract permits it.

Never infer private values from raw errors or scrape legacy run-event text.`,
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

func workflowSkillIndex() []hadronSkillDoc {
	return []hadronSkillDoc{
		{Name: "start-here", Description: "Orientation for the graph-native Hadron MCP surface."},
		{Name: "workflow-lifecycle", Description: "Discover, author, qualify, publish, and expose graph-native workflows."},
		{Name: "run-inspection", Description: "Inspect typed, bounded, redacted graph-native run diagnostics."},
	}
}

func (a *Adapter) registerSkillsTool(s *gomcp.Server) {
	registerTool(s, "hadron_skills", "Hadron MCP orientation and skill index. Call with no args for the catalog; call with `name` to read one skill in full.",
		gomcp.ObjectSchema(map[string]any{
			"name": strProp("Skill name to read in full. Omit to list available skills."),
		}), a.handleHadronSkills)
}

func (a *Adapter) handleHadronSkills(_ context.Context, args map[string]any) (any, error) {
	name := argString(args, "name", "")
	if name == "" {
		items := hadronSkillIndex()
		if a.workflowOnly {
			items = workflowSkillIndex()
		}
		return map[string]any{
			"items": items,
			"meta": map[string]any{
				"count":                 len(items),
				"progressive_discovery": true,
				"next":                  "hadron_skills",
			},
		}, nil
	}
	if a.workflowOnly && name != "start-here" && name != "workflow-lifecycle" && name != "run-inspection" {
		return nil, budget.NewToolError("skill_not_found", errHadronSkillNotFound.Error()).WithField("name")
	}
	body, err := getHadronSkill(name)
	if err != nil {
		if errors.Is(err, errHadronSkillNotFound) {
			return nil, budget.NewToolError("skill_not_found", err.Error()).WithField("name").
				WithHelpTool("hadron_skills").WithNextStep("call hadron_skills with no arguments to list available skill names")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return body, nil
}

func getHadronSkill(name string) (string, error) {
	body, ok := hadronSkillBodies[name]
	if !ok {
		return "", errHadronSkillNotFound
	}
	return body, nil
}
