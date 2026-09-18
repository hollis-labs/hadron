package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	gomcp "github.com/hollis-labs/go-mcp/server"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Prompts, resources, and resource templates are not wrapped by go-mcp (see
// its README): they carry none of the required-annotation contract Tool
// does, so registering them directly against the official SDK via
// SDKServer() is the documented pattern, not a workaround.

const (
	resourceMCPStartHere        = "hadron://docs/mcp/start-here"
	resourceMCPBlueprints       = "hadron://docs/mcp/blueprint-discovery"
	resourceMCPRunInspection    = "hadron://docs/mcp/run-inspection"
	resourceMCPMessageWorkflows = "hadron://docs/mcp/message-workflows"
	resourceMCPInputSchemaGuide = "hadron://docs/mcp/input-schema-guide"
)

const inputSchemaGuide = `# Input Schema Guide

Hadron exposes blueprint input contracts through the ` + "`hadron_blueprint_schema`" + ` tool and the ` + "`hadron://blueprints/{blueprint_ref}/input-schema`" + ` resource template.

Use these before ` + "`hadron_run_enqueue`" + ` so the agent knows:
- required input names
- input types
- descriptions and enum constraints

Preferred sequence:
1. discover or broker a blueprint
2. inspect its schema
3. provide normalized inputs
4. enqueue the run`

func (a *Adapter) registerPrompts(s *gomcp.Server) {
	sdk := s.SDKServer()

	sdk.AddPrompt(&mcpsdk.Prompt{
		Name:        "hadron_pick_blueprint",
		Description: "Guide an agent through selecting a Hadron blueprint for a task, then inspecting its input schema before enqueueing.",
		Arguments: []*mcpsdk.PromptArgument{
			{Name: "task", Description: "Task or workflow the agent needs to accomplish", Required: true},
			{Name: "tag", Description: "Optional exact blueprint tag filter"},
		},
	}, a.handlePromptPickBlueprint)

	sdk.AddPrompt(&mcpsdk.Prompt{
		Name:        "hadron_debug_run",
		Description: "Guide an agent through debugging a Hadron run using structured diagnostics first, then raw events if needed.",
		Arguments: []*mcpsdk.PromptArgument{
			{Name: "run_id", Description: "Hadron run id to inspect", Required: true},
			{Name: "workspace_id", Description: "Optional workspace id for scope checks"},
		},
	}, a.handlePromptDebugRun)
}

func (a *Adapter) registerResources(s *gomcp.Server) {
	sdk := s.SDKServer()

	for uri, body := range map[string]string{
		resourceMCPStartHere:        hadronSkillBodies["start-here"],
		resourceMCPBlueprints:       hadronSkillBodies["blueprint-discovery"],
		resourceMCPRunInspection:    hadronSkillBodies["run-inspection"],
		resourceMCPMessageWorkflows: hadronSkillBodies["message-workflows"],
		resourceMCPInputSchemaGuide: inputSchemaGuide,
	} {
		staticTextResource(sdk, uri, resourceDisplayName(uri), body)
	}

	sdk.AddResourceTemplate(&mcpsdk.ResourceTemplate{
		URITemplate: "hadron://blueprints/{blueprint_ref}/input-schema",
		Name:        "Hadron Blueprint Input Schema",
		Description: "Return the agent-facing JSON input schema for a blueprint identified by slug, name, file basename, or registry entry.",
		MIMEType:    "application/json",
	}, a.handleBlueprintSchemaResource)
}

func resourceDisplayName(uri string) string {
	switch uri {
	case resourceMCPStartHere:
		return "Hadron MCP Start Here"
	case resourceMCPBlueprints:
		return "Hadron Blueprint Discovery"
	case resourceMCPRunInspection:
		return "Hadron Run Inspection"
	case resourceMCPMessageWorkflows:
		return "Hadron Message Workflows"
	case resourceMCPInputSchemaGuide:
		return "Hadron Input Schema Guide"
	default:
		return uri
	}
}

func staticTextResource(sdk *mcpsdk.Server, uri, name, body string) {
	sdk.AddResource(&mcpsdk.Resource{
		URI:         uri,
		Name:        name,
		Description: name,
		MIMEType:    "text/markdown",
	}, func(_ context.Context, _ *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		return &mcpsdk.ReadResourceResult{
			Contents: []*mcpsdk.ResourceContents{{URI: uri, MIMEType: "text/markdown", Text: body}},
		}, nil
	})
}

func (a *Adapter) handlePromptPickBlueprint(_ context.Context, req *mcpsdk.GetPromptRequest) (*mcpsdk.GetPromptResult, error) {
	task := strings.TrimSpace(req.Params.Arguments["task"])
	if task == "" {
		return nil, fmt.Errorf("task is required")
	}
	tag := strings.TrimSpace(req.Params.Arguments["tag"])
	lines := []string{
		"Use Hadron to choose the most appropriate blueprint for this task.",
		"Start with hadron_blueprint_broker or hadron_blueprint_discover.",
		"After choosing a candidate, call hadron_blueprint_schema before enqueueing any run.",
		fmt.Sprintf("Task: %s", task),
	}
	if tag != "" {
		lines = append(lines, "Tag filter: "+tag)
	}
	return &mcpsdk.GetPromptResult{
		Description: "Select and prepare a Hadron blueprint",
		Messages: []*mcpsdk.PromptMessage{
			{Role: "user", Content: &mcpsdk.TextContent{Text: strings.Join(lines, "\n")}},
			{Role: "assistant", Content: &mcpsdk.EmbeddedResource{Resource: &mcpsdk.ResourceContents{
				URI:      resourceMCPBlueprints,
				MIMEType: "text/markdown",
				Text:     hadronSkillBodies["blueprint-discovery"],
			}}},
		},
	}, nil
}

func (a *Adapter) handlePromptDebugRun(_ context.Context, req *mcpsdk.GetPromptRequest) (*mcpsdk.GetPromptResult, error) {
	runID := strings.TrimSpace(req.Params.Arguments["run_id"])
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	workspaceID := strings.TrimSpace(req.Params.Arguments["workspace_id"])
	lines := []string{
		fmt.Sprintf("Debug Hadron run %s.", runID),
		"Start with hadron_run_get and hadron_run_operations.",
		"Use hadron_run_events only when you need the raw append-only detail after structured diagnostics.",
	}
	if workspaceID != "" {
		lines = append(lines, "Workspace scope: "+workspaceID)
	}
	return &mcpsdk.GetPromptResult{
		Description: "Debug a Hadron run",
		Messages: []*mcpsdk.PromptMessage{
			{Role: "user", Content: &mcpsdk.TextContent{Text: strings.Join(lines, "\n")}},
			{Role: "assistant", Content: &mcpsdk.EmbeddedResource{Resource: &mcpsdk.ResourceContents{
				URI:      resourceMCPRunInspection,
				MIMEType: "text/markdown",
				Text:     hadronSkillBodies["run-inspection"],
			}}},
		},
	}, nil
}

func (a *Adapter) handleBlueprintSchemaResource(_ context.Context, req *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
	const prefix = "hadron://blueprints/"
	const suffix = "/input-schema"
	uri := req.Params.URI
	if !strings.HasPrefix(uri, prefix) || !strings.HasSuffix(uri, suffix) {
		return nil, fmt.Errorf("unsupported blueprint schema resource uri: %s", uri)
	}
	ref := strings.TrimSuffix(strings.TrimPrefix(uri, prefix), suffix)
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("blueprint_ref is required")
	}

	path, err := a.resolveBlueprintReference(ref)
	if err != nil {
		return nil, err
	}
	bp, err := blueprint.ParseFile(path)
	if err != nil {
		return nil, err
	}
	skill := agentcard.SkillFromBlueprint(bp, path)
	body, err := json.MarshalIndent(map[string]any{
		"path":         path,
		"id":           skill.ID,
		"name":         skill.Name,
		"description":  skill.Description,
		"tags":         skill.Tags,
		"input_schema": skill.InputSchema,
	}, "", "  ")
	if err != nil {
		return nil, err
	}
	return &mcpsdk.ReadResourceResult{
		Contents: []*mcpsdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(body)}},
	}, nil
}
