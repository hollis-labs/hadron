package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

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

func (a *Adapter) registerPrompts(s *server.MCPServer) {
	s.AddPrompt(mcp.NewPrompt("hadron_pick_blueprint",
		mcp.WithPromptDescription("Guide an agent through selecting a Hadron blueprint for a task, then inspecting its input schema before enqueueing."),
		mcp.WithArgument("task",
			mcp.ArgumentDescription("Task or workflow the agent needs to accomplish"),
			mcp.RequiredArgument(),
		),
		mcp.WithArgument("tag",
			mcp.ArgumentDescription("Optional exact blueprint tag filter"),
		),
	), a.handlePromptPickBlueprint)

	s.AddPrompt(mcp.NewPrompt("hadron_debug_run",
		mcp.WithPromptDescription("Guide an agent through debugging a Hadron run using structured diagnostics first, then raw events if needed."),
		mcp.WithArgument("run_id",
			mcp.ArgumentDescription("Hadron run id to inspect"),
			mcp.RequiredArgument(),
		),
		mcp.WithArgument("workspace_id",
			mcp.ArgumentDescription("Optional workspace id for scope checks"),
		),
	), a.handlePromptDebugRun)
}

func (a *Adapter) registerResources(s *server.MCPServer) {
	resources := []server.ServerResource{
		staticTextResource(resourceMCPStartHere, "Hadron MCP Start Here", hadronSkillBodies["start-here"]),
		staticTextResource(resourceMCPBlueprints, "Hadron Blueprint Discovery", hadronSkillBodies["blueprint-discovery"]),
		staticTextResource(resourceMCPRunInspection, "Hadron Run Inspection", hadronSkillBodies["run-inspection"]),
		staticTextResource(resourceMCPMessageWorkflows, "Hadron Message Workflows", hadronSkillBodies["message-workflows"]),
		staticTextResource(resourceMCPInputSchemaGuide, "Hadron Input Schema Guide", inputSchemaGuide),
	}
	s.AddResources(resources...)

	s.AddResourceTemplate(mcp.NewResourceTemplate(
		"hadron://blueprints/{blueprint_ref}/input-schema",
		"Hadron Blueprint Input Schema",
		mcp.WithTemplateDescription("Return the agent-facing JSON input schema for a blueprint identified by slug, name, file basename, or registry entry."),
		mcp.WithTemplateMIMEType("application/json"),
	), a.handleBlueprintSchemaResource)
}

func staticTextResource(uri, name, body string) server.ServerResource {
	return server.ServerResource{
		Resource: mcp.NewResource(
			uri,
			name,
			mcp.WithResourceDescription(name),
			mcp.WithMIMEType("text/markdown"),
		),
		Handler: func(_ context.Context, _ mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      uri,
					MIMEType: "text/markdown",
					Text:     body,
				},
			}, nil
		},
	}
}

func (a *Adapter) handlePromptPickBlueprint(_ context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
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
	return mcp.NewGetPromptResult(
		"Select and prepare a Hadron blueprint",
		[]mcp.PromptMessage{
			mcp.NewPromptMessage(mcp.RoleUser, mcp.NewTextContent(strings.Join(lines, "\n"))),
			mcp.NewPromptMessage(mcp.RoleAssistant, mcp.NewEmbeddedResource(mcp.TextResourceContents{
				URI:      resourceMCPBlueprints,
				MIMEType: "text/markdown",
				Text:     hadronSkillBodies["blueprint-discovery"],
			})),
		},
	), nil
}

func (a *Adapter) handlePromptDebugRun(_ context.Context, req mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
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
	return mcp.NewGetPromptResult(
		"Debug a Hadron run",
		[]mcp.PromptMessage{
			mcp.NewPromptMessage(mcp.RoleUser, mcp.NewTextContent(strings.Join(lines, "\n"))),
			mcp.NewPromptMessage(mcp.RoleAssistant, mcp.NewEmbeddedResource(mcp.TextResourceContents{
				URI:      resourceMCPRunInspection,
				MIMEType: "text/markdown",
				Text:     hadronSkillBodies["run-inspection"],
			})),
		},
	), nil
}

func (a *Adapter) handleBlueprintSchemaResource(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
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
	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      uri,
			MIMEType: "application/json",
			Text:     string(body),
		},
	}, nil
}
