package mcpadapter

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/lint"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/mark3labs/mcp-go/mcp"
)

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
