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
)

func (a *Adapter) handleBlueprintLint(_ context.Context, args map[string]any) (any, error) {
	bpPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if bpPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}

	rawContent, err := os.ReadFile(bpPath) // #nosec G304 -- MCP validate intentionally reads a caller-provided blueprint path.
	if err != nil {
		if os.IsNotExist(err) {
			return nil, budget.NewToolError("not_found", "file not found").WithField("blueprint_path")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
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
			return map[string]any{
				"path":   bpPath,
				"valid":  false,
				"error":  bpErr.Error(),
				"issues": []lint.Issue{},
			}, nil
		}
	}

	hasErrors := false
	for _, issue := range issues {
		if issue.Severity == lint.SeverityError {
			hasErrors = true
			break
		}
	}

	return map[string]any{
		"path":        bpPath,
		"valid":       !hasErrors,
		"issue_count": len(issues),
		"issues":      issues,
	}, nil
}

func (a *Adapter) handleBlueprintValidate(_ context.Context, args map[string]any) (any, error) {
	content := argString(args, "content", "")
	if strings.TrimSpace(content) == "" {
		return nil, budget.NewToolError("validation_error", "content is required").WithField("content")
	}
	_, err := blueprint.ParseBytes([]byte(content))
	if err != nil {
		return map[string]any{"valid": false, "error": err.Error()}, nil //nolint:nilerr
	}
	return map[string]any{"valid": true}, nil
}

func (a *Adapter) handleBlueprintsList(_ context.Context, args map[string]any) (any, error) {
	dir := a.blueprintDir
	tagFilter := strings.TrimSpace(strings.ToLower(argString(args, "tag", "")))
	limit := budget.ExtractLimit(args, budget.DefaultLimit)

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
			return budget.Apply([]map[string]any{}, budget.Config{Limit: limit}, ""), nil
		}
		return nil, budget.NewToolError("internal_error", walkErr.Error())
	}
	if items == nil {
		items = []map[string]any{}
	}
	return budget.Apply(items, budget.Config{Limit: limit},
		"%d blueprints found. Use hadron_blueprint_get with a specific path for full details. Add tag filter to narrow results."), nil
}

func (a *Adapter) handleBlueprintGet(_ context.Context, args map[string]any) (any, error) {
	bpPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if bpPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}
	absPath, err := a.resolveBlueprintPath(bpPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, notFoundBlueprint()
		}
		if strings.Contains(err.Error(), "outside") {
			return nil, budget.NewToolError("validation_error", err.Error()).WithField("blueprint_path")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	data, err := os.ReadFile(absPath) // #nosec G304 -- path was validated to stay within the blueprint directory.
	if err != nil {
		if os.IsNotExist(err) {
			return nil, notFoundBlueprint()
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{
		"path":    absPath,
		"content": string(data),
	}, nil
}

func notFoundBlueprint() *budget.ToolError {
	return budget.NewToolError("not_found", "blueprint file not found").
		WithField("blueprint_path").WithHelpTool("hadron_blueprints_list").
		WithNextStep("call hadron_blueprints_list or hadron_blueprint_discover to find a valid blueprint_path")
}

func (a *Adapter) handleAgentCard(_ context.Context, args map[string]any) (any, error) {
	bpPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	baseURL := strings.TrimSpace(argString(args, "url", ""))
	if baseURL == "" {
		baseURL = "http://localhost:8095"
	}

	if bpPath != "" {
		// Single blueprint mode
		bp, err := blueprint.ParseFile(bpPath)
		if err != nil {
			return nil, budget.NewToolError("validation_error", err.Error()).WithField("blueprint_path")
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
			return nil, budget.NewToolError("internal_error", err.Error())
		}
		return string(data), nil
	}

	// All blueprints mode
	dir := a.blueprintDir
	if dir == "" {
		return nil, budget.NewToolError("validation_error", "no blueprint directory configured")
	}
	card, err := agentcard.FromDirectory(dir, baseURL)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	data, err := card.JSON()
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return string(data), nil
}
