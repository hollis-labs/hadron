package mcpadapter

import (
	"context"
	"strings"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
)

func (a *Adapter) handleWorkspacesList(ctx context.Context, _ map[string]any) (any, error) {
	items, err := a.store.ListWorkspaces(ctx)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	out := make([]map[string]any, 0, len(items))
	for _, w := range items {
		out = append(out, map[string]any{
			"id":         w.ID,
			"name":       w.Name,
			"created_at": w.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at": w.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	return map[string]any{"items": out, "count": len(out)}, nil
}

func (a *Adapter) handleWorkspaceGet(ctx context.Context, args map[string]any) (any, error) {
	id := strings.TrimSpace(argString(args, "workspace_id", ""))
	if id == "" {
		return nil, budget.NewToolError("validation_error", "workspace_id is required").WithField("workspace_id")
	}
	w, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "workspace not found").
				WithField("workspace_id").WithHelpTool("hadron_workspaces_list").
				WithNextStep("call hadron_workspaces_list to find a valid workspace_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{
		"id":         w.ID,
		"name":       w.Name,
		"created_at": w.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": w.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (a *Adapter) handleWorkspaceCreate(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeWorkspaceWrite); err != nil {
		return nil, err
	}
	id := strings.TrimSpace(argString(args, "workspace_id", ""))
	if id == "" {
		return nil, budget.NewToolError("validation_error", "workspace_id is required").WithField("workspace_id")
	}
	name := strings.TrimSpace(argString(args, "name", ""))
	if name == "" {
		name = id
	}
	if err := a.store.CreateWorkspace(ctx, id, name); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	rec, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{
		"id":         rec.ID,
		"name":       rec.Name,
		"created_at": rec.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": rec.UpdatedAt.UTC().Format(time.RFC3339),
	}, nil
}
