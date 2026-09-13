package mcpadapter

import (
	"context"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handleWorkspacesList(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	items, err := a.store.ListWorkspaces(ctx)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
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
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleWorkspaceGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := strings.TrimSpace(req.GetString("workspace_id", ""))
	if id == "" {
		return toolError("validation_error", "workspace_id is required"), nil
	}
	w, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":         w.ID,
		"name":       w.Name,
		"created_at": w.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": w.UpdatedAt.UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleWorkspaceCreate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeWorkspaceWrite); deny != nil {
		return deny, nil
	}
	id := strings.TrimSpace(req.GetString("workspace_id", ""))
	if id == "" {
		return toolError("validation_error", "workspace_id is required"), nil
	}
	name := strings.TrimSpace(req.GetString("name", ""))
	if name == "" {
		name = id
	}
	if err := a.store.CreateWorkspace(ctx, id, name); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	rec, err := a.store.GetWorkspace(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":         rec.ID,
		"name":       rec.Name,
		"created_at": rec.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": rec.UpdatedAt.UTC().Format(time.RFC3339),
	}), nil
}
