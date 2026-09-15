package mcpadapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handleSchedulesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListSchedulesByWorkspace(ctx, workspaceID)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, sc := range items {
		out = append(out, map[string]any{
			"id":             sc.ID,
			"workspace_id":   sc.WorkspaceID,
			"name":           sc.Name,
			"blueprint_path": sc.BlueprintPath,
			"cron_expr":      sc.CronExpr,
			"enabled":        sc.Enabled,
			"created_at":     sc.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at":     sc.UpdatedAt.UTC().Format(time.RFC3339),
			"last_run_at":    nullString(sc.LastRunAt),
			"next_run_at":    nullString(sc.NextRunAt),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d schedules found. Use schedule IDs to manage individual schedules.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleScheduleCreate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	cronExpr := strings.TrimSpace(req.GetString("cron_expr", ""))
	if cronExpr == "" {
		return toolError("validation_error", "cron_expr is required"), nil
	}
	if err := scheduler.ValidateCron(cronExpr); err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	name := strings.TrimSpace(req.GetString("name", ""))
	if name == "" {
		name = blueprintPath
	}
	enabled := req.GetBool("enabled", true)

	nextRun, err := scheduler.NextRun(cronExpr, time.Now())
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}

	now := time.Now().UTC()
	schedID := fmt.Sprintf("mcp-sched-%s", time.Now().UTC().Format("20060102-150405"))
	rec := persistence.ScheduleRecord{
		ID:            schedID,
		WorkspaceID:   workspaceID,
		Name:          name,
		BlueprintPath: blueprintPath,
		CronExpr:      cronExpr,
		Enabled:       enabled,
		CreatedAt:     now,
		UpdatedAt:     now,
		NextRunAt: sql.NullString{
			String: nextRun.UTC().Format(time.RFC3339),
			Valid:  true,
		},
	}
	if err := a.store.CreateSchedule(ctx, rec); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"name":           rec.Name,
		"blueprint_path": rec.BlueprintPath,
		"cron_expr":      rec.CronExpr,
		"enabled":        rec.Enabled,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
	}), nil
}

func (a *Adapter) handleScheduleUpdate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	scheduleID := strings.TrimSpace(req.GetString("schedule_id", ""))
	if scheduleID == "" {
		return toolError("validation_error", "schedule_id is required"), nil
	}
	enabled := req.GetBool("enabled", true)
	if err := a.store.UpdateScheduleEnabledAndNext(ctx, scheduleID, enabled, nil); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"schedule_id": scheduleID, "enabled": enabled}), nil
}
func (a *Adapter) handleScheduleDelete(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeScheduleWrite); deny != nil {
		return deny, nil
	}
	scheduleID := strings.TrimSpace(req.GetString("schedule_id", ""))
	if scheduleID == "" {
		return toolError("validation_error", "schedule_id is required"), nil
	}
	if err := a.store.DeleteSchedule(ctx, scheduleID); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"schedule_id": scheduleID, "deleted": true}), nil
}
