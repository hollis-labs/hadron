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
)

func (a *Adapter) handleSchedulesList(ctx context.Context, args map[string]any) (any, error) {
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))
	limit := budget.ExtractLimit(args, budget.DefaultLimit)
	items, err := a.store.ListSchedulesByWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
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
	return budget.Apply(out, budget.Config{Limit: limit},
		"%d schedules found. Use schedule IDs to manage individual schedules."), nil
}

func (a *Adapter) handleScheduleCreate(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeScheduleWrite); err != nil {
		return nil, err
	}
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))
	blueprintPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if blueprintPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}
	cronExpr := strings.TrimSpace(argString(args, "cron_expr", ""))
	if cronExpr == "" {
		return nil, budget.NewToolError("validation_error", "cron_expr is required").WithField("cron_expr")
	}
	if err := scheduler.ValidateCron(cronExpr); err != nil {
		return nil, budget.NewToolError("validation_error", err.Error()).WithField("cron_expr")
	}
	name := strings.TrimSpace(argString(args, "name", ""))
	if name == "" {
		name = blueprintPath
	}
	enabled := argBool(args, "enabled", true)

	nextRun, err := scheduler.NextRun(cronExpr, time.Now())
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
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
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"name":           rec.Name,
		"blueprint_path": rec.BlueprintPath,
		"cron_expr":      rec.CronExpr,
		"enabled":        rec.Enabled,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (a *Adapter) handleScheduleUpdate(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeScheduleWrite); err != nil {
		return nil, err
	}
	scheduleID := strings.TrimSpace(argString(args, "schedule_id", ""))
	if scheduleID == "" {
		return nil, budget.NewToolError("validation_error", "schedule_id is required").WithField("schedule_id")
	}
	enabled := argBool(args, "enabled", true)
	if err := a.store.UpdateScheduleEnabledAndNext(ctx, scheduleID, enabled, nil); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{"schedule_id": scheduleID, "enabled": enabled}, nil
}

func (a *Adapter) handleScheduleDelete(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeScheduleWrite); err != nil {
		return nil, err
	}
	scheduleID := strings.TrimSpace(argString(args, "schedule_id", ""))
	if scheduleID == "" {
		return nil, budget.NewToolError("validation_error", "schedule_id is required").WithField("schedule_id")
	}
	if err := a.store.DeleteSchedule(ctx, scheduleID); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{"schedule_id": scheduleID, "deleted": true}, nil
}
