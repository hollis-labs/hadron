package mcpadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

var runSeq uint64
var pipelineSeq uint64

func (a *Adapter) registerTools(s *server.MCPServer) {
	s.AddTool(mcp.NewTool("hadron_health",
		mcp.WithDescription("Read Hadron MCP adapter health/status."),
	), a.handleHealth)

	s.AddTool(mcp.NewTool("hadron_workspaces_list",
		mcp.WithDescription("List all Hadron workspaces."),
	), a.handleWorkspacesList)

	s.AddTool(mcp.NewTool("hadron_workspace_get",
		mcp.WithDescription("Get a workspace by id."),
		mcp.WithString("workspace_id", mcp.Required(), mcp.Description("Workspace id")),
	), a.handleWorkspaceGet)

	s.AddTool(mcp.NewTool("hadron_workspace_create",
		mcp.WithDescription("Create a workspace (requires scope workspace.write)."),
		mcp.WithString("workspace_id", mcp.Required(), mcp.Description("Workspace id")),
		mcp.WithString("name", mcp.Description("Workspace display name")),
	), a.handleWorkspaceCreate)

	s.AddTool(mcp.NewTool("hadron_runs_list",
		mcp.WithDescription("List runs for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithNumber("limit", mcp.Description("Max items (default 20, max 200)")),
	), a.handleRunsList)

	s.AddTool(mcp.NewTool("hadron_run_get",
		mcp.WithDescription("Get a run by id."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handleRunGet)

	s.AddTool(mcp.NewTool("hadron_run_enqueue",
		mcp.WithDescription("Enqueue a blueprint run (requires scope run.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint path")),
		mcp.WithString("inputs_json", mcp.Description("JSON object string for inputs")),
	), a.handleRunEnqueue)

	s.AddTool(mcp.NewTool("hadron_run_cancel",
		mcp.WithDescription("Cancel a running run (requires scope run.cancel)."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
	), a.handleRunCancel)

	s.AddTool(mcp.NewTool("hadron_run_events",
		mcp.WithDescription("List run events for a run id."),
		mcp.WithString("run_id", mcp.Required(), mcp.Description("Run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
		mcp.WithNumber("limit", mcp.Description("Max items (default 100, max 500)")),
	), a.handleRunEvents)

	s.AddTool(mcp.NewTool("hadron_schedules_list",
		mcp.WithDescription("List schedules for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
	), a.handleSchedulesList)

	s.AddTool(mcp.NewTool("hadron_schedule_create",
		mcp.WithDescription("Create a schedule (requires scope schedule.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("name", mcp.Description("Schedule display name")),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Blueprint path")),
		mcp.WithString("cron_expr", mcp.Required(), mcp.Description("Cron expression (standard 5-field)")),
		mcp.WithBoolean("enabled", mcp.Description("Whether schedule is enabled (default true)")),
	), a.handleScheduleCreate)

	s.AddTool(mcp.NewTool("hadron_schedule_update",
		mcp.WithDescription("Update schedule enabled state (requires scope schedule.write)."),
		mcp.WithString("schedule_id", mcp.Required(), mcp.Description("Schedule id")),
		mcp.WithBoolean("enabled", mcp.Required(), mcp.Description("Enable or disable the schedule")),
	), a.handleScheduleUpdate)

	s.AddTool(mcp.NewTool("hadron_pipelines_list",
		mcp.WithDescription("List pipeline runs for a workspace."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithNumber("limit", mcp.Description("Max items (default 20, max 200)")),
	), a.handlePipelinesList)

	s.AddTool(mcp.NewTool("hadron_pipeline_enqueue",
		mcp.WithDescription("Start a pipeline run (requires scope pipeline.write)."),
		mcp.WithString("workspace_id", mcp.Description("Workspace id (default: default)")),
		mcp.WithString("pipeline_path", mcp.Required(), mcp.Description("Pipeline spec path")),
	), a.handlePipelineEnqueue)

	s.AddTool(mcp.NewTool("hadron_pipeline_stages",
		mcp.WithDescription("List stages for a pipeline run."),
		mcp.WithString("pipeline_run_id", mcp.Required(), mcp.Description("Pipeline run id")),
		mcp.WithString("workspace_id", mcp.Description("Optional workspace scope check")),
	), a.handlePipelineStages)

	s.AddTool(mcp.NewTool("hadron_blueprint_validate",
		mcp.WithDescription("Validate blueprint YAML/JSON content."),
		mcp.WithString("content", mcp.Required(), mcp.Description("Blueprint YAML or JSON content")),
	), a.handleBlueprintValidate)

	s.AddTool(mcp.NewTool("hadron_blueprints_list",
		mcp.WithDescription("List available blueprint files from the blueprints directory."),
	), a.handleBlueprintsList)

	s.AddTool(mcp.NewTool("hadron_blueprint_get",
		mcp.WithDescription("Read a blueprint file's YAML content by path."),
		mcp.WithString("blueprint_path", mcp.Required(), mcp.Description("Path to the blueprint file")),
	), a.handleBlueprintGet)

	s.AddTool(mcp.NewTool("hadron_schedule_delete",
		mcp.WithDescription("Delete a schedule by id (requires scope schedule.write)."),
		mcp.WithString("schedule_id", mcp.Required(), mcp.Description("Schedule id")),
	), a.handleScheduleDelete)
}

func (a *Adapter) handleHealth(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return toolJSON(map[string]any{
		"status":    "ok",
		"service":   "hadron-mcp",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}), nil
}

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

func (a *Adapter) handleRunsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := clamp(req.GetInt("limit", 20), 1, 200)
	items, err := a.store.ListRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, r := range items {
		out = append(out, map[string]any{
			"id":             r.ID,
			"workspace_id":   r.WorkspaceID,
			"blueprint_path": r.BlueprintPath,
			"status":         r.Status,
			"created_at":     r.CreatedAt.UTC().Format(time.RFC3339),
			"started_at":     nullString(r.StartedAt),
			"ended_at":       nullString(r.EndedAt),
			"error_message":  nullString(r.ErrorMessage),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleRunGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	rec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	var inputs map[string]any
	if rec.InputJSON != "" {
		_ = json.Unmarshal([]byte(rec.InputJSON), &inputs)
	}
	return toolJSON(map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"blueprint_path": rec.BlueprintPath,
		"status":         rec.Status,
		"inputs":         inputs,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":     nullString(rec.StartedAt),
		"ended_at":       nullString(rec.EndedAt),
		"error_message":  nullString(rec.ErrorMessage),
	}), nil
}

func (a *Adapter) handleRunEnqueue(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeRunWrite); deny != nil {
		return deny, nil
	}
	if a.runner == nil {
		return toolError("unavailable", "runner unavailable"), nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	if _, err := a.store.GetWorkspace(ctx, workspaceID); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	blueprintPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if blueprintPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	bp, err := blueprint.ParseFile(blueprintPath)
	if err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	inputsRaw := strings.TrimSpace(req.GetString("inputs_json", ""))
	inputs := map[string]any{}
	if inputsRaw != "" {
		if err := json.Unmarshal([]byte(inputsRaw), &inputs); err != nil {
			return toolError("validation_error", "inputs_json must be a JSON object"), nil
		}
	}
	normalized, err := blueprint.NormalizeInputs(bp, inputs)
	if err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	runID := fmt.Sprintf("mcp-run-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	if err := a.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   workspaceID,
		BlueprintPath: blueprintPath,
		Inputs:        normalized,
	}); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"run_id": runID, "status": "queued", "workspace_id": workspaceID}), nil
}

func (a *Adapter) handleRunCancel(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopeRunCancel); deny != nil {
		return deny, nil
	}
	if a.runner == nil {
		return toolError("unavailable", "runner unavailable"), nil
	}
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	if ok := a.runner.Cancel(runID); !ok {
		return toolError("not_found", "run not running"), nil
	}
	return toolJSON(map[string]any{"run_id": runID, "status": "cancellation_requested"}), nil
}

func (a *Adapter) handleRunEvents(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	runID := strings.TrimSpace(req.GetString("run_id", ""))
	if runID == "" {
		return toolError("validation_error", "run_id is required"), nil
	}
	runRec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	limit := clamp(req.GetInt("limit", 100), 1, 500)
	items, err := a.store.ListRunEvents(ctx, runID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, ev := range items {
		out = append(out, map[string]any{
			"id":         ev.ID,
			"run_id":     ev.RunID,
			"step_name":  nullString(ev.StepName),
			"event_type": ev.EventType,
			"message":    nullString(ev.Message),
			"created_at": ev.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleSchedulesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
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
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
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

func (a *Adapter) handlePipelinesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := clamp(req.GetInt("limit", 20), 1, 200)
	items, err := a.store.ListPipelineRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, map[string]any{
			"id":            p.ID,
			"workspace_id":  p.WorkspaceID,
			"pipeline_path": p.PipelinePath,
			"status":        p.Status,
			"created_at":    p.CreatedAt.UTC().Format(time.RFC3339),
			"started_at":    nullString(p.StartedAt),
			"ended_at":      nullString(p.EndedAt),
			"error_message": nullString(p.ErrorMessage),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handlePipelineEnqueue(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if deny := a.checkScope(ScopePipelineWrite); deny != nil {
		return deny, nil
	}
	if a.pipeline == nil {
		return toolError("unavailable", "pipeline runner unavailable"), nil
	}
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	if _, err := a.store.GetWorkspace(ctx, workspaceID); err != nil {
		if isNotFound(err) {
			return toolError("not_found", "workspace not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	pipelinePath := strings.TrimSpace(req.GetString("pipeline_path", ""))
	if pipelinePath == "" {
		return toolError("validation_error", "pipeline_path is required"), nil
	}
	if _, err := pipeline.ParseFile(pipelinePath); err != nil {
		return toolError("validation_error", err.Error()), nil
	}
	id := fmt.Sprintf("mcp-pl-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&pipelineSeq, 1))
	if err := a.pipeline.Start(ctx, id, pipelinePath, workspaceID); err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	return toolJSON(map[string]any{"pipeline_run_id": id, "status": "queued", "workspace_id": workspaceID}), nil
}

func (a *Adapter) handlePipelineStages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := strings.TrimSpace(req.GetString("pipeline_run_id", ""))
	if id == "" {
		return toolError("validation_error", "pipeline_run_id is required"), nil
	}
	runRec, err := a.store.GetPipelineRun(ctx, id)
	if err != nil {
		if isNotFound(err) {
			return toolError("not_found", "pipeline run not found"), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "pipeline run not found in workspace"), nil
	}
	items, err := a.store.ListPipelineStageRuns(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		out = append(out, map[string]any{
			"id":              st.ID,
			"workspace_id":    st.WorkspaceID,
			"pipeline_run_id": st.PipelineRunID,
			"stage_index":     st.StageIndex,
			"stage_name":      st.StageName,
			"run_id":          st.RunID,
			"status":          st.Status,
			"created_at":      st.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at":      st.UpdatedAt.UTC().Format(time.RFC3339),
		})
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleBlueprintValidate(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	content := req.GetString("content", "")
	if strings.TrimSpace(content) == "" {
		return toolError("validation_error", "content is required"), nil
	}
	_, err := blueprint.ParseBytes([]byte(content))
	if err != nil {
		return toolJSON(map[string]any{"valid": false, "error": err.Error()}), nil
	}
	return toolJSON(map[string]any{"valid": true}), nil
}

func (a *Adapter) handleBlueprintsList(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dir := a.blueprintDir
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return toolJSON(map[string]any{"items": []any{}, "count": 0, "directory": dir}), nil
		}
		return toolError("internal_error", err.Error()), nil
	}
	var items []map[string]any
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := filepath.Ext(name)
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		items = append(items, map[string]any{
			"name":      name,
			"path":      filepath.Join(dir, name),
			"size":      info.Size(),
			"modified":  info.ModTime().UTC().Format(time.RFC3339),
		})
	}
	if items == nil {
		items = []map[string]any{}
	}
	return toolJSON(map[string]any{"items": items, "count": len(items), "directory": dir}), nil
}

func (a *Adapter) handleBlueprintGet(_ context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	bpPath := strings.TrimSpace(req.GetString("blueprint_path", ""))
	if bpPath == "" {
		return toolError("validation_error", "blueprint_path is required"), nil
	}
	// Resolve to absolute and ensure the path is within the blueprints directory.
	absPath, err := filepath.Abs(bpPath)
	if err != nil {
		return toolError("validation_error", "invalid path"), nil
	}
	absDir, err := filepath.Abs(a.blueprintDir)
	if err != nil {
		return toolError("internal_error", "cannot resolve blueprint directory"), nil
	}
	if !strings.HasPrefix(absPath, absDir+string(filepath.Separator)) && absPath != absDir {
		return toolError("validation_error", "path is outside the blueprints directory"), nil
	}
	data, err := os.ReadFile(absPath)
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

// ── helpers ───────────────────────────────────────────────────────────────────

func workspaceDefault(workspaceID string) string {
	if strings.TrimSpace(workspaceID) == "" {
		return "default"
	}
	return strings.TrimSpace(workspaceID)
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func nullString(s sql.NullString) any {
	if s.Valid {
		return s.String
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows) || strings.Contains(strings.ToLower(err.Error()), "no rows")
}
