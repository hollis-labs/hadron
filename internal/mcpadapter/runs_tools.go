package mcpadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
)

func (a *Adapter) handleRunsList(ctx context.Context, args map[string]any) (any, error) {
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))
	limit := budget.ExtractLimit(args, budget.DefaultLimit)
	items, err := a.store.ListRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	out := make([]map[string]any, 0, len(items))
	for _, r := range items {
		out = append(out, map[string]any{
			"id":         r.ID,
			"blueprint":  r.BlueprintPath,
			"status":     r.Status,
			"created_at": r.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return budget.Apply(out, budget.Config{Limit: limit},
		"%d runs found. Use hadron_run_get with a specific run_id for full details including error messages."), nil
}

func (a *Adapter) handleRunGet(ctx context.Context, args map[string]any) (any, error) {
	runID := strings.TrimSpace(argString(args, "run_id", ""))
	if runID == "" {
		return nil, budget.NewToolError("validation_error", "run_id is required").WithField("run_id")
	}
	rec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return nil, notFoundRun()
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && rec.WorkspaceID != ws {
		return nil, notFoundRun()
	}
	var inputs map[string]any
	if rec.InputJSON != "" {
		_ = json.Unmarshal([]byte(rec.InputJSON), &inputs)
	}
	return map[string]any{
		"id":             rec.ID,
		"workspace_id":   rec.WorkspaceID,
		"blueprint_path": rec.BlueprintPath,
		"status":         rec.Status,
		"inputs":         inputs,
		"created_at":     rec.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":     nullString(rec.StartedAt),
		"ended_at":       nullString(rec.EndedAt),
		"error_message":  nullString(rec.ErrorMessage),
	}, nil
}

func (a *Adapter) handleRunEnqueue(ctx context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeRunWrite); err != nil {
		return nil, err
	}
	if a.runner == nil {
		return nil, budget.NewToolError("unavailable", "runner unavailable").WithRetryable(true)
	}
	workspaceID := workspaceDefault(argString(args, "workspace_id", "default"))
	if _, err := a.store.GetWorkspace(ctx, workspaceID); err != nil {
		if isNotFound(err) {
			return nil, budget.NewToolError("not_found", "workspace not found").
				WithField("workspace_id").WithHelpTool("hadron_workspaces_list").
				WithNextStep("call hadron_workspaces_list to find a valid workspace_id")
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	blueprintPath := strings.TrimSpace(argString(args, "blueprint_path", ""))
	if blueprintPath == "" {
		return nil, budget.NewToolError("validation_error", "blueprint_path is required").WithField("blueprint_path")
	}
	bp, err := blueprint.ParseFile(blueprintPath)
	if err != nil {
		return nil, budget.NewToolError("validation_error", err.Error()).WithField("blueprint_path")
	}
	inputsRaw := strings.TrimSpace(argString(args, "inputs_json", ""))
	inputs := map[string]any{}
	if inputsRaw != "" {
		if unmarshalErr := json.Unmarshal([]byte(inputsRaw), &inputs); unmarshalErr != nil {
			return nil, budget.NewToolError("validation_error", "inputs_json must be a JSON object").WithField("inputs_json")
		}
	}
	normalized, err := blueprint.NormalizeInputs(bp, inputs)
	if err != nil {
		return nil, budget.NewToolError("validation_error", err.Error()).WithField("inputs_json").
			WithHelpTool("hadron_blueprint_schema").WithNextStep("call hadron_blueprint_schema to inspect the required inputs")
	}
	runID := fmt.Sprintf("mcp-run-%s-%04d", time.Now().UTC().Format("20060102-150405"), atomic.AddUint64(&runSeq, 1))
	if err := a.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   workspaceID,
		BlueprintPath: blueprintPath,
		Inputs:        normalized,
	}); err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	return map[string]any{"run_id": runID, "status": "queued", "workspace_id": workspaceID}, nil
}

func (a *Adapter) handleRunCancel(_ context.Context, args map[string]any) (any, error) {
	if err := a.checkScope(ScopeRunCancel); err != nil {
		return nil, err
	}
	if a.runner == nil {
		return nil, budget.NewToolError("unavailable", "runner unavailable").WithRetryable(true)
	}
	runID := strings.TrimSpace(argString(args, "run_id", ""))
	if runID == "" {
		return nil, budget.NewToolError("validation_error", "run_id is required").WithField("run_id")
	}
	if ok := a.runner.Cancel(runID); !ok {
		return nil, budget.NewToolError("not_found", "run not running").WithField("run_id")
	}
	return map[string]any{"run_id": runID, "status": "cancellation_requested"}, nil
}

func (a *Adapter) handleRunEvents(ctx context.Context, args map[string]any) (any, error) {
	runID := strings.TrimSpace(argString(args, "run_id", ""))
	if runID == "" {
		return nil, budget.NewToolError("validation_error", "run_id is required").WithField("run_id")
	}
	runRec, getRunErr := a.store.GetRun(ctx, runID)
	if getRunErr != nil {
		if isNotFound(getRunErr) {
			return nil, notFoundRun()
		}
		return nil, budget.NewToolError("internal_error", getRunErr.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return nil, notFoundRun()
	}
	limit := budget.ExtractLimit(args, budget.DefaultLimit)
	items, err := a.store.ListRunEvents(ctx, runID, limit)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
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
	return budget.Apply(out, budget.Config{Limit: limit},
		"%d events found. Increase limit parameter for more events."), nil
}

func (a *Adapter) handleRunMCPCalls(ctx context.Context, args map[string]any) (any, error) {
	runID := strings.TrimSpace(argString(args, "run_id", ""))
	if runID == "" {
		return nil, budget.NewToolError("validation_error", "run_id is required").WithField("run_id")
	}
	runRec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return nil, notFoundRun()
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return nil, notFoundRun()
	}
	events, listEventsErr := a.store.ListRunEvents(ctx, runID, 1000)
	if listEventsErr != nil {
		return nil, budget.NewToolError("internal_error", listEventsErr.Error())
	}
	items := rundiagnostics.SummarizeMCPCalls(events)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"sequence":      item.Sequence,
			"step_name":     item.StepName,
			"server":        item.Server,
			"tool":          item.Tool,
			"transport":     item.Transport,
			"status":        item.Status,
			"retry_count":   item.RetryCount,
			"attempt_count": item.AttemptCount,
			"reused_client": item.ReusedClient,
			"health_probe":  item.HealthProbe,
			"reconnected":   item.Reconnected,
			"truncated":     item.Truncated,
			"result_json":   item.ResultJSON,
			"error_message": item.ErrorMessage,
			"started_at":    item.StartedAt,
			"finished_at":   item.FinishedAt,
		})
	}
	return map[string]any{"items": out, "count": len(out)}, nil
}

func (a *Adapter) handleRunOperations(ctx context.Context, args map[string]any) (any, error) {
	runID := strings.TrimSpace(argString(args, "run_id", ""))
	if runID == "" {
		return nil, budget.NewToolError("validation_error", "run_id is required").WithField("run_id")
	}
	runRec, err := a.store.GetRun(ctx, runID)
	if err != nil {
		if isNotFound(err) {
			return nil, notFoundRun()
		}
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	if ws := strings.TrimSpace(argString(args, "workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return nil, notFoundRun()
	}
	events, err := a.store.ListRunEvents(ctx, runID, 1000)
	if err != nil {
		return nil, budget.NewToolError("internal_error", err.Error())
	}
	items := rundiagnostics.SummarizeOperations(events)
	limit := budget.ExtractLimit(args, budget.DefaultLimit)
	kind := strings.TrimSpace(argString(args, "kind", ""))
	cursor := strings.TrimSpace(argString(args, "cursor", ""))
	page, nextCursor, totalCount, ok := filterPagedOperations(items, kind, limit, cursor)
	if !ok {
		return nil, budget.NewToolError("validation_error", "invalid cursor").WithField("cursor")
	}
	out := make([]map[string]any, 0, len(page))
	for _, item := range page {
		out = append(out, map[string]any{
			"sequence":         item.Sequence,
			"kind":             item.Kind,
			"step_name":        item.StepName,
			"status":           item.Status,
			"started_at":       item.StartedAt,
			"finished_at":      item.FinishedAt,
			"error_message":    item.ErrorMessage,
			"truncated":        item.Truncated,
			"result_json":      item.ResultJSON,
			"server":           item.Server,
			"tool":             item.Tool,
			"transport":        item.Transport,
			"retry_count":      item.RetryCount,
			"attempt_count":    item.AttemptCount,
			"reused_client":    item.ReusedClient,
			"health_probe":     item.HealthProbe,
			"reconnected":      item.Reconnected,
			"method":           item.Method,
			"url":              item.URL,
			"status_code":      item.StatusCode,
			"duration_ms":      item.DurationMS,
			"substrate":        item.Substrate,
			"to":               item.To,
			"correlation_id":   item.CorrelationID,
			"timeout_ms":       item.TimeoutMS,
			"poll_count":       item.PollCount,
			"message_id":       item.MessageID,
			"logical_agent_id": item.LogicalAgentID,
			"launch_id":        item.LaunchID,
			"gate_id":          item.GateID,
			"decision":         item.Decision,
			"prompt":           item.Prompt,
		})
	}
	var next any
	if nextCursor != "" {
		next = nextCursor
	}
	return map[string]any{
		"items":       out,
		"count":       len(out),
		"total_count": totalCount,
		"next_cursor": next,
	}, nil
}

func notFoundRun() *budget.ToolError {
	return budget.NewToolError("not_found", "run not found").
		WithField("run_id").WithHelpTool("hadron_runs_list").
		WithNextStep("call hadron_runs_list to find a valid run_id")
}

func filterPagedOperations(items []rundiagnostics.OperationDiagnostic, kind string, limit int, cursor string) ([]rundiagnostics.OperationDiagnostic, string, int, bool) {
	page, nextCursor, totalCount, err := rundiagnostics.FilterAndPageOperations(items, kind, limit, cursor)
	if err != nil {
		return nil, "", 0, false
	}
	return page, nextCursor, totalCount, true
}
