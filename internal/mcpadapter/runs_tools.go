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
	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handleRunsList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
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
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d runs found. Use hadron_run_get with a specific run_id for full details including error messages.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
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
		if unmarshalErr := json.Unmarshal([]byte(inputsRaw), &inputs); unmarshalErr != nil {
			return toolError("validation_error", "inputs_json must be a JSON object"), nil //nolint:nilerr
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
	runRec, getRunErr := a.store.GetRun(ctx, runID)
	if getRunErr != nil {
		if isNotFound(getRunErr) {
			return toolError("not_found", "run not found"), nil
		}
		return toolError("internal_error", getRunErr.Error()), nil
	}
	if ws := strings.TrimSpace(req.GetString("workspace_id", "")); ws != "" && runRec.WorkspaceID != ws {
		return toolError("not_found", "run not found in workspace"), nil
	}
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
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
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d events found. Increase limit parameter for more events.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
}

func (a *Adapter) handleRunMCPCalls(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	events, listEventsErr := a.store.ListRunEvents(ctx, runID, 1000)
	if listEventsErr != nil {
		return toolError("internal_error", listEventsErr.Error()), nil
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
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handleRunOperations(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	events, err := a.store.ListRunEvents(ctx, runID, 1000)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	items := rundiagnostics.SummarizeOperations(events)
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	kind := strings.TrimSpace(req.GetString("kind", ""))
	cursor := strings.TrimSpace(req.GetString("cursor", ""))
	page, nextCursor, totalCount, ok := filterPagedOperations(items, kind, limit, cursor)
	if !ok {
		return toolError("validation_error", "invalid cursor"), nil
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
	return toolJSON(map[string]any{
		"items":       out,
		"count":       len(out),
		"total_count": totalCount,
		"next_cursor": next,
	}), nil
}

func filterPagedOperations(items []rundiagnostics.OperationDiagnostic, kind string, limit int, cursor string) ([]rundiagnostics.OperationDiagnostic, string, int, bool) {
	page, nextCursor, totalCount, err := rundiagnostics.FilterAndPageOperations(items, kind, limit, cursor)
	if err != nil {
		return nil, "", 0, false
	}
	return page, nextCursor, totalCount, true
}
