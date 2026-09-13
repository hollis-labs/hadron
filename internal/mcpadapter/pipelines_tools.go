package mcpadapter

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/mark3labs/mcp-go/mcp"
)

func (a *Adapter) handlePipelinesList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	workspaceID := workspaceDefault(req.GetString("workspace_id", "default"))
	limit := budget.ExtractLimit(req.GetArguments(), budget.DefaultLimit)
	items, err := a.store.ListPipelineRunsByWorkspace(ctx, workspaceID, limit)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, map[string]any{
			"id":            p.ID,
			"pipeline_path": p.PipelinePath,
			"status":        p.Status,
			"created_at":    p.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	env := budget.Apply(out, budget.Config{Limit: limit},
		"%d pipeline runs found. Use hadron_pipeline_stages with a specific pipeline_run_id for full details.")
	return mcp.NewToolResultText(budget.ToolJSON(env)), nil
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

	// Try to load pipeline spec for DAG metadata enrichment.
	specStages := parsePipelineSpecStages(runRec.PipelinePath)

	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		entry := map[string]any{
			"id":              st.ID,
			"workspace_id":    st.WorkspaceID,
			"pipeline_run_id": st.PipelineRunID,
			"stage_index":     st.StageIndex,
			"stage_name":      st.StageName,
			"run_id":          st.RunID,
			"status":          st.Status,
			"created_at":      st.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at":      st.UpdatedAt.UTC().Format(time.RFC3339),
		}
		if spec, ok := specStages[st.StageName]; ok {
			entry["depends_on"] = spec.DependsOn
			if spec.Position != nil {
				entry["position"] = map[string]any{"x": spec.Position.X, "y": spec.Position.Y}
			} else {
				entry["position"] = nil
			}
			entry["outputs"] = spec.Outputs
		} else {
			entry["depends_on"] = nil
			entry["position"] = nil
			entry["outputs"] = nil
		}
		out = append(out, entry)
	}
	return toolJSON(map[string]any{"items": out, "count": len(out)}), nil
}

func (a *Adapter) handlePipelineGraph(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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

	// Load stage run records for status.
	stageRuns, err := a.store.ListPipelineStageRuns(ctx, id)
	if err != nil {
		return toolError("internal_error", err.Error()), nil
	}
	statusMap := make(map[string]string, len(stageRuns))
	for _, sr := range stageRuns {
		statusMap[sr.StageName] = sr.Status
	}

	// Parse pipeline spec for DAG structure.
	spec, parseErr := pipeline.ParseFile(runRec.PipelinePath)
	if parseErr != nil {
		return toolError("internal_error", "cannot parse pipeline spec: "+parseErr.Error()), nil //nolint:nilerr
	}

	nodes := make([]map[string]any, 0, len(spec.Stages))
	edges := make([]map[string]any, 0)

	for _, stage := range spec.Stages {
		status := statusMap[stage.Name]
		if status == "" {
			status = "pending"
		}

		var pos map[string]any
		if stage.Position != nil {
			pos = map[string]any{"x": stage.Position.X, "y": stage.Position.Y}
		}

		outputs := map[string]string{}
		if stage.Outputs != nil {
			outputs = stage.Outputs
		}

		nodes = append(nodes, map[string]any{
			"id":             stage.Name,
			"name":           stage.Name,
			"blueprint_path": stage.BlueprintPath,
			"position":       pos,
			"status":         status,
			"outputs":        outputs,
		})

		for _, dep := range stage.DependsOn {
			edges = append(edges, map[string]any{
				"source":    dep,
				"target":    stage.Name,
				"condition": stage.If,
			})
		}
	}

	return toolJSON(map[string]any{
		"nodes": nodes,
		"edges": edges,
	}), nil
}

// parsePipelineSpecStages attempts to parse a pipeline spec file and returns
// a map of stage name → Stage for DAG metadata enrichment. Returns nil on
// any parse error so callers can degrade gracefully.
func parsePipelineSpecStages(pipelinePath string) map[string]pipeline.Stage {
	if pipelinePath == "" {
		return nil
	}
	spec, err := pipeline.ParseFile(pipelinePath)
	if err != nil {
		return nil
	}
	m := make(map[string]pipeline.Stage, len(spec.Stages))
	for _, st := range spec.Stages {
		m[st.Name] = st
	}
	return m
}
