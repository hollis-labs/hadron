package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
)

func toWorkspaceResponse(ws persistence.WorkspaceRecord) map[string]any {
	return map[string]any{
		"id":         ws.ID,
		"name":       ws.Name,
		"created_at": ws.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": ws.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func toRunResponse(r persistence.RunRecord) map[string]any {
	return map[string]any{
		"id":             r.ID,
		"workspace_id":   r.WorkspaceID,
		"blueprint_path": r.BlueprintPath,
		"status":         r.Status,
		"input_json":     r.InputJSON,
		"error_message":  nullableString(r.ErrorMessage),
		"created_at":     r.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":     nullableString(r.StartedAt),
		"ended_at":       nullableString(r.EndedAt),
	}
}

func toRunEventResponse(ev persistence.RunEventRecord) map[string]any {
	return map[string]any{
		"id":         ev.ID,
		"run_id":     ev.RunID,
		"step_name":  nullableString(ev.StepName),
		"event_type": ev.EventType,
		"message":    nullableString(ev.Message),
		"created_at": ev.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func toScheduleResponse(sc persistence.ScheduleRecord) map[string]any {
	return map[string]any{
		"id":             sc.ID,
		"workspace_id":   sc.WorkspaceID,
		"name":           sc.Name,
		"blueprint_path": sc.BlueprintPath,
		"cron_expr":      sc.CronExpr,
		"enabled":        sc.Enabled,
		"created_at":     sc.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":     sc.UpdatedAt.UTC().Format(time.RFC3339),
		"last_run_at":    nullableString(sc.LastRunAt),
		"next_run_at":    nullableString(sc.NextRunAt),
	}
}

func toPipelineResponse(p persistence.PipelineRunRecord) map[string]any {
	return map[string]any{
		"id":            p.ID,
		"workspace_id":  p.WorkspaceID,
		"pipeline_path": p.PipelinePath,
		"status":        p.Status,
		"error_message": nullableString(p.ErrorMessage),
		"created_at":    p.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":    nullableString(p.StartedAt),
		"ended_at":      nullableString(p.EndedAt),
	}
}

func toPipelineStageResponse(st persistence.PipelineStageRunRecord) map[string]any {
	return map[string]any{
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
}

// parsePipelineSpecStages attempts to parse a pipeline spec file and returns
// a map of stage name to Stage for DAG metadata enrichment. Returns an empty
// map on any parse error so callers can degrade gracefully.
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

func (s *Server) nextRunID() string {
	n := s.runSeq.Add(1)
	return fmt.Sprintf("run-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

func (s *Server) nextScheduleID() string {
	n := s.scheduleSeq.Add(1)
	return fmt.Sprintf("sched-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

func (s *Server) nextPipelineID() string {
	n := s.pipelineSeq.Add(1)
	return fmt.Sprintf("pl-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no rows") ||
		fmt.Sprintf("%v", err) == "sql: no rows in result set"
}

func isInvalidCursor(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "invalid cursor")
}

func nullableString(s sql.NullString) any {
	if s.Valid {
		return s.String
	}
	return nil
}
