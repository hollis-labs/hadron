package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/internal/pipeline"
)

func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listPipelines(w, r)
	case http.MethodPost:
		s.createPipeline(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handlePipelineByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/pipelines/")
	parts := strings.SplitN(path, "/", 2)
	pipelineID := parts[0]
	if pipelineID == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case sub == "" && r.Method == http.MethodGet:
		s.getPipeline(w, r, pipelineID)
	case sub == "stages" && r.Method == http.MethodGet:
		s.getPipelineStages(w, r, pipelineID)
	case sub == "graph" && r.Method == http.MethodGet:
		s.getPipelineGraph(w, r, pipelineID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wsID := q.Get("workspace_id")
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	items, err := s.deps.Pipelines.ListPipelineRunsByWorkspace(r.Context(), wsID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, toPipelineResponse(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nil})
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceID  string         `json:"workspace_id"`
		PipelinePath string         `json:"pipeline_path"`
		Inputs       map[string]any `json:"inputs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.PipelinePath == "" {
		writeError(w, http.StatusBadRequest, "pipeline_path is required")
		return
	}
	wsID := body.WorkspaceID
	if wsID == "" {
		wsID = "default"
	}
	if s.deps.Pipeline == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline runner unavailable")
		return
	}
	pipelineID := s.nextPipelineID()
	if err := s.deps.Pipeline.Start(r.Context(), pipelineID, body.PipelinePath, wsID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rec, err := s.deps.Pipelines.GetPipelineRun(r.Context(), pipelineID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, toPipelineResponse(rec))
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request, id string) {
	rec, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toPipelineResponse(rec))
}

func (s *Server) getPipelineStages(w http.ResponseWriter, r *http.Request, id string) {
	pipelineRun, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items, err := s.deps.Pipelines.ListPipelineStageRuns(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Try to load the pipeline spec for DAG metadata (depends_on, position, outputs).
	specStages := parsePipelineSpecStages(pipelineRun.PipelinePath)

	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		entry := toPipelineStageResponse(st)
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
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) getPipelineGraph(w http.ResponseWriter, r *http.Request, id string) {
	pipelineRun, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Load stage run records for status.
	stageRuns, err := s.deps.Pipelines.ListPipelineStageRuns(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	statusMap := make(map[string]string, len(stageRuns))
	for _, sr := range stageRuns {
		statusMap[sr.StageName] = sr.Status
	}

	// Parse pipeline spec for DAG structure.
	spec, parseErr := pipeline.ParseFile(pipelineRun.PipelinePath)
	if parseErr != nil {
		writeError(w, http.StatusUnprocessableEntity, "cannot parse pipeline spec: "+parseErr.Error())
		return
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

	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": nodes,
		"edges": edges,
	})
}
