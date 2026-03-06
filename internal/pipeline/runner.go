package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

type Store interface {
	CreatePipelineRun(ctx context.Context, rec persistence.PipelineRunRecord) error
	SetPipelineRunStarted(ctx context.Context, id string, startedAt time.Time) error
	SetPipelineRunFinished(ctx context.Context, id, status string, endedAt time.Time, errMsg *string) error
	AddPipelineStageRun(ctx context.Context, rec persistence.PipelineStageRunRecord) error
	UpdatePipelineStageRunStatus(ctx context.Context, pipelineRunID string, stageIndex int, status string) error
	GetRun(ctx context.Context, id string) (persistence.RunRecord, error)
	ListRunEvents(ctx context.Context, runID string, limit int) ([]persistence.RunEventRecord, error)
}

type Enqueuer interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

type Runner struct {
	store    Store
	enqueuer Enqueuer
}

func NewRunner(store Store, enq Enqueuer) *Runner {
	return &Runner{store: store, enqueuer: enq}
}

func (r *Runner) Start(ctx context.Context, pipelineRunID, pipelinePath, workspaceID string) error {
	if pipelineRunID == "" {
		return fmt.Errorf("pipeline run id is required")
	}
	if workspaceID == "" {
		workspaceID = "default"
	}
	now := time.Now().UTC()
	if err := r.store.CreatePipelineRun(ctx, persistence.PipelineRunRecord{
		ID:           pipelineRunID,
		WorkspaceID:  workspaceID,
		PipelinePath: pipelinePath,
		Status:       "queued",
		CreatedAt:    now,
	}); err != nil {
		return err
	}

	go r.execute(pipelineRunID, pipelinePath, workspaceID)
	return nil
}

func (r *Runner) execute(pipelineRunID, pipelinePath, workspaceID string) {
	ctx := context.Background()
	_ = r.store.SetPipelineRunStarted(ctx, pipelineRunID, time.Now().UTC())

	spec, err := ParseFile(pipelinePath)
	if err != nil {
		msg := fmt.Sprintf("parse pipeline: %v", err)
		_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
		return
	}

	stopOnFail := spec.ShouldStopOnFail()
	baseDir := filepath.Dir(pipelinePath)

	// Inter-stage data: map of stageName → outputs
	stageOutputs := map[string]map[string]string{}

	// Pipeline-level inputs as strings for template resolution
	pipelineInputs := map[string]string{}
	for k, v := range spec.Inputs {
		pipelineInputs[k] = fmt.Sprintf("%v", v)
	}

	for i, st := range spec.Stages {
		blueprintPath := st.BlueprintPath
		if !filepath.IsAbs(blueprintPath) {
			blueprintPath = filepath.Join(baseDir, blueprintPath)
		}
		runID := fmt.Sprintf("plr-%s-%02d-%d", pipelineRunID, i, time.Now().UTC().UnixNano())
		now := time.Now().UTC()
		if err := r.store.AddPipelineStageRun(ctx, persistence.PipelineStageRunRecord{
			WorkspaceID:   workspaceID,
			PipelineRunID: pipelineRunID,
			StageIndex:    i,
			StageName:     st.Name,
			RunID:         runID,
			Status:        "queued",
			CreatedAt:     now,
			UpdatedAt:     now,
		}); err != nil {
			msg := fmt.Sprintf("add stage run %d: %v", i, err)
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}

		// Evaluate conditional: skip stage if `if:` resolves to false
		if st.If != "" {
			condition := resolveTemplate(st.If, stageOutputs, pipelineInputs)
			if !evaluateCondition(condition) {
				_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "skipped")
				continue
			}
		}

		if err := r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "running"); err != nil {
			msg := fmt.Sprintf("update stage %d running: %v", i, err)
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}

		// Resolve stage inputs: substitute {{ stages.<name>.<key> }} and {{ inputs.<key> }}
		resolvedInputs := resolveStageInputs(st.Inputs, stageOutputs, pipelineInputs)

		if err := r.enqueuer.Enqueue(ctx, execution.Request{
			WorkspaceID:   workspaceID,
			RunID:         runID,
			BlueprintPath: blueprintPath,
			Inputs:        resolvedInputs,
		}); err != nil {
			msg := fmt.Sprintf("enqueue stage %d run: %v", i, err)
			_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "failed")
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}

		runRec, waitErr := r.waitForRunTerminal(ctx, runID, 60*time.Second)
		if waitErr != nil {
			msg := fmt.Sprintf("stage %d wait failed: %v", i, waitErr)
			_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "failed")
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}

		// Extract outputs from run events (lines matching ::set-output key=value)
		outputs := r.extractStageOutputs(ctx, runID)
		if len(outputs) > 0 {
			stageOutputs[st.Name] = outputs
		}

		if runRec.Status == "success" {
			_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "success")
			continue
		}

		_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "failed")
		if stopOnFail {
			msg := fmt.Sprintf("stage %d (%s) failed; stop_on_fail=true", i, st.Name)
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}
	}

	_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "success", time.Now().UTC(), nil)
}

// extractStageOutputs scans run events for ::set-output directives.
// Blueprints emit outputs by printing: ::set-output key=value
func (r *Runner) extractStageOutputs(ctx context.Context, runID string) map[string]string {
	events, err := r.store.ListRunEvents(ctx, runID, 1000)
	if err != nil {
		return nil
	}
	outputs := map[string]string{}
	for _, ev := range events {
		if !ev.Message.Valid {
			continue
		}
		msg := ev.Message.String
		// Scan each line for ::set-output directives
		for _, line := range strings.Split(msg, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "::set-output ") {
				kv := strings.TrimPrefix(line, "::set-output ")
				if idx := strings.Index(kv, "="); idx > 0 {
					key := strings.TrimSpace(kv[:idx])
					val := strings.TrimSpace(kv[idx+1:])
					outputs[key] = val
				}
			}
		}
	}
	return outputs
}

// resolveStageInputs substitutes {{ stages.<name>.<key> }} and {{ inputs.<key> }}
// references in stage inputs.
func resolveStageInputs(inputs map[string]any, stageOutputs map[string]map[string]string, pipelineInputs map[string]string) map[string]any {
	if len(inputs) == 0 {
		return inputs
	}
	resolved := make(map[string]any, len(inputs))
	for k, v := range inputs {
		if s, ok := v.(string); ok {
			resolved[k] = resolveTemplate(s, stageOutputs, pipelineInputs)
		} else {
			resolved[k] = v
		}
	}
	return resolved
}

// resolveTemplate replaces {{ stages.<name>.<key> }} and {{ inputs.<key> }} in a string.
func resolveTemplate(s string, stageOutputs map[string]map[string]string, pipelineInputs map[string]string) string {
	result := s
	for {
		start := strings.Index(result, "{{")
		if start == -1 {
			break
		}
		end := strings.Index(result[start:], "}}")
		if end == -1 {
			break
		}
		end += start + 2 // include "}}"

		ref := strings.TrimSpace(result[start+2 : end-2])

		var replacement string
		var found bool

		// {{ stages.<stageName>.<outputKey> }}
		if strings.HasPrefix(ref, "stages.") {
			parts := strings.SplitN(ref, ".", 3)
			if len(parts) == 3 {
				if outputs, ok := stageOutputs[parts[1]]; ok {
					if val, ok := outputs[parts[2]]; ok {
						replacement = val
						found = true
					}
				}
			}
		}

		// {{ inputs.<key> }}
		if !found && strings.HasPrefix(ref, "inputs.") {
			key := strings.TrimPrefix(ref, "inputs.")
			if val, ok := pipelineInputs[key]; ok {
				replacement = val
				found = true
			}
		}

		// {{ .inputs.<key> }} (alternate Go template syntax used in some blueprints)
		if !found && strings.HasPrefix(ref, ".inputs.") {
			key := strings.TrimPrefix(ref, ".inputs.")
			if val, ok := pipelineInputs[key]; ok {
				replacement = val
				found = true
			}
		}

		if found {
			result = result[:start] + replacement + result[end:]
			continue
		}
		// Unresolvable reference — skip past to avoid infinite loop
		result = result[:start] + result[start+2:]
	}
	return result
}

// evaluateCondition evaluates a simple condition string.
// Supports: "true"/"false" literals, non-empty string = true,
// "value == value" and "value != value" comparisons.
func evaluateCondition(cond string) bool {
	cond = strings.TrimSpace(cond)
	if cond == "" || cond == "false" || cond == "0" {
		return false
	}
	if cond == "true" || cond == "1" {
		return true
	}
	// Simple == comparison
	if parts := strings.SplitN(cond, "==", 2); len(parts) == 2 {
		return strings.TrimSpace(parts[0]) == strings.TrimSpace(parts[1])
	}
	// Simple != comparison
	if parts := strings.SplitN(cond, "!=", 2); len(parts) == 2 {
		return strings.TrimSpace(parts[0]) != strings.TrimSpace(parts[1])
	}
	// Non-empty string is truthy
	return true
}

func (r *Runner) waitForRunTerminal(ctx context.Context, runID string, timeout time.Duration) (persistence.RunRecord, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rec, err := r.store.GetRun(ctx, runID)
		if err != nil {
			if err == sql.ErrNoRows {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return persistence.RunRecord{}, err
		}
		if rec.Status == "success" || rec.Status == "failed" {
			return rec, nil
		}
		time.Sleep(75 * time.Millisecond)
	}
	return persistence.RunRecord{}, fmt.Errorf("timed out waiting for run %s terminal state", runID)
}
