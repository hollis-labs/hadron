package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"text/template"
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
	UpdatePipelineStageRunOutputs(ctx context.Context, pipelineRunID string, stageIndex int, outputsJSON string) error
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

// StageResult holds captured outputs and status for a completed stage.
type StageResult struct {
	Status   string
	Outputs  map[string]any
	ExitCode int
	Stdout   string
	Stderr   string
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

	// TopoSort stages into execution levels.
	levels, err := TopoSort(spec.Stages)
	if err != nil {
		msg := fmt.Sprintf("toposort pipeline: %v", err)
		_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
		return
	}

	// Build stage name → index map for DB operations.
	stageIndex := make(map[string]int, len(spec.Stages))
	for i, st := range spec.Stages {
		stageIndex[st.Name] = i
	}

	// Shared state: stage results keyed by stage name.
	var mu sync.Mutex
	stageResults := make(map[string]*StageResult)

	// Pipeline-level inputs as strings for template resolution.
	pipelineInputs := map[string]string{}
	for k, v := range spec.Inputs {
		pipelineInputs[k] = fmt.Sprintf("%v", v)
	}

	// Pre-register all stage runs with "pending" status.
	for i, st := range spec.Stages {
		now := time.Now().UTC()
		runID := fmt.Sprintf("plr-%s-%02d-%d", pipelineRunID, i, now.UnixNano())
		if err := r.store.AddPipelineStageRun(ctx, persistence.PipelineStageRunRecord{
			WorkspaceID:   workspaceID,
			PipelineRunID: pipelineRunID,
			StageIndex:    i,
			StageName:     st.Name,
			RunID:         runID,
			Status:        "pending",
			CreatedAt:     now,
			UpdatedAt:     now,
		}); err != nil {
			msg := fmt.Sprintf("add stage run %d: %v", i, err)
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}
	}

	// Execute level by level.
	for _, level := range levels {
		// Check if any stage in this level can run.
		// Build a snapshot of stageOutputs (string map) for template resolution.
		mu.Lock()
		stageOutputsSnapshot := buildStageOutputsSnapshot(stageResults)
		mu.Unlock()

		type stageExecResult struct {
			stage  Stage
			result *StageResult
			err    error
		}

		results := make([]stageExecResult, len(level))
		var wg sync.WaitGroup

		for idx, st := range level {
			idx, st := idx, st // capture
			wg.Add(1)
			go func() {
				defer wg.Done()
				si := stageIndex[st.Name]
				res, execErr := r.executeStage(ctx, executeStageParams{
					pipelineRunID:        pipelineRunID,
					workspaceID:          workspaceID,
					baseDir:              baseDir,
					stage:                st,
					stageIdx:             si,
					stageResults:         stageResults,
					mu:                   &mu,
					pipelineInputs:       pipelineInputs,
					stageOutputsSnapshot: stageOutputsSnapshot,
				})
				results[idx] = stageExecResult{stage: st, result: res, err: execErr}
			}()
		}
		wg.Wait()

		// Collect results into shared state and check for failures.
		levelFailed := false
		for _, r := range results {
			mu.Lock()
			stageResults[r.stage.Name] = r.result
			mu.Unlock()
			if r.result.Status == "failed" {
				levelFailed = true
			}
		}

		if levelFailed && stopOnFail {
			// Mark all remaining stages as skipped.
			r.skipRemainingStages(ctx, pipelineRunID, stageIndex, stageResults, levels, level)
			msg := fmt.Sprintf("level containing %s failed; stop_on_fail=true", levelStageNames(level))
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}
	}

	_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "success", time.Now().UTC(), nil)
}

type executeStageParams struct {
	pipelineRunID        string
	workspaceID          string
	baseDir              string
	stage                Stage
	stageIdx             int
	stageResults         map[string]*StageResult
	mu                   *sync.Mutex
	pipelineInputs       map[string]string
	stageOutputsSnapshot map[string]map[string]string
}

func (r *Runner) executeStage(ctx context.Context, p executeStageParams) (*StageResult, error) {
	st := p.stage

	// Skip propagation: if ALL dependencies were skipped, auto-skip this stage.
	if len(st.DependsOn) > 0 && allDepsSkipped(st.DependsOn, p.stageResults, p.mu) {
		_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "skipped")
		return &StageResult{Status: "skipped"}, nil
	}

	// Evaluate conditional: skip stage if `if:` resolves to false.
	if st.If != "" {
		condition := evaluateIfTemplate(st.If, p.stageOutputsSnapshot, p.pipelineInputs)
		if !evaluateCondition(condition) {
			_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "skipped")
			return &StageResult{Status: "skipped"}, nil
		}
	}

	_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "running")

	blueprintPath := st.BlueprintPath
	if !filepath.IsAbs(blueprintPath) {
		blueprintPath = filepath.Join(p.baseDir, blueprintPath)
	}

	runID := fmt.Sprintf("plr-%s-%02d-%d", p.pipelineRunID, p.stageIdx, time.Now().UTC().UnixNano())

	// Resolve stage inputs.
	resolvedInputs := resolveStageInputs(st.Inputs, p.stageOutputsSnapshot, p.pipelineInputs)

	if err := r.enqueuer.Enqueue(ctx, execution.Request{
		WorkspaceID:   p.workspaceID,
		RunID:         runID,
		BlueprintPath: blueprintPath,
		Inputs:        resolvedInputs,
	}); err != nil {
		_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "failed")
		return &StageResult{Status: "failed"}, fmt.Errorf("enqueue stage %d run: %v", p.stageIdx, err)
	}

	runRec, waitErr := r.waitForRunTerminal(ctx, runID, 60*time.Second)
	if waitErr != nil {
		_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "failed")
		return &StageResult{Status: "failed"}, fmt.Errorf("stage %d wait failed: %v", p.stageIdx, waitErr)
	}

	// Capture outputs from run events.
	capturedOutputs := r.captureStageOutputs(ctx, runID, st)

	// Determine exit code from run status.
	exitCode := 0
	if runRec.Status != "success" {
		exitCode = 1
	}

	// Build the full outputs map including built-in captures.
	outputsMap := map[string]any{}
	for k, v := range capturedOutputs {
		outputsMap[k] = v
	}
	outputsMap["exit_code"] = exitCode
	outputsMap["status"] = runRec.Status

	// Persist outputs.
	outputsJSON, _ := json.Marshal(outputsMap)
	_ = r.store.UpdatePipelineStageRunOutputs(ctx, p.pipelineRunID, p.stageIdx, string(outputsJSON))

	result := &StageResult{
		ExitCode: exitCode,
		Outputs:  outputsMap,
	}

	if runRec.Status == "success" {
		_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "success")
		result.Status = "success"
		return result, nil
	}

	_ = r.store.UpdatePipelineStageRunStatus(ctx, p.pipelineRunID, p.stageIdx, "failed")
	result.Status = "failed"
	return result, nil
}

// captureStageOutputs extracts outputs from run events.
// It supports ::set-output directives and the stage's Outputs map (stdout, stderr, exit_code expressions).
func (r *Runner) captureStageOutputs(ctx context.Context, runID string, st Stage) map[string]string {
	events, err := r.store.ListRunEvents(ctx, runID, 1000)
	if err != nil {
		return nil
	}

	// Collect all log lines for stdout/stderr capture.
	var allLines []string
	outputs := map[string]string{}

	for _, ev := range events {
		if !ev.Message.Valid {
			continue
		}
		msg := ev.Message.String

		// Collect log lines for stdout capture.
		if ev.EventType == "log" {
			allLines = append(allLines, msg)
		}

		// Scan for ::set-output directives.
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

	// Build captured stdout (last 64KB).
	stdout := truncateToLastN(strings.Join(allLines, "\n"), 65536)

	// Evaluate stage-level Outputs map, but only if not already set by ::set-output.
	for key, expr := range st.Outputs {
		if _, alreadySet := outputs[key]; alreadySet {
			continue // ::set-output takes priority
		}
		expr = strings.TrimSpace(expr)
		switch expr {
		case "stdout":
			outputs[key] = stdout
		case "stderr":
			outputs[key] = "" // stderr not separately captured via PTY
		case "exit_code":
			// Will be set by the caller.
		default:
			// Treat as a literal or passthrough.
			outputs[key] = expr
		}
	}

	// Always include stdout as a built-in.
	if _, ok := outputs["stdout"]; !ok {
		outputs["stdout"] = stdout
	}

	return outputs
}

// skipRemainingStages marks all stages not yet in stageResults as "skipped".
func (r *Runner) skipRemainingStages(ctx context.Context, pipelineRunID string, stageIndex map[string]int, stageResults map[string]*StageResult, allLevels [][]Stage, currentLevel []Stage) {
	// Mark stages in subsequent levels as skipped.
	foundCurrent := false
	for _, level := range allLevels {
		if !foundCurrent {
			if len(level) > 0 && level[0].Name == currentLevel[0].Name {
				foundCurrent = true
			}
			continue
		}
		for _, st := range level {
			if _, done := stageResults[st.Name]; !done {
				si := stageIndex[st.Name]
				_ = r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, si, "skipped")
			}
		}
	}
}

// buildStageOutputsSnapshot converts StageResult map to the string-based map
// used by resolveTemplate.
func buildStageOutputsSnapshot(results map[string]*StageResult) map[string]map[string]string {
	snapshot := make(map[string]map[string]string, len(results))
	for name, res := range results {
		m := make(map[string]string)
		m["status"] = res.Status
		m["exit_code"] = fmt.Sprintf("%d", res.ExitCode)
		for k, v := range res.Outputs {
			m[k] = fmt.Sprintf("%v", v)
		}
		snapshot[name] = m
	}
	return snapshot
}

func levelStageNames(level []Stage) string {
	names := make([]string, len(level))
	for i, st := range level {
		names[i] = st.Name
	}
	return strings.Join(names, ", ")
}

// truncateToLastN returns the last n bytes of s.
func truncateToLastN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// extractStageOutputs scans run events for ::set-output directives.
// Blueprints emit outputs by printing: ::set-output key=value
// Kept for backward compatibility.
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

		// {{ .stages.<stageName>.outputs.<outputKey> }}
		if !found && strings.HasPrefix(ref, ".stages.") {
			trimmed := strings.TrimPrefix(ref, ".stages.")
			parts := strings.SplitN(trimmed, ".", 3)
			if len(parts) >= 3 && parts[1] == "outputs" {
				if outputs, ok := stageOutputs[parts[0]]; ok {
					if val, ok := outputs[parts[2]]; ok {
						replacement = val
						found = true
					}
				}
			}
			// {{ .stages.<stageName>.status }}
			if !found && len(parts) >= 2 && parts[1] == "status" {
				if outputs, ok := stageOutputs[parts[0]]; ok {
					if val, ok := outputs["status"]; ok {
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

// allDepsSkipped returns true if every dependency in deps has status "skipped".
// If deps is empty, returns false (no deps = always eligible to run).
func allDepsSkipped(deps []string, stageResults map[string]*StageResult, mu *sync.Mutex) bool {
	if len(deps) == 0 {
		return false
	}
	mu.Lock()
	defer mu.Unlock()
	for _, dep := range deps {
		res, ok := stageResults[dep]
		if !ok {
			// Dep hasn't run yet — not skipped.
			return false
		}
		if res.Status != "skipped" {
			return false
		}
	}
	return true
}

// evaluateIfTemplate renders an If expression using Go text/template.
// The template context provides:
//
//	.stages.<name>.status   — "success", "failed", "skipped", or "pending"
//	.stages.<name>.outputs.<key> — captured output values
//
// Built-in functions: eq, ne, and, or, not, etc. (standard text/template funcs).
// If template execution fails, it falls back to the simple resolveTemplate path.
func evaluateIfTemplate(ifExpr string, stageOutputs map[string]map[string]string, pipelineInputs map[string]string) string {
	// Build the template data structure.
	stagesData := make(map[string]map[string]any, len(stageOutputs))
	for name, outputs := range stageOutputs {
		entry := map[string]any{
			"status": outputs["status"],
		}
		outputsMap := make(map[string]string, len(outputs))
		for k, v := range outputs {
			outputsMap[k] = v
		}
		entry["outputs"] = outputsMap
		stagesData[name] = entry
	}

	data := map[string]any{
		"stages": stagesData,
		"inputs": pipelineInputs,
	}

	tmpl, err := template.New("if").Parse(ifExpr)
	if err != nil {
		// Fall back to simple resolution.
		return resolveTemplate(ifExpr, stageOutputs, pipelineInputs)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		// Fall back to simple resolution.
		return resolveTemplate(ifExpr, stageOutputs, pipelineInputs)
	}

	return strings.TrimSpace(buf.String())
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
