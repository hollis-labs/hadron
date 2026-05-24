package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func TestRunner_ExecutesStagesInOrder(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "simple-success.yaml")
	if err := r.Start(context.Background(), "pl-test-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-test-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-test-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 2 {
		t.Fatalf("expected 2 stage runs, got %d", len(stages))
	}
	if stages[0].StageIndex != 0 || stages[1].StageIndex != 1 {
		t.Fatalf("unexpected stage order: %+v", stages)
	}
}

func TestRunner_StopOnFailDefault(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "stop-on-fail.yaml")
	if err := r.Start(context.Background(), "pl-test-002", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-test-002", "failed")
	if !rec.ErrorMessage.Valid || !strings.Contains(rec.ErrorMessage.String, "stop_on_fail") {
		t.Fatalf("expected stop_on_fail error, got %+v", rec.ErrorMessage)
	}
}

func TestRunner_FanOutParallelExecution(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	// Use 4 workers so parallel stages can truly overlap.
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "fan-out.yaml")
	if err := r.Start(context.Background(), "pl-fan-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-fan-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-fan-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 3 {
		t.Fatalf("expected 3 stage runs, got %d", len(stages))
	}

	// All stages should be success.
	for _, st := range stages {
		if st.Status != "success" {
			t.Fatalf("stage %s expected success, got %s", st.StageName, st.Status)
		}
	}
}

func TestRunner_FanOutStopOnFail(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "fan-out-fail.yaml")
	if err := r.Start(context.Background(), "pl-fan-002", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-fan-002", "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-fan-002")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	// Collector stage (index 3) should be skipped.
	stageMap := map[string]string{}
	for _, st := range stages {
		stageMap[st.StageName] = st.Status
	}
	if stageMap["collector"] != "skipped" {
		t.Fatalf("expected collector to be skipped, got %s", stageMap["collector"])
	}
}

func TestRunner_OutputCapture(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 2, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "output-capture.yaml")
	if err := r.Start(context.Background(), "pl-out-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-out-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-out-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	// Check the producer stage has outputs_json.
	var producerStage persistence.PipelineStageRunRecord
	for _, st := range stages {
		if st.StageName == "producer" {
			producerStage = st
			break
		}
	}

	if producerStage.OutputsJSON == "" || producerStage.OutputsJSON == "{}" {
		t.Fatalf("expected non-empty outputs_json for producer stage, got %q", producerStage.OutputsJSON)
	}

	var outputs map[string]any
	if err := json.Unmarshal([]byte(producerStage.OutputsJSON), &outputs); err != nil {
		t.Fatalf("unmarshal outputs_json: %v", err)
	}

	// Should have greeting from ::set-output.
	if greeting, ok := outputs["greeting"]; !ok {
		t.Fatalf("expected 'greeting' in outputs, got %v", outputs)
	} else if greeting != "hello-world" {
		t.Fatalf("expected greeting=hello-world, got %v", greeting)
	}

	// Should have exit_code.
	if ec, ok := outputs["exit_code"]; !ok {
		t.Fatalf("expected 'exit_code' in outputs")
	} else {
		// JSON numbers are float64.
		if ecFloat, ok := ec.(float64); !ok || ecFloat != 0 {
			t.Fatalf("expected exit_code=0, got %v", ec)
		}
	}
}

func TestRunner_StageWaitTimeoutOverride(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	dir := t.TempDir()
	blueprintPath := filepath.Join(dir, "slow-success.yaml")
	writeFile(t, blueprintPath, `blueprint:
  name: slow-success
steps:
  - section: main
    tasks:
      - name: slow
        cmd: sleep 2
`)
	pipelinePath := filepath.Join(dir, "pipeline.yaml")
	writeFile(t, pipelinePath, `meta:
  name: wait-override
defaults:
  stage_wait_timeout_seconds: 1
stages:
  - name: slow
    blueprint_path: slow-success.yaml
    wait_timeout_seconds: 3
`)

	if err := r.Start(context.Background(), "pl-wait-override", pipelinePath, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-wait-override", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
}

func TestRunner_StageWaitTimeoutFailure(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	dir := t.TempDir()
	blueprintPath := filepath.Join(dir, "slow.yaml")
	writeFile(t, blueprintPath, `blueprint:
  name: slow
steps:
  - section: main
    tasks:
      - name: slow
        cmd: sleep 2
`)
	pipelinePath := filepath.Join(dir, "pipeline.yaml")
	writeFile(t, pipelinePath, `meta:
  name: wait-timeout
defaults:
  stage_wait_timeout_seconds: 1
stages:
  - name: slow
    blueprint_path: slow.yaml
`)

	if err := r.Start(context.Background(), "pl-wait-timeout", pipelinePath, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-wait-timeout", "failed")
	if !rec.ErrorMessage.Valid || !strings.Contains(rec.ErrorMessage.String, "timed out waiting for run") {
		t.Fatalf("expected timeout error message, got %+v", rec.ErrorMessage)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-wait-timeout")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}
	if stages[0].Status != "failed" {
		t.Fatalf("expected stage failed, got %s", stages[0].Status)
	}

	var outputs map[string]any
	if err := json.Unmarshal([]byte(stages[0].OutputsJSON), &outputs); err != nil {
		t.Fatalf("unmarshal outputs_json: %v", err)
	}
	if outputs["status"] != "failed" {
		t.Fatalf("expected status=failed output, got %v", outputs["status"])
	}
	if outputs["error"] == "" {
		t.Fatalf("expected error output, got %v", outputs)
	}
	if got := outputs["wait_timeout_seconds"]; got != float64(1) {
		t.Fatalf("expected wait_timeout_seconds=1, got %v", got)
	}
}

func TestRunner_AsyncStageSucceedsAfterEnqueue(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	enq := &recordingEnqueuer{store: store}
	r := NewRunner(store, enq)

	dir := t.TempDir()
	pipelinePath := filepath.Join(dir, "pipeline.yaml")
	writeFile(t, filepath.Join(dir, "launch.yaml"), `blueprint:
  name: launch
steps:
  - section: main
    tasks:
      - name: launch
        cmd: sleep 30
`)
	writeFile(t, pipelinePath, `meta:
  name: async-launch
stages:
  - name: launch
    blueprint_path: launch.yaml
    async: true
    wait_timeout_seconds: 1
    outputs:
      launched_run_id: run_id
`)

	if err := r.Start(context.Background(), "pl-async-launch", pipelinePath, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-async-launch", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	if len(enq.requests) != 1 {
		t.Fatalf("expected 1 enqueue, got %d", len(enq.requests))
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-async-launch")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}
	if stages[0].Status != "success" {
		t.Fatalf("expected async stage status success, got %s", stages[0].Status)
	}

	var outputs map[string]any
	if err := json.Unmarshal([]byte(stages[0].OutputsJSON), &outputs); err != nil {
		t.Fatalf("unmarshal outputs_json: %v", err)
	}
	if outputs["run_id"] != enq.requests[0].RunID {
		t.Fatalf("expected run_id output %q, got %v", enq.requests[0].RunID, outputs["run_id"])
	}
	if outputs["launched_run_id"] != enq.requests[0].RunID {
		t.Fatalf("expected launched_run_id alias %q, got %v", enq.requests[0].RunID, outputs["launched_run_id"])
	}
	if outputs["status"] != "launched" {
		t.Fatalf("expected status=launched, got %v", outputs["status"])
	}
}

func TestRunner_AsyncStageOutputFeedsDownstream(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	enq := &recordingEnqueuer{store: store}
	r := NewRunner(store, enq)

	dir := t.TempDir()
	pipelinePath := filepath.Join(dir, "pipeline.yaml")
	writeFile(t, filepath.Join(dir, "launch.yaml"), `blueprint:
  name: launch
steps:
  - section: main
    tasks:
      - name: launch
        cmd: sleep 30
`)
	writeFile(t, filepath.Join(dir, "poll.yaml"), `blueprint:
  name: poll
steps:
  - section: main
    tasks:
      - name: poll
        cmd: echo polling
`)
	writeFile(t, pipelinePath, `meta:
  name: async-downstream
stages:
  - name: launch
    blueprint_path: launch.yaml
    async: true
  - name: poll
    blueprint_path: poll.yaml
    async: true
    depends_on: [launch]
    inputs:
      launched_run_id: "{{ .stages.launch.outputs.run_id }}"
`)

	if err := r.Start(context.Background(), "pl-async-downstream", pipelinePath, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-async-downstream", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	if len(enq.requests) != 2 {
		t.Fatalf("expected 2 enqueues, got %d", len(enq.requests))
	}
	if got := enq.requests[1].Inputs["launched_run_id"]; got != enq.requests[0].RunID {
		t.Fatalf("expected downstream launched_run_id %q, got %v", enq.requests[0].RunID, got)
	}
}

func TestRunner_V1LinearIdentical(t *testing.T) {
	// Verify v1 linear pipeline (no depends_on) produces identical results to the old runner.
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "simple-success.yaml")
	if err := r.Start(context.Background(), "pl-v1-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-v1-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-v1-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(stages))
	}

	// Both should succeed, in index order.
	for i, st := range stages {
		if st.StageIndex != i {
			t.Fatalf("stage %d has index %d", i, st.StageIndex)
		}
		if st.Status != "success" {
			t.Fatalf("stage %d expected success, got %s", i, st.Status)
		}
	}
}

func TestRunner_DownstreamAccessesUpstreamOutputs(t *testing.T) {
	// This tests that template resolution works for inter-stage data.
	stageResults := map[string]*StageResult{
		"build": {
			Status:   "success",
			ExitCode: 0,
			Outputs: map[string]any{
				"version":   "1.2.3",
				"exit_code": 0,
				"status":    "success",
			},
		},
	}

	snapshot := buildStageOutputsSnapshot(stageResults)

	// Test {{ stages.build.version }}
	resolved := resolveTemplate("{{ stages.build.version }}", snapshot, nil)
	if resolved != "1.2.3" {
		t.Fatalf("expected '1.2.3', got %q", resolved)
	}

	// Test {{ stages.build.status }}
	resolved = resolveTemplate("{{ stages.build.status }}", snapshot, nil)
	if resolved != "success" {
		t.Fatalf("expected 'success', got %q", resolved)
	}

	// Test {{ .stages.build.outputs.version }}
	resolved = resolveTemplate("{{ .stages.build.outputs.version }}", snapshot, nil)
	if resolved != "1.2.3" {
		t.Fatalf("expected '1.2.3' from .stages syntax, got %q", resolved)
	}

	// Test {{ .stages.build.status }}
	resolved = resolveTemplate("{{ .stages.build.status }}", snapshot, nil)
	if resolved != "success" {
		t.Fatalf("expected 'success' from .stages syntax, got %q", resolved)
	}
}

// TestRunner_ConditionalSkipBranch: A→B(if: A failed)→C(depends A).
// A succeeds → B skipped (condition false), C runs (depends on A, not B).
func TestRunner_ConditionalSkipBranch(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "conditional-skip-branch.yaml")
	if err := r.Start(context.Background(), "pl-cond-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-cond-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-cond-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	stageMap := map[string]string{}
	for _, st := range stages {
		stageMap[st.StageName] = st.Status
	}

	if stageMap["A"] != "success" {
		t.Fatalf("expected A=success, got %s", stageMap["A"])
	}
	if stageMap["B"] != "skipped" {
		t.Fatalf("expected B=skipped, got %s", stageMap["B"])
	}
	if stageMap["C"] != "success" {
		t.Fatalf("expected C=success, got %s", stageMap["C"])
	}
}

// TestRunner_FanOutConditional: A→{B(always), C(if: A failed)}.
// A succeeds → B runs, C skipped.
func TestRunner_FanOutConditional(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "fan-out-conditional.yaml")
	if err := r.Start(context.Background(), "pl-cond-002", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-cond-002", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-cond-002")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	stageMap := map[string]string{}
	for _, st := range stages {
		stageMap[st.StageName] = st.Status
	}

	if stageMap["A"] != "success" {
		t.Fatalf("expected A=success, got %s", stageMap["A"])
	}
	if stageMap["B"] != "success" {
		t.Fatalf("expected B=success, got %s", stageMap["B"])
	}
	if stageMap["C"] != "skipped" {
		t.Fatalf("expected C=skipped, got %s", stageMap["C"])
	}
}

// TestRunner_FanInMixedSkip: root→{A(always), B(if: root failed)}→C.
// root succeeds → A succeeds, B skipped. C has deps {A, B} — A succeeded so C runs.
func TestRunner_FanInMixedSkip(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "fan-in-mixed-skip.yaml")
	if err := r.Start(context.Background(), "pl-cond-003", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-cond-003", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-cond-003")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	stageMap := map[string]string{}
	for _, st := range stages {
		stageMap[st.StageName] = st.Status
	}

	if stageMap["root"] != "success" {
		t.Fatalf("expected root=success, got %s", stageMap["root"])
	}
	if stageMap["A"] != "success" {
		t.Fatalf("expected A=success, got %s", stageMap["A"])
	}
	if stageMap["B"] != "skipped" {
		t.Fatalf("expected B=skipped, got %s", stageMap["B"])
	}
	if stageMap["C"] != "success" {
		t.Fatalf("expected C=success (mixed deps, at least one non-skipped), got %s", stageMap["C"])
	}
}

// TestRunner_AllDepsSkipped: root→{A(if: root failed), B(if: root failed)}→C.
// root succeeds → A skipped, B skipped → C has all deps skipped → C skipped.
func TestRunner_AllDepsSkipped(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 4, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "all-deps-skipped.yaml")
	if err := r.Start(context.Background(), "pl-cond-004", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-cond-004", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-cond-004")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}

	stageMap := map[string]string{}
	for _, st := range stages {
		stageMap[st.StageName] = st.Status
	}

	if stageMap["root"] != "success" {
		t.Fatalf("expected root=success, got %s", stageMap["root"])
	}
	if stageMap["A"] != "skipped" {
		t.Fatalf("expected A=skipped, got %s", stageMap["A"])
	}
	if stageMap["B"] != "skipped" {
		t.Fatalf("expected B=skipped, got %s", stageMap["B"])
	}
	if stageMap["C"] != "skipped" {
		t.Fatalf("expected C=skipped (all deps skipped), got %s", stageMap["C"])
	}
}

// TestEvaluateIfTemplate verifies Go text/template rendering for If expressions.
func TestEvaluateIfTemplate(t *testing.T) {
	stageOutputs := map[string]map[string]string{
		"build": {
			"status":    "success",
			"exit_code": "0",
			"version":   "1.2.3",
		},
		"test": {
			"status":    "failed",
			"exit_code": "1",
		},
	}

	tests := []struct {
		name string
		expr string
		want string
	}{
		{"eq true", `{{ eq .stages.build.status "success" }}`, "true"},
		{"eq false", `{{ eq .stages.build.status "failed" }}`, "false"},
		{"ne true", `{{ ne .stages.test.status "success" }}`, "true"},
		{"simple ref", `{{ .stages.build.outputs.version }}`, "1.2.3"},
		{"status ref", `{{ .stages.test.status }}`, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evaluateIfTemplate(tt.expr, stageOutputs, nil)
			if got != tt.want {
				t.Fatalf("evaluateIfTemplate(%q) = %q, want %q", tt.expr, got, tt.want)
			}
		})
	}
}

func openStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "pipeline.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

type recordingEnqueuer struct {
	store    *persistence.Store
	requests []execution.Request
}

func (e *recordingEnqueuer) Enqueue(ctx context.Context, req execution.Request) error {
	e.requests = append(e.requests, req)
	inputJSON := "{}"
	if len(req.Inputs) > 0 {
		b, err := json.Marshal(req.Inputs)
		if err != nil {
			return err
		}
		inputJSON = string(b)
	}
	workspaceID := req.WorkspaceID
	if workspaceID == "" {
		workspaceID = "default"
	}
	return e.store.CreateRun(ctx, persistence.RunRecord{
		ID:            req.RunID,
		WorkspaceID:   workspaceID,
		BlueprintPath: req.BlueprintPath,
		Status:        "queued",
		InputJSON:     inputJSON,
		CreatedAt:     time.Now().UTC(),
	})
}

func waitPipelineStatus(t *testing.T, store *persistence.Store, id, want string) persistence.PipelineRunRecord {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := store.GetPipelineRun(context.Background(), id)
		if err == nil && rec.Status == want {
			return rec
		}
		time.Sleep(60 * time.Millisecond)
	}
	rec, err := store.GetPipelineRun(context.Background(), id)
	if err != nil {
		t.Fatalf("wait pipeline status get error: %v", err)
	}
	t.Fatalf("timed out waiting for pipeline status %s, got %s", want, rec.Status)
	return persistence.PipelineRunRecord{}
}
