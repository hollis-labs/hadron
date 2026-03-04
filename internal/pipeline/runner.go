package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
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

		if err := r.store.UpdatePipelineStageRunStatus(ctx, pipelineRunID, i, "running"); err != nil {
			msg := fmt.Sprintf("update stage %d running: %v", i, err)
			_ = r.store.SetPipelineRunFinished(ctx, pipelineRunID, "failed", time.Now().UTC(), &msg)
			return
		}

		if err := r.enqueuer.Enqueue(ctx, execution.Request{
			WorkspaceID:   workspaceID,
			RunID:         runID,
			BlueprintPath: blueprintPath,
			Inputs:        st.Inputs,
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
