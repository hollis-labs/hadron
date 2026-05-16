package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) CreatePipelineRun(ctx context.Context, rec PipelineRunRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pipeline_runs(id, workspace_id, pipeline_path, status, error_message, created_at, started_at, ended_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.WorkspaceID,
		rec.PipelinePath,
		rec.Status,
		rec.ErrorMessage,
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.StartedAt,
		rec.EndedAt,
	)
	if err != nil {
		return fmt.Errorf("create pipeline run: %w", err)
	}
	return nil
}

func (s *Store) SetPipelineRunStarted(ctx context.Context, id string, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipeline_runs
		SET status = ?, started_at = ?
		WHERE id = ?
	`, "running", startedAt.UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("set pipeline run started: %w", err)
	}
	return nil
}

func (s *Store) SetPipelineRunFinished(ctx context.Context, id, status string, endedAt time.Time, errMsg *string) error {
	var ns sql.NullString
	if errMsg != nil {
		ns = sql.NullString{String: *errMsg, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipeline_runs
		SET status = ?, ended_at = ?, error_message = ?
		WHERE id = ?
	`, status, endedAt.UTC().Format(time.RFC3339), ns, id)
	if err != nil {
		return fmt.Errorf("set pipeline run finished: %w", err)
	}
	return nil
}

func (s *Store) ListPipelineRuns(ctx context.Context, limit int) ([]PipelineRunRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, pipeline_path, status, error_message, created_at, started_at, ended_at
		FROM pipeline_runs
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pipeline runs: %w", err)
	}
	defer closeRows(rows)

	out := make([]PipelineRunRecord, 0, limit)
	for rows.Next() {
		var rec PipelineRunRecord
		var created string
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.PipelinePath,
			&rec.Status,
			&rec.ErrorMessage,
			&created,
			&rec.StartedAt,
			&rec.EndedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pipeline run: %w", err)
		}
		ts, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse pipeline run created_at: %w", err)
		}
		rec.CreatedAt = ts
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pipeline runs rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListPipelineRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]PipelineRunRecord, error) {
	if workspaceID == "" {
		return s.ListPipelineRuns(ctx, limit)
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, pipeline_path, status, error_message, created_at, started_at, ended_at
		FROM pipeline_runs
		WHERE workspace_id = ?
		ORDER BY created_at DESC
		LIMIT ?
	`, workspaceID, limit)
	if err != nil {
		return nil, fmt.Errorf("list pipeline runs by workspace: %w", err)
	}
	defer closeRows(rows)

	out := make([]PipelineRunRecord, 0, limit)
	for rows.Next() {
		var rec PipelineRunRecord
		var created string
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.PipelinePath,
			&rec.Status,
			&rec.ErrorMessage,
			&created,
			&rec.StartedAt,
			&rec.EndedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pipeline run by workspace: %w", err)
		}
		ts, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse pipeline run created_at: %w", err)
		}
		rec.CreatedAt = ts
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pipeline runs by workspace rows: %w", err)
	}
	return out, nil
}

func (s *Store) GetPipelineRun(ctx context.Context, id string) (PipelineRunRecord, error) {
	var rec PipelineRunRecord
	var created string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, workspace_id, pipeline_path, status, error_message, created_at, started_at, ended_at
		FROM pipeline_runs
		WHERE id = ?
	`, id).Scan(
		&rec.ID,
		&rec.WorkspaceID,
		&rec.PipelinePath,
		&rec.Status,
		&rec.ErrorMessage,
		&created,
		&rec.StartedAt,
		&rec.EndedAt,
	); err != nil {
		return PipelineRunRecord{}, fmt.Errorf("get pipeline run: %w", err)
	}
	ts, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return PipelineRunRecord{}, fmt.Errorf("parse pipeline run created_at: %w", err)
	}
	rec.CreatedAt = ts
	return rec, nil
}

func (s *Store) AddPipelineStageRun(ctx context.Context, rec PipelineStageRunRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO pipeline_stage_runs(workspace_id, pipeline_run_id, stage_index, stage_name, run_id, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.WorkspaceID,
		rec.PipelineRunID,
		rec.StageIndex,
		rec.StageName,
		rec.RunID,
		rec.Status,
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("add pipeline stage run: %w", err)
	}
	return nil
}

func (s *Store) UpdatePipelineStageRunStatus(ctx context.Context, pipelineRunID string, stageIndex int, status string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipeline_stage_runs
		SET status = ?, updated_at = ?
		WHERE pipeline_run_id = ? AND stage_index = ?
	`, status, time.Now().UTC().Format(time.RFC3339), pipelineRunID, stageIndex)
	if err != nil {
		return fmt.Errorf("update pipeline stage run status: %w", err)
	}
	return nil
}

func (s *Store) UpdatePipelineStageRunOutputs(ctx context.Context, pipelineRunID string, stageIndex int, outputsJSON string) error {
	if outputsJSON == "" {
		outputsJSON = "{}"
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipeline_stage_runs
		SET outputs_json = ?, updated_at = ?
		WHERE pipeline_run_id = ? AND stage_index = ?
	`, outputsJSON, time.Now().UTC().Format(time.RFC3339), pipelineRunID, stageIndex)
	if err != nil {
		return fmt.Errorf("update pipeline stage run outputs: %w", err)
	}
	return nil
}

func (s *Store) ListPipelineStageRuns(ctx context.Context, pipelineRunID string) ([]PipelineStageRunRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, pipeline_run_id, stage_index, stage_name, run_id, status, outputs_json, created_at, updated_at
		FROM pipeline_stage_runs
		WHERE pipeline_run_id = ?
		ORDER BY stage_index ASC
	`, pipelineRunID)
	if err != nil {
		return nil, fmt.Errorf("list pipeline stage runs: %w", err)
	}
	defer closeRows(rows)

	out := make([]PipelineStageRunRecord, 0)
	for rows.Next() {
		var rec PipelineStageRunRecord
		var created, updated string
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.PipelineRunID,
			&rec.StageIndex,
			&rec.StageName,
			&rec.RunID,
			&rec.Status,
			&rec.OutputsJSON,
			&created,
			&updated,
		); err != nil {
			return nil, fmt.Errorf("scan pipeline stage run: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse pipeline stage created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updated)
		if err != nil {
			return nil, fmt.Errorf("parse pipeline stage updated_at: %w", err)
		}
		rec.CreatedAt = createdAt
		rec.UpdatedAt = updatedAt
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pipeline stage runs rows: %w", err)
	}
	return out, nil
}
