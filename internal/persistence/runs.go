package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (s *Store) CreateRun(ctx context.Context, rec RunRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	if rec.InputJSON == "" {
		rec.InputJSON = "{}"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO runs(id, workspace_id, blueprint_path, status, input_json, error_message, created_at, started_at, ended_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.WorkspaceID,
		rec.BlueprintPath,
		rec.Status,
		rec.InputJSON,
		rec.ErrorMessage,
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.StartedAt,
		rec.EndedAt,
	)
	if err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	return nil
}

func (s *Store) GetRun(ctx context.Context, id string) (RunRecord, error) {
	var rec RunRecord
	var created string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, workspace_id, blueprint_path, status, input_json, error_message, created_at, started_at, ended_at
		FROM runs
		WHERE id = ?
	`, id).Scan(
		&rec.ID,
		&rec.WorkspaceID,
		&rec.BlueprintPath,
		&rec.Status,
		&rec.InputJSON,
		&rec.ErrorMessage,
		&created,
		&rec.StartedAt,
		&rec.EndedAt,
	); err != nil {
		return RunRecord{}, fmt.Errorf("get run: %w", err)
	}

	ts, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return RunRecord{}, fmt.Errorf("parse run created_at: %w", err)
	}
	rec.CreatedAt = ts
	return rec, nil
}

func (s *Store) ListRuns(ctx context.Context, limit int) ([]RunRecord, error) {
	return s.ListRunsByWorkspaceFiltered(ctx, "", limit, "", nil, nil)
}

func (s *Store) ListRunsByWorkspaceFiltered(ctx context.Context, workspaceID string, limit int, cursorID string, createdAfter, createdBefore *time.Time) ([]RunRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT id, workspace_id, blueprint_path, status, input_json, error_message, created_at, started_at, ended_at
		FROM runs
	`
	clauses := make([]string, 0, 5)
	args := make([]any, 0, 8)
	if workspaceID != "" {
		clauses = append(clauses, "workspace_id = ?")
		args = append(args, workspaceID)
	}
	if createdAfter != nil {
		clauses = append(clauses, "created_at > ?")
		args = append(args, createdAfter.UTC().Format(time.RFC3339))
	}
	if createdBefore != nil {
		clauses = append(clauses, "created_at < ?")
		args = append(args, createdBefore.UTC().Format(time.RFC3339))
	}
	if strings.TrimSpace(cursorID) != "" {
		cursorRun, err := s.GetRun(ctx, cursorID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: run cursor not found", ErrInvalidCursor)
			}
			return nil, fmt.Errorf("resolve run cursor: %w", err)
		}
		if workspaceID != "" && cursorRun.WorkspaceID != workspaceID {
			return nil, fmt.Errorf("%w: run cursor outside workspace", ErrInvalidCursor)
		}
		cursorCreated := cursorRun.CreatedAt.UTC().Format(time.RFC3339)
		clauses = append(clauses, "(created_at < ? OR (created_at = ? AND id < ?))")
		args = append(args, cursorCreated, cursorCreated, cursorRun.ID)
	}
	if len(clauses) > 0 {
		// #nosec G202 -- clauses are selected from fixed strings above; user input stays in args.
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer closeRows(rows)

	out := make([]RunRecord, 0, limit)
	for rows.Next() {
		var rec RunRecord
		var created string
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.BlueprintPath,
			&rec.Status,
			&rec.InputJSON,
			&rec.ErrorMessage,
			&created,
			&rec.StartedAt,
			&rec.EndedAt,
		); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		ts, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse run created_at: %w", err)
		}
		rec.CreatedAt = ts
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]RunRecord, error) {
	return s.ListRunsByWorkspaceFiltered(ctx, workspaceID, limit, "", nil, nil)
}

func (s *Store) UpdateRunStatus(ctx context.Context, id, status string, errMsg *string) error {
	var ns sql.NullString
	if errMsg != nil {
		ns = sql.NullString{String: *errMsg, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = ?, error_message = ?
		WHERE id = ?
	`, status, ns, id)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	return nil
}

func (s *Store) SetRunStarted(ctx context.Context, id string, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = ?, started_at = ?
		WHERE id = ?
	`, "running", startedAt.UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("set run started: %w", err)
	}
	return nil
}

func (s *Store) SetRunFinished(ctx context.Context, id, status string, endedAt time.Time, errMsg *string) error {
	var ns sql.NullString
	if errMsg != nil {
		ns = sql.NullString{String: *errMsg, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs
		SET status = ?, ended_at = ?, error_message = ?
		WHERE id = ?
	`, status, endedAt.UTC().Format(time.RFC3339), ns, id)
	if err != nil {
		return fmt.Errorf("set run finished: %w", err)
	}
	return nil
}
