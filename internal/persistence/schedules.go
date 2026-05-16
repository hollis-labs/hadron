package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) CreateSchedule(ctx context.Context, rec ScheduleRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO schedules(id, workspace_id, name, blueprint_path, cron_expr, enabled, created_at, updated_at, last_run_at, next_run_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.WorkspaceID,
		rec.Name,
		rec.BlueprintPath,
		rec.CronExpr,
		boolToInt(rec.Enabled),
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.UpdatedAt.UTC().Format(time.RFC3339),
		rec.LastRunAt,
		rec.NextRunAt,
	)
	if err != nil {
		return fmt.Errorf("create schedule: %w", err)
	}
	return nil
}

func (s *Store) ListSchedules(ctx context.Context) ([]ScheduleRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, name, blueprint_path, cron_expr, enabled, created_at, updated_at, last_run_at, next_run_at
		FROM schedules
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer closeRows(rows)

	out := make([]ScheduleRecord, 0)
	for rows.Next() {
		var rec ScheduleRecord
		var created, updated string
		var enabled int
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.Name,
			&rec.BlueprintPath,
			&rec.CronExpr,
			&enabled,
			&created,
			&updated,
			&rec.LastRunAt,
			&rec.NextRunAt,
		); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse schedule created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updated)
		if err != nil {
			return nil, fmt.Errorf("parse schedule updated_at: %w", err)
		}
		rec.Enabled = enabled == 1
		rec.CreatedAt = createdAt
		rec.UpdatedAt = updatedAt
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list schedules rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListSchedulesByWorkspace(ctx context.Context, workspaceID string) ([]ScheduleRecord, error) {
	if workspaceID == "" {
		return s.ListSchedules(ctx)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, name, blueprint_path, cron_expr, enabled, created_at, updated_at, last_run_at, next_run_at
		FROM schedules
		WHERE workspace_id = ?
		ORDER BY created_at DESC
	`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list schedules by workspace: %w", err)
	}
	defer closeRows(rows)

	out := make([]ScheduleRecord, 0)
	for rows.Next() {
		var rec ScheduleRecord
		var created, updated string
		var enabled int
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.Name,
			&rec.BlueprintPath,
			&rec.CronExpr,
			&enabled,
			&created,
			&updated,
			&rec.LastRunAt,
			&rec.NextRunAt,
		); err != nil {
			return nil, fmt.Errorf("scan schedule by workspace: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse schedule created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updated)
		if err != nil {
			return nil, fmt.Errorf("parse schedule updated_at: %w", err)
		}
		rec.Enabled = enabled == 1
		rec.CreatedAt = createdAt
		rec.UpdatedAt = updatedAt
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list schedules by workspace rows: %w", err)
	}
	return out, nil
}

func (s *Store) UpdateScheduleEnabled(ctx context.Context, id string, enabled bool) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET enabled = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(enabled), time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("update schedule enabled: %w", err)
	}
	return nil
}

func (s *Store) GetSchedule(ctx context.Context, id string) (ScheduleRecord, error) {
	var rec ScheduleRecord
	var created, updated string
	var enabled int
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, workspace_id, name, blueprint_path, cron_expr, enabled, created_at, updated_at, last_run_at, next_run_at
		FROM schedules
		WHERE id = ?
	`, id).Scan(
		&rec.ID,
		&rec.WorkspaceID,
		&rec.Name,
		&rec.BlueprintPath,
		&rec.CronExpr,
		&enabled,
		&created,
		&updated,
		&rec.LastRunAt,
		&rec.NextRunAt,
	); err != nil {
		return ScheduleRecord{}, fmt.Errorf("get schedule: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return ScheduleRecord{}, fmt.Errorf("parse schedule created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updated)
	if err != nil {
		return ScheduleRecord{}, fmt.Errorf("parse schedule updated_at: %w", err)
	}
	rec.Enabled = enabled == 1
	rec.CreatedAt = createdAt
	rec.UpdatedAt = updatedAt
	return rec, nil
}

// UpdateScheduleEnabledAndNext updates a schedule's enabled flag and,
// optionally, its next-run time. When nextRun is nil the next_run_at column
// is left untouched — a plain enable/disable toggle must not wipe a cron
// schedule's next-run time, or a re-enabled schedule would never be picked
// up by ListDueSchedules.
func (s *Store) UpdateScheduleEnabledAndNext(ctx context.Context, id string, enabled bool, nextRun *time.Time) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if nextRun == nil {
		_, err := s.db.ExecContext(ctx, `
			UPDATE schedules
			SET enabled = ?, updated_at = ?
			WHERE id = ?
		`, boolToInt(enabled), now, id)
		if err != nil {
			return fmt.Errorf("update schedule enabled: %w", err)
		}
		return nil
	}

	next := sql.NullString{String: nextRun.UTC().Format(time.RFC3339), Valid: true}
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET enabled = ?, next_run_at = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(enabled), next, now, id)
	if err != nil {
		return fmt.Errorf("update schedule enabled+next: %w", err)
	}
	return nil
}

// UpdateScheduleFields updates mutable schedule fields (name, cron, blueprint path, enabled, next run).
func (s *Store) UpdateScheduleFields(ctx context.Context, id string, name, cronExpr, blueprintPath string, enabled bool, nextRun *time.Time) error {
	var next sql.NullString
	if nextRun != nil {
		next = sql.NullString{String: nextRun.UTC().Format(time.RFC3339), Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET name = ?, cron_expr = ?, blueprint_path = ?, enabled = ?, next_run_at = ?, updated_at = ?
		WHERE id = ?
	`, name, cronExpr, blueprintPath, boolToInt(enabled), next, time.Now().UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("update schedule fields: %w", err)
	}
	return nil
}

// DisableSchedule sets enabled=false for a schedule (used after one-time schedules fire).
func (s *Store) DisableSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules SET enabled = 0, updated_at = ? WHERE id = ?
	`, time.Now().UTC().Format(time.RFC3339), id)
	return err
}

func (s *Store) SetScheduleNextRun(ctx context.Context, id string, nextRun time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET next_run_at = ?, updated_at = ?
		WHERE id = ?
		  AND enabled = 1
	`,
		nextRun.UTC().Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return fmt.Errorf("set schedule next_run_at: %w", err)
	}
	return nil
}

func (s *Store) ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]ScheduleRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, workspace_id, name, blueprint_path, cron_expr, enabled, created_at, updated_at, last_run_at, next_run_at
		FROM schedules
		WHERE enabled = 1
		  AND next_run_at IS NOT NULL
		  AND next_run_at <= ?
		ORDER BY next_run_at ASC
		LIMIT ?
	`, now.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("list due schedules: %w", err)
	}
	defer closeRows(rows)

	out := make([]ScheduleRecord, 0, limit)
	for rows.Next() {
		var rec ScheduleRecord
		var created, updated string
		var enabled int
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkspaceID,
			&rec.Name,
			&rec.BlueprintPath,
			&rec.CronExpr,
			&enabled,
			&created,
			&updated,
			&rec.LastRunAt,
			&rec.NextRunAt,
		); err != nil {
			return nil, fmt.Errorf("scan due schedule: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse due schedule created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updated)
		if err != nil {
			return nil, fmt.Errorf("parse due schedule updated_at: %w", err)
		}
		rec.Enabled = enabled == 1
		rec.CreatedAt = createdAt
		rec.UpdatedAt = updatedAt
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due schedules rows: %w", err)
	}
	return out, nil
}

func (s *Store) ClaimAndUpdateScheduleRun(ctx context.Context, id string, expectedNext time.Time, lastRun time.Time, nextRun time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET last_run_at = ?, next_run_at = ?, updated_at = ?
		WHERE id = ?
		  AND next_run_at = ?
		  AND enabled = 1
	`,
		lastRun.UTC().Format(time.RFC3339),
		nextRun.UTC().Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
		id,
		expectedNext.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return false, fmt.Errorf("claim and update schedule run: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim schedule rows affected: %w", err)
	}
	return n > 0, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM schedules WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	return nil
}
