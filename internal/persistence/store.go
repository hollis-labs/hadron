package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Register the modernc SQLite driver for database/sql.
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

var ErrInvalidCursor = errors.New("invalid cursor")

type RunRecord struct {
	ID            string
	WorkspaceID   string
	BlueprintPath string
	Status        string
	InputJSON     string
	ErrorMessage  sql.NullString
	CreatedAt     time.Time
	StartedAt     sql.NullString
	EndedAt       sql.NullString
}

type ScheduleRecord struct {
	ID            string
	WorkspaceID   string
	Name          string
	BlueprintPath string
	CronExpr      string
	Enabled       bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	LastRunAt     sql.NullString
	NextRunAt     sql.NullString
}

type RunEventRecord struct {
	ID        int64
	RunID     string
	StepName  sql.NullString
	EventType string
	Message   sql.NullString
	CreatedAt time.Time
}

type PipelineRunRecord struct {
	ID           string
	WorkspaceID  string
	PipelinePath string
	Status       string
	ErrorMessage sql.NullString
	CreatedAt    time.Time
	StartedAt    sql.NullString
	EndedAt      sql.NullString
}

type PipelineStageRunRecord struct {
	ID            int64
	WorkspaceID   string
	PipelineRunID string
	StageIndex    int
	StageName     string
	RunID         string
	Status        string
	OutputsJSON   string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type TriggerRecord struct {
	ID              string
	Type            string
	Name            string
	Path            string
	BlueprintPath   string
	WorkspaceID     string
	SecretHash      sql.NullString
	ExtractInputs   sql.NullString
	Enabled         bool
	OneShot         bool
	TTLExpiresAt    sql.NullString
	CreatedAt       time.Time
	UpdatedAt       time.Time
	FiredCount      int
	LastFiredAt     sql.NullString
	DebounceSeconds int
	CreatedBy       string
}

type WorkspaceRecord struct {
	ID        string
	Name      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("db path is required")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir db dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite db: %w", err)
	}

	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	if err := applyMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

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
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at DESC, id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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

func (s *Store) UpdateScheduleEnabledAndNext(ctx context.Context, id string, enabled bool, nextRun *time.Time) error {
	var next sql.NullString
	if nextRun != nil {
		next = sql.NullString{String: nextRun.UTC().Format(time.RFC3339), Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE schedules
		SET enabled = ?, next_run_at = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(enabled), next, time.Now().UTC().Format(time.RFC3339), id)
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
	defer rows.Close()

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

func (s *Store) AppendRunEvent(ctx context.Context, rec RunEventRecord) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO run_events(run_id, step_name, event_type, message, created_at)
		VALUES (?, ?, ?, ?, ?)
	`,
		rec.RunID,
		rec.StepName,
		rec.EventType,
		rec.Message,
		rec.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	return nil
}

func (s *Store) ListRunEvents(ctx context.Context, runID string, limit int) ([]RunEventRecord, error) {
	return s.ListRunEventsFiltered(ctx, runID, limit, 0, nil, nil)
}

func (s *Store) ListRunEventsFiltered(ctx context.Context, runID string, limit int, cursorID int64, createdAfter, createdBefore *time.Time) ([]RunEventRecord, error) {
	if limit <= 0 {
		limit = 200
	}
	query := `
		SELECT id, run_id, step_name, event_type, message, created_at
		FROM run_events
		WHERE run_id = ?
	`
	args := []any{runID}
	if createdAfter != nil {
		query += " AND created_at > ?"
		args = append(args, createdAfter.UTC().Format(time.RFC3339Nano))
	}
	if createdBefore != nil {
		query += " AND created_at < ?"
		args = append(args, createdBefore.UTC().Format(time.RFC3339Nano))
	}
	if cursorID > 0 {
		query += " AND id < ?"
		args = append(args, cursorID)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list run events: %w", err)
	}
	defer rows.Close()

	out := make([]RunEventRecord, 0, limit)
	for rows.Next() {
		var rec RunEventRecord
		var created string
		if err := rows.Scan(
			&rec.ID,
			&rec.RunID,
			&rec.StepName,
			&rec.EventType,
			&rec.Message,
			&created,
		); err != nil {
			return nil, fmt.Errorf("scan run event: %w", err)
		}
		ts, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, fmt.Errorf("parse run event created_at: %w", err)
		}
		rec.CreatedAt = ts
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list run events rows: %w", err)
	}
	return out, nil
}

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
	defer rows.Close()

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
	defer rows.Close()

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
	defer rows.Close()

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

func (s *Store) CreateWorkspace(ctx context.Context, id, name string) error {
	if id == "" {
		return fmt.Errorf("workspace id is required")
	}
	if name == "" {
		name = id
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workspaces(id, name, created_at, updated_at)
		VALUES (?, ?, ?, ?)
	`, id, name, now, now)
	if err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	return nil
}

func (s *Store) ListWorkspaces(ctx context.Context) ([]WorkspaceRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, created_at, updated_at
		FROM workspaces
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	defer rows.Close()

	out := []WorkspaceRecord{}
	for rows.Next() {
		var rec WorkspaceRecord
		var created, updated string
		if err := rows.Scan(&rec.ID, &rec.Name, &created, &updated); err != nil {
			return nil, fmt.Errorf("scan workspace: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, created)
		if err != nil {
			return nil, fmt.Errorf("parse workspace created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updated)
		if err != nil {
			return nil, fmt.Errorf("parse workspace updated_at: %w", err)
		}
		rec.CreatedAt = createdAt
		rec.UpdatedAt = updatedAt
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workspaces rows: %w", err)
	}
	return out, nil
}

func (s *Store) GetWorkspace(ctx context.Context, id string) (WorkspaceRecord, error) {
	var rec WorkspaceRecord
	var created, updated string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, name, created_at, updated_at
		FROM workspaces
		WHERE id = ?
	`, id).Scan(&rec.ID, &rec.Name, &created, &updated); err != nil {
		return WorkspaceRecord{}, fmt.Errorf("get workspace: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return WorkspaceRecord{}, fmt.Errorf("parse workspace created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updated)
	if err != nil {
		return WorkspaceRecord{}, fmt.Errorf("parse workspace updated_at: %w", err)
	}
	rec.CreatedAt = createdAt
	rec.UpdatedAt = updatedAt
	return rec, nil
}

// ── Triggers ──────────────────────────────────────────────────────────────────

func (s *Store) CreateTrigger(ctx context.Context, rec TriggerRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	if rec.Type == "" {
		rec.Type = "webhook"
	}
	if rec.DebounceSeconds <= 0 {
		rec.DebounceSeconds = 5
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO triggers(id, type, name, path, blueprint_path, workspace_id, secret_hash, extract_inputs, enabled, one_shot, ttl_expires_at, created_at, updated_at, fired_count, last_fired_at, debounce_seconds, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.Type,
		rec.Name,
		rec.Path,
		rec.BlueprintPath,
		rec.WorkspaceID,
		rec.SecretHash,
		rec.ExtractInputs,
		boolToInt(rec.Enabled),
		boolToInt(rec.OneShot),
		rec.TTLExpiresAt,
		now,
		now,
		rec.FiredCount,
		rec.LastFiredAt,
		rec.DebounceSeconds,
		rec.CreatedBy,
	)
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}
	return nil
}

const triggerSelectCols = `id, type, name, path, blueprint_path, workspace_id, secret_hash, extract_inputs, enabled, one_shot, ttl_expires_at, created_at, updated_at, fired_count, last_fired_at, debounce_seconds, created_by`

func scanTriggerRow(scanner interface{ Scan(...any) error }) (TriggerRecord, error) {
	var rec TriggerRecord
	var created, updated string
	var enabled, oneShot int
	if err := scanner.Scan(
		&rec.ID,
		&rec.Type,
		&rec.Name,
		&rec.Path,
		&rec.BlueprintPath,
		&rec.WorkspaceID,
		&rec.SecretHash,
		&rec.ExtractInputs,
		&enabled,
		&oneShot,
		&rec.TTLExpiresAt,
		&created,
		&updated,
		&rec.FiredCount,
		&rec.LastFiredAt,
		&rec.DebounceSeconds,
		&rec.CreatedBy,
	); err != nil {
		return TriggerRecord{}, err
	}
	rec.Enabled = enabled == 1
	rec.OneShot = oneShot == 1
	createdAt, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return TriggerRecord{}, fmt.Errorf("parse trigger created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updated)
	if err != nil {
		return TriggerRecord{}, fmt.Errorf("parse trigger updated_at: %w", err)
	}
	rec.CreatedAt = createdAt
	rec.UpdatedAt = updatedAt
	return rec, nil
}

func (s *Store) GetTrigger(ctx context.Context, id string) (TriggerRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+triggerSelectCols+` FROM triggers WHERE id = ?`, id)
	rec, err := scanTriggerRow(row)
	if err != nil {
		return TriggerRecord{}, fmt.Errorf("get trigger: %w", err)
	}
	return rec, nil
}

func (s *Store) ListTriggers(ctx context.Context) ([]TriggerRecord, error) {
	return s.queryTriggers(ctx, `SELECT `+triggerSelectCols+` FROM triggers ORDER BY created_at DESC`)
}

func (s *Store) queryTriggers(ctx context.Context, query string, args ...any) ([]TriggerRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query triggers: %w", err)
	}
	defer rows.Close()

	out := make([]TriggerRecord, 0)
	for rows.Next() {
		rec, err := scanTriggerRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan trigger: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query triggers rows: %w", err)
	}
	return out, nil
}

func (s *Store) DeleteTrigger(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM triggers WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete trigger: %w", err)
	}
	return nil
}

func (s *Store) UpdateTriggerFired(ctx context.Context, id string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		UPDATE triggers
		SET fired_count = fired_count + 1, last_fired_at = ?, updated_at = ?
		WHERE id = ?
	`, now, now, id)
	if err != nil {
		return fmt.Errorf("update trigger fired: %w", err)
	}
	return nil
}

func (s *Store) ListWebhookTriggers(ctx context.Context) ([]TriggerRecord, error) {
	return s.queryTriggers(ctx, `SELECT `+triggerSelectCols+` FROM triggers WHERE type = 'webhook' AND enabled = 1 ORDER BY created_at DESC`)
}

func (s *Store) ListFileWatchTriggers(ctx context.Context) ([]TriggerRecord, error) {
	return s.queryTriggers(ctx, `SELECT `+triggerSelectCols+` FROM triggers WHERE type = 'file_watch' AND enabled = 1 ORDER BY created_at DESC`)
}

func (s *Store) ListTriggersByCreatedBy(ctx context.Context, createdBy string) ([]TriggerRecord, error) {
	return s.queryTriggers(ctx, `SELECT `+triggerSelectCols+` FROM triggers WHERE created_by = ? AND enabled = 1 ORDER BY created_at DESC`, createdBy)
}

func (s *Store) DeleteExpiredTriggers(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM triggers WHERE ttl_expires_at IS NOT NULL AND ttl_expires_at != '' AND ttl_expires_at < ?`, now.UTC().Format(time.RFC3339))
	if err != nil {
		return 0, fmt.Errorf("delete expired triggers: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) GetTriggerByPath(ctx context.Context, path string) (TriggerRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+triggerSelectCols+` FROM triggers WHERE path = ? AND enabled = 1`, path)
	rec, err := scanTriggerRow(row)
	if err != nil {
		return TriggerRecord{}, fmt.Errorf("get trigger by path: %w", err)
	}
	return rec, nil
}
