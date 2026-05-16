package persistence

import (
	"context"
	"fmt"
	"time"
)

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
	defer closeRows(rows)

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
