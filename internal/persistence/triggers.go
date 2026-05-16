package persistence

import (
	"context"
	"fmt"
	"time"
)

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
	defer closeRows(rows)

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
