package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) CreateHumanGate(ctx context.Context, rec HumanGateRecord) error {
	if rec.WorkspaceID == "" {
		rec.WorkspaceID = "default"
	}
	if rec.OptionsJSON == "" {
		rec.OptionsJSON = "[]"
	}
	now := time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = now
	}
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = rec.CreatedAt
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO human_gates(id, workspace_id, run_id, step_name, prompt, options_json, status, decision, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.ID,
		rec.WorkspaceID,
		rec.RunID,
		rec.StepName,
		rec.Prompt,
		rec.OptionsJSON,
		rec.Status,
		rec.Decision,
		rec.CreatedAt.UTC().Format(time.RFC3339),
		rec.UpdatedAt.UTC().Format(time.RFC3339),
		rec.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("create human gate: %w", err)
	}
	return nil
}

func (s *Store) GetHumanGate(ctx context.Context, id string) (HumanGateRecord, error) {
	var rec HumanGateRecord
	var created, updated string
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, workspace_id, run_id, step_name, prompt, options_json, status, decision, created_at, updated_at, expires_at
		FROM human_gates
		WHERE id = ?
	`, id).Scan(
		&rec.ID,
		&rec.WorkspaceID,
		&rec.RunID,
		&rec.StepName,
		&rec.Prompt,
		&rec.OptionsJSON,
		&rec.Status,
		&rec.Decision,
		&created,
		&updated,
		&rec.ExpiresAt,
	); err != nil {
		return HumanGateRecord{}, fmt.Errorf("get human gate: %w", err)
	}
	createdAt, err := time.Parse(time.RFC3339, created)
	if err != nil {
		return HumanGateRecord{}, fmt.Errorf("parse human gate created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updated)
	if err != nil {
		return HumanGateRecord{}, fmt.Errorf("parse human gate updated_at: %w", err)
	}
	rec.CreatedAt = createdAt
	rec.UpdatedAt = updatedAt
	return rec, nil
}

func (s *Store) SubmitHumanGateDecision(ctx context.Context, id, decision string, decidedAt time.Time) error {
	ns := sql.NullString{String: decision, Valid: true}
	res, err := s.db.ExecContext(ctx, `
		UPDATE human_gates
		SET status = 'decided', decision = ?, updated_at = ?
		WHERE id = ? AND status = 'waiting'
	`, ns, decidedAt.UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("submit human gate decision: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("submit human gate decision rows: %w", err)
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) ExpireHumanGate(ctx context.Context, id string, expiredAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE human_gates
		SET status = 'expired', updated_at = ?
		WHERE id = ? AND status = 'waiting'
	`, expiredAt.UTC().Format(time.RFC3339), id)
	if err != nil {
		return fmt.Errorf("expire human gate: %w", err)
	}
	return nil
}
