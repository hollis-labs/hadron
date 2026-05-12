package persistence

import (
	"context"
	"fmt"
	"time"
)

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
	defer closeRows(rows)

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
