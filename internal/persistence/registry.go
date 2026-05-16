package persistence

import (
	"context"
	"fmt"
	"strings"
)

// RegistryEntry represents a blueprint in the local registry.
type RegistryEntry struct {
	ID          string
	Name        string
	Slug        string
	Title       string
	Description string
	Author      string
	Tags        string
	VersionHash string
	FilePath    string
	InputsJSON  string
	IndexedAt   string
}

func (s *Store) UpsertRegistryEntry(ctx context.Context, entry RegistryEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO blueprint_registry(id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
		ON CONFLICT(file_path) DO UPDATE SET
			id = excluded.id,
			name = excluded.name,
			slug = excluded.slug,
			title = excluded.title,
			description = excluded.description,
			author = excluded.author,
			tags = excluded.tags,
			version_hash = excluded.version_hash,
			inputs_json = excluded.inputs_json,
			indexed_at = datetime('now')
	`,
		entry.ID,
		entry.Name,
		entry.Slug,
		entry.Title,
		entry.Description,
		entry.Author,
		entry.Tags,
		entry.VersionHash,
		entry.FilePath,
		entry.InputsJSON,
	)
	if err != nil {
		return fmt.Errorf("upsert registry entry: %w", err)
	}
	return nil
}

func (s *Store) GetRegistryEntryByName(ctx context.Context, name string) (RegistryEntry, error) {
	var entry RegistryEntry
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at
		FROM blueprint_registry
		WHERE name = ?
	`, name).Scan(
		&entry.ID,
		&entry.Name,
		&entry.Slug,
		&entry.Title,
		&entry.Description,
		&entry.Author,
		&entry.Tags,
		&entry.VersionHash,
		&entry.FilePath,
		&entry.InputsJSON,
		&entry.IndexedAt,
	); err != nil {
		return RegistryEntry{}, fmt.Errorf("get registry entry by name: %w", err)
	}
	return entry, nil
}

func (s *Store) GetRegistryEntryBySlug(ctx context.Context, slug string) (RegistryEntry, error) {
	var entry RegistryEntry
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at
		FROM blueprint_registry
		WHERE slug = ?
	`, slug).Scan(
		&entry.ID,
		&entry.Name,
		&entry.Slug,
		&entry.Title,
		&entry.Description,
		&entry.Author,
		&entry.Tags,
		&entry.VersionHash,
		&entry.FilePath,
		&entry.InputsJSON,
		&entry.IndexedAt,
	); err != nil {
		return RegistryEntry{}, fmt.Errorf("get registry entry by slug: %w", err)
	}
	return entry, nil
}

func (s *Store) SearchRegistry(ctx context.Context, query string) ([]RegistryEntry, error) {
	like := "%" + strings.ToLower(query) + "%"
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at
		FROM blueprint_registry
		WHERE LOWER(name) LIKE ?
		   OR LOWER(slug) LIKE ?
		   OR LOWER(description) LIKE ?
		   OR LOWER(tags) LIKE ?
		ORDER BY name ASC
	`, like, like, like, like)
	if err != nil {
		return nil, fmt.Errorf("search registry: %w", err)
	}
	defer closeRows(rows)

	var out []RegistryEntry
	for rows.Next() {
		var entry RegistryEntry
		if err := rows.Scan(
			&entry.ID,
			&entry.Name,
			&entry.Slug,
			&entry.Title,
			&entry.Description,
			&entry.Author,
			&entry.Tags,
			&entry.VersionHash,
			&entry.FilePath,
			&entry.InputsJSON,
			&entry.IndexedAt,
		); err != nil {
			return nil, fmt.Errorf("scan registry entry: %w", err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("search registry rows: %w", err)
	}
	return out, nil
}

func (s *Store) ListRegistryEntries(ctx context.Context) ([]RegistryEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at
		FROM blueprint_registry
		ORDER BY name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list registry entries: %w", err)
	}
	defer closeRows(rows)

	var out []RegistryEntry
	for rows.Next() {
		var entry RegistryEntry
		if err := rows.Scan(
			&entry.ID,
			&entry.Name,
			&entry.Slug,
			&entry.Title,
			&entry.Description,
			&entry.Author,
			&entry.Tags,
			&entry.VersionHash,
			&entry.FilePath,
			&entry.InputsJSON,
			&entry.IndexedAt,
		); err != nil {
			return nil, fmt.Errorf("scan registry entry: %w", err)
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list registry entries rows: %w", err)
	}
	return out, nil
}

// BlueprintVersion represents a historical version of a blueprint.
type BlueprintVersion struct {
	ID            int64
	BlueprintName string
	VersionHash   string
	FilePath      string
	IndexedAt     string
}

// InsertBlueprintVersion records a version snapshot. Duplicate (name,hash) pairs are silently ignored.
func (s *Store) InsertBlueprintVersion(ctx context.Context, name, hash, filePath string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO blueprint_versions(blueprint_name, version_hash, file_path, indexed_at)
		VALUES (?, ?, ?, datetime('now'))
	`, name, hash, filePath)
	if err != nil {
		return fmt.Errorf("insert blueprint version: %w", err)
	}
	return nil
}

// ListBlueprintVersions returns the version history for a blueprint, newest first.
func (s *Store) ListBlueprintVersions(ctx context.Context, name string) ([]BlueprintVersion, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, blueprint_name, version_hash, file_path, indexed_at
		FROM blueprint_versions
		WHERE blueprint_name = ?
		ORDER BY indexed_at DESC
	`, name)
	if err != nil {
		return nil, fmt.Errorf("list blueprint versions: %w", err)
	}
	defer closeRows(rows)

	var out []BlueprintVersion
	for rows.Next() {
		var v BlueprintVersion
		if err := rows.Scan(&v.ID, &v.BlueprintName, &v.VersionHash, &v.FilePath, &v.IndexedAt); err != nil {
			return nil, fmt.Errorf("scan blueprint version: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list blueprint versions rows: %w", err)
	}
	return out, nil
}

// SetRunBlueprintHash sets the blueprint_hash on a run record.
func (s *Store) SetRunBlueprintHash(ctx context.Context, runID, hash string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE runs SET blueprint_hash = ? WHERE id = ?
	`, hash, runID)
	if err != nil {
		return fmt.Errorf("set run blueprint hash: %w", err)
	}
	return nil
}

// GetRegistryEntryHash returns the current version hash for a blueprint by name or slug.
func (s *Store) GetRegistryEntryHash(ctx context.Context, nameOrSlug string) (string, error) {
	var hash string
	err := s.db.QueryRowContext(ctx, `
		SELECT version_hash FROM blueprint_registry WHERE name = ? OR slug = ? LIMIT 1
	`, nameOrSlug, nameOrSlug).Scan(&hash)
	if err != nil {
		return "", fmt.Errorf("get registry entry hash: %w", err)
	}
	return hash, nil
}

func (s *Store) DeleteRegistryEntry(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM blueprint_registry WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete registry entry: %w", err)
	}
	return nil
}

// GetRegistryEntryByFilePath looks up a registry entry by its file path.
func (s *Store) GetRegistryEntryByFilePath(ctx context.Context, filePath string) (RegistryEntry, error) {
	var entry RegistryEntry
	if err := s.db.QueryRowContext(ctx, `
		SELECT id, name, slug, title, description, author, tags, version_hash, file_path, inputs_json, indexed_at
		FROM blueprint_registry
		WHERE file_path = ?
	`, filePath).Scan(
		&entry.ID,
		&entry.Name,
		&entry.Slug,
		&entry.Title,
		&entry.Description,
		&entry.Author,
		&entry.Tags,
		&entry.VersionHash,
		&entry.FilePath,
		&entry.InputsJSON,
		&entry.IndexedAt,
	); err != nil {
		return RegistryEntry{}, fmt.Errorf("get registry entry by file_path: %w", err)
	}
	return entry, nil
}
