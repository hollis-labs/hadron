package registry

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// Registry provides indexing, search, and resolution for blueprint files.
type Registry struct {
	store *persistence.Store
}

// New creates a Registry backed by the given persistence store.
func New(store *persistence.Store) *Registry {
	return &Registry{store: store}
}

// IndexResult reports the outcome of an Index operation.
type IndexResult struct {
	Indexed   int
	Updated   int
	Unchanged int
}

// Index scans a directory recursively for blueprint YAML files, parses each,
// computes a content hash, and upserts into the registry table.
// Returns count of indexed (new), updated (hash changed), and unchanged blueprints.
func (r *Registry) Index(dir string) (result IndexResult, err error) {
	ctx := context.Background()

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return result, fmt.Errorf("resolve dir: %w", err)
	}

	walkErr := filepath.WalkDir(absDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil // skip unreadable files
		}

		bp, parseErr := blueprint.ParseFile(path)
		if parseErr != nil {
			return nil // skip invalid blueprints
		}

		hash := sha256sum(raw)

		// Check if entry already exists with same hash.
		existing, getErr := r.store.GetRegistryEntryByFilePath(ctx, path)
		if getErr == nil && existing.VersionHash == hash {
			result.Unchanged++
			return nil
		}

		// Serialize inputs to JSON.
		inputsJSON := "[]"
		if len(bp.Inputs) > 0 {
			if b, err := json.Marshal(bp.Inputs); err == nil {
				inputsJSON = string(b)
			}
		}

		tags := strings.Join(bp.Spec.Tags, ",")
		name := bp.Spec.Name
		if name == "" {
			name = bp.Spec.Slug
		}

		id := hash[:16] // use first 16 chars of hash as ID

		entry := persistence.RegistryEntry{
			ID:          id,
			Name:        name,
			Slug:        bp.Spec.Slug,
			Title:       bp.Spec.Title,
			Description: bp.Spec.Description,
			Author:      bp.Spec.Author,
			Tags:        tags,
			VersionHash: hash,
			FilePath:    path,
			InputsJSON:  inputsJSON,
		}

		if upsertErr := r.store.UpsertRegistryEntry(ctx, entry); upsertErr != nil {
			return nil // skip on upsert error
		}

		isNew := errors.Is(getErr, sql.ErrNoRows) || (getErr != nil && strings.Contains(getErr.Error(), "no rows"))
		if isNew {
			result.Indexed++
		} else {
			result.Updated++
		}

		return nil
	})

	if walkErr != nil {
		return result, fmt.Errorf("walk dir: %w", walkErr)
	}
	return result, nil
}

// Resolve looks up a blueprint by name or slug and returns its file path.
// This enables pipeline stages to use `blueprint: deploy-staging` instead of file paths.
func (r *Registry) Resolve(nameOrSlug string) (string, error) {
	ctx := context.Background()

	// Try by name first.
	entry, err := r.store.GetRegistryEntryByName(ctx, nameOrSlug)
	if err == nil {
		return entry.FilePath, nil
	}

	// Try by slug.
	entry, err = r.store.GetRegistryEntryBySlug(ctx, nameOrSlug)
	if err == nil {
		return entry.FilePath, nil
	}

	return "", fmt.Errorf("blueprint %q not found in registry", nameOrSlug)
}

// Search performs search across name, description, tags.
func (r *Registry) Search(query string) ([]persistence.RegistryEntry, error) {
	ctx := context.Background()
	return r.store.SearchRegistry(ctx, query)
}

// Show returns full details for a blueprint by name or slug.
func (r *Registry) Show(nameOrSlug string) (*persistence.RegistryEntry, error) {
	ctx := context.Background()

	// Try by name first.
	entry, err := r.store.GetRegistryEntryByName(ctx, nameOrSlug)
	if err == nil {
		return &entry, nil
	}

	// Try by slug.
	entry, err = r.store.GetRegistryEntryBySlug(ctx, nameOrSlug)
	if err == nil {
		return &entry, nil
	}

	return nil, fmt.Errorf("blueprint %q not found in registry", nameOrSlug)
}

// List returns all indexed registry entries.
func (r *Registry) List() ([]persistence.RegistryEntry, error) {
	ctx := context.Background()
	return r.store.ListRegistryEntries(ctx)
}

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
