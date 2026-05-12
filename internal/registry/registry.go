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
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			// Indexing is best-effort; unreadable files should not block valid peers.
			return nil //nolint:nilerr
		}

		bp, parseErr := blueprint.ParseFile(path)
		if parseErr != nil {
			// Indexing is best-effort; invalid blueprints should not block valid peers.
			return nil //nolint:nilerr
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
			// Keep indexing best-effort when one entry cannot be persisted.
			return nil //nolint:nilerr
		}

		// Record version history (duplicate name+hash pairs are silently ignored).
		_ = r.store.InsertBlueprintVersion(ctx, name, hash, path)

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

// Versions returns the version history for a blueprint by name, newest first.
func (r *Registry) Versions(name string) ([]persistence.BlueprintVersion, error) {
	ctx := context.Background()
	return r.store.ListBlueprintVersions(ctx, name)
}

// CurrentHash returns the current content hash for a blueprint looked up by name or slug.
func (r *Registry) CurrentHash(nameOrSlug string) (string, error) {
	entry, err := r.Show(nameOrSlug)
	if err != nil {
		return "", err
	}
	return entry.VersionHash, nil
}

// VerifyPin checks that a blueprint file's current content hash matches the expected pinned hash.
// Returns the file path and nil error on match, or an error describing the mismatch.
func (r *Registry) VerifyPin(blueprintPath, pinnedHash string) error {
	raw, err := os.ReadFile(blueprintPath)
	if err != nil {
		return fmt.Errorf("read blueprint for pin verification: %w", err)
	}
	currentHash := sha256sum(raw)
	if currentHash != pinnedHash {
		return fmt.Errorf("blueprint hash mismatch: pinned=%s current=%s\nThe blueprint has changed since the pin was set. Re-index and update the pin, or remove --pin to run the latest version", pinnedHash[:16], currentHash[:16])
	}
	return nil
}

// VerifyFileHash checks that a blueprint file's current content hash matches
// the expected pinned hash. This is a standalone function that does not require
// a Registry instance.
func VerifyFileHash(blueprintPath, pinnedHash string) error {
	raw, err := os.ReadFile(blueprintPath)
	if err != nil {
		return fmt.Errorf("read blueprint for pin verification: %w", err)
	}
	currentHash := sha256sum(raw)
	if currentHash != pinnedHash {
		return fmt.Errorf("blueprint hash mismatch: pinned=%s current=%s\nThe blueprint has changed since the pin was set. Re-index and update the pin, or remove --pin to run the latest version", pinnedHash[:16], currentHash[:16])
	}
	return nil
}

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
