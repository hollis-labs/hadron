package registry

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/persistence"
)

func setupTestRegistry(t *testing.T) (*Registry, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := persistence.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	reg := New(store)
	cleanup := func() { store.Close() }
	return reg, cleanup
}

func examplesDir(t *testing.T) string {
	t.Helper()
	// Walk up to find the project root containing examples/
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "examples")); err == nil {
			return filepath.Join(dir, "examples")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("cannot find examples/ directory")
		}
		dir = parent
	}
}

func TestIndex_AllBlueprints(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	result, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	total := result.Indexed + result.Updated + result.Unchanged
	if total == 0 {
		t.Fatal("expected at least one blueprint to be indexed")
	}
	if result.Indexed == 0 {
		t.Fatal("expected new blueprints on first index")
	}
	t.Logf("first index: %d new, %d updated, %d unchanged", result.Indexed, result.Updated, result.Unchanged)
}

func TestIndex_ReindexUnchanged(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	_, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("first index: %v", err)
	}

	// Re-index same directory — all should be unchanged.
	result2, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("second index: %v", err)
	}
	if result2.Indexed != 0 {
		t.Errorf("expected 0 new, got %d", result2.Indexed)
	}
	if result2.Updated != 0 {
		t.Errorf("expected 0 updated, got %d", result2.Updated)
	}
	if result2.Unchanged == 0 {
		t.Error("expected some unchanged entries")
	}
	t.Logf("re-index: %d new, %d updated, %d unchanged", result2.Indexed, result2.Updated, result2.Unchanged)
}

func TestIndex_DetectsUpdate(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	// Create a temp directory with a blueprint.
	tmpDir := t.TempDir()
	bpPath := filepath.Join(tmpDir, "test.yaml")
	original := `version: "0.4"
blueprint:
  name: test-blueprint
  slug: test-bp
  title: Test Blueprint
  description: A test
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello
`
	if err := os.WriteFile(bpPath, []byte(original), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}

	result1, err := reg.Index(tmpDir)
	if err != nil {
		t.Fatalf("first index: %v", err)
	}
	if result1.Indexed != 1 {
		t.Fatalf("expected 1 new, got %d", result1.Indexed)
	}

	// Modify the blueprint.
	modified := `version: "0.4"
blueprint:
  name: test-blueprint
  slug: test-bp
  title: Test Blueprint Modified
  description: A modified test
  author: Test
  tags: [test, modified]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello modified
`
	if err := os.WriteFile(bpPath, []byte(modified), 0o644); err != nil {
		t.Fatalf("write modified blueprint: %v", err)
	}

	result2, err := reg.Index(tmpDir)
	if err != nil {
		t.Fatalf("second index: %v", err)
	}
	if result2.Updated != 1 {
		t.Errorf("expected 1 updated, got %d", result2.Updated)
	}
}

func TestSearch_FindsLaravel(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	_, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	entries, err := reg.Search("laravel")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected to find laravel blueprint")
	}
	found := false
	for _, e := range entries {
		if e.Name == "laravel-app" {
			found = true
			break
		}
	}
	if !found {
		t.Error("laravel-app not found in search results")
	}
}

func TestSearch_Nonexistent(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	_, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	entries, err := reg.Search("zzz_nonexistent_blueprint_xyz")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty results, got %d", len(entries))
	}
}

func TestResolve_ByName(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	_, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	path, err := reg.Resolve("hello-hadron")
	if err != nil {
		t.Fatalf("resolve by name: %v", err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("resolved path does not exist: %s", path)
	}
}

func TestResolve_BySlug(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	// Create a blueprint with a distinct slug.
	tmpDir := t.TempDir()
	bpPath := filepath.Join(tmpDir, "slugtest.yaml")
	content := `version: "0.4"
blueprint:
  name: slug-test-name
  slug: slug-test-unique
  title: Slug Test
  description: Testing slug resolution
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello
`
	if err := os.WriteFile(bpPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}

	_, err := reg.Index(tmpDir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	path, err := reg.Resolve("slug-test-unique")
	if err != nil {
		t.Fatalf("resolve by slug: %v", err)
	}
	if path != bpPath {
		t.Errorf("expected %s, got %s", bpPath, path)
	}
}

func TestResolve_Unknown(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	_, err := reg.Resolve("nonexistent-blueprint-xyz")
	if err == nil {
		t.Fatal("expected error for unknown blueprint")
	}
}

func TestShow_ByName(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	dir := examplesDir(t)
	_, err := reg.Index(dir)
	if err != nil {
		t.Fatalf("index: %v", err)
	}

	entry, err := reg.Show("laravel-app")
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if entry.Name != "laravel-app" {
		t.Errorf("expected name laravel-app, got %s", entry.Name)
	}
	if entry.InputsJSON == "" || entry.InputsJSON == "[]" {
		t.Error("expected non-empty inputs_json for laravel-app")
	}
	if entry.FilePath == "" {
		t.Error("expected non-empty file_path")
	}
	if entry.VersionHash == "" {
		t.Error("expected non-empty version_hash")
	}
}
