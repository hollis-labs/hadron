package blueprint

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const mainWithRegistryImport = `version: "0.4"
blueprint:
  name: main-resolver
  slug: main-resolver
  title: Main Resolver Test
  description: Tests registry-based import resolution
  author: Test
  tags: [test]
imports:
  - path: shared-utils
steps:
  - section: Main
    tasks:
      - name: main-step
        cmd: echo main
`

const sharedUtils = `version: "0.4"
blueprint:
  name: shared-utils
  slug: shared-utils
  title: Shared Utils
  description: A shared utility blueprint
  author: Test
  tags: [test]
steps:
  - section: Utils
    tasks:
      - name: util-step
        cmd: echo utils
`

func TestLoadWithImportsAndResolver_FilePathFallback(t *testing.T) {
	// Standard file-based import should still work without a resolver.
	dir := t.TempDir()
	mainContent := `version: "0.4"
blueprint:
  name: file-import-test
  slug: file-import-test
  title: File Import Test
  description: Test
  author: Test
  tags: [test]
imports:
  - path: child.yaml
steps:
  - section: Main
    tasks:
      - name: main-step
        cmd: echo main
`
	childContent := `version: "0.4"
blueprint:
  name: child
  slug: child
  title: Child
  description: Test child
  author: Test
  tags: [test]
steps:
  - section: Child
    tasks:
      - name: child-step
        cmd: echo child
`
	if err := os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(mainContent), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "child.yaml"), []byte(childContent), 0o644); err != nil {
		t.Fatal(err)
	}

	bp, err := LoadWithImportsAndResolver(filepath.Join(dir, "main.yaml"), nil)
	if err != nil {
		t.Fatalf("LoadWithImportsAndResolver: %v", err)
	}
	if len(bp.Steps) != 2 {
		t.Errorf("expected 2 sections, got %d", len(bp.Steps))
	}
}

func TestLoadWithImportsAndResolver_RegistryResolve(t *testing.T) {
	dir := t.TempDir()
	utilsDir := t.TempDir()

	// Write main blueprint that imports by name (no slashes, no extension).
	if err := os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(mainWithRegistryImport), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write the shared-utils blueprint in a separate directory.
	utilsPath := filepath.Join(utilsDir, "shared-utils.yaml")
	if err := os.WriteFile(utilsPath, []byte(sharedUtils), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a resolver that maps "shared-utils" to the actual file.
	resolver := func(nameOrSlug string) (string, error) {
		if nameOrSlug == "shared-utils" {
			return utilsPath, nil
		}
		return "", fmt.Errorf("not found: %s", nameOrSlug)
	}

	bp, err := LoadWithImportsAndResolver(filepath.Join(dir, "main.yaml"), resolver)
	if err != nil {
		t.Fatalf("LoadWithImportsAndResolver: %v", err)
	}

	// Should have 2 sections: Main + Utils.
	if len(bp.Steps) != 2 {
		t.Errorf("expected 2 sections, got %d", len(bp.Steps))
	}
}

func TestLoadWithImportsAndResolver_ResolverFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(mainWithRegistryImport), 0o644); err != nil {
		t.Fatal(err)
	}

	// Resolver that always fails.
	resolver := func(nameOrSlug string) (string, error) {
		return "", fmt.Errorf("not found")
	}

	_, err := LoadWithImportsAndResolver(filepath.Join(dir, "main.yaml"), resolver)
	if err == nil {
		t.Fatal("expected error when resolver fails and file not found")
	}
}

func TestLoadWithImportsAndResolver_NilResolver(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(mainWithRegistryImport), 0o644); err != nil {
		t.Fatal(err)
	}

	// nil resolver + non-existent file import should fail.
	_, err := LoadWithImportsAndResolver(filepath.Join(dir, "main.yaml"), nil)
	if err == nil {
		t.Fatal("expected error when no resolver and import is a name")
	}
}
