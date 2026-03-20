package pack

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

const mainBP = `version: "0.4"
blueprint:
  name: main-bp
  slug: main-bp
  title: Main Blueprint
  description: A main blueprint for testing
  author: Test
  tags: [test]
imports:
  - path: sub.yaml
steps:
  - section: Main
    tasks:
      - name: main-step
        cmd: echo main
`

const subBP = `version: "0.4"
blueprint:
  name: sub-bp
  slug: sub-bp
  title: Sub Blueprint
  description: A sub blueprint
  author: Test
  tags: [test]
steps:
  - section: Sub
    tasks:
      - name: sub-step
        cmd: echo sub
`

const standaloneBP = `version: "0.4"
blueprint:
  name: standalone-bp
  slug: standalone-bp
  title: Standalone Blueprint
  description: No imports
  author: Test
  tags: [test]
steps:
  - section: Standalone
    tasks:
      - name: only-step
        cmd: echo standalone
`

func setupTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.yaml"), []byte(mainBP), 0o644); err != nil {
		t.Fatalf("write main.yaml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub.yaml"), []byte(subBP), 0o644); err != nil {
		t.Fatalf("write sub.yaml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "standalone.yaml"), []byte(standaloneBP), 0o644); err != nil {
		t.Fatalf("write standalone.yaml: %v", err)
	}
	return dir
}

func TestPack_WithImports(t *testing.T) {
	dir := setupTestDir(t)
	outPath := filepath.Join(t.TempDir(), "test.hbp")

	err := Pack(filepath.Join(dir, "main.yaml"), outPath, nil)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}

	// Verify the archive exists.
	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat archive: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("archive is empty")
	}
}

func TestPack_NoImports(t *testing.T) {
	dir := setupTestDir(t)
	outPath := filepath.Join(t.TempDir(), "standalone.hbp")

	err := Pack(filepath.Join(dir, "standalone.yaml"), outPath, nil)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}

	info, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("stat archive: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("archive is empty")
	}
}

func TestUnpack(t *testing.T) {
	dir := setupTestDir(t)
	archivePath := filepath.Join(t.TempDir(), "test.hbp")

	if err := Pack(filepath.Join(dir, "main.yaml"), archivePath, nil); err != nil {
		t.Fatalf("pack: %v", err)
	}

	outDir := t.TempDir()
	manifest, err := Unpack(archivePath, outDir)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if manifest.Name != "main-bp" {
		t.Errorf("expected name main-bp, got %s", manifest.Name)
	}
	if manifest.Hash == "" {
		t.Error("expected non-empty hash")
	}
	if manifest.Version != "0.4" {
		t.Errorf("expected version 0.4, got %s", manifest.Version)
	}

	// Verify files were extracted.
	if _, err := os.Stat(filepath.Join(outDir, "main.yaml")); err != nil {
		t.Errorf("main.yaml not extracted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "sub.yaml")); err != nil {
		t.Errorf("sub.yaml not extracted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); err != nil {
		t.Errorf("manifest.json not extracted: %v", err)
	}
}

func TestManifest_Dependencies(t *testing.T) {
	dir := setupTestDir(t)
	archivePath := filepath.Join(t.TempDir(), "test.hbp")

	if err := Pack(filepath.Join(dir, "main.yaml"), archivePath, nil); err != nil {
		t.Fatalf("pack: %v", err)
	}

	outDir := t.TempDir()
	manifest, err := Unpack(archivePath, outDir)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if len(manifest.Dependencies) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(manifest.Dependencies))
	}
	if manifest.Dependencies[0] != "sub.yaml" {
		t.Errorf("expected dependency sub.yaml, got %s", manifest.Dependencies[0])
	}
}

func TestRoundTrip_PackUnpackLoad(t *testing.T) {
	dir := setupTestDir(t)
	archivePath := filepath.Join(t.TempDir(), "roundtrip.hbp")

	if err := Pack(filepath.Join(dir, "main.yaml"), archivePath, nil); err != nil {
		t.Fatalf("pack: %v", err)
	}

	outDir := t.TempDir()
	_, err := Unpack(archivePath, outDir)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	// Verify the unpacked blueprint can be loaded with imports.
	bp, err := blueprint.LoadWithImports(filepath.Join(outDir, "main.yaml"))
	if err != nil {
		t.Fatalf("LoadWithImports after round-trip: %v", err)
	}

	// Should have 2 sections: Main + Sub.
	if len(bp.Steps) != 2 {
		t.Errorf("expected 2 sections, got %d", len(bp.Steps))
	}
}

func TestPack_NoImports_ManifestNoDeps(t *testing.T) {
	dir := setupTestDir(t)
	archivePath := filepath.Join(t.TempDir(), "standalone.hbp")

	if err := Pack(filepath.Join(dir, "standalone.yaml"), archivePath, nil); err != nil {
		t.Fatalf("pack: %v", err)
	}

	outDir := t.TempDir()
	manifest, err := Unpack(archivePath, outDir)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}

	if manifest.Name != "standalone-bp" {
		t.Errorf("expected name standalone-bp, got %s", manifest.Name)
	}
	if len(manifest.Dependencies) != 0 {
		t.Errorf("expected 0 dependencies, got %d", len(manifest.Dependencies))
	}
}
