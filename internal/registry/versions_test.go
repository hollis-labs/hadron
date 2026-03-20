package registry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVersions_RecordedOnIndex(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	tmpDir := t.TempDir()
	bpPath := filepath.Join(tmpDir, "versioned.yaml")
	v1 := `version: "0.4"
blueprint:
  name: versioned-bp
  slug: versioned-bp
  title: Versioned Blueprint
  description: Version 1
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo v1
`
	if err := os.WriteFile(bpPath, []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}

	// First index.
	result, err := reg.Index(tmpDir)
	if err != nil {
		t.Fatalf("index v1: %v", err)
	}
	if result.Indexed != 1 {
		t.Fatalf("expected 1 indexed, got %d", result.Indexed)
	}

	// Check versions.
	versions, err := reg.Versions("versioned-bp")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(versions))
	}

	// Modify blueprint and re-index.
	v2 := `version: "0.4"
blueprint:
  name: versioned-bp
  slug: versioned-bp
  title: Versioned Blueprint
  description: Version 2
  author: Test
  tags: [test, updated]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo v2
`
	if err := os.WriteFile(bpPath, []byte(v2), 0o644); err != nil {
		t.Fatal(err)
	}

	result2, err := reg.Index(tmpDir)
	if err != nil {
		t.Fatalf("index v2: %v", err)
	}
	if result2.Updated != 1 {
		t.Errorf("expected 1 updated, got %d", result2.Updated)
	}

	// Should now have 2 versions.
	versions2, err := reg.Versions("versioned-bp")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions2) != 2 {
		t.Errorf("expected 2 versions, got %d", len(versions2))
	}

	// Most recent should be first.
	if versions2[0].VersionHash == versions2[1].VersionHash {
		t.Error("version hashes should differ")
	}
}

func TestVersions_DuplicateHashIgnored(t *testing.T) {
	reg, cleanup := setupTestRegistry(t)
	defer cleanup()

	tmpDir := t.TempDir()
	bpPath := filepath.Join(tmpDir, "stable.yaml")
	content := `version: "0.4"
blueprint:
  name: stable-bp
  slug: stable-bp
  title: Stable Blueprint
  description: Does not change
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo stable
`
	if err := os.WriteFile(bpPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Index twice with same content.
	if _, err := reg.Index(tmpDir); err != nil {
		t.Fatal(err)
	}
	// Force re-index by clearing the registry entry but keeping version.
	// Actually, just index again — unchanged won't insert version again anyway.
	if _, err := reg.Index(tmpDir); err != nil {
		t.Fatal(err)
	}

	versions, err := reg.Versions("stable-bp")
	if err != nil {
		t.Fatal(err)
	}
	// Should only have 1 version since the hash didn't change.
	if len(versions) != 1 {
		t.Errorf("expected 1 version (deduped), got %d", len(versions))
	}
}

func TestVerifyFileHash(t *testing.T) {
	tmpDir := t.TempDir()
	bpPath := filepath.Join(tmpDir, "pin-test.yaml")
	content := `version: "0.4"
blueprint:
  name: pin-test
  slug: pin-test
  title: Pin Test
  description: Testing pin verification
  author: Test
  tags: [test]
steps:
  - section: Test
    tasks:
      - name: hello
        cmd: echo hello
`
	if err := os.WriteFile(bpPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// Compute the expected hash.
	raw, _ := os.ReadFile(bpPath)
	hash := sha256sum(raw)

	// Verify should pass with correct hash.
	if err := VerifyFileHash(bpPath, hash); err != nil {
		t.Fatalf("expected verification to pass: %v", err)
	}

	// Verify should fail with wrong hash.
	if err := VerifyFileHash(bpPath, "0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("expected verification to fail with wrong hash")
	}
}
