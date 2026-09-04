// Package workflowdependency guards Hadron's extracted workflow-library
// consumer boundary.
package workflowdependency

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const (
	sharedModule  = "github.com/hollis-labs/go-workflow"
	formerModule  = "github.com/hollis-labs/hadron/" + "workflow"
	pinnedVersion = "v0.1.0"
)

func TestExtractedWorkflowDependencyBoundary(t *testing.T) {
	root := repositoryRoot(t)
	if _, err := os.Stat(filepath.Join(root, "workflow")); !os.IsNotExist(err) {
		t.Fatalf("local workflow implementation must remain extracted: stat error=%v", err)
	}

	moduleData, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	moduleText := string(moduleData)
	requireLine := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(sharedModule) + `\s+` + regexp.QuoteMeta(pinnedVersion) + `\s*$`)
	if !requireLine.MatchString(moduleText) {
		t.Fatalf("go.mod must require exact %s %s", sharedModule, pinnedVersion)
	}
	replaceLine := regexp.MustCompile(`(?m)^\s*(?:replace\s+)?` + regexp.QuoteMeta(sharedModule) + `(?:\s+\S+)?\s*=>`)
	if replaceLine.MatchString(moduleText) {
		t.Fatalf("go.mod must not replace released dependency %s", sharedModule)
	}

	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bin", "dist", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(data), formerModule) {
			t.Errorf("former workflow module import remains in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve dependency guard source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
