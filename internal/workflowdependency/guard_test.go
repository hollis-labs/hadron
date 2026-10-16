// Package workflowdependency guards Hadron's extracted workflow-library
// consumer boundary.
package workflowdependency

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const (
	sharedModule   = "github.com/hollis-labs/libs/workflow/engine"
	formerModule   = "github.com/hollis-labs/hadron/" + "workflow"
	minimumVersion = "v0.2.0"
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
	// A floor, not an exact pin: any released version at or above minimumVersion satisfies this.
	requireLine := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(sharedModule) + `\s+(v\S+)\s*$`)
	match := requireLine.FindStringSubmatch(moduleText)
	if match == nil || !releaseAtLeast(match[1], minimumVersion) {
		t.Fatalf("go.mod must require a released %s at or above %s", sharedModule, minimumVersion)
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

// releaseAtLeast reports whether version is a released vMAJOR.MINOR.PATCH (no
// prerelease or pseudo-version suffix) at or above minimum.
func releaseAtLeast(version, minimum string) bool {
	parse := func(v string) ([3]int, bool) {
		var out [3]int
		parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
		if !strings.HasPrefix(v, "v") || len(parts) != 3 {
			return out, false
		}
		for i, part := range parts {
			n, err := strconv.Atoi(part)
			if err != nil || n < 0 {
				return out, false
			}
			out[i] = n
		}
		return out, true
	}
	got, ok := parse(version)
	want, wantOK := parse(minimum)
	if !ok || !wantOK {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return got[i] > want[i]
		}
	}
	return true
}
