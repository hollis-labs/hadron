package telemetry

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateRunID(t *testing.T) {
	valid := []string{
		"sched-bp-1715900000",
		"run_42",
		"a2a-task-abc123",
		"UUID-0f8fad5b-d9cb-469f-a165-70867728950e",
		"foo..bar", // embedded dots are fine — not a path element
	}
	for _, id := range valid {
		if err := ValidateRunID(id); err != nil {
			t.Errorf("ValidateRunID(%q) = %v, want nil", id, err)
		}
	}

	invalid := []string{
		"",
		".",
		"..",
		"../etc/passwd",
		"../../escape",
		"sub/dir",
		`back\slash`,
		`..\..\windows`,
		"/abs/path",
		"with\x00nul",
	}
	for _, id := range invalid {
		if err := ValidateRunID(id); err == nil {
			t.Errorf("ValidateRunID(%q) = nil, want error", id)
		}
	}
}

// TestWriteRejectsTraversalRunID verifies a malicious run ID cannot make
// Write open a file outside the configured logs directory.
func TestWriteRejectsTraversalRunID(t *testing.T) {
	root := t.TempDir()
	logsDir := filepath.Join(root, "logs")
	l := New(logsDir, true)

	// A traversal run ID would resolve to root/pwned.jsonl, one level above
	// logsDir. Write must drop it.
	l.Write(LogEntry{RunID: "../pwned", Event: "exploit"})
	if _, err := os.Stat(filepath.Join(root, "pwned.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("traversal run ID escaped logsDir: file exists or unexpected error %v", err)
	}

	// A well-formed run ID is still written, inside logsDir.
	l.Write(LogEntry{RunID: "run-1", Event: "ok"})
	if _, err := os.Stat(filepath.Join(logsDir, "run-1.jsonl")); err != nil {
		t.Fatalf("valid run ID was not written: %v", err)
	}
}
