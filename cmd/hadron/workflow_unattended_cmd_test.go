package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/unattended"
)

const unattendedTestDigest = "sha256:abababababababababababababababababababababababababababababababab"

func runUnattended(t *testing.T, now time.Time, args ...string) (string, error) {
	t.Helper()
	dependencies := workflowCommandDependencies{now: func() time.Time { return now }, random: func(buffer []byte) error {
		for index := range buffer {
			buffer[index] = byte(index + 1)
		}
		return nil
	}}
	command := buildWorkflowCmdWithDependencies(dependencies)
	var stdout bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stdout)
	command.SetArgs(append([]string{"unattended"}, args...))
	err := command.Execute()
	return stdout.String(), err
}

func TestWorkflowUnattendedAllowListRevoke(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

	out, err := runUnattended(t, now, "allow", "--data-dir", dir, "--plan", "nightly", "--digest", unattendedTestDigest,
		"--reason", "nightly report", "--activation", "act-1", "--expires", "2h")
	if err != nil || !strings.Contains(out, "added ua-010203040506") {
		t.Fatalf("allow = %q, %v", out, err)
	}
	path := filepath.Join(dir, unattended.FileName)
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("allow-list mode = %v, %v", info.Mode().Perm(), err)
	}
	file, _, err := unattended.ReadChecked(path, os.Getuid())
	if err != nil || len(file.Entries) != 1 {
		t.Fatalf("written file = %#v, %v", file, err)
	}
	entry := file.Entries[0]
	if entry.PlanID != "nightly" || entry.Scope.ActivationID != "act-1" || !entry.AddedAt.Equal(now) ||
		entry.ExpiresAt == nil || !entry.ExpiresAt.Equal(now.Add(2*time.Hour)) || !strings.HasPrefix(entry.AddedBy, "local:") {
		t.Fatalf("entry = %#v", entry)
	}

	if _, runErr1 := runUnattended(t, now, "allow", "--data-dir", dir, "--plan", "nightly", "--digest", unattendedTestDigest, "--reason", "dup"); runErr1 == nil {
		t.Fatal("duplicate generated id accepted")
	}
	if _, runErr2 := runUnattended(t, now, "allow", "--data-dir", dir, "--plan", "nightly", "--digest", "sha256:short", "--reason", "bad", "--id", "bad"); runErr2 == nil {
		t.Fatal("malformed digest accepted")
	}
	if _, runErr3 := runUnattended(t, now, "allow", "--data-dir", dir, "--plan", "nightly", "--digest", unattendedTestDigest, "--id", "no-reason"); runErr3 == nil {
		t.Fatal("missing --reason accepted")
	}

	out, err = runUnattended(t, now, "list", "--data-dir", dir)
	if err != nil || !strings.Contains(out, "ua-010203040506") || !strings.Contains(out, "live") || !strings.Contains(out, "activation=act-1") {
		t.Fatalf("list = %q, %v", out, err)
	}
	out, err = runUnattended(t, now.Add(3*time.Hour), "list", "--data-dir", dir, "--json")
	var items []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err != nil || json.Unmarshal([]byte(out), &items) != nil || len(items) != 1 || items[0].Status != "expired" {
		t.Fatalf("list after expiry = %q, %v", out, err)
	}

	if out, err = runUnattended(t, now, "revoke", "--data-dir", dir, "ua-010203040506"); err != nil || !strings.Contains(out, "revoked") {
		t.Fatalf("revoke = %q, %v", out, err)
	}
	if _, runErr4 := runUnattended(t, now, "revoke", "--data-dir", dir, "ua-010203040506"); runErr4 == nil {
		t.Fatal("revoking a missing entry succeeded")
	}
	if out, err = runUnattended(t, now, "list", "--data-dir", dir); err != nil || !strings.Contains(out, "no unattended allow-list entries") {
		t.Fatalf("list after revoke = %q, %v", out, err)
	}
}

// The CLI refuses to edit a file the daemon would refuse to honor, rather
// than quietly rewriting a loosened file back to 0600.
func TestWorkflowUnattendedRefusesLoosenedFile(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	if _, err := runUnattended(t, now, "allow", "--data-dir", dir, "--plan", "nightly", "--digest", unattendedTestDigest, "--reason", "r"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dir, unattended.FileName), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, listErr := runUnattended(t, now, "list", "--data-dir", dir); listErr == nil || !strings.Contains(listErr.Error(), "writable") {
		t.Fatalf("list of world-writable file = %v", listErr)
	}
}
