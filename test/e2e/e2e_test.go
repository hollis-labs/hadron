//go:build e2e

// Package e2e contains end-to-end tests that build and run the hadrond and
// hadron binaries, exercising the full CLI → daemon → execute → events path.
//
// Run with: go test -tags e2e ./test/e2e/...
// Or via:   make e2e
//
// Prerequisites: the hadron and hadrond binaries must be pre-built under
// {repo}/hadron/bin/. The e2e Makefile target handles this automatically.
package e2e

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

var (
	daemonAddr string
	hadronBin  string
)

// TestMain starts hadrond on a random port, runs all tests, then shuts it down.
func TestMain(m *testing.M) {
	// Locate binaries relative to this file.
	_, filename, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(filename), "../..")
	binDir := filepath.Join(repoRoot, "bin")

	hadrondBin := filepath.Join(binDir, "hadrond")
	hadronBin = filepath.Join(binDir, "hadron")

	if _, err := os.Stat(hadrondBin); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: hadrond binary not found at %s — run 'make build' first\n", hadrondBin)
		os.Exit(1)
	}
	if _, err := os.Stat(hadronBin); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: hadron binary not found at %s — run 'make build' first\n", hadronBin)
		os.Exit(1)
	}

	// Pick a free port.
	port := freePort()
	daemonAddr = fmt.Sprintf("127.0.0.1:%d", port)

	// Set up temp dirs for the daemon.
	tmpDir, err := os.MkdirTemp("", "hadron-e2e-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: create temp dir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "hadron.db")
	logsDir := filepath.Join(tmpDir, "logs")
	dataDir := filepath.Join(tmpDir, "data")

	// Start daemon. Route output to a log file so the test harness avoids
	// I/O-goroutine cleanup races when the process is killed on exit.
	logFile, _ := os.Create(filepath.Join(tmpDir, "hadrond.log"))
	daemon := exec.Command(hadrondBin, "serve",
		"-addr", daemonAddr,
		"-db", dbPath,
		"-logs", logsDir,
		"-data", dataDir,
	)
	daemon.Stdout = logFile
	daemon.Stderr = logFile
	if err := daemon.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: start daemon:", err)
		os.Exit(1)
	}
	defer daemon.Process.Kill() //nolint:errcheck

	// Wait for health endpoint.
	if !waitForHealth(daemonAddr, 15*time.Second) {
		fmt.Fprintln(os.Stderr, "e2e: daemon did not become healthy within timeout")
		daemon.Process.Kill()
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// ── helpers ───────────────────────────────────────────────────────────────────

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 18095
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForHealth(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/v1/health") //nolint:noctx
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// hadron runs the hadron CLI with the given arguments and returns combined output.
func hadron(args ...string) (string, int) {
	allArgs := append([]string{"--addr", "http://" + daemonAddr}, args...)
	cmd := exec.Command(hadronBin, allArgs...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	return string(out), code
}

// examplesDir returns the absolute path to the examples directory.
func examplesDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "../../examples")
}

// ── test cases ────────────────────────────────────────────────────────────────

func TestHealthCheck(t *testing.T) {
	out, code := hadron("daemon")
	if code != 0 {
		t.Fatalf("hadron daemon exited %d: %s", code, out)
	}
	if !strings.Contains(out, "ok") {
		t.Errorf("expected 'ok' in output, got: %s", out)
	}
}

func TestRunHelloHadron(t *testing.T) {
	bp := filepath.Join(examplesDir(), "hello-hadron.yaml")
	if _, err := os.Stat(bp); err != nil {
		t.Skipf("hello-hadron.yaml not found: %v", err)
	}
	out, code := hadron("run", bp)
	if code != 0 {
		t.Fatalf("hadron run exited %d: %s", code, out)
	}
	if !strings.Contains(out, "queued") {
		t.Errorf("expected 'queued' in output, got: %s", out)
	}
}

func TestValidateValidBlueprint(t *testing.T) {
	bp := filepath.Join(examplesDir(), "hello-hadron.yaml")
	if _, err := os.Stat(bp); err != nil {
		t.Skipf("hello-hadron.yaml not found: %v", err)
	}
	out, code := hadron("validate", bp)
	if code != 0 {
		t.Fatalf("hadron validate exited %d: %s", code, out)
	}
	if !strings.Contains(strings.ToLower(out), "valid") {
		t.Errorf("expected 'valid' in output, got: %s", out)
	}
}

func TestValidateInvalidBlueprint(t *testing.T) {
	tmp, err := os.CreateTemp("", "bad-bp-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	tmp.WriteString("not: valid: blueprint: :::") //nolint:errcheck
	tmp.Close()

	_, code := hadron("validate", tmp.Name())
	if code == 0 {
		t.Fatal("expected non-zero exit for invalid blueprint")
	}
}

func TestBlueprintLint(t *testing.T) {
	out, code := hadron("lint", examplesDir())
	if code != 0 {
		t.Fatalf("hadron lint exited %d: %s", code, out)
	}
}

func TestScheduleLifecycle(t *testing.T) {
	// Create schedule.
	out, code := hadron("schedule", "create",
		"--blueprint", "examples/hello-hadron.yaml",
		"--cron", "0 * * * *",
		"--name", "e2e-sched",
	)
	if code != 0 {
		t.Fatalf("schedule create exited %d: %s", code, out)
	}
	// Extract ID from "created schedule <id>".
	parts := strings.Fields(out)
	if len(parts) < 3 {
		t.Fatalf("unexpected create output: %s", out)
	}
	id := parts[len(parts)-1]

	// List — should contain the new schedule.
	listOut, code := hadron("schedule", "list")
	if code != 0 {
		t.Fatalf("schedule list exited %d: %s", code, listOut)
	}
	if !strings.Contains(listOut, id) {
		t.Errorf("schedule %s not found in list: %s", id, listOut)
	}

	// Disable.
	disableOut, code := hadron("schedule", "disable", id)
	if code != 0 {
		t.Fatalf("schedule disable exited %d: %s", code, disableOut)
	}
	if !strings.Contains(disableOut, "false") {
		t.Errorf("expected enabled=false in disable output: %s", disableOut)
	}
}

func TestWorkspaceCreate(t *testing.T) {
	out, code := hadron("workspace", "create", "e2e-ws")
	if code != 0 {
		t.Fatalf("workspace create exited %d: %s", code, out)
	}
	listOut, listCode := hadron("workspace", "list")
	if listCode != 0 {
		t.Fatalf("workspace list exited %d: %s", listCode, listOut)
	}
	if !strings.Contains(listOut, "e2e-ws") {
		t.Errorf("workspace e2e-ws not found in list: %s", listOut)
	}
}

func TestRunWithInputs(t *testing.T) {
	bp := filepath.Join(examplesDir(), "parameterized.yaml")
	if _, err := os.Stat(bp); err != nil {
		t.Skipf("parameterized.yaml not found: %v", err)
	}
	out, code := hadron("run", bp, "--input", "app_name=e2e-test")
	if code != 0 {
		t.Fatalf("hadron run with inputs exited %d: %s", code, out)
	}
	if !strings.Contains(out, "queued") {
		t.Errorf("expected 'queued' in output, got: %s", out)
	}
}

// TestDaemonHealthJSON checks the /v1/health response is valid JSON with status "ok".
func TestDaemonHealthJSON(t *testing.T) {
	resp, err := http.Get("http://" + daemonAddr + "/v1/health") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal("decode health response:", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", body["status"])
	}
}
