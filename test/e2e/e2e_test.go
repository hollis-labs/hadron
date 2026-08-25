//go:build e2e

// Package e2e exercises the production graph-native CLI and daemon boundary.
//
// Run with: go test -tags e2e ./test/e2e/...
// Or via:   make e2e
//
// Prerequisites: make build
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var (
	daemonAddr          string
	hadronBin           string
	validWorkflowPath   string
	invalidWorkflowPath string
	workspaceSequence   atomic.Uint64
)

const validWorkflowSource = `workflow:
  id: e2e-transform
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    outputs:
      result:
        type: string
    effects: [compute]
outputs:
  result:
    type: string
    value: steps.echo.outputs.result
`

const invalidWorkflowSource = `workflow:
  id: e2e-invalid
  version: v1
steps:
  - id: broken
    kind_version: v1
    transform:
      result: "first"
    outputs:
      result:
        type: string
    effects: [compute]
  - id: broken
    kind_version: v1
    transform:
      result: "duplicate"
    outputs:
      result:
        type: string
    effects: [compute]
`

// TestMain starts the production daemon on a random loopback port with its
// graph source rooted under the daemon data directory.
func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(filename), "../..")
	binDir := filepath.Join(repoRoot, "bin")
	hadrondBin := filepath.Join(binDir, "hadrond")
	hadronBin = filepath.Join(binDir, "hadron")

	for _, binary := range []string{hadrondBin, hadronBin} {
		if _, err := os.Stat(binary); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: binary not found at %s — run 'make build' first\n", binary)
			os.Exit(1)
		}
	}

	tmpDir, err := os.MkdirTemp("", "hadron-e2e-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: create temp dir:", err)
		os.Exit(1)
	}
	tmpDir, err = filepath.EvalSymlinks(tmpDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: canonicalize temp dir:", err)
		os.Exit(1)
	}
	dataDir := filepath.Join(tmpDir, "data")
	workflowDir := filepath.Join(dataDir, "workflows")
	if err := os.MkdirAll(workflowDir, 0o750); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: create workflow dir:", err)
		os.Exit(1)
	}
	validWorkflowPath = filepath.Join(workflowDir, "e2e-transform.workflow.yaml")
	invalidWorkflowPath = filepath.Join(workflowDir, "e2e-invalid.workflow.yaml")
	for path, source := range map[string]string{
		validWorkflowPath: validWorkflowSource, invalidWorkflowPath: invalidWorkflowSource,
	} {
		if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: write workflow fixture:", err)
			os.Exit(1)
		}
	}

	daemonAddr = fmt.Sprintf("127.0.0.1:%d", freePort())
	logPath := filepath.Join(tmpDir, "hadrond.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: create daemon log:", err)
		os.Exit(1)
	}
	daemon := exec.Command(hadrondBin, "serve",
		"-addr", daemonAddr,
		"-db", filepath.Join(tmpDir, "hadron.db"),
		"-logs", filepath.Join(tmpDir, "logs"),
		"-data", dataDir,
	)
	daemon.Stdout = logFile
	daemon.Stderr = logFile
	if err := daemon.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: start daemon:", err)
		_ = logFile.Close()
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	if !waitForHealth(daemonAddr, 15*time.Second) {
		_ = daemon.Process.Kill()
		_ = daemon.Wait()
		_ = logFile.Close()
		logBytes, _ := os.ReadFile(logPath)
		fmt.Fprintf(os.Stderr, "e2e: daemon did not become healthy within timeout\n%s", logBytes)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	code := m.Run()
	_ = daemon.Process.Kill()
	_ = daemon.Wait()
	_ = logFile.Close()
	if code != 0 {
		if logBytes, readErr := os.ReadFile(logPath); readErr == nil {
			fmt.Fprintf(os.Stderr, "\n--- hadrond e2e log ---\n%s", logBytes)
		}
	}
	_ = os.RemoveAll(tmpDir)
	os.Exit(code)
}

func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 18095
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForHealth(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/v1/health") //nolint:noctx
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func hadron(args ...string) (string, int) {
	stdout, stderr, code := hadronStreams(args...)
	return stdout + stderr, code
}

func hadronStreams(args ...string) (string, string, int) {
	allArgs := append([]string{"--addr", "http://" + daemonAddr}, args...)
	command := exec.Command(hadronBin, allArgs...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil {
		return stdout.String(), stderr.String(), 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return stdout.String(), stderr.String(), exitErr.ExitCode()
	}
	return stdout.String(), stderr.String(), -1
}

func decodeJSONOutput(t *testing.T, output string, target any) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(output))
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode CLI JSON output: %v\n%s", err, output)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatalf("CLI JSON output has trailing data: %v\n%s", err, output)
	}
}

func TestHealthCheck(t *testing.T) {
	output, code := hadron("daemon")
	if code != 0 {
		t.Fatalf("hadron daemon exited %d: %s", code, output)
	}
	if !strings.Contains(output, "status: ready") || !strings.Contains(output, "version:") {
		t.Fatalf("daemon status did not report graph host readiness: %s", output)
	}
}

func TestWorkflowValidate(t *testing.T) {
	output, code := hadron("workflow", "validate", validWorkflowPath, "--json")
	if code != 0 {
		t.Fatalf("workflow validate exited %d: %s", code, output)
	}
	var result struct {
		Plan *struct {
			ID      string `json:"id"`
			Version string `json:"version"`
			Digest  string `json:"digest"`
		} `json:"plan"`
		Diagnostics []json.RawMessage `json:"diagnostics"`
	}
	decodeJSONOutput(t, output, &result)
	if result.Plan == nil || result.Plan.ID != "e2e-transform" || result.Plan.Version != "v1" || result.Plan.Digest == "" {
		t.Fatalf("unexpected validated plan: %#v", result.Plan)
	}
	if len(result.Diagnostics) != 0 {
		t.Fatalf("valid workflow returned diagnostics: %s", output)
	}
}

func TestWorkflowValidateRejectsInvalidGraph(t *testing.T) {
	output, stderr, code := hadronStreams("workflow", "validate", invalidWorkflowPath, "--json")
	if code == 0 {
		t.Fatalf("invalid graph validation succeeded: %s%s", output, stderr)
	}
	if !strings.Contains(stderr, "workflow validation failed") {
		t.Fatalf("invalid graph did not return a concise CLI error: %s", stderr)
	}
	var result struct {
		Plan        json.RawMessage   `json:"plan"`
		Diagnostics []json.RawMessage `json:"diagnostics"`
	}
	decodeJSONOutput(t, output, &result)
	if len(result.Diagnostics) == 0 || len(result.Plan) != 0 {
		t.Fatalf("invalid graph did not return structured diagnostics without a plan: %s", output)
	}
}

func TestWorkflowRunWithTypedJSONInputAndOutput(t *testing.T) {
	output, code := hadron(
		"workflow", "run", validWorkflowPath,
		"--run-id", "e2e-transform-run",
		"--idempotency-key", "e2e-transform-start",
		"--input-json", `{"message":"hello graph"}`,
		"--json",
	)
	if code != 0 {
		t.Fatalf("workflow run exited %d: %s", code, output)
	}
	var result struct {
		Bound *struct {
			ID string `json:"id"`
		} `json:"bound"`
		Run *struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
		Phase       string            `json:"phase"`
		Diagnostics []json.RawMessage `json:"diagnostics"`
	}
	decodeJSONOutput(t, output, &result)
	if result.Bound == nil || result.Bound.ID != "e2e-transform-run" || result.Run == nil || result.Run.ID != "e2e-transform-run" {
		t.Fatalf("workflow run returned the wrong durable identity: %s", output)
	}
	if result.Phase != "running" || (result.Run.Status != "running" && result.Run.Status != "succeeded") || len(result.Diagnostics) != 0 {
		t.Fatalf("workflow run was not admitted cleanly: %s", output)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		inspectionOutput, inspectionCode := hadron("workflow", "inspect", result.Run.ID, "--json")
		if inspectionCode != 0 {
			t.Fatalf("workflow inspect exited %d: %s", inspectionCode, inspectionOutput)
		}
		var inspection struct {
			Run struct {
				Status  string `json:"status"`
				Outputs *struct {
					ID     string `json:"id"`
					Digest string `json:"digest"`
				} `json:"outputs"`
			} `json:"run"`
		}
		decodeJSONOutput(t, inspectionOutput, &inspection)
		if inspection.Run.Status == "succeeded" {
			if inspection.Run.Outputs == nil || inspection.Run.Outputs.ID == "" || inspection.Run.Outputs.Digest == "" {
				t.Fatalf("succeeded workflow did not persist exact outputs: %s", inspectionOutput)
			}
			break
		}
		if inspection.Run.Status == "failed" || inspection.Run.Status == "canceled" {
			t.Fatalf("workflow reached terminal status %q: %s", inspection.Run.Status, inspectionOutput)
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow did not succeed before timeout: %s", inspectionOutput)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestWorkspaceCreate(t *testing.T) {
	name := fmt.Sprintf("e2e-ws-%d", workspaceSequence.Add(1))
	output, code := hadron("workspace", "create", name)
	if code != 0 {
		t.Fatalf("workspace create exited %d: %s", code, output)
	}
	listOutput, listCode := hadron("workspace", "list")
	if listCode != 0 {
		t.Fatalf("workspace list exited %d: %s", listCode, listOutput)
	}
	if !strings.Contains(listOutput, name) {
		t.Fatalf("workspace %s not found in list: %s", name, listOutput)
	}
}

func TestDaemonHealthJSON(t *testing.T) {
	resp, err := http.Get("http://" + daemonAddr + "/v1/health") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Status   string `json:"status"`
		Version  string `json:"version"`
		Service  string `json:"service"`
		Workflow struct {
			Started    bool `json:"started"`
			Ready      bool `json:"ready"`
			Recovering bool `json:"recovering"`
		} `json:"workflow"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal("decode health response:", err)
	}
	if resp.StatusCode != http.StatusOK || body.Status != "ready" || body.Version == "" || body.Service != "hadrond" || !body.Workflow.Started || !body.Workflow.Ready || body.Workflow.Recovering {
		t.Fatalf("unexpected workflow health status=%d body=%#v", resp.StatusCode, body)
	}
}

func TestRetiredLegacyRootsAreUnavailable(t *testing.T) {
	for _, root := range []string{"run", "validate", "lint", "schedule"} {
		t.Run(root, func(t *testing.T) {
			output, code := hadron(root)
			if code == 0 {
				t.Fatalf("retired legacy root %q remains executable: %s", root, output)
			}
			if !strings.Contains(output, `unknown command "`+root+`"`) {
				t.Fatalf("retired legacy root %q returned unexpected error: %s", root, output)
			}
		})
	}
}
