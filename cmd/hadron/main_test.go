package main_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hollis-labs/libs/workflow/engine/values"
)

// build compiles the real binary once per test and returns its path.
func build(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "hadron")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

// env points the binary at a private data directory and a free port using the
// standard SUBSTRATE_CONFIG override, the way every Substrate app is configured.
func env(t *testing.T, addr string) (dir string, environ []string) {
	t.Helper()
	dir = t.TempDir()
	cfg := filepath.Join(dir, "override.yaml")
	body := "httphost:\n  addr: \"" + addr + "\"\n"
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, append(os.Environ(), "SUBSTRATE_CONFIG="+cfg, "HOLLIS_AUTH=off")
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func TestVersionAndHealthRunWithoutStartingBackgroundWork(t *testing.T) {
	bin := build(t)
	dir, environ := env(t, freeAddr(t))
	for _, args := range [][]string{{"version"}, {"health"}} {
		c := exec.Command(bin, args...)
		c.Dir, c.Env = dir, environ
		if out, err := c.CombinedOutput(); err != nil {
			t.Errorf("hadron %v: %v\n%s", args, err, out)
		}
	}
}

func TestServeAnswersHealthCapabilitiesAndMe(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, environ := env(t, addr)
	c := exec.Command(bin, "serve")
	c.Dir, c.Env = dir, environ
	var stderr bytes.Buffer
	c.Stderr = &stderr
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Process.Kill(); _ = c.Wait() }()

	get := func(path string) (int, string) {
		deadline := time.Now().Add(10 * time.Second)
		for {
			resp, err := http.Get("http://" + addr + path)
			if err == nil {
				defer func() { _ = resp.Body.Close() }()
				b, _ := io.ReadAll(resp.Body)
				return resp.StatusCode, string(b)
			}
			if time.Now().After(deadline) {
				t.Fatalf("GET %s: %v\nstderr: %s", path, err, stderr.String())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if code, body := get("/healthz"); code != 200 || !strings.Contains(body, `"database"`) {
		t.Errorf("/healthz: %d %s", code, body)
	}
	if code, body := get("/api/capabilities"); code != 200 || !strings.Contains(body, `"scheduler"`) || strings.Contains(body, `"queue"`) {
		t.Errorf("/api/capabilities: %d %s", code, body)
	}
	if code, body := get("/api/me"); code != 200 || !strings.Contains(body, `"mode":"off"`) {
		t.Errorf("/api/me: %d %s", code, body)
	}
	// The workflow API is mounted under /api/workflows/ and fails flat: a malformed
	// body is an invalid request, never a 404 and never the old {code}-only shape.
	resp, err := http.Post("http://"+addr+"/api/workflows/validate", "application/json", strings.NewReader("not json"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `"code":"invalid_request"`) || !strings.Contains(string(body), `"message"`) {
		t.Errorf("POST /api/workflows/validate: %d %s", resp.StatusCode, body)
	}
	// A2A: the agent card lists the published workflows (none on a fresh install)
	// and the task endpoint fails flat, both outside /api/.
	if code, body := get("/.well-known/agent.json"); code != 200 || !strings.Contains(body, `"Hadron Workflows"`) {
		t.Errorf("agent card: %d %s", code, body)
	}
	if resp, err := http.Post("http://"+addr+"/a2a/tasks", "application/json", strings.NewReader("not json")); err != nil {
		t.Fatal(err)
	} else {
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"message"`) {
			t.Errorf("POST /a2a/tasks: %d %s", resp.StatusCode, b)
		}
	}
	if code, _ := get("/api/nope"); code == 200 {
		t.Error("an unknown API path answered 200")
	}

	// Graceful stop on interrupt: the process exits cleanly.
	if err := c.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve did not exit cleanly on interrupt: %v\n%s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Error("serve did not stop on interrupt")
	}
}

func TestMCPStdioAnswersInitialize(t *testing.T) {
	bin := build(t)
	dir, environ := env(t, freeAddr(t))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, bin, "mcp")
	c.Dir, c.Env = dir, environ
	in, _ := c.StdinPipe()
	out, _ := c.StdoutPipe()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close(); _ = c.Wait() }()

	req := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}` + "\n"
	if _, err := io.WriteString(in, req); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil {
		t.Fatalf("no reply: %v", err)
	}
	var resp struct {
		Result struct {
			ServerInfo struct{ Name string } `json:"serverInfo"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &resp); err != nil || resp.Result.ServerInfo.Name != "hadron" {
		t.Fatalf("initialize reply: %v %s", err, line)
	}
}

func TestMCPStdioListsAndCallsWorkflowTools(t *testing.T) {
	bin := build(t)
	dir, environ := env(t, freeAddr(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, bin, "mcp")
	c.Dir, c.Env = dir, environ
	in, _ := c.StdinPipe()
	out, _ := c.StdoutPipe()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close(); _ = c.Wait() }()
	r := bufio.NewReader(out)
	send := func(msg string) {
		t.Helper()
		if _, err := io.WriteString(in, msg+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	read := func() string {
		t.Helper()
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("no reply: %v", err)
		}
		return line
	}
	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`)
	read()
	send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	list := read()
	for _, name := range []string{"hadron_workflow_run", "hadron_workflow_run_inspect", "hadron_workflow_run_events", "hadron_workflow_catalog_search", "hadron_workflow_author_register"} {
		if !strings.Contains(list, `"`+name+`"`) {
			t.Errorf("tools/list lacks %s: %s", name, list)
		}
	}
	// A call reaches the engine host and fails as a tool error with a safe code
	// (a digest that is not a digest is an invalid request), never a transport
	// error or a crash.
	send(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"hadron_workflow_catalog_inspect","arguments":{"name":"team/none","version":"v1","digest":"sha256:00"}}}`)
	call := read()
	if !strings.Contains(call, `"isError":true`) || !strings.Contains(call, `"code":"invalid_request"`) {
		t.Errorf("tools/call: %s", call)
	}
}

func TestWorkflowCommandsTalkToTheRunningDaemon(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, environ := env(t, addr)
	daemon := exec.Command(bin, "serve")
	daemon.Dir, daemon.Env = dir, environ
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Process.Kill(); _ = daemon.Wait() }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not start")
		}
	}
	cli := func(args ...string) (string, error) {
		c := exec.Command(bin, append([]string{"workflow"}, args...)...)
		c.Dir, c.Env = dir, environ
		out, err := c.CombinedOutput()
		return string(out), err
	}

	out, err := cli("search", "--namespace", "team")
	if err != nil || !strings.Contains(out, `"matches"`) {
		t.Errorf("search: %v\n%s", err, out)
	}
	// An exact definition that is not registered fails with the flat code, and
	// the command exits non-zero.
	out, err = cli("run", "team/none@v1")
	if err == nil || !strings.Contains(out, "not_found") {
		t.Errorf("run of an unknown definition: err=%v\n%s", err, out)
	}
	out, err = cli("inspect", "no-such-run")
	if err == nil || !strings.Contains(out, "(") {
		t.Errorf("inspect of an unknown run: err=%v\n%s", err, out)
	}
	// Every remaining subcommand reaches a real route: an unknown run or profile
	// is a flat workflow error, never an unmapped path (a bare 404/405 body).
	payload := filepath.Join(dir, "payload.json")
	typed, err := values.NewInline("ok", values.Metadata{
		Producer: values.Producer{Kind: "test", Reference: "cli", Output: "value"}, MediaType: "text/plain",
		Redaction: values.RedactionPublic, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(typed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(payload, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"signal", "no-run", "--name", "n", "--correlation", "c", "--payload", payload},
		{"resume", "no-run", "--wait-id", "w", "--correlation", "c", "--payload", payload},
		{"rerun", "no-run", "--from-node", "n"},
		{"waits", "no-run"},
		{"values", "no-run"},
		{"author", "validate", "--draft", payload},
		{"registry", "publish", "team/none@v1#sha256:" + strings.Repeat("0", 64)},
		{"exposure", "inspect", "no-profile"},
		{"inspect-version", "team/none@v1#sha256:" + strings.Repeat("0", 64)},
	} {
		out, err := cli(args...)
		if err == nil || strings.Contains(out, "daemon answered") || !strings.Contains(out, "(4") && !strings.Contains(out, "(5") {
			t.Errorf("hadron workflow %v: err=%v\n%s", args, err, out)
		}
	}
	// A profile can be created, read back and deleted from the CLI.
	profile := filepath.Join(dir, "profile.json")
	if err := os.WriteFile(profile, []byte(`{"id":"cli-profile","max_direct_tools":3,"search_scope":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := cli("exposure", "put", profile); err != nil || !strings.Contains(out, "cli-profile") {
		t.Fatalf("exposure put: %v\n%s", err, out)
	}
	if out, err := cli("exposure", "inspect", "cli-profile", "-o", "json"); err != nil || !strings.Contains(out, `"generation": 1`) {
		t.Fatalf("exposure inspect: %v\n%s", err, out)
	}
	if out, err := cli("exposure", "delete", "cli-profile", "--expected-generation", "1"); err != nil || !strings.Contains(out, "deleted") {
		t.Fatalf("exposure delete: %v\n%s", err, out)
	}
	if out, err := cli("exposure", "inspect", "cli-profile"); err == nil {
		t.Fatalf("a deleted profile still inspects: %s", out)
	}
	// Nothing listening: a clear message, not a stack of connection errors.
	out, err = cli("--server", "http://"+freeAddr(t), "search", "--namespace", "team")
	if err == nil || !strings.Contains(out, "daemon unreachable") {
		t.Errorf("no daemon: err=%v\n%s", err, out)
	}
}

const helloSource = `workflow: {id: hello, version: v1}
inputs:
  - {name: message, type: string, required: true}
steps:
  - id: echo
    kind_version: v1
    transform: {result: inputs.message}
    with: {message: inputs.message}
    outputs: {result: {type: string}}
    effects: [compute]
outputs:
  result: {type: string, value: steps.echo.outputs.result}
`

func TestWorkflowCommandsOutputFormatsAndFileRefs(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, environ := env(t, addr)
	root := filepath.Join(dir, "hadron-data", "workflows")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "hello.workflow.yaml"), []byte(helloSource), 0o600); err != nil {
		t.Fatal(err)
	}
	daemon := exec.Command(bin, "serve")
	daemon.Dir, daemon.Env = dir, environ
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Process.Kill(); _ = daemon.Wait() }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not start")
		}
	}
	cli := func(args ...string) (string, error) {
		c := exec.Command(bin, append([]string{"workflow"}, args...)...)
		c.Dir, c.Env = dir, environ
		out, err := c.CombinedOutput()
		return string(out), err
	}

	// Text is the default: a file reference validates to a plan line.
	out, err := cli("validate", "hello.workflow.yaml")
	if err != nil || !strings.Contains(out, "valid hello@v1") || !strings.Contains(out, "graph ") {
		t.Fatalf("validate text: %v\n%s", err, out)
	}
	// -o json is the daemon's body; -o yaml is the same data.
	out, err = cli("validate", "hello.workflow.yaml", "-o", "json")
	var asJSON struct{ Plan struct{ ID string } }
	if err != nil || json.Unmarshal([]byte(out[strings.Index(out, "{"):]), &asJSON) != nil || asJSON.Plan.ID != "hello" {
		t.Fatalf("validate json: %v\n%s", err, out)
	}
	if out, err = cli("validate", "hello.workflow.yaml", "-o", "yaml"); err != nil || !strings.Contains(out, "plan:") {
		t.Fatalf("validate yaml: %v\n%s", err, out)
	}
	if _, err = cli("validate", "hello.workflow.yaml", "-o", "xml"); err == nil {
		t.Error("an unknown --output was accepted")
	}
	// A run prints one summary line.
	out, err = cli("run", "hello.workflow.yaml", "--input-json", `{"message":"hi"}`, "--run-id", "cli-hello", "--confirm")
	if err != nil || !strings.Contains(out, "run cli-hello phase=") {
		t.Fatalf("run text: %v\n%s", err, out)
	}
	// Diagnostics print as "severity CODE file:line:col: message" and fail the command.
	if err := os.WriteFile(filepath.Join(root, "broken.workflow.yaml"), []byte("workflow: {id: broken, version: v1}\nsteps:\n  - id: s\n    kind_version: v1\n    nosuchkind: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = cli("validate", "broken.workflow.yaml")
	if err == nil || !regexp.MustCompile(`(?m)^error \S+ .*:\d+:\d+: `).MatchString(out) {
		t.Errorf("validate of a broken workflow: err=%v\n%s", err, out)
	}
	// A registry reference that is not exact is refused by the client, before any call.
	if out, err = cli("registry", "publish", "team/none@v1"); err == nil || !strings.Contains(out, "exact registry reference") {
		t.Errorf("non-exact ref: err=%v\n%s", err, out)
	}
}

func TestBuildProducesARunnableDaemonlessExecutable(t *testing.T) {
	bin := build(t)
	dir, environ := env(t, freeAddr(t))
	if err := os.WriteFile(filepath.Join(dir, "hello.workflow.yaml"), []byte(helloSource), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(dir, "hello-exe")
	c := exec.Command(bin, "build", "hello.workflow.yaml", "-o", artifact)
	c.Dir, c.Env = dir, environ
	if out, err := c.CombinedOutput(); err != nil || !strings.Contains(string(out), "built ") {
		t.Fatalf("hadron build: %v\n%s", err, out)
	}
	run := exec.Command(artifact, "--message", "parity")
	out, err := run.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "parity") {
		t.Fatalf("built executable: %v\n%s", err, out)
	}
	// An unsupported or invalid workflow is rejected with located diagnostics, and nothing is written.
	bad := filepath.Join(dir, "bad.workflow.yaml")
	if err := os.WriteFile(bad, []byte("workflow: {id: bad, version: v1}\nsteps:\n  - id: s\n    kind_version: v1\n    nosuchkind: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c = exec.Command(bin, "build", bad, "-o", filepath.Join(dir, "bad-exe"))
	c.Dir, c.Env = dir, environ
	if out, err := c.CombinedOutput(); err == nil || !strings.Contains(string(out), "offline build rejected") {
		t.Errorf("building an invalid workflow: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir, "bad-exe")); err == nil {
		t.Error("a rejected build left an executable behind")
	}
}

// The shipped examples are acceptance inputs: they validate, the runnable ones
// run to success on the real daemon, and the gate example validates and waits.
func TestShippedExamplesValidateAndRun(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, environ := env(t, addr)
	root := filepath.Join(dir, "hadron-data", "workflows")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	examples := map[string]string{}
	for _, glob := range []string{"../../examples/workflow/production/*.workflow.yaml", "../../examples/workflow/*.workflow.yaml"} {
		matches, err := filepath.Glob(glob)
		if err != nil || len(matches) == 0 {
			t.Fatalf("no examples under %s: %v", glob, err)
		}
		for _, m := range matches {
			data, err := os.ReadFile(m)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Base(m)
			examples[name] = m
			if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	daemon := exec.Command(bin, "serve")
	daemon.Dir, daemon.Env = dir, environ
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Process.Kill(); _ = daemon.Wait() }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not start")
		}
	}
	cli := func(args ...string) (string, error) {
		c := exec.Command(bin, append([]string{"workflow"}, args...)...)
		c.Dir, c.Env = dir, environ
		out, err := c.CombinedOutput()
		return string(out), err
	}
	for name := range examples {
		if out, err := cli("validate", name); err != nil || !strings.Contains(out, "valid ") {
			t.Errorf("validate %s: %v\n%s", name, err, out)
		}
	}
	for _, name := range []string{"hello-transform", "normalize-script"} {
		runID := "example-" + name
		out, err := cli("run", name+".workflow.yaml", "--run-id", runID, "--input-json", `{"message":"  Release READY  "}`, "--confirm")
		if err != nil {
			t.Fatalf("run %s: %v\n%s", name, err, out)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			out, err := cli("inspect", runID, "-o", "json")
			if err != nil {
				t.Fatalf("inspect %s: %v\n%s", runID, err, out)
			}
			var res struct{ Run struct{ Status string } }
			if json.Unmarshal([]byte(out[strings.Index(out, "{"):]), &res) != nil {
				t.Fatalf("inspect output: %s", out)
			}
			if res.Run.Status == "succeeded" {
				break
			}
			if res.Run.Status == "failed" || res.Run.Status == "canceled" || time.Now().After(deadline) {
				t.Fatalf("%s ended %q: %s", runID, res.Run.Status, out)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

const slowSource = `workflow: {id: slow, version: v1}
inputs:
  - {name: message, type: string, required: true}
steps:
  - id: spin
    kind: script
    kind_version: v1
    config:
      runtime: goja
      code: |
        function main(input) {
          var s = 0;
          for (var i = 0; i < 4000000; i++) { s += i; }
          return { n: input.message + ":" + s };
        }
      input_schema: {type: object, additionalProperties: false, required: [message], properties: {message: {type: string}}}
      output_schema: {type: object, additionalProperties: false, required: [n], properties: {n: {type: string}}}
    with: {message: inputs.message}
    outputs: {n: {type: string}}
    effects: [compute]
outputs:
  n: {type: string, value: steps.spin.outputs.n}
`

// launchd, systemd and Cerberus stop a service with SIGTERM. A step that is
// running then finishes, the daemon exits cleanly, and the next process completes
// the run at once instead of waiting out a claim lease and marking the step crashed.
func TestSIGTERMLetsARunningStepFinishAndTheNextProcessCompleteTheRun(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, environ := env(t, addr)
	root := filepath.Join(dir, "hadron-data", "workflows")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "slow.workflow.yaml"), []byte(slowSource), 0o600); err != nil {
		t.Fatal(err)
	}
	start := func() *exec.Cmd {
		d := exec.Command(bin, "serve")
		d.Dir, d.Env = dir, environ
		if err := d.Start(); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
			if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
				_ = resp.Body.Close()
				return d
			}
			if time.Now().After(deadline) {
				_ = d.Process.Kill()
				t.Fatal("daemon did not start")
			}
		}
	}
	cli := func(args ...string) string {
		c := exec.Command(bin, append([]string{"workflow"}, args...)...)
		c.Dir, c.Env = dir, environ
		out, _ := c.CombinedOutput()
		return string(out)
	}

	d := start()
	defer func() { _ = d.Process.Kill() }()
	if out := cli("run", "slow.workflow.yaml", "--input-json", `{"message":"x"}`, "--run-id", "slow-1"); !strings.Contains(out, "run slow-1") {
		t.Fatalf("run: %s", out)
	}
	// Poll over HTTP (no process per poll) so a short step can still be caught running.
	// The script sandbox allows one second of wall time, so the step cannot be made
	// long; if it finishes before we look, the stop is trivially graceful and the
	// assertions below still hold.
	spinStatus := func() string {
		resp, err := http.Post("http://"+addr+"/api/workflows/runs/slow-1/inspect", "application/json", strings.NewReader(`{"run_id":"slow-1","identity":{}}`))
		if err != nil {
			return ""
		}
		defer func() { _ = resp.Body.Close() }()
		var body struct {
			Nodes []struct {
				ID struct {
					NodeID string `json:"node_id"`
				} `json:"id"`
				Status string `json:"status"`
			} `json:"nodes"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		for _, n := range body.Nodes {
			if n.ID.NodeID == "spin" {
				return n.Status
			}
		}
		return ""
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(2 * time.Millisecond) {
		if st := spinStatus(); st == "running" || st == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the step never started")
		}
	}
	if err := d.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- d.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the daemon did not exit cleanly on SIGTERM: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the daemon did not exit on SIGTERM")
	}

	d = start()
	defer func() { _ = d.Process.Kill(); _ = d.Wait() }()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		out := cli("inspect", "slow-1")
		if strings.Contains(out, "status=succeeded") {
			break
		}
		if strings.Contains(out, "crashed") || time.Now().After(deadline) {
			t.Fatalf("the run was not completed by the next process:\n%s", out)
		}
	}
}
