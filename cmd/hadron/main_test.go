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
	"strings"
	"testing"
	"time"
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
