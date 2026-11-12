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
