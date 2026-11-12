package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gosched "github.com/hollis-labs/libs/core/util/scheduler"
	libauth "github.com/hollis-labs/libs/security/auth"
	"github.com/hollis-labs/libs/workflow/engine/graph"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/persistencetest"
	"github.com/hollis-labs/hadron/internal/runtime"
	"github.com/hollis-labs/hadron/internal/runtimetest"
)

// seedCatalog does what an administrator does before agents arrive: publish two
// workflows and give the "reviewers" profile one of them. It runs the runtime
// in-process on the files the daemon will then open.
func seedCatalog(t *testing.T, dir string) (pinned, other graph.DefinitionRef) {
	t.Helper()
	store, err := persistencetest.Open(filepath.Join(dir, "hadron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	activations, err := persistence.NewWorkflowActivationStore(store)
	if err != nil {
		t.Fatal(err)
	}
	fires := &runtime.Fires{}
	engine := gosched.New(activations, fires,
		gosched.WithClaimLease(hoststate.ActivationClaimLease),
		gosched.WithObserver(appworkflow.ActivationObserver{Store: activations}))
	rt, err := runtime.New(store, runtime.Config{DataDir: filepath.Join(dir, "hadron-data"), Workers: 1},
		runtime.WithFires(fires), runtime.WithSourceEngine(engine))
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Shutdown(context.Background()) }()
	admin, err := runtime.Authenticate(libauth.WithPrincipal(t.Context(), libauth.Principal{Verified: true, ID: "user:seed", Scopes: []string{runtime.ScopeManage, runtime.ScopeRun}}), libauth.ModeEnforce, "http")
	if err != nil {
		t.Fatal(err)
	}
	pinned = runtimetest.Publish(admin, t, rt.Lifecycle(), "pinned-flow", "v1")
	other = runtimetest.Publish(admin, t, rt.Lifecycle(), "other-flow", "v1")
	if _, err := rt.Exposure().PutProfile(admin, hoststate.ExposureProfileRecord{
		ID: "reviewers", Pins: []graph.DefinitionRef{pinned}, MaxDirectTools: 5, SearchScope: hoststate.ExposureSearchNone,
	}, 0); err != nil {
		t.Fatal(err)
	}
	return pinned, other
}

// With auth and policy enforcing and real API keys, a caller sees and runs what
// its exposure profile covers, and nothing else, over MCP and HTTP alike.
func TestEnforcedCallersSeeAndRunOnlyTheirProfile(t *testing.T) {
	bin := build(t)
	addr := freeAddr(t)
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pinned, other := seedCatalog(t, dir)

	keys := map[string]string{"admin": "admin-secret-key", "agent1": "agent1-secret-key", "agent2": "agent2-secret-key"}
	scopes := map[string]string{
		"admin":  `[hadron.manage, hadron.run, hadron.mcp]`,
		"agent1": `[hadron.run, hadron.mcp, hadron.profile.reviewers]`,
		"agent2": `[hadron.run, hadron.mcp]`,
	}
	var b strings.Builder
	fmt.Fprintf(&b, "httphost:\n  addr: %q\nauth:\n  mode: enforce\n  api_keys:\n", addr)
	for id, key := range keys {
		fmt.Fprintf(&b, "    - {id: %s, hash: %s, scopes: %s}\n", id, libauth.HashKey(key), scopes[id])
	}
	b.WriteString("policy:\n  mode: enforce\n")
	cfg := filepath.Join(dir, "override.yaml")
	if err := os.WriteFile(cfg, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	var environ []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "HOLLIS_AUTH") && !strings.HasPrefix(e, "SUBSTRATE_CONFIG") {
			environ = append(environ, e)
		}
	}
	daemon := exec.Command(bin, "serve")
	daemon.Dir, daemon.Env = dir, append(environ, "SUBSTRATE_CONFIG="+cfg)
	var stderr strings.Builder
	daemon.Stderr = &stderr
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Process.Kill(); _ = daemon.Wait() }()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon did not start: %s", stderr.String())
		}
	}

	post := func(path, key, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(data)
	}
	rpc := func(key, method, params string) string {
		t.Helper()
		code, body := post("/mcp", key, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params))
		if code != 200 {
			t.Fatalf("%s as %s: %d %s", method, key, code, body)
		}
		return body
	}
	toolNames := func(key string) []string {
		var res struct {
			Result struct{ Tools []struct{ Name string } }
		}
		if err := json.Unmarshal([]byte(extractJSON(rpc(key, "tools/list", `{}`))), &res); err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, tl := range res.Result.Tools {
			names = append(names, tl.Name)
		}
		return names
	}
	has := func(names []string, sub string) bool {
		for _, n := range names {
			if strings.Contains(n, sub) {
				return true
			}
		}
		return false
	}

	// Who sees which tools.
	admin, a1, a2 := toolNames(keys["admin"]), toolNames(keys["agent1"]), toolNames(keys["agent2"])
	if !has(admin, "pinned") || !has(admin, "other") || !has(admin, "hadron_workflow_author_register") {
		t.Errorf("administrator tools: %v", admin)
	}
	if !has(a1, "pinned") || has(a1, "other") || !has(a1, "hadron_workflow_run") || has(a1, "hadron_workflow_author_register") {
		t.Errorf("profile caller tools: %v", a1)
	}
	if has(a2, "pinned") || has(a2, "other") || !has(a2, "hadron_workflow_run") {
		t.Errorf("caller with no profile tools: %v", a2)
	}
	var pinnedTool, otherTool string
	for _, n := range admin {
		switch {
		case strings.Contains(n, "pinned"):
			pinnedTool = n
		case strings.Contains(n, "other"):
			otherTool = n
		}
	}

	// Who can run what, by typed tool.
	call := func(key, tool string) (isError bool, body string) {
		body = rpc(key, "tools/call", fmt.Sprintf(`{"name":%q,"arguments":{"message":"hello"}}`, tool))
		return strings.Contains(body, `"isError":true`), body
	}
	if isErr, body := call(keys["agent1"], pinnedTool); isErr || !strings.Contains(body, "run_id") {
		t.Errorf("profile caller running its workflow: %s", body)
	}
	if isErr, body := call(keys["agent1"], otherTool); !isErr {
		t.Errorf("profile caller ran a workflow outside its profile: %s", body)
	}
	if isErr, body := call(keys["agent2"], pinnedTool); !isErr {
		t.Errorf("caller with no profile ran a workflow: %s", body)
	}
	if isErr, body := call(keys["admin"], otherTool); isErr || !strings.Contains(body, "run_id") {
		t.Errorf("administrator running a workflow: %s", body)
	}

	// The same boundary over HTTP: the generic run path, and no key at all.
	validate := func(key string, ref graph.DefinitionRef) (int, string) {
		body, _ := json.Marshal(map[string]any{"definition": ref, "identity": map[string]any{}})
		return post("/api/workflows/validate", key, string(body))
	}
	if code, body := validate(keys["agent1"], pinned); code != 200 || strings.Contains(body, "HADR-HOST-021") {
		t.Errorf("profile caller validating its workflow: %d %s", code, body)
	}
	if code, body := validate(keys["agent1"], other); code != 200 || !strings.Contains(body, "HADR-HOST-021") {
		t.Errorf("profile caller validating outside its profile: %d %s", code, body)
	}
	if code, body := validate(keys["agent2"], pinned); code != 200 || !strings.Contains(body, "HADR-HOST-021") {
		t.Errorf("caller with no profile validating: %d %s", code, body)
	}
	if code, _ := validate("", pinned); code != http.StatusUnauthorized {
		t.Errorf("no credentials: %d, want 401", code)
	}
	if code, _ := post("/api/workflows/lifecycle/author/validate", keys["agent1"], `{}`); code != http.StatusForbidden {
		t.Errorf("a runner on a manage route: %d, want 403", code)
	}
}

// extractJSON returns the JSON object in a response that may be an event stream.
func extractJSON(body string) string {
	if i := strings.Index(body, "{"); i >= 0 {
		return body[i:]
	}
	return body
}
