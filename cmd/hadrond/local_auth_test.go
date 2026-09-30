package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/localauth"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/webui"
)

type authHarness struct {
	t       *testing.T
	srv     *httptest.Server
	runtime *productionWorkflowRuntime
	cfg     *config.Config
	token   string
}

// newAuthHarness serves the production API composition exactly as
// `hadrond serve` wires it.
func newAuthHarness(t *testing.T) *authHarness {
	t.Helper()
	runtime, cfg, store := newTestProductionWorkflowRuntime(t)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	srv := httptest.NewServer(apiServer(runtime, store).Handler())
	t.Cleanup(srv.Close)
	token, err := localauth.ReadToken(localauth.TokenPath(cfg.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	return &authHarness{t: t, srv: srv, runtime: runtime, cfg: cfg, token: token}
}

func apiServer(runtime *productionWorkflowRuntime, store *persistence.Store) *api.Server {
	return api.NewServer("", api.Dependencies{
		Workspaces: store, Workflows: runtime.operations, WorkflowReads: runtime.operations,
		WorkflowLifecycle: runtime.lifecycle, WorkflowAuth: runtime.auth, OperatorAuth: runtime.operator,
		WorkflowActivations: runtime.externalActivations, A2ATasks: runtime.a2a, AgentCard: runtime.card,
		WorkflowHealth: runtime.host, BuildVersion: "test", WebUI: webui.Handler(),
	})
}

// do sends a request; headers may include "Host". Redirects are not followed.
func (h *authHarness) do(method, path, body string, headers map[string]string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		if key == "Host" {
			req.Host = value
			continue
		}
		req.Header.Set(key, value)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (h *authHarness) bearer() map[string]string {
	return map[string]string{"Authorization": "Bearer " + h.token}
}

// signIn runs the code exchange and returns the session cookie.
func (h *authHarness) signIn() *http.Cookie {
	h.t.Helper()
	resp := h.do(http.MethodPost, "/v1/auth/code", "", h.bearer())
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("code request = %d", resp.StatusCode)
	}
	var issued struct {
		Code      string `json:"code"`
		LoginPath string `json:"login_path"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&issued); err != nil || issued.Code == "" {
		h.t.Fatalf("code response = %#v, %v", issued, err)
	}
	login := h.do(http.MethodGet, issued.LoginPath, "", nil)
	if login.StatusCode != http.StatusSeeOther || login.Header.Get("Location") != "/" {
		h.t.Fatalf("login = %d %q", login.StatusCode, login.Header.Get("Location"))
	}
	for _, cookie := range login.Cookies() {
		if cookie.Name == localauth.SessionCookie {
			if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
				h.t.Fatalf("session cookie attributes = %#v", cookie)
			}
			return cookie
		}
	}
	h.t.Fatal("login set no session cookie")
	return nil
}

func withCookie(cookie *http.Cookie, extra map[string]string) map[string]string {
	headers := map[string]string{"Cookie": cookie.Name + "=" + cookie.Value}
	for k, v := range extra {
		headers[k] = v
	}
	return headers
}

func TestUnauthenticatedLoopbackIsReadOnlyShell(t *testing.T) {
	h := newAuthHarness(t)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/v1/health", http.StatusOK},
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodGet, "/v1/workspaces", http.StatusUnauthorized},
		{http.MethodPost, "/v1/workspaces", http.StatusUnauthorized},
		{http.MethodPost, "/v1/workflows/validate", http.StatusUnauthorized},
	} {
		if resp := h.do(tc.method, tc.path, "{}", nil); resp.StatusCode != tc.want {
			t.Errorf("%s %s without a credential = %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
	// A well-formed run request is refused for the credential, not the body.
	run := `{"run_id":"r1","idempotency_key":"k1","definition":{"kind":"file","id":"production-transform","locator":"production-transform.workflow.yaml","version":"v1"},"inputs":{"message":"x"},"confirmed":true}`
	if resp := h.do(http.MethodPost, "/v1/workflows/runs", run, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("credential-less run start = %d, want 401", resp.StatusCode)
	}
}

func TestOperatorTokenAuthenticates(t *testing.T) {
	h := newAuthHarness(t)
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", h.bearer()); resp.StatusCode != http.StatusOK {
		t.Fatalf("workspaces with the operator token = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workflows/validate", "{}", h.bearer()); resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("workflow route refused the operator token")
	}
	wrong := map[string]string{"Authorization": "Bearer " + strings.Repeat("0", 64)}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", wrong); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("workspaces with a wrong token = %d", resp.StatusCode)
	}
}

func TestLoginCodeFlowOpensSameOriginSession(t *testing.T) {
	h := newAuthHarness(t)
	if resp := h.do(http.MethodPost, "/v1/auth/code", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("code without the token = %d", resp.StatusCode)
	}
	cookie := h.signIn()
	origin := map[string]string{"Origin": h.srv.URL}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", withCookie(cookie, nil)); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET with session = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workflows/validate", "{}", withCookie(cookie, origin)); resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		t.Fatalf("same-origin POST with session = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workflows/validate", "{}", withCookie(cookie, map[string]string{"Referer": h.srv.URL + "/workflows"})); resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		t.Fatalf("POST with session and same-origin Referer = %d", resp.StatusCode)
	}
	// A valid cookie on a cross-origin state change is refused, and so is a
	// state change that names no source at all.
	if resp := h.do(http.MethodPost, "/v1/workflows/validate", "{}", withCookie(cookie, map[string]string{"Origin": "http://evil.example"})); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin POST with a valid session = %d, want 403", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workspaces", `{"name":"x"}`, withCookie(cookie, map[string]string{"Referer": "http://evil.example/page"})); resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-site Referer POST with a valid session = %d, want refusal", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workflows/validate", "{}", withCookie(cookie, nil)); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("sourceless POST with a valid session = %d, want 401", resp.StatusCode)
	}
	if resp := h.do(http.MethodPost, "/v1/workspaces", `{"name":"x"}`, withCookie(cookie, nil)); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("sourceless workspace POST with a valid session = %d, want 401", resp.StatusCode)
	}
	// Sessions answer only loopback Hosts (DNS rebinding).
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", withCookie(cookie, map[string]string{"Host": "rebind.example"})); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session on a rebinding Host = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", map[string]string{"Cookie": localauth.SessionCookie + "=forged"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged session = %d", resp.StatusCode)
	}
	logout := h.do(http.MethodPost, "/v1/auth/logout", "", withCookie(cookie, origin))
	if logout.StatusCode != http.StatusOK {
		t.Fatalf("logout = %d", logout.StatusCode)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", withCookie(cookie, nil)); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session after logout = %d", resp.StatusCode)
	}
}

func TestLoginCodeIsSingleUseAndLoopbackOnly(t *testing.T) {
	h := newAuthHarness(t)
	codeFor := func() string {
		resp := h.do(http.MethodPost, "/v1/auth/code", "", h.bearer())
		var issued struct {
			LoginPath string `json:"login_path"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&issued)
		return issued.LoginPath
	}
	path := codeFor()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, h.srv.URL+path, nil)
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			resp, err := client.Do(req)
			if err == nil {
				if resp.StatusCode == http.StatusSeeOther {
					wins.Add(1)
				}
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent logins with one code succeeded, want exactly 1", wins.Load())
	}
	if resp := h.do(http.MethodGet, codeFor(), "", map[string]string{"Host": "rebind.example"}); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("login on a non-loopback Host = %d, want 403", resp.StatusCode)
	}
}

func TestTokenRotationEndsSessions(t *testing.T) {
	h := newAuthHarness(t)
	cookie := h.signIn()
	rotated, err := localauth.RotateToken(localauth.TokenPath(h.cfg.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", h.bearer()); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old token after rotation = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", withCookie(cookie, nil)); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("session after rotation = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", map[string]string{"Authorization": "Bearer " + rotated}); resp.StatusCode != http.StatusOK {
		t.Fatalf("new token = %d", resp.StatusCode)
	}
}

func TestTransitionFlagRestoresLoopbackTrust(t *testing.T) {
	h := newAuthHarness(t)
	h.runtime.operator.allowUnauthenticatedLoopback = true
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("credential-less loopback under the flag = %d", resp.StatusCode)
	}
	if resp := h.do(http.MethodGet, "/v1/workspaces", "", map[string]string{"Host": "rebind.example"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("rebinding Host under the flag = %d", resp.StatusCode)
	}
	// The flag never mints login codes: that needs the token itself.
	if resp := h.do(http.MethodPost, "/v1/auth/code", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("code under the flag without the token = %d", resp.StatusCode)
	}
}
