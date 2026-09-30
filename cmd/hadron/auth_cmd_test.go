package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/localauth"
)

func withTokenFile(t *testing.T, addr string) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), localauth.TokenFileName)
	token, err := localauth.EnsureToken(path)
	if err != nil {
		t.Fatal(err)
	}
	oldAddr, oldFile := globalAddr, globalTokenFile
	globalAddr, globalTokenFile = addr, path
	t.Cleanup(func() { globalAddr, globalTokenFile = oldAddr, oldFile })
	t.Setenv(localauth.EnvToken, "")
	return path, token
}

// The CLI sends the operator token to the configured daemon, and only there.
func TestOperatorTransportSendsTokenOnlyToTheDaemon(t *testing.T) {
	var seen string
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer daemon.Close()
	var leaked string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization")
	}))
	defer other.Close()
	_, token := withTokenFile(t, daemon.URL)

	if err := httpGet(daemon.URL+"/v1/workspaces", nil); err != nil {
		t.Fatal(err)
	}
	if seen != "Bearer "+token {
		t.Fatalf("daemon saw Authorization %q", seen)
	}
	resp, err := httpClient.Get(other.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if leaked != "" {
		t.Fatalf("token sent to a non-daemon host: %q", leaked)
	}
}

func TestOperatorTokenPrefersFlagThenEnvThenDefault(t *testing.T) {
	_, token := withTokenFile(t, "http://127.0.0.1:1")
	t.Setenv(localauth.EnvToken, strings.Repeat("a", 64))
	if got, err := operatorToken(); err != nil || got != token {
		t.Fatalf("--token-file should win over the env var: %q, %v", got, err)
	}
	globalTokenFile = ""
	if got, err := operatorToken(); err != nil || got != strings.Repeat("a", 64) {
		t.Fatalf("env var = %q, %v", got, err)
	}
	t.Setenv(localauth.EnvToken, "not-a-token")
	if _, err := operatorToken(); err == nil {
		t.Fatal("malformed env token accepted")
	}
}

func TestUICommandPrintsSingleUseLink(t *testing.T) {
	var auth string
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/auth/code" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"code":"c0de","login_path":"/auth/login?code=c0de"}`))
	}))
	defer daemon.Close()
	_, token := withTokenFile(t, daemon.URL)
	out, err := runRoot(t, "--addr", daemon.URL, "--token-file", globalTokenFile, "ui", "--no-open")
	if err != nil || !strings.Contains(out, daemon.URL+"/auth/login?code=c0de") {
		t.Fatalf("ui = %q, %v", out, err)
	}
	if auth != "Bearer "+token {
		t.Fatalf("code request Authorization = %q", auth)
	}
}

func TestAuthRotateReplacesTheToken(t *testing.T) {
	path, token := withTokenFile(t, "http://127.0.0.1:1")
	out, err := runRoot(t, "--token-file", path, "auth", "rotate")
	if err != nil || !strings.Contains(out, "rotated") {
		t.Fatalf("auth rotate = %q, %v", out, err)
	}
	rotated, err := localauth.ReadToken(path)
	if err != nil || rotated == token {
		t.Fatalf("token after rotate = %q, %v", rotated, err)
	}
	if info, statErr := os.Stat(path); statErr != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rotated mode = %v, %v", info.Mode().Perm(), statErr)
	}
}

func TestUnauthorizedErrorNamesTheTokenFile(t *testing.T) {
	daemon := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"operator credential required"}`))
	}))
	defer daemon.Close()
	oldAddr, oldFile := globalAddr, globalTokenFile
	globalAddr, globalTokenFile = daemon.URL, filepath.Join(t.TempDir(), "missing.token")
	t.Cleanup(func() { globalAddr, globalTokenFile = oldAddr, oldFile })
	t.Setenv(localauth.EnvToken, "")
	err := httpGet(daemon.URL+"/v1/workspaces", nil)
	if err == nil || !strings.Contains(err.Error(), "missing.token") {
		t.Fatalf("401 error = %v; want it to name the token file", err)
	}
}

func runRoot(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := buildRootCommand()
	var out strings.Builder
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}
