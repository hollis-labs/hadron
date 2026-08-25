package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestDaemonWebURLRequiresHostPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
		ok    bool
	}{
		{"", "http://127.0.0.1:8095/", true},
		{"localhost:9000", "http://localhost:9000/", true},
		{"[::1]:9000", "http://[::1]:9000/", true},
		{"http://localhost:9000", "", false},
		{"localhost", "", false},
		{"localhost:zero", "", false},
		{"localhost:0", "", false},
		{"localhost:65536", "", false},
	}
	for _, test := range tests {
		got, err := daemonWebURL(test.input)
		if (err == nil) != test.ok {
			t.Fatalf("daemonWebURL(%q) error = %v", test.input, err)
		}
		if test.ok && got.String() != test.want {
			t.Fatalf("daemonWebURL(%q) = %q, want %q", test.input, got.String(), test.want)
		}
	}
}

func TestResolveDaemonBinaryPrefersExecutableSibling(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	name := "hadrond"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	sibling := filepath.Join(directory, name)
	if err := os.WriteFile(sibling, []byte("test"), 0o755); err != nil {
		t.Fatal(err)
	}
	lookedUp := false
	got, err := resolveDaemonBinary(filepath.Join(directory, "hadron-app"), os.Stat, func(string) (string, error) {
		lookedUp = true
		return "", errors.New("unexpected PATH lookup")
	})
	if err != nil {
		t.Fatal(err)
	}
	if lookedUp {
		t.Fatal("resolved PATH after finding executable sibling")
	}
	if got != sibling {
		t.Fatalf("resolveDaemonBinary() = %q, want %q", got, sibling)
	}
}

func TestResolveDaemonBinaryFallsBackToPATH(t *testing.T) {
	t.Parallel()
	want := filepath.Join(t.TempDir(), "path-hadrond")
	got, err := resolveDaemonBinary(filepath.Join(t.TempDir(), "hadron-app"), os.Stat, func(name string) (string, error) {
		if !strings.HasPrefix(name, "hadrond") {
			t.Fatalf("PATH lookup name = %q", name)
		}
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("resolveDaemonBinary() = %q, want %q", got, want)
	}
}

func TestDesktopAppAdoptsHealthyDaemonWithoutOpeningBrowser(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	app, err := newDesktopApp(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	if err := app.run(context.Background(), false); err != nil {
		t.Fatal(err)
	}
}
