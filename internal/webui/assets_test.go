package webui

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
)

func TestHandlerServesIndexAndSPAFallback(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"/", "/runs/example"} {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		response := httptest.NewRecorder()
		Handler().ServeHTTP(response, request)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `<div id="root">`) {
			t.Fatalf("GET %s = %d %q", target, response.Code, response.Body.String())
		}
		if got := response.Header().Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("GET %s cache-control = %q", target, got)
		}
	}
}

func TestHandlerRejectsMutation(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST / = %d", response.Code)
	}
}

func TestHandlerServesHEADAndCachesHashedAssets(t *testing.T) {
	t.Parallel()
	root, err := fs.Sub(assets, "dist")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(root, "assets")
	if err != nil {
		t.Fatal(err)
	}
	var asset string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".js") {
			asset = path.Join("/assets", entry.Name())
			break
		}
	}
	if asset == "" {
		t.Fatal("generated web UI contains no JavaScript asset")
	}

	request := httptest.NewRequest(http.MethodHead, asset, nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("HEAD %s = %d", asset, response.Code)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("HEAD %s returned %d body bytes", asset, response.Body.Len())
	}
	if got := response.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("HEAD %s cache-control = %q", asset, got)
	}

	indexRequest := httptest.NewRequest(http.MethodHead, "/", nil)
	indexResponse := httptest.NewRecorder()
	Handler().ServeHTTP(indexResponse, indexRequest)
	if indexResponse.Code != http.StatusOK || indexResponse.Body.Len() != 0 {
		t.Fatalf("HEAD / = %d with %d body bytes", indexResponse.Code, indexResponse.Body.Len())
	}
	if got := indexResponse.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("HEAD / cache-control = %q", got)
	}
}

func TestHandlerDoesNotMaskMissingOrUnsafeAssets(t *testing.T) {
	t.Parallel()
	for _, target := range []string{
		"/assets/missing.js",
		"/robots.txt",
		"/../assets/missing.js",
		"/assets/%2e%2e/index.html",
	} {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		response := httptest.NewRecorder()
		Handler().ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("GET %s = %d, want 404", target, response.Code)
		}
		if strings.Contains(response.Body.String(), `<div id="root">`) {
			t.Fatalf("GET %s returned the SPA shell", target)
		}
	}
}
