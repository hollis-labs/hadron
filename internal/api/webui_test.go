package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServerRoutesBrowserUIWithoutMaskingAPI404(t *testing.T) {
	t.Parallel()
	ui := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("operator-ui"))
	})
	server := NewServer("127.0.0.1:0", Dependencies{WebUI: ui})

	browser := httptest.NewRecorder()
	server.Handler().ServeHTTP(browser, httptest.NewRequest(http.MethodGet, "/runs/example", nil))
	if browser.Code != http.StatusOK || browser.Body.String() != "operator-ui" {
		t.Fatalf("browser route = %d %q", browser.Code, browser.Body.String())
	}

	for _, target := range []string{"/v1", "/v1/not-a-route", "/a2a/not-a-route", "/.well-known/not-a-route"} {
		api := httptest.NewRecorder()
		server.Handler().ServeHTTP(api, httptest.NewRequest(http.MethodGet, target, nil))
		if api.Code != http.StatusNotFound || !strings.Contains(api.Body.String(), `"error":"not found"`) {
			t.Fatalf("API route %s = %d %q", target, api.Code, api.Body.String())
		}
	}

	for _, target := range []string{"/assets/../index.html", "/assets/%2e%2e/index.html"} {
		unsafe := httptest.NewRecorder()
		server.Handler().ServeHTTP(unsafe, httptest.NewRequest(http.MethodGet, target, nil))
		if unsafe.Code != http.StatusNotFound || unsafe.Body.String() == "operator-ui" {
			t.Fatalf("unsafe browser route %s = %d %q", target, unsafe.Code, unsafe.Body.String())
		}
	}
}
