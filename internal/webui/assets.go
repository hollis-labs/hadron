// Package webui owns Hadron's generated browser application assets.
//
// The React source remains under cmd/hadron-app/frontend. Its production build
// writes into this package so hadrond can expose the operator UI without Wails,
// Node, or a separate static-file installation at runtime.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var assets embed.FS

// Handler returns a static SPA handler with an index fallback for browser
// routes. API routing stays outside this package.
func Handler() http.Handler {
	root, err := fs.Sub(assets, "dist")
	if err != nil {
		panic(err)
	}
	files := http.FileServer(http.FS(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if hasTraversalSegment(r.URL.Path) {
			http.NotFound(w, r)
			return
		}
		requested := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if requested == "." || requested == "" {
			serveIndex(w, r, files)
			return
		}
		if info, statErr := fs.Stat(root, requested); statErr != nil || info.IsDir() {
			if isAssetRequest(requested) {
				http.NotFound(w, r)
				return
			}
			serveIndex(w, r, files)
			return
		}
		if requested == "index.html" {
			serveIndex(w, r, files)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		files.ServeHTTP(w, r)
	})
}

func serveIndex(w http.ResponseWriter, r *http.Request, files http.Handler) {
	clone := r.Clone(r.Context())
	clone.URL.Path = "/"
	clone.URL.RawPath = ""
	w.Header().Set("Cache-Control", "no-cache")
	files.ServeHTTP(w, clone)
}

func hasTraversalSegment(requestPath string) bool {
	for _, segment := range strings.Split(requestPath, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

func isAssetRequest(requested string) bool {
	return requested == "assets" || strings.HasPrefix(requested, "assets/") || path.Ext(requested) != ""
}
