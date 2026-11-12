package api_test

import (
	"testing"

	libauth "github.com/hollis-labs/libs/security/auth"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/runtime"
)

func TestEveryWorkflowRouteHasItsScope(t *testing.T) {
	table := libauth.NewScopeTable()
	api.RegisterScopes(table)
	for _, c := range []struct{ method, path, scope string }{
		{"POST", "/api/workflows/validate", runtime.ScopeRun},
		{"POST", "/api/workflows/explain", runtime.ScopeRun},
		{"POST", "/api/workflows/runs", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/inspect", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/cancel", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/resume", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/rerun", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/signal", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/waits", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/values", runtime.ScopeRun},
		{"POST", "/api/workflows/runs/r1/events", runtime.ScopeRun},
		{"GET", "/api/workflows/runs/r1/events/stream", runtime.ScopeRun},
		{"POST", "/api/workflows/activations/a1/fire", runtime.ScopeRun},
		{"POST", "/api/workflows/lifecycle/catalog/search", runtime.ScopeRun},
		{"POST", "/api/workflows/lifecycle/catalog/inspect", runtime.ScopeRun},
		{"GET", "/.well-known/agent.json", runtime.ScopeRun},
		{"POST", "/a2a/tasks", runtime.ScopeRun},
		{"GET", "/a2a/tasks/t1", runtime.ScopeRun},
		{"POST", "/a2a/tasks/t1/cancel", runtime.ScopeRun},
		{"POST", "/a2a/tasks/t1/resume", runtime.ScopeRun},
		{"POST", "/api/workflows/lifecycle/author/register", runtime.ScopeManage},
		{"POST", "/api/workflows/lifecycle/registry/publish", runtime.ScopeManage},
		{"POST", "/api/workflows/lifecycle/exposure/pin-definition", runtime.ScopeManage},
		{"POST", "/api/workflows/lifecycle/exposure/put-profile", runtime.ScopeManage},
		{"POST", "/api/workflows/lifecycle/exposure/delete-profile", runtime.ScopeManage},
	} {
		got, known := table.ForRoute(c.method, c.path)
		if !known || got != c.scope {
			t.Errorf("%s %s: scope=%q known=%v, want %q", c.method, c.path, got, known, c.scope)
		}
	}
	// Anything not declared is unmapped, which the guard refuses in enforce mode.
	if _, known := table.ForRoute("POST", "/api/workflows/unlisted"); known {
		t.Error("an undeclared workflow route is mapped")
	}
}
