package api

import (
	libauth "github.com/hollis-labs/libs/security/auth"

	"github.com/hollis-labs/hadron/internal/runtime"
)

// RegisterScopes declares the scope every route requires. A route that is not
// listed here is refused by the auth guard in enforce mode, so adding a handler
// without a scope fails closed.
//
//	hadron.run     run, inspect, cancel, resume, read and stream runs; fire
//	               activations; search and inspect the catalog
//	hadron.manage  author, register, publish, pin and expose workflows
func RegisterScopes(t *libauth.ScopeTable) {
	const base = "/api/workflows"
	run, manage := runtime.ScopeRun, runtime.ScopeManage
	t.Route("POST", base+"/validate", run).
		Route("POST", base+"/explain", run).
		Route("POST", base+"/runs", run).
		Route("GET", base+"/runs/{id}/events/stream", run).
		Route("POST", base+"/runs/{id}/inspect", run).
		Route("POST", base+"/runs/{id}/cancel", run).
		Route("POST", base+"/runs/{id}/resume", run).
		Route("POST", base+"/runs/{id}/rerun", run).
		Route("POST", base+"/runs/{id}/waits", run).
		Route("POST", base+"/runs/{id}/values", run).
		Route("POST", base+"/runs/{id}/events", run).
		Route("POST", base+"/activations/{id}/fire", run).
		Route("POST", base+"/lifecycle/catalog/search", run).
		Route("POST", base+"/lifecycle/catalog/inspect", run).
		Route("POST", base+"/lifecycle/author/*", manage).
		Route("POST", base+"/lifecycle/registry/*", manage).
		Route("POST", base+"/lifecycle/exposure/*", manage)
}
