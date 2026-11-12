package mcptools

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/runtime"
)

// A tool without a scope, a handler or a schema would be reachable but ungated
// or unusable; every tool must name all three, and names must be unique.
func TestEveryToolIsGatedAndComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, tl := range (&Surface{}).staticTools() {
		if !strings.HasPrefix(tl.name, "hadron_workflow_") || seen[tl.name] {
			t.Errorf("tool name %q is not unique or not namespaced", tl.name)
		}
		seen[tl.name] = true
		if tl.scope != runtime.ScopeRun && tl.scope != runtime.ScopeManage {
			t.Errorf("%s: scope %q is not a Hadron scope", tl.name, tl.scope)
		}
		if tl.handler == nil || tl.schema == nil || tl.description == "" {
			t.Errorf("%s: incomplete (handler=%v schema=%v description=%q)", tl.name, tl.handler != nil, tl.schema != nil, tl.description)
		}
	}
}

func TestMutatingLifecycleToolsNeedTheManageScope(t *testing.T) {
	for _, tl := range (&Surface{}).staticTools() {
		manage := strings.Contains(tl.name, "_author_") || strings.Contains(tl.name, "_registry_") || strings.Contains(tl.name, "_exposure_")
		if manage != (tl.scope == runtime.ScopeManage) {
			t.Errorf("%s: scope %q does not match its lifecycle role", tl.name, tl.scope)
		}
	}
}
