package agentsubstrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	agentlaunch "github.com/hollis-labs/agentkit/agentlaunch"
	"github.com/hollis-labs/agentkit/agentruntime/runtimebind"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

// A provider the launcher has no adapter for is refused with a typed error,
// even with allow_generic_subprocess set, instead of being launched with
// argv [prompt] and no output parsing.
func TestLaunchAgent_RefusesUnsupportedRuntime(t *testing.T) {
	for _, allowGeneric := range []bool{false, true} {
		l := NewLauncher(t.TempDir(), map[string]settings.AgentSubstrateSettings{
			"s": {
				Kind:                   kindGoAgentRuntime,
				Provider:               "some-new-cli",
				Runtime:                "subprocess",
				WorkingDirMode:         workingDirModeProcess,
				AllowGenericSubprocess: allowGeneric,
			},
		})
		t.Cleanup(func() { _ = l.Close() })
		_, err := l.LaunchAgent(context.Background(), execution.AgentLaunchRequest{Substrate: "s", LogicalAgentID: "a"})
		if !errors.Is(err, ErrRuntimeNotSupported) {
			t.Fatalf("allow_generic=%v: err = %v; want ErrRuntimeNotSupported", allowGeneric, err)
		}
		var ure *UnsupportedRuntimeError
		if !errors.As(err, &ure) || ure.Provider != "some-new-cli" || ure.Runtime != "subprocess" {
			t.Errorf("allow_generic=%v: err = %#v", allowGeneric, err)
		}
		if got := err.Error(); !strings.Contains(got, "runtime some-new-cli/subprocess not supported by Hadron launcher yet") {
			t.Errorf("message = %q", got)
		}
	}
}

// A known provider on a runtime the launcher has no case for is refused the
// same way.
func TestNewAdapter_RefusesUnsupportedBinding(t *testing.T) {
	_, _, err := newAdapter(runtimebind.Binding{Provider: "codex", Runtime: agentlaunch.RuntimeServeHTTP}, settings.AgentSubstrateSettings{}, t.TempDir())
	if !errors.Is(err, ErrRuntimeNotSupported) {
		t.Fatalf("err = %v; want ErrRuntimeNotSupported", err)
	}
}

func TestMCPSpecFromSettings(t *testing.T) {
	spec := mcpSpecFromSettings(map[string]settings.MCPServerSettings{
		"zeta":   {Transport: "stdio", Command: "/bin/z", Args: []string{"serve"}, Env: map[string]string{"K": "V"}},
		"alpha":  {Transport: "stdio", Command: "a"},
		"remote": {Transport: "http", URL: "https://example.invalid/mcp"},
	})
	want := []agentlaunch.MCPServerSpec{
		{Name: "alpha", Command: "a"},
		{Name: "zeta", Command: "/bin/z", Args: []string{"serve"}, Env: map[string]string{"K": "V"}},
	}
	if !reflect.DeepEqual(spec.Servers, want) {
		t.Errorf("servers = %#v; want %#v (URL server skipped, sorted by name)", spec.Servers, want)
	}
	if got := mcpSpecFromSettings(nil); len(got.Servers) != 0 {
		t.Errorf("nil settings → %#v", got)
	}
}

// The launch plan carries the settings' MCP servers.
func TestBuildLaunchPlan_CarriesMCPSpec(t *testing.T) {
	mcp := mcpSpecFromSettings(map[string]settings.MCPServerSettings{"tools": {Transport: "stdio", Command: "tools-mcp"}})
	plan, err := buildLaunchPlan(settings.AgentSubstrateSettings{Provider: "claude"}, execution.AgentLaunchRequest{LogicalAgentID: "a"}, t.TempDir(), t.TempDir(), "", "", nil, mcp)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.MCP, mcp) {
		t.Errorf("plan.MCP = %#v; want %#v", plan.MCP, mcp)
	}
}
