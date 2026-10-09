package agentsubstrate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/launchartifacts"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/substrate/harness/adapters/provider"
	"github.com/hollis-labs/substrate/harness/adapters/runtimebind"
	"github.com/hollis-labs/substrate/harness/agentlaunch/launcher"
)

func TestArtifactAdmissionRefusesChangedAcceptedOperation(t *testing.T) {
	for _, change := range []string{"request", "release", "closed", "root", "plan"} {
		t.Run(change, func(t *testing.T) {
			l := NewLauncher(t.TempDir(), nil)
			defer func() {
				if closeErr := l.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}()
			cfg := settings.AgentSubstrateSettings{Provider: "claude", Runtime: "subprocess"}
			req := execution.AgentLaunchRequest{LogicalAgentID: "a", Metadata: map[string]any{"accepted": "original"}}
			admission, release, err := l.acceptArtifactOperation(context.Background(), "session", cfg, req)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			plan, err := buildLaunchPlan(cfg, req, t.TempDir(), t.TempDir(), "boot", "context", mcpSpecFromSettings(nil))
			if err != nil {
				t.Fatal(err)
			}
			plan.Mode = "interactive"
			compiled, err := launcher.Compile(context.Background(), plan)
			if err != nil {
				t.Fatal(err)
			}
			prepared, custody, err := launchartifacts.Prepare(context.Background(), compiled, admission)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if closeErr := custody.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}()
			target := prepared.PlantedBootDir
			switch change {
			case "request":
				req.Metadata["accepted"] = "changed"
			case "release":
				release()
			case "closed":
				if closeErr := l.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			case "root":
				if err := os.Rename(target, target+"-held"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "plan":
				compiled.Plan.Provider.Binary = "substituted"
			}
			if _, err := custody.Authorize(context.Background(), target); err == nil {
				t.Fatal("changed operation accepted")
			}
		})
	}
}

func TestArtifactAdmissionPlantsAndMaterializesCallbacks(t *testing.T) {
	l := NewLauncher(t.TempDir(), nil)
	defer func() {
		if closeErr := l.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	cfg := settings.AgentSubstrateSettings{Provider: "claude", Runtime: "subprocess"}
	req := execution.AgentLaunchRequest{LogicalAgentID: "a"}
	admission, release, err := l.acceptArtifactOperation(context.Background(), "session", cfg, req)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	plan, err := buildLaunchPlan(cfg, req, t.TempDir(), t.TempDir(), "boot", "context", mcpSpecFromSettings(nil))
	if err != nil {
		t.Fatal(err)
	}
	plan.Mode = "interactive"
	compiled, err := launcher.Compile(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	prepared, custody, err := launchartifacts.Prepare(context.Background(), compiled, admission)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := custody.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if err := plantCanonicalArtifacts(context.Background(), prepared, &scopedCLIAdapter{inner: provider.NewClaudeAdapter()}, []bootFile{{RelPath: "hadron-reply", Content: "#!/bin/sh\n", Mode: 0755}}, custody.Authorize); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(prepared.PlantedBootDir, "hadron-reply")); err != nil || info.Mode().Perm() != 0755 {
		t.Fatalf("executable callback unavailable: %v", err)
	}
}

func TestCanonicalPlantingSupportedProviderModes(t *testing.T) {
	for _, item := range []struct{ provider, mode string }{{"claude", "subprocess"}, {"claude", "streaming-stdio"}, {"claude", "pty"}, {"opencode", "subprocess"}, {"opencode", "serve-http"}} {
		t.Run(item.provider+"/"+item.mode, func(t *testing.T) {
			l := NewLauncher(t.TempDir(), nil)
			defer func() {
				if closeErr := l.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}()
			cfg := settings.AgentSubstrateSettings{Provider: item.provider, Runtime: item.mode}
			req := execution.AgentLaunchRequest{LogicalAgentID: "worker"}
			admission, release, err := l.acceptArtifactOperation(context.Background(), "session", cfg, req)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			project := t.TempDir()
			plan, err := buildLaunchPlan(cfg, req, project, t.TempDir(), "instructions", "kickoff", mcpSpecFromSettings(nil))
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := launcher.Compile(context.Background(), plan)
			if err != nil {
				t.Fatal(err)
			}
			prepared, custody, err := launchartifacts.Prepare(context.Background(), compiled, admission)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if closeErr := custody.Close(); closeErr != nil {
					t.Error(closeErr)
				}
			}()
			binding, err := runtimebind.Resolve(runtimebind.Request{Provider: item.provider, RequestedRuntime: runtimeMode(item.mode), AllowPTY: true})
			if err != nil {
				t.Fatal(err)
			}
			adapter, _, err := newAdapter(binding, cfg, project)
			if err != nil {
				t.Fatal(err)
			}
			if err := plantCanonicalArtifacts(context.Background(), prepared, adapter, nil, custody.Authorize); err != nil {
				t.Fatal(err)
			}
		})
	}
}
