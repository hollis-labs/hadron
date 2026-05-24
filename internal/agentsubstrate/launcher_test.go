package agentsubstrate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

func TestLaunchAgent_LaunchesSessionAndPlantsNativeFiles(t *testing.T) {
	dataDir := t.TempDir()
	scriptPath := writeLauncherTestScript(t, dataDir)
	blueprintDir := filepath.Join(dataDir, "blueprints")
	if err := os.MkdirAll(blueprintDir, 0o755); err != nil {
		t.Fatalf("mkdir blueprint dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "AGENTS.md"), []byte("Project rules live here.\n"), 0o644); err != nil {
		t.Fatalf("write AGENTS.md: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, ".agent-ops"), 0o755); err != nil {
		t.Fatalf("mkdir .agent-ops: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, ".agent-ops", "project.yaml"), []byte("project:\n  id: hadron-test\n"), 0o644); err != nil {
		t.Fatalf("write project.yaml: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, bootProfilesDir), 0o755); err != nil {
		t.Fatalf("mkdir boot profiles: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, bootProfilesDir, "reviewer.md"), []byte("Custom profile for {{.LogicalAgentID}} in {{.ProjectDir}}.\n"), 0o644); err != nil {
		t.Fatalf("write boot profile: %v", err)
	}
	blueprintPath := filepath.Join(blueprintDir, "agent.yaml")
	if err := os.WriteFile(blueprintPath, []byte("blueprint: {}\n"), 0o644); err != nil {
		t.Fatalf("write blueprint path: %v", err)
	}

	launcher := NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:      kindGoAgentRuntime,
			Provider:  "codex",
			Runtime:   "subprocess",
			Command:   scriptPath,
			Authority: "hadron",
			Boot: settings.AgentBootSettings{
				Profile:          "reviewer",
				CallbacksProfile: sharedCallbacksProfile,
				PlantNativeFiles: true,
			},
			WorkingDirMode: defaultWorkingDirMode,
		},
	})
	defer func() {
		if err := launcher.Close(); err != nil {
			t.Fatalf("close launcher: %v", err)
		}
	}()

	result, err := launcher.LaunchAgent(context.Background(), execution.AgentLaunchRequest{
		Substrate:      "local_runtime",
		LaunchID:       "build-review",
		LogicalAgentID: "reviewer-1",
		PromptAppend:   "Inspect the injected plan.",
		BlueprintPath:  blueprintPath,
		Injection: execution.AgentInjection{
			NativeFiles: []execution.AgentNativeFile{
				{RelPath: "context/plan.txt", Source: "review plan"},
			},
		},
		Metadata: map[string]any{"workflow": "review"},
	})
	if err != nil {
		t.Fatalf("launch agent: %v", err)
	}

	if result.SessionID == "" {
		t.Fatal("expected session id")
	}
	if got, want := result.Mailbox, "msg://agent/hadron/reviewer-1"; got != want {
		t.Fatalf("mailbox = %q, want %q", got, want)
	}
	if got, _ := result.Handles["provider"].(string); got != "codex" {
		t.Fatalf("provider handle = %q, want codex", got)
	}
	if got, _ := result.Handles["runtime"].(string); got != "subprocess" {
		t.Fatalf("runtime handle = %q, want subprocess", got)
	}
	if got, _ := result.Handles["session_urn"].(string); got == "" {
		t.Fatal("expected session_urn handle")
	}
	if got, _ := result.Handles["workdir"].(string); got != blueprintDir {
		t.Fatalf("workdir = %q, want %q", got, blueprintDir)
	}
	bootDir, _ := result.Handles["boot_dir"].(string)
	if bootDir == "" {
		t.Fatal("expected boot_dir handle")
	}
	injectedPath := filepath.Join(bootDir, "context", "plan.txt")
	data, err := os.ReadFile(injectedPath)
	if err != nil {
		t.Fatalf("read injected file: %v", err)
	}
	if string(data) != "review plan" {
		t.Fatalf("injected file = %q, want review plan", string(data))
	}
	callbacksJSON, err := os.ReadFile(filepath.Join(bootDir, callbackDetailsRelPath))
	if err != nil {
		t.Fatalf("read callbacks json: %v", err)
	}
	if !strings.Contains(string(callbacksJSON), `"mailbox_urn": "msg://agent/hadron/reviewer-1"`) {
		t.Fatalf("callbacks json missing mailbox: %s", string(callbacksJSON))
	}
	bootText, err := os.ReadFile(filepath.Join(bootDir, "boot.md"))
	if err != nil {
		t.Fatalf("read boot.md: %v", err)
	}
	if !strings.Contains(string(bootText), "Custom profile for reviewer-1") {
		t.Fatalf("boot.md missing custom profile text: %s", string(bootText))
	}
	if !strings.Contains(string(bootText), "Project rules live here.") {
		t.Fatalf("boot.md missing project instructions: %s", string(bootText))
	}
	if !strings.Contains(string(bootText), "Callback Contract") {
		t.Fatalf("boot.md missing callback contract: %s", string(bootText))
	}
}

func writeLauncherTestScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fake-agent.sh")
	body := "#!/bin/sh\nexit 0\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}
