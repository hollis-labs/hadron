package agentsubstrate

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	"github.com/hollis-labs/hadron/internal/persistence"
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
	callbackEnv, err := os.ReadFile(filepath.Join(bootDir, callbackEnvRelPath))
	if err != nil {
		t.Fatalf("read callback env: %v", err)
	}
	if !strings.Contains(string(callbackEnv), "HADRON_REPLY_TO='msg://agent/hadron/reviewer-1'") {
		t.Fatalf("callback env missing reply target: %s", string(callbackEnv))
	}
	helperInfo, err := os.Stat(filepath.Join(bootDir, replyHelperRelPath))
	if err != nil {
		t.Fatalf("stat hadron-reply helper: %v", err)
	}
	if helperInfo.Mode()&0o111 == 0 {
		t.Fatalf("hadron-reply helper is not executable: mode=%v", helperInfo.Mode())
	}
}

func TestLaunchAgent_PlantedReplyHelperWritesOutbox(t *testing.T) {
	dataDir := t.TempDir()
	scriptPath := writeLauncherTestScript(t, dataDir)
	blueprintDir := filepath.Join(dataDir, "blueprints")
	if mkErr := os.MkdirAll(blueprintDir, 0o755); mkErr != nil {
		t.Fatalf("mkdir blueprint dir: %v", mkErr)
	}
	blueprintPath := filepath.Join(blueprintDir, "agent.yaml")
	if writeErr := os.WriteFile(blueprintPath, []byte("blueprint: {}\n"), 0o644); writeErr != nil {
		t.Fatalf("write blueprint path: %v", writeErr)
	}

	launcher := NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:      kindGoAgentRuntime,
			Provider:  "codex",
			Runtime:   "subprocess",
			Command:   scriptPath,
			Authority: "hadron",
			Boot: settings.AgentBootSettings{
				CallbacksProfile: sharedCallbacksProfile,
				PlantNativeFiles: true,
			},
			WorkingDirMode: defaultWorkingDirMode,
		},
	})
	defer func() {
		if closeErr := launcher.Close(); closeErr != nil {
			t.Fatalf("close launcher: %v", closeErr)
		}
	}()

	result, err := launcher.LaunchAgent(context.Background(), execution.AgentLaunchRequest{
		Substrate:      "local_runtime",
		LaunchID:       "live-reply-proof",
		LogicalAgentID: "reviewer-2",
		PromptAppend:   "Reply using the helper.",
		BlueprintPath:  blueprintPath,
		Metadata: map[string]any{
			"correlation_id":  "review-helper-123",
			"reply_substrate": "local_mailbox",
		},
	})
	if err != nil {
		t.Fatalf("launch agent: %v", err)
	}

	bootDir, _ := result.Handles["boot_dir"].(string)
	if bootDir == "" {
		t.Fatal("expected boot_dir handle")
	}
	cmd := exec.Command(filepath.Join(bootDir, replyHelperRelPath), "hadron live reply ok")
	cmd.Dir = bootDir
	if out, cmdErr := cmd.CombinedOutput(); cmdErr != nil {
		t.Fatalf("run hadron-reply helper: %v\n%s", cmdErr, string(out))
	}

	entries, err := os.ReadDir(filepath.Join(bootDir, replyOutboxRelDir))
	if err != nil {
		t.Fatalf("read reply outbox: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one helper outbox file, got %d", len(entries))
	}
	body, err := os.ReadFile(filepath.Join(bootDir, replyOutboxRelDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read helper outbox file: %v", err)
	}
	if !strings.Contains(string(body), "hadron live reply ok") || !strings.Contains(string(body), "review-helper-123") {
		t.Fatalf("unexpected helper outbox payload: %s", string(body))
	}
}

func TestLaunchAgent_CodexJSONRPCKickoffRelaysExplicitReplyOutbox(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("jsonrpc fixture uses /bin/sh")
	}

	store, err := persistence.Open(filepath.Join(t.TempDir(), "hadron.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	messageService := messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"local_mailbox": {Kind: "go_messaging", Authority: "hadron"},
	})

	dataDir := t.TempDir()
	scriptPath := writeLauncherJSONRPCReplyScript(t, dataDir)
	blueprintDir := filepath.Join(dataDir, "blueprints")
	if mkErr := os.MkdirAll(blueprintDir, 0o755); mkErr != nil {
		t.Fatalf("mkdir blueprint dir: %v", mkErr)
	}
	blueprintPath := filepath.Join(blueprintDir, "agent.yaml")
	if writeErr := os.WriteFile(blueprintPath, []byte("blueprint: {}\n"), 0o644); writeErr != nil {
		t.Fatalf("write blueprint path: %v", writeErr)
	}

	launcher := NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:      kindGoAgentRuntime,
			Provider:  "codex",
			Runtime:   "jsonrpc-stdio",
			Command:   scriptPath,
			Authority: "hadron",
			Boot: settings.AgentBootSettings{
				CallbacksProfile: sharedCallbacksProfile,
				PlantNativeFiles: true,
			},
			WorkingDirMode: defaultWorkingDirMode,
		},
	})
	launcher.SetReplyMessenger(messageService)
	defer func() {
		if closeErr := launcher.Close(); closeErr != nil {
			t.Fatalf("close launcher: %v", closeErr)
		}
	}()

	result, err := launcher.LaunchAgent(context.Background(), execution.AgentLaunchRequest{
		Substrate:      "local_runtime",
		LaunchID:       "jsonrpc-explicit-reply-proof",
		LogicalAgentID: "reviewer-explicit",
		PromptAppend:   "Reply using the helper.",
		BlueprintPath:  blueprintPath,
		Metadata: map[string]any{
			"correlation_id":  "jsonrpc-explicit-reply-123",
			"reply_substrate": "local_mailbox",
		},
	})
	if err != nil {
		t.Fatalf("launch agent: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		thread, err := messageService.Thread(context.Background(), "local_mailbox", "jsonrpc-explicit-reply-123", 10)
		if err == nil && len(thread) > 0 {
			payload, err := json.Marshal(thread[0].Payload)
			if err != nil {
				t.Fatalf("marshal explicit reply payload: %v", err)
			}
			if !strings.Contains(string(payload), "jsonrpc explicit reply ok") {
				t.Fatalf("unexpected explicit reply payload: %s", string(payload))
			}
			if thread[0].To.URN() != result.Mailbox {
				t.Fatalf("reply target = %q, want %q", thread[0].To.URN(), result.Mailbox)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for explicit outbox reply")
}

func TestLaunchAgent_CodexJSONRPCKickoffPostsFallbackReply(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("jsonrpc fixture uses /bin/sh")
	}

	store, err := persistence.Open(filepath.Join(t.TempDir(), "hadron.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	messageService := messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"local_mailbox": {Kind: "go_messaging", Authority: "hadron"},
	})

	dataDir := t.TempDir()
	scriptPath := writeLauncherJSONRPCTestScript(t, dataDir)
	blueprintDir := filepath.Join(dataDir, "blueprints")
	if mkErr := os.MkdirAll(blueprintDir, 0o755); mkErr != nil {
		t.Fatalf("mkdir blueprint dir: %v", mkErr)
	}
	blueprintPath := filepath.Join(blueprintDir, "agent.yaml")
	if writeErr := os.WriteFile(blueprintPath, []byte("blueprint: {}\n"), 0o644); writeErr != nil {
		t.Fatalf("write blueprint path: %v", writeErr)
	}

	launcher := NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:      kindGoAgentRuntime,
			Provider:  "codex",
			Runtime:   "jsonrpc-stdio",
			Command:   scriptPath,
			Authority: "hadron",
			Boot: settings.AgentBootSettings{
				CallbacksProfile: sharedCallbacksProfile,
				PlantNativeFiles: true,
			},
			WorkingDirMode: defaultWorkingDirMode,
		},
	})
	launcher.SetReplyMessenger(messageService)
	defer func() {
		if closeErr := launcher.Close(); closeErr != nil {
			t.Fatalf("close launcher: %v", closeErr)
		}
	}()

	result, err := launcher.LaunchAgent(context.Background(), execution.AgentLaunchRequest{
		Substrate:      "local_runtime",
		LaunchID:       "jsonrpc-reply-proof",
		LogicalAgentID: "reviewer-jsonrpc",
		PromptAppend:   "Reply on the mailbox.",
		BlueprintPath:  blueprintPath,
		Metadata: map[string]any{
			"correlation_id":  "jsonrpc-reply-123",
			"reply_substrate": "local_mailbox",
		},
	})
	if err != nil {
		t.Fatalf("launch agent: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		thread, err := messageService.Thread(context.Background(), "local_mailbox", "jsonrpc-reply-123", 10)
		if err == nil && len(thread) > 0 {
			payload, err := json.Marshal(thread[0].Payload)
			if err != nil {
				t.Fatalf("marshal helper payload: %v", err)
			}
			if !strings.Contains(string(payload), `"status":"assistant_output"`) &&
				!strings.Contains(string(payload), `"status":"agent_completed_no_output"`) {
				t.Fatalf("unexpected fallback payload: %s", string(payload))
			}
			if thread[0].To.URN() != result.Mailbox {
				t.Fatalf("reply target = %q, want %q", thread[0].To.URN(), result.Mailbox)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out waiting for jsonrpc kickoff fallback reply")
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

func writeLauncherJSONRPCTestScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fake-jsonrpc.sh")
	body := `#!/bin/sh
printf '%s\n' '{"jsonrpc":"2.0","method":"server.ready","params":{"port":0}}'
while IFS= read -r line; do
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    if [ -n "$id" ]; then
        printf '%s\n' '{"jsonrpc":"2.0","method":"item.delta","params":{"text":"hi"}}'
        case "$method" in
        initialize)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"capabilities":{}}}\n' "$id"
            ;;
        thread/start)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-test-001"}}}\n' "$id"
            ;;
        turn/start)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"accepted":true}}\n' "$id"
            ;;
        *)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"echoed":true,"method":"%s"}}\n' "$id" "$method"
            ;;
        esac
    fi
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write jsonrpc script: %v", err)
	}
	return path
}

func writeLauncherJSONRPCReplyScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fake-jsonrpc-reply.sh")
	body := `#!/bin/sh
printf '%s\n' '{"jsonrpc":"2.0","method":"server.ready","params":{"port":0}}'
while IFS= read -r line; do
    id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    method=$(printf '%s' "$line" | sed -n 's/.*"method":"\([^"]*\)".*/\1/p')
    if [ -n "$id" ]; then
        case "$method" in
        initialize)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"capabilities":{}}}\n' "$id"
            ;;
        thread/start)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-test-002"}}}\n' "$id"
            ;;
        turn/start)
            ./hadron-reply "jsonrpc explicit reply ok" >/dev/null 2>&1 || true
            printf '{"jsonrpc":"2.0","id":%s,"result":{"accepted":true}}\n' "$id"
            ;;
        *)
            printf '{"jsonrpc":"2.0","id":%s,"result":{"echoed":true,"method":"%s"}}\n' "$id" "$method"
            ;;
        esac
    fi
done
exit 0
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write jsonrpc reply script: %v", err)
	}
	return path
}
