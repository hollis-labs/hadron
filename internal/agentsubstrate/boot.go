package agentsubstrate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	runtimebootdir "github.com/hollis-labs/go-agent-runtime/bootdir"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

const (
	defaultBootProfile      = "hadron.default"
	sharedCallbacksProfile  = "shared"
	bootProfilesDir         = "boot-profiles"
	callbackProfilesDir     = "callback-profiles"
	callbackDetailsRelPath  = "hadron/callbacks.json"
	callbackGuideRelPath    = "hadron/callbacks.md"
	callbackEnvRelPath      = "hadron/callback.env"
	replyHelperRelPath      = "hadron-reply"
	defaultBootSystemPrompt = "You are a task-scoped automation agent launched by Hadron. Work within the provided project context, boot files, and callback contract."
)

type bootRenderContext struct {
	SessionID        string
	SessionURN       string
	MailboxURN       string
	ProjectDir       string
	BlueprintPath    string
	BootDir          string
	LaunchID         string
	LogicalAgentID   string
	PromptAppend     string
	Metadata         map[string]any
	InjectedFiles    []execution.AgentNativeFile
	ProjectAgentFile string
	ProjectAgentText string
	ProjectSpecPath  string
	ProjectSpecText  string
}

func renderBootArtifacts(dataDir string, cfg settings.AgentSubstrateSettings, ctx bootRenderContext) (string, string, []runtimebootdir.File, error) {
	projectAgentFile, projectAgentText := findProjectAgentInstructions(ctx.ProjectDir)
	projectSpecPath, projectSpecText := findProjectSpec(ctx.ProjectDir)
	ctx.ProjectAgentFile = projectAgentFile
	ctx.ProjectAgentText = projectAgentText
	ctx.ProjectSpecPath = projectSpecPath
	ctx.ProjectSpecText = projectSpecText

	systemPrompt := strings.TrimSpace(defaultBootSystemPrompt)
	bootSections := []string{renderDefaultBootContent(ctx)}

	if extra, err := renderBootProfile(dataDir, ctx.ProjectDir, cfg.Boot.Profile, ctx); err != nil {
		return "", "", nil, err
	} else if strings.TrimSpace(extra) != "" {
		bootSections = append(bootSections, strings.TrimSpace(extra))
	}

	callbackText, callbackFiles, err := renderCallbacksProfile(dataDir, ctx.ProjectDir, cfg.Boot.CallbacksProfile, ctx)
	if err != nil {
		return "", "", nil, err
	}
	if strings.TrimSpace(callbackText) != "" {
		bootSections = append(bootSections, strings.TrimSpace(callbackText))
	}

	return systemPrompt, strings.Join(bootSections, "\n\n"), callbackFiles, nil
}

func renderDefaultBootContent(ctx bootRenderContext) string {
	sections := []string{
		"# Hadron Launch",
		fmt.Sprintf("- launch_id: `%s`", ctx.LaunchID),
		fmt.Sprintf("- logical_agent_id: `%s`", ctx.LogicalAgentID),
		fmt.Sprintf("- mailbox_urn: `%s`", ctx.MailboxURN),
		fmt.Sprintf("- session_urn: `%s`", ctx.SessionURN),
		fmt.Sprintf("- project_dir: `%s`", ctx.ProjectDir),
	}
	if ctx.BlueprintPath != "" {
		sections = append(sections, fmt.Sprintf("- blueprint_path: `%s`", ctx.BlueprintPath))
	}

	if ctx.ProjectSpecText != "" {
		sections = append(sections,
			"",
			"## Project Spec",
			fmt.Sprintf("Source: `%s`", ctx.ProjectSpecPath),
			"```yaml",
			strings.TrimSpace(ctx.ProjectSpecText),
			"```",
		)
	}
	if ctx.ProjectAgentText != "" {
		sections = append(sections,
			"",
			"## Project Instructions",
			fmt.Sprintf("Source: `%s`", ctx.ProjectAgentFile),
			strings.TrimSpace(ctx.ProjectAgentText),
		)
	}
	if len(ctx.InjectedFiles) > 0 {
		sections = append(sections, "", "## Injected Files")
		for _, nf := range ctx.InjectedFiles {
			sections = append(sections, "- `"+nf.RelPath+"`")
		}
	}
	if strings.TrimSpace(ctx.PromptAppend) != "" {
		sections = append(sections, "", "## Task", strings.TrimSpace(ctx.PromptAppend))
	}
	if len(ctx.Metadata) > 0 {
		if b, err := json.MarshalIndent(ctx.Metadata, "", "  "); err == nil {
			sections = append(sections, "", "## Metadata", "```json", string(b), "```")
		}
	}
	return strings.Join(sections, "\n")
}

func renderBootProfile(dataDir, projectDir, profile string, ctx bootRenderContext) (string, error) {
	profile = strings.TrimSpace(profile)
	if profile == "" || profile == defaultBootProfile {
		return "", nil
	}
	content, err := readProfileText(dataDir, projectDir, bootProfilesDir, profile)
	if err != nil {
		return "", fmt.Errorf("load boot profile %q: %w", profile, err)
	}
	return renderProfileTemplate(content, ctx), nil
}

func renderCallbacksProfile(dataDir, projectDir, profile string, ctx bootRenderContext) (string, []runtimebootdir.File, error) {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return "", nil, nil
	}
	if profile == sharedCallbacksProfile {
		replySubstrate := strings.TrimSpace(anyString(ctx.Metadata["reply_substrate"]))
		if replySubstrate == "" {
			replySubstrate = "local_mailbox"
		}
		correlationID := strings.TrimSpace(anyString(ctx.Metadata["correlation_id"]))
		details := map[string]any{
			"mailbox_urn":      ctx.MailboxURN,
			"session_id":       ctx.SessionID,
			"session_urn":      ctx.SessionURN,
			"launch_id":        ctx.LaunchID,
			"logical_agent_id": ctx.LogicalAgentID,
			"project_dir":      ctx.ProjectDir,
			"blueprint_path":   ctx.BlueprintPath,
			"metadata":         ctx.Metadata,
			"reply_substrate":  replySubstrate,
			"correlation_id":   correlationID,
		}
		body, _ := json.MarshalIndent(details, "", "  ")
		files := []runtimebootdir.File{
			{RelPath: callbackDetailsRelPath, Content: string(body) + "\n", Mode: 0o644},
			{RelPath: callbackGuideRelPath, Content: renderSharedCallbacksText(ctx) + "\n", Mode: 0o644},
			{RelPath: callbackEnvRelPath, Content: renderSharedCallbackEnv(ctx, replySubstrate, correlationID) + "\n", Mode: 0o644},
			{RelPath: replyHelperRelPath, Content: renderReplyHelperScript(), Mode: 0o755},
		}
		return renderSharedCallbacksText(ctx), files, nil
	}
	content, err := readProfileText(dataDir, projectDir, callbackProfilesDir, profile)
	if err != nil {
		return "", nil, fmt.Errorf("load callbacks profile %q: %w", profile, err)
	}
	return renderProfileTemplate(content, ctx), nil, nil
}

func renderSharedCallbacksText(ctx bootRenderContext) string {
	lines := []string{
		"## Callback Contract",
		"",
		"Durable reply target:",
		fmt.Sprintf("- mailbox_urn: `%s`", ctx.MailboxURN),
		fmt.Sprintf("- session_urn: `%s`", ctx.SessionURN),
		"",
		"The file `hadron/callbacks.json` contains the launch metadata Hadron generated for this session.",
		"When replying through a go-messaging-compatible substrate, preserve the workflow's correlation value in `thread_id` or `metadata.correlation_id`.",
		"Use the local `hadron-reply` helper to send a reply back to the workflow.",
		"Example: `hadron-reply \"hadron live reply ok\"`",
	}
	return strings.Join(lines, "\n")
}

func renderSharedCallbackEnv(ctx bootRenderContext, replySubstrate, correlationID string) string {
	fromURN := fmt.Sprintf("msg://service/%s/launched-agent", authorityFromMailbox(ctx.MailboxURN))
	return strings.Join([]string{
		fmt.Sprintf("HADRON_REPLY_SUBSTRATE='%s'", shellSingleQuote(replySubstrate)),
		fmt.Sprintf("HADRON_REPLY_TO='%s'", shellSingleQuote(ctx.MailboxURN)),
		fmt.Sprintf("HADRON_REPLY_FROM='%s'", shellSingleQuote(fromURN)),
		fmt.Sprintf("HADRON_REPLY_CORRELATION_ID='%s'", shellSingleQuote(correlationID)),
		": \"${HADRON_DAEMON_ADDR:=http://127.0.0.1:8095}\"",
	}, "\n")
}

func renderReplyHelperScript() string {
	return strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"",
		"if [ \"$#\" -lt 1 ]; then",
		"  echo 'usage: hadron-reply <message>' >&2",
		"  exit 2",
		"fi",
		"",
		"BOOT_DIR=\"$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\"",
		". \"$BOOT_DIR/hadron/callback.env\"",
		"",
		"body=\"$*\"",
		"escaped_body=$(printf '%s' \"$body\" | tr '\n' ' ' | sed 's/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g')",
		"escaped_corr=$(printf '%s' \"$HADRON_REPLY_CORRELATION_ID\" | sed 's/\\\\/\\\\\\\\/g; s/\"/\\\\\"/g')",
		"payload=$(cat <<EOF",
		"{",
		"  \"substrate\": \"$HADRON_REPLY_SUBSTRATE\",",
		"  \"kind\": \"notice\",",
		"  \"from\": \"$HADRON_REPLY_FROM\",",
		"  \"to\": \"$HADRON_REPLY_TO\",",
		"  \"thread_id\": \"$HADRON_REPLY_CORRELATION_ID\",",
		"  \"payload\": {\"text\": \"$escaped_body\"},",
		"  \"metadata\": {\"correlation_id\": \"$escaped_corr\"}",
		"}",
		"EOF",
		")",
		"curl -fsS -X POST \"$HADRON_DAEMON_ADDR/v1/messages\" \\",
		"  -H 'Content-Type: application/json' \\",
		"  -d \"$payload\"",
	}, "\n")
}

func readProfileText(dataDir, projectDir, dirName, profile string) (string, error) {
	candidates := profileCandidates(dataDir, projectDir, dirName, profile)
	for _, candidate := range candidates {
		data, err := os.ReadFile(candidate) // #nosec G304 -- candidate is resolved from Hadron-owned search roots.
		if err == nil {
			return string(data), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
	}
	return "", os.ErrNotExist
}

func profileCandidates(dataDir, projectDir, dirName, profile string) []string {
	if filepath.IsAbs(profile) || strings.Contains(profile, string(filepath.Separator)) || strings.HasSuffix(profile, ".md") || strings.HasSuffix(profile, ".txt") {
		return []string{filepath.Clean(expandHome(profile))}
	}
	return []string{
		filepath.Join(projectDir, ".hadron", dirName, profile+".md"),
		filepath.Join(projectDir, dirName, profile+".md"),
		filepath.Join(dataDir, dirName, profile+".md"),
	}
}

func renderProfileTemplate(content string, ctx bootRenderContext) string {
	repl := map[string]string{
		"{{.SessionID}}":      ctx.SessionID,
		"{{.SessionURN}}":     ctx.SessionURN,
		"{{.MailboxURN}}":     ctx.MailboxURN,
		"{{.LaunchID}}":       ctx.LaunchID,
		"{{.LogicalAgentID}}": ctx.LogicalAgentID,
		"{{.ProjectDir}}":     ctx.ProjectDir,
		"{{.BlueprintPath}}":  ctx.BlueprintPath,
		"{{.BootDir}}":        ctx.BootDir,
	}
	out := content
	for token, value := range repl {
		out = strings.ReplaceAll(out, token, value)
	}
	return strings.TrimSpace(out)
}

func anyString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func authorityFromMailbox(mailboxURN string) string {
	parts := strings.Split(strings.TrimSpace(mailboxURN), "/")
	if len(parts) >= 5 && strings.TrimSpace(parts[3]) != "" {
		return parts[3]
	}
	return "local"
}

func shellSingleQuote(v string) string {
	return strings.ReplaceAll(v, "'", "'\"'\"'")
}

func findProjectAgentInstructions(projectDir string) (string, string) {
	if path := walkUpForFile(projectDir, "AGENTS.md"); path != "" {
		if data, err := os.ReadFile(path); err == nil { // #nosec G304 -- path comes from bounded walk-up inside the workspace tree.
			return path, string(data)
		}
	}
	return "", ""
}

func findProjectSpec(projectDir string) (string, string) {
	if path := walkUpForFile(projectDir, filepath.Join(".agent-ops", "project.yaml")); path != "" {
		if data, err := os.ReadFile(path); err == nil { // #nosec G304 -- path comes from bounded walk-up inside the workspace tree.
			return path, string(data)
		}
	}
	return "", ""
}

func walkUpForFile(startDir, relPath string) string {
	dir := startDir
	for {
		candidate := filepath.Join(dir, relPath)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func expandHome(path string) string {
	if path == "" || path[0] != '~' {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	if path == "~" {
		return home
	}
	return filepath.Join(home, strings.TrimPrefix(path, "~/"))
}
