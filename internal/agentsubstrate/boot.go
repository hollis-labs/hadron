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
	replyOutboxRelDir       = "memories/hadron-replies"
	defaultBootSystemPrompt = "You are a task-scoped automation agent launched by Hadron. Work within the provided project context, boot files, and callback contract."
)

type bootRenderContext struct {
	DataDir          string
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
	ctx.DataDir = dataDir
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
	stateDBPath := filepath.Join(ctx.DataDir, "state", "hadron.db")
	replyOutboxPath := filepath.Join(ctx.BootDir, replyOutboxRelDir)
	return strings.Join([]string{
		fmt.Sprintf("export HADRON_REPLY_SUBSTRATE='%s'", shellSingleQuote(replySubstrate)),
		fmt.Sprintf("export HADRON_REPLY_TO='%s'", shellSingleQuote(ctx.MailboxURN)),
		fmt.Sprintf("export HADRON_REPLY_FROM='%s'", shellSingleQuote(fromURN)),
		fmt.Sprintf("export HADRON_REPLY_CORRELATION_ID='%s'", shellSingleQuote(correlationID)),
		fmt.Sprintf("export HADRON_REPLY_OUTBOX='%s'", shellSingleQuote(replyOutboxPath)),
		fmt.Sprintf("export HADRON_STATE_DB='%s'", shellSingleQuote(stateDBPath)),
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
		"if [ \"$HADRON_REPLY_SUBSTRATE\" = \"local_mailbox\" ] && [ -n \"${HADRON_REPLY_OUTBOX:-}\" ] && command -v python3 >/dev/null 2>&1; then",
		"  python3 - \"$body\" <<'PY'",
		"import json",
		"import os",
		"import pathlib",
		"import sys",
		"import uuid",
		"",
		"body = sys.argv[1]",
		"outbox_dir = pathlib.Path(os.environ['HADRON_REPLY_OUTBOX'])",
		"outbox_dir.mkdir(parents=True, exist_ok=True)",
		"payload = {",
		"    'substrate': os.environ['HADRON_REPLY_SUBSTRATE'],",
		"    'to': os.environ['HADRON_REPLY_TO'],",
		"    'from': os.environ['HADRON_REPLY_FROM'],",
		"    'correlation_id': os.environ.get('HADRON_REPLY_CORRELATION_ID', ''),",
		"    'text': body,",
		"}",
		"tmp_path = outbox_dir / ('.reply-' + uuid.uuid4().hex + '.tmp')",
		"final_path = outbox_dir / ('reply-' + uuid.uuid4().hex + '.json')",
		"tmp_path.write_text(json.dumps(payload, separators=(',', ':')), encoding='utf-8')",
		"tmp_path.replace(final_path)",
		"PY",
		"  exit 0",
		"fi",
		"",
		"if [ \"$HADRON_REPLY_SUBSTRATE\" = \"local_mailbox\" ] && [ -n \"${HADRON_STATE_DB:-}\" ] && [ -f \"$HADRON_STATE_DB\" ] && command -v python3 >/dev/null 2>&1; then",
		"  python3 - \"$body\" <<'PY'",
		"import datetime",
		"import json",
		"import os",
		"import sqlite3",
		"import sys",
		"import uuid",
		"",
		"body = sys.argv[1]",
		"db_path = os.environ['HADRON_STATE_DB']",
		"substrate = os.environ['HADRON_REPLY_SUBSTRATE']",
		"to_urn = os.environ['HADRON_REPLY_TO']",
		"from_urn = os.environ['HADRON_REPLY_FROM']",
		"corr = os.environ.get('HADRON_REPLY_CORRELATION_ID', '')",
		"now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')",
		"message_id = 'msg-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8]",
		"payload_json = json.dumps({'text': body}, separators=(',', ':'))",
		"metadata_json = json.dumps({'correlation_id': corr}, separators=(',', ':'))",
		"conn = sqlite3.connect(db_path)",
		"try:",
		"    conn.execute(",
		"        '''INSERT INTO messages(",
		"            id, substrate, kind, channel, from_urn, to_urn, thread_id, in_reply_to,",
		"            correlation_id, payload_json, content_type, metadata_json, created_at,",
		"            delivered_at, consumed_at, canceled_at",
		"        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)''',",
		"        (message_id, substrate, 'notice', '', from_urn, to_urn, corr, '', corr, payload_json, 'application/json', metadata_json, now),",
		"    )",
		"    conn.commit()",
		"finally:",
		"    conn.close()",
		"PY",
		"  exit 0",
		"fi",
		"",
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

func anyBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(x))
		return trimmed == "1" || trimmed == "true" || trimmed == "yes"
	default:
		return false
	}
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
