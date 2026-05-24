package agentsubstrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	agentlaunch "github.com/hollis-labs/go-agent-launch/agentlaunch"
	runtimebootdir "github.com/hollis-labs/go-agent-runtime/bootdir"
	"github.com/hollis-labs/go-agent-runtime/runtimebind"
	"github.com/hollis-labs/go-agent-runtime/runtimekind"
	agentsessions "github.com/hollis-labs/go-agent-sessions/agentsessions"
	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-providers/provider"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

const (
	kindGoAgentRuntime = "go_agent_runtime"

	defaultAuthority       = "local"
	defaultWorkingDirMode  = "blueprint_dir"
	workingDirModeStepDir  = "step_dir"
	workingDirModeCWD      = "cwd"
	workingDirModeProcess  = "process_cwd"
	sessionShutdownTimeout = 5 * time.Second
)

type Launcher struct {
	dataDir    string
	substrates map[string]settings.AgentSubstrateSettings
	sessions   *agentsessions.Manager
	seq        atomic.Uint64
}

func NewLauncher(dataDir string, substrates map[string]settings.AgentSubstrateSettings) *Launcher {
	cloned := make(map[string]settings.AgentSubstrateSettings, len(substrates))
	for name, cfg := range substrates {
		cloned[name] = cfg
	}
	return &Launcher{
		dataDir:    dataDir,
		substrates: cloned,
		sessions:   agentsessions.NewManager(nil),
	}
}

func (l *Launcher) Close() error {
	if l == nil || l.sessions == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionShutdownTimeout)
	defer cancel()
	for _, info := range l.sessions.List() {
		_ = l.sessions.Stop(ctx, info.ID)
	}
	return l.sessions.Shutdown(ctx)
}

func (l *Launcher) LaunchAgent(ctx context.Context, req execution.AgentLaunchRequest) (execution.AgentLaunchResult, error) {
	cfg, ok := l.substrates[req.Substrate]
	if !ok {
		return execution.AgentLaunchResult{}, fmt.Errorf("agent substrate %q is not configured", req.Substrate)
	}
	if cfg.Kind != "" && cfg.Kind != kindGoAgentRuntime {
		return execution.AgentLaunchResult{}, fmt.Errorf("agent substrate %q kind %q is not supported", req.Substrate, cfg.Kind)
	}

	workdir, err := resolveWorkdir(cfg, req)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}
	projectDir, err := filepath.Abs(workdir)
	if err != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("resolve workdir %q: %w", workdir, err)
	}

	binding, err := runtimebind.Resolve(runtimebind.Request{
		Provider:               cfg.Provider,
		RequestedRuntime:       agentlaunch.RuntimeKind(cfg.Runtime),
		AllowPTY:               true,
		AllowGenericSubprocess: cfg.AllowGenericSubprocess,
	})
	if err != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("resolve provider/runtime: %w", err)
	}

	adapter, caps, err := newAdapter(binding, cfg, projectDir)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}
	runtime, err := agentsessions.NewFromAdapter(agentsessions.AdapterRuntimeConfig{
		ID:      req.Substrate,
		Kind:    "cli",
		Adapter: adapter,
		Caps:    caps,
	})
	if err != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("build runtime: %w", err)
	}
	if prepErr := runtime.Prepare(ctx); prepErr != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("prepare runtime: %w", prepErr)
	}

	sessionID := l.nextSessionID(req)
	workspaceDir := filepath.Join(l.dataDir, "agents", "sessions", sessionID)
	bootDir := filepath.Join(workspaceDir, "boot")
	if mkdirErr := os.MkdirAll(filepath.Join(workspaceDir, "logs"), 0o750); mkdirErr != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("ensure workspace dir: %w", mkdirErr)
	}
	if mkdirErr := os.MkdirAll(bootDir, 0o750); mkdirErr != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("ensure boot dir: %w", mkdirErr)
	}

	mailbox := mailboxURN(cfg.Authority, req.LogicalAgentID)
	startOpts, err := buildStartOptions(l.dataDir, workspaceDir, bootDir, projectDir, cfg, req, sessionID, mailbox, adapter)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}

	if err := l.sessions.Start(ctx, agentsessions.StartRequest{
		ID:      sessionID,
		Runtime: runtime,
		Options: startOpts,
		SessionMeta: map[string]string{
			"substrate":        req.Substrate,
			"launch_id":        req.LaunchID,
			"logical_agent_id": req.LogicalAgentID,
			"provider":         binding.Provider,
			"runtime":          string(binding.Runtime),
		},
	}); err != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("start session: %w", err)
	}

	result := execution.AgentLaunchResult{
		SessionID: sessionID,
		Mailbox:   mailbox,
		Handles: map[string]any{
			"logical_agent_id": req.LogicalAgentID,
			"provider":         binding.Provider,
			"runtime":          string(binding.Runtime),
			"session_urn":      sessionURN(cfg.Authority, sessionID),
			"workdir":          projectDir,
			"workspace_dir":    workspaceDir,
			"boot_dir":         bootDir,
		},
	}
	return result, nil
}

func buildStartOptions(dataDir, workspaceDir, bootDir, projectDir string, cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest, sessionID, mailbox string, adapter provider.CLIAdapter) (agentsessions.StartOptions, error) {
	bootPrompt, bootContent, extraFiles, err := renderBootArtifacts(dataDir, cfg, bootRenderContext{
		SessionID:      sessionID,
		SessionURN:     sessionURN(cfg.Authority, sessionID),
		MailboxURN:     mailbox,
		ProjectDir:     projectDir,
		BlueprintPath:  req.BlueprintPath,
		BootDir:        bootDir,
		LaunchID:       req.LaunchID,
		LogicalAgentID: req.LogicalAgentID,
		PromptAppend:   req.PromptAppend,
		Metadata:       req.Metadata,
		InjectedFiles:  req.Injection.NativeFiles,
	})
	if err != nil {
		return agentsessions.StartOptions{}, err
	}

	opts := agentsessions.StartOptions{
		Workdir:      bootDir,
		WorkspaceDir: workspaceDir,
		LogPath:      filepath.Join(workspaceDir, "logs", "session.log"),
		Env:          mergeEnv(cfg.Env, nil),
	}

	if bp, ok := adapter.(provider.BootDirProvider); ok {
		spec := bp.BootDirSpec()
		plantCtx := provider.PlantContext{
			SystemPrompt: bootPrompt,
			BootContent:  bootContent,
			AgentName:    req.LogicalAgentID,
			ProjectDir:   projectDir,
			BootDir:      bootDir,
		}
		if err := materializeBootDir(bootDir, spec, plantCtx, req, cfg); err != nil {
			return agentsessions.StartOptions{}, err
		}
		if len(extraFiles) > 0 {
			if _, err := (runtimebootdir.Writer{}).WriteFiles(bootDir, extraFiles); err != nil {
				return agentsessions.StartOptions{}, err
			}
		}
		opts.Workdir = spec.SpawnWorkdir(bootDir, projectDir)
		opts.Env = mergeEnv(cfg.Env, renderEnv(spec.EnvAmendments, bootDir, projectDir))
		opts.ExtraArgs = append(opts.ExtraArgs, renderProjectDirArgs(spec.ProjectDirArg, bootDir, projectDir)...)
	}

	return opts, nil
}

func materializeBootDir(bootDir string, spec provider.BootDirSpec, plantCtx provider.PlantContext, req execution.AgentLaunchRequest, cfg settings.AgentSubstrateSettings) error {
	for _, pf := range spec.PlantedFiles {
		if pf.Render == nil {
			continue
		}
		content, err := pf.Render(plantCtx)
		if err != nil {
			return fmt.Errorf("render provider boot file %q: %w", pf.RelPath, err)
		}
		mode := pf.Mode
		if mode == 0 {
			mode = 0o644
		}
		if _, err := (runtimebootdir.Writer{}).WriteFiles(bootDir, []runtimebootdir.File{{
			RelPath: pf.RelPath,
			Content: content,
			Mode:    mode,
		}}); err != nil {
			return err
		}
	}
	if !cfg.Boot.PlantNativeFiles || len(req.Injection.NativeFiles) == 0 {
		return nil
	}
	files := make([]runtimebootdir.File, 0, len(req.Injection.NativeFiles))
	for _, nf := range req.Injection.NativeFiles {
		files = append(files, runtimebootdir.File{
			RelPath: nf.RelPath,
			Content: nf.Source,
			Mode:    0o644,
		})
	}
	if _, err := (runtimebootdir.Writer{}).WriteFiles(bootDir, files); err != nil {
		return err
	}
	return nil
}

func resolveWorkdir(cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest) (string, error) {
	if req.StepDir != "" {
		return req.StepDir, nil
	}
	switch strings.TrimSpace(cfg.WorkingDirMode) {
	case "", defaultWorkingDirMode:
		if req.BlueprintPath == "" {
			return "", errors.New("agent launch requires blueprint path to resolve working directory")
		}
		return filepath.Dir(req.BlueprintPath), nil
	case workingDirModeStepDir:
		if req.StepDir == "" {
			return "", errors.New("agent substrate working_dir_mode=step_dir requires step.dir on the launch step")
		}
		return req.StepDir, nil
	case workingDirModeCWD, workingDirModeProcess:
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve process cwd: %w", err)
		}
		return cwd, nil
	default:
		return "", fmt.Errorf("unsupported agent substrate working_dir_mode %q", cfg.WorkingDirMode)
	}
}

func newAdapter(binding runtimebind.Binding, cfg settings.AgentSubstrateSettings, projectDir string) (provider.CLIAdapter, agentsessions.Capabilities, error) {
	var (
		adapter provider.CLIAdapter
		caps    agentsessions.Capabilities
	)

	switch {
	case binding.Provider == "claude" && binding.Runtime == agentlaunch.RuntimeSubprocess:
		a := provider.NewClaudeAdapter()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "claude" && binding.Runtime == agentlaunch.RuntimeStreamingStdio:
		a := provider.NewClaudeAdapterStreamingStdio()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{StreamingStdio: true, ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "claude" && (binding.Runtime == agentlaunch.RuntimePTY || binding.Runtime == runtimekind.PTYDebug):
		a := provider.NewClaudeAdapterPTY()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{PTY: true, Resize: true, ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "codex" && binding.Runtime == agentlaunch.RuntimeSubprocess:
		a := provider.NewCodexAdapter()
		a.WritableRoots = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{BinaryRequired: true}
	case binding.Provider == "codex" && binding.Runtime == agentlaunch.RuntimeJsonRpcStdio:
		a := provider.NewCodexAdapterAppServer()
		a.WritableRoots = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{JsonRpcStdio: true, BinaryRequired: true}
	case binding.Provider == "opencode" && binding.Runtime == agentlaunch.RuntimeSubprocess:
		a := provider.NewOpencodeAdapter()
		a.Dir = projectDir
		adapter = a
		caps = agentsessions.Capabilities{BinaryRequired: true}
	case binding.Provider == "opencode" && binding.Runtime == agentlaunch.RuntimeServeHTTP:
		a := provider.NewOpencodeAdapterServeHTTP()
		a.Dir = projectDir
		adapter = a
		caps = agentsessions.Capabilities{ServeHTTP: true, BinaryRequired: true}
	case cfg.AllowGenericSubprocess && binding.Runtime == agentlaunch.RuntimeSubprocess:
		adapter = &genericCLIAdapter{name: normalizedProviderName(cfg.Provider)}
		caps = agentsessions.Capabilities{BinaryRequired: true}
	default:
		return nil, agentsessions.Capabilities{}, fmt.Errorf("unsupported provider/runtime binding %q/%q", binding.Provider, binding.Runtime)
	}

	return &scopedCLIAdapter{
		inner:    adapter,
		binary:   cfg.Command,
		baseArgs: append([]string(nil), cfg.Args...),
	}, caps, nil
}

type scopedCLIAdapter struct {
	inner    provider.CLIAdapter
	binary   string
	baseArgs []string
}

func (a *scopedCLIAdapter) Name() string { return a.inner.Name() }

func (a *scopedCLIAdapter) BuildArgs(prompt, systemPrompt, cliSessionID string) []string {
	out := append([]string(nil), a.baseArgs...)
	return append(out, a.inner.BuildArgs(prompt, systemPrompt, cliSessionID)...)
}

func (a *scopedCLIAdapter) ParseLine(line []byte) ([]llmtypes.StreamEvent, error) {
	return a.inner.ParseLine(line)
}

func (a *scopedCLIAdapter) Detect() (string, bool) {
	if a.binary != "" {
		return a.binary, true
	}
	return a.inner.Detect()
}

func (a *scopedCLIAdapter) BootDirSpec() provider.BootDirSpec {
	if bp, ok := a.inner.(provider.BootDirProvider); ok {
		return bp.BootDirSpec()
	}
	return provider.BootDirSpec{}
}

type genericCLIAdapter struct {
	name string
}

func (a *genericCLIAdapter) Name() string { return a.name }

func (a *genericCLIAdapter) BuildArgs(prompt, _ string, _ string) []string {
	if prompt == "" {
		return nil
	}
	return []string{prompt}
}

func (a *genericCLIAdapter) ParseLine(_ []byte) ([]llmtypes.StreamEvent, error) {
	return nil, nil
}

func (a *genericCLIAdapter) Detect() (string, bool) {
	return "", false
}

func normalizedProviderName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "generic"
	}
	return name
}

func mergeEnv(extra map[string]string, appended []string) []string {
	env := append([]string(nil), os.Environ()...)
	for k, v := range extra {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	env = append(env, appended...)
	return env
}

func renderEnv(in []string, bootDir, projectDir string) []string {
	out := make([]string, 0, len(in))
	for _, entry := range in {
		out = append(out, renderPathTemplate(entry, bootDir, projectDir))
	}
	return out
}

func renderPathTemplate(in, bootDir, projectDir string) string {
	out := strings.ReplaceAll(in, "{{.BootDir}}", bootDir)
	out = strings.ReplaceAll(out, "{{.ProjectDir}}", projectDir)
	return out
}

func renderProjectDirArgs(pattern, bootDir, projectDir string) []string {
	rendered := strings.TrimSpace(renderPathTemplate(pattern, bootDir, projectDir))
	if rendered == "" {
		return nil
	}
	parts := strings.SplitN(rendered, " ", 2)
	if len(parts) == 1 {
		return parts
	}
	if strings.TrimSpace(parts[1]) == "" {
		return []string{parts[0]}
	}
	return []string{parts[0], strings.TrimSpace(parts[1])}
}

func mailboxURN(authority, logicalAgentID string) string {
	return fmt.Sprintf("msg://agent/%s/%s", authorityOrDefault(authority), logicalAgentID)
}

func sessionURN(authority, sessionID string) string {
	return fmt.Sprintf("msg://session/%s/%s", authorityOrDefault(authority), sessionID)
}

func authorityOrDefault(authority string) string {
	if strings.TrimSpace(authority) == "" {
		return defaultAuthority
	}
	return authority
}

func (l *Launcher) nextSessionID(req execution.AgentLaunchRequest) string {
	n := l.seq.Add(1)
	base := sanitizeIDPart(req.LaunchID)
	if base == "" {
		base = "agent"
	}
	return fmt.Sprintf("sess-%s-%s-%04d", base, time.Now().UTC().Format("20060102-150405"), n)
}

func sanitizeIDPart(in string) string {
	in = strings.ToLower(strings.TrimSpace(in))
	if in == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range in {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
