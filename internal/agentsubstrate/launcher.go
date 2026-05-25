package agentsubstrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	agentlaunch "github.com/hollis-labs/go-agent-launch/agentlaunch"
	runtimebootdir "github.com/hollis-labs/go-agent-runtime/bootdir"
	"github.com/hollis-labs/go-agent-runtime/runtimebind"
	"github.com/hollis-labs/go-agent-runtime/runtimekind"
	"github.com/hollis-labs/go-agent-runtime/turn"
	agentsessions "github.com/hollis-labs/go-agent-sessions/agentsessions"
	llmtypes "github.com/hollis-labs/go-llm-types"
	"github.com/hollis-labs/go-messaging"
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
	replyOutboxWatchWindow = 2 * time.Minute
	hadronClientName       = "hadron"
	hadronClientVersion    = "0.1-dev"
)

type Launcher struct {
	dataDir    string
	substrates map[string]settings.AgentSubstrateSettings
	sessions   *agentsessions.Manager
	codexTurns turn.CodexAppServerCache
	replies    replyMessenger
	seq        atomic.Uint64
}

type replyMessenger interface {
	Send(ctx context.Context, substrate string, env messaging.Envelope) (messaging.Envelope, error)
	List(ctx context.Context, substrate, toURN, correlationID string, limit int) ([]messaging.Envelope, error)
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

func (l *Launcher) SetReplyMessenger(m replyMessenger) {
	l.replies = m
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
	if mkdirErr := os.MkdirAll(filepath.Join(bootDir, replyOutboxRelDir), 0o750); mkdirErr != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("ensure reply outbox dir: %w", mkdirErr)
	}

	mailbox := mailboxURN(cfg.Authority, req.LogicalAgentID)
	startOpts, err := buildStartOptions(l.dataDir, workspaceDir, bootDir, projectDir, cfg, req, sessionID, mailbox, adapter)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}
	kickoffPayload := launchKickoffPayload(adapter)
	eventCh := make(chan llmtypes.StreamEvent, 128)
	startOpts.EventFanout = eventCh

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
	if len(kickoffPayload) > 0 {
		kickoffCtx := context.WithoutCancel(ctx)
		//nolint:gosec // kickoff turn is intentionally async and detached from request cancellation
		go l.runKickoffTurn(kickoffCtx, sessionID, mailbox, req, binding.Provider, eventCh, kickoffPayload)
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
	opts.Env = prependEnvPath(opts.Env, bootDir)

	return opts, nil
}

func launchKickoffPayload(adapter provider.CLIAdapter) []byte {
	if _, ok := adapter.(provider.BootDirProvider); ok {
		return []byte("Boot @./boot.md")
	}
	return nil
}

func (l *Launcher) runKickoffTurn(ctx context.Context, sessionID, mailbox string, req execution.AgentLaunchRequest, providerName string, eventCh <-chan llmtypes.StreamEvent, payload []byte) {
	replySubstrate := strings.TrimSpace(anyString(req.Metadata["reply_substrate"]))
	correlationID := strings.TrimSpace(anyString(req.Metadata["correlation_id"]))
	disableFallbackReply := anyBool(req.Metadata["disable_fallback_reply"])
	outboxDir := filepath.Join(l.dataDir, "agents", "sessions", sessionID, "boot", replyOutboxRelDir)

	var output strings.Builder
	stopDrain := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-stopDrain:
				return
			case ev := <-eventCh:
				if ev.Type == llmtypes.EventDelta && ev.Content != "" {
					output.WriteString(ev.Content)
				}
			}
		}
	}()
	if replySubstrate != "" && correlationID != "" && l.replies != nil {
		watchCtx, watchCancel := context.WithTimeout(context.Background(), replyOutboxWatchWindow)
		go func() {
			defer watchCancel()
			l.watchReplyOutbox(watchCtx, outboxDir)
		}()
	}

	sendErr := l.sendTurn(ctx, sessionID, providerName, string(payload))
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer finalCancel()
finalReplyDrain:
	for {
		_, err := l.deliverReplyOutbox(finalCtx, outboxDir)
		if err == nil {
			break
		}
		log.Printf("agentsubstrate: deliver reply outbox retry: session=%s dir=%s err=%v", sessionID, outboxDir, err)
		select {
		case <-finalCtx.Done():
			break finalReplyDrain
		case <-time.After(100 * time.Millisecond):
		}
	}
	close(stopDrain)
	<-drained

	if replySubstrate == "" || correlationID == "" || l.replies == nil {
		return
	}
	if disableFallbackReply {
		return
	}
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sendCancel()
	if existing, err := l.replies.List(sendCtx, replySubstrate, mailbox, correlationID, 1); err == nil && len(existing) > 0 {
		return
	}

	replyPayload := map[string]any{
		"session_id": sessionID,
		"source":     "hadron-launcher",
	}
	text := strings.TrimSpace(output.String())
	switch {
	case text != "":
		replyPayload["text"] = text
		replyPayload["status"] = "assistant_output"
	case sendErr != nil:
		replyPayload["status"] = "agent_failed"
		replyPayload["error"] = sendErr.Error()
	default:
		replyPayload["status"] = "agent_completed_no_output"
		replyPayload["error"] = "agent session completed without sending an explicit reply"
	}
	body, err := json.Marshal(replyPayload)
	if err != nil {
		return
	}
	to, err := messaging.ParseURN(mailbox)
	if err != nil {
		return
	}
	_, _ = l.replies.Send(sendCtx, replySubstrate, messaging.Envelope{
		Kind:        messaging.MsgKindNotice,
		From:        messaging.Address{Kind: messaging.KindService, Authority: authorityFromMailbox(mailbox), ID: "launcher"},
		To:          to,
		ThreadID:    correlationID,
		Payload:     body,
		Metadata:    map[string]string{"correlation_id": correlationID},
		ContentType: "application/json",
	})
}

func (l *Launcher) watchReplyOutbox(ctx context.Context, outboxDir string) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		delivered, err := l.deliverReplyOutbox(ctx, outboxDir)
		if err != nil {
			log.Printf("agentsubstrate: deliver reply outbox: dir=%s err=%v", outboxDir, err)
		}
		if delivered > 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *Launcher) deliverReplyOutbox(ctx context.Context, outboxDir string) (int, error) {
	if l == nil || l.replies == nil || strings.TrimSpace(outboxDir) == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(outboxDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	delivered := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(outboxDir, entry.Name())
		body, err := os.ReadFile(path) // #nosec G304 -- path is scoped to the Hadron-managed outbox directory.
		if err != nil {
			continue
		}
		var payload struct {
			Substrate     string `json:"substrate"`
			To            string `json:"to"`
			From          string `json:"from"`
			CorrelationID string `json:"correlation_id"`
			Text          string `json:"text"`
		}
		if unmarshalErr := json.Unmarshal(body, &payload); unmarshalErr != nil {
			_ = os.Remove(path)
			continue
		}
		to, err := messaging.ParseURN(payload.To)
		if err != nil {
			_ = os.Remove(path)
			continue
		}
		fromAuthority := authorityFromMailbox(payload.To)
		fromID := "launched-agent"
		if from := strings.TrimSpace(payload.From); from != "" {
			parts := strings.Split(from, "/")
			if len(parts) >= 5 {
				if strings.TrimSpace(parts[3]) != "" {
					fromAuthority = strings.TrimSpace(parts[3])
				}
				if strings.TrimSpace(parts[4]) != "" {
					fromID = strings.TrimSpace(parts[4])
				}
			}
		}
		env := messaging.Envelope{
			Kind:        messaging.MsgKindNotice,
			From:        messaging.Address{Kind: messaging.KindService, Authority: fromAuthority, ID: fromID},
			To:          to,
			ThreadID:    payload.CorrelationID,
			Payload:     json.RawMessage(fmt.Sprintf(`{"text":%q}`, payload.Text)),
			Metadata:    map[string]string{"correlation_id": payload.CorrelationID},
			ContentType: "application/json",
		}
		if _, err := l.replies.Send(ctx, payload.Substrate, env); err != nil {
			return delivered, err
		}
		_ = os.Remove(path)
		delivered++
	}
	return delivered, nil
}

func (l *Launcher) sendTurn(ctx context.Context, sessionID, providerName, text string) error {
	info, ok := l.sessions.Get(sessionID)
	if !ok {
		return agentsessions.ErrSessionNotRunning
	}
	switch {
	case info.Caps.JsonRpcStdio:
		if normalizedProviderName(providerName) == "codex" {
			return l.codexTurns.SendTurn(ctx, sessionID, launcherJSONRPCSender{
				manager:   l.sessions,
				sessionID: sessionID,
			}, text, turn.CodexAppServerOptions{
				ClientName:    hadronClientName,
				ClientVersion: hadronClientVersion,
				CWD:           info.Workdir,
			})
		}
		return turn.SendTurn(ctx, launcherInputSender{
			manager:   l.sessions,
			sessionID: sessionID,
		}, text, turn.Options{
			Provider: providerName,
			Runtime:  runtimekind.JSONRPCStdio,
		})
	case info.Caps.StreamingStdio:
		framed, err := turn.Frame(text, turn.Options{
			Provider: providerName,
			Runtime:  runtimekind.StreamingStdio,
		})
		if err != nil {
			return fmt.Errorf("frame streaming-stdio turn: %w", err)
		}
		return l.sessions.SendInput(sessionID, framed)
	default:
		return l.sessions.SendInput(sessionID, []byte(text))
	}
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

type launcherInputSender struct {
	manager   *agentsessions.Manager
	sessionID string
}

func (s launcherInputSender) SendInput(_ context.Context, data []byte) error {
	return s.manager.SendInput(s.sessionID, data)
}

type launcherJSONRPCSender struct {
	manager   *agentsessions.Manager
	sessionID string
}

func (s launcherJSONRPCSender) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	return s.manager.JsonRpcCall(ctx, s.sessionID, method, params)
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

func prependEnvPath(env []string, dir string) []string {
	if strings.TrimSpace(dir) == "" {
		return env
	}
	prefix := dir
	for i, entry := range env {
		if !strings.HasPrefix(entry, "PATH=") {
			continue
		}
		current := strings.TrimPrefix(entry, "PATH=")
		if strings.HasPrefix(current, prefix+string(os.PathListSeparator)) || current == prefix {
			return env
		}
		cloned := append([]string(nil), env...)
		cloned[i] = "PATH=" + prefix + string(os.PathListSeparator) + current
		return cloned
	}
	return append(env, "PATH="+prefix)
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
