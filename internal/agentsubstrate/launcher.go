package agentsubstrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hollis-labs/hadron/internal/localauth"
	"github.com/hollis-labs/substrate/llm-core/contracts/runtimes"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agentsessions "github.com/hollis-labs/substrate/harness/adapters/agentsessions"
	"github.com/hollis-labs/substrate/harness/adapters/provider"
	"github.com/hollis-labs/substrate/harness/adapters/runtimebind"
	"github.com/hollis-labs/substrate/harness/adapters/turn"
	agentlaunch "github.com/hollis-labs/substrate/harness/agentlaunch"
	launcherpkg "github.com/hollis-labs/substrate/harness/agentlaunch/launcher"
	"github.com/hollis-labs/substrate/harness/agentlaunch/sessionshim"
	llmtypes "github.com/hollis-labs/substrate/llm-core/llmtypes"
	"github.com/hollis-labs/substrate/mesh/messaging"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/launchartifacts"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/substrate/harness/interception/permission"
)

const (
	kindGoAgentRuntime = "go_agent_runtime"

	defaultAuthority       = "local"
	defaultWorkingDirMode  = "blueprint_dir"
	workingDirModeStepDir  = "step_dir"
	workingDirModeCWD      = "cwd"
	workingDirModeProcess  = "process_cwd"
	sessionShutdownTimeout = 5 * time.Second
	replyOutboxWatchWindow = 15 * time.Minute
	hadronClientName       = "hadron"
	hadronClientVersion    = "0.1-dev"
)

type Launcher struct {
	dataDir            string
	substrates         map[string]settings.AgentSubstrateSettings
	mcpServers         map[string]settings.MCPServerSettings
	sessions           *agentsessions.Manager
	codexTurns         turn.CodexAppServerCache
	replies            replyMessenger
	seq                atomic.Uint64
	mu                 sync.Mutex
	goroutines         sync.WaitGroup
	closed             bool
	artifactOperations map[string]string
	closeOnce          sync.Once
	closeCtx           context.Context
	cancel             context.CancelFunc
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
	closeCtx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel func is stored on Launcher and invoked from Close()
	return &Launcher{
		dataDir:    dataDir,
		substrates: cloned,
		sessions:   agentsessions.NewManager(nil),
		closeCtx:   closeCtx,
		cancel:     cancel,
	}
}

// SetMCPServers records the settings' MCP servers for every launch's plan.
// See mcpSpecFromSettings: the spec is inert until the libs plant it.
func (l *Launcher) SetMCPServers(servers map[string]settings.MCPServerSettings) {
	cloned := make(map[string]settings.MCPServerSettings, len(servers))
	for name, srv := range servers {
		cloned[name] = srv
	}
	l.mcpServers = cloned
}

func (l *Launcher) SetReplyMessenger(m replyMessenger) {
	l.replies = m
}

func (l *Launcher) lifecycleContext(ctx context.Context) context.Context {
	if l == nil || l.closeCtx == nil {
		return context.WithoutCancel(ctx)
	}
	return launcherLifecycleContext{Context: context.WithoutCancel(ctx), done: l.closeCtx.Done()}
}

func (l *Launcher) startLifecycleGoroutine(fn func()) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.goroutines.Add(1)
	go func() {
		defer l.goroutines.Done()
		fn()
	}()
	return true
}

type launcherLifecycleContext struct {
	context.Context
	done <-chan struct{}
}

func (c launcherLifecycleContext) Done() <-chan struct{} {
	return c.done
}

func (c launcherLifecycleContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (l *Launcher) Close() error {
	if l == nil || l.sessions == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionShutdownTimeout)
	defer cancel()
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	if l.cancel != nil {
		l.closeOnce.Do(l.cancel)
	}
	for _, info := range l.sessions.List() {
		_ = l.sessions.Stop(ctx, info.ID)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.goroutines.Wait()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
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

	// AllowGenericSubprocess is not passed on: the generic subprocess path
	// launched argv [prompt] and parsed no output, so an unknown provider is
	// refused below rather than launched blind.
	binding, err := runtimebind.Resolve(runtimebind.Request{
		Provider:         cfg.Provider,
		RequestedRuntime: runtimeMode(cfg.Runtime),
		Posture:          runtimePosture(cfg.Runtime),
		AllowPTY:         true,
	})
	if err != nil {
		if errors.Is(err, runtimebind.ErrUnsupportedBinding) {
			return execution.AgentLaunchResult{}, &UnsupportedRuntimeError{Provider: cfg.Provider, Runtime: cfg.Runtime, Cause: err}
		}
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
	mailbox := mailboxURN(cfg.Authority, req.LogicalAgentID)
	admission, release, err := l.acceptArtifactOperation(ctx, sessionID, cfg, req)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}
	defer release()
	sessionLaunch, bootDir, workspaceDir, err := buildSessionLaunch(ctx, l.dataDir, cfg, req, sessionID, mailbox, projectDir, adapter, mcpSpecFromSettings(l.mcpServers), admission)
	if err != nil {
		return execution.AgentLaunchResult{}, err
	}
	kickoffPayload := launchKickoffPayload(adapter)
	eventCh := make(chan llmtypes.StreamEvent, 128)
	sessionLaunch.Options.EventFanout = eventCh

	if err := l.sessions.Start(ctx, agentsessions.StartRequest{
		ID:      sessionID,
		Runtime: runtime,
		Options: sessionLaunch.Options,
		SessionMeta: map[string]string{
			"substrate":        req.Substrate,
			"launch_id":        req.LaunchID,
			"logical_agent_id": req.LogicalAgentID,
			"provider":         binding.Provider,
			"runtime":          runtimeSetting(binding.Runtime),
			"mailbox":          mailbox,
			"session_uri":      sessionURN(cfg.Authority, sessionID),
			"correlation":      strings.TrimSpace(anyString(req.Metadata["correlation_id"])),
		},
	}); err != nil {
		return execution.AgentLaunchResult{}, fmt.Errorf("start session: %w", err)
	}
	if len(kickoffPayload) > 0 {
		kickoffCtx := l.lifecycleContext(ctx)
		//nolint:gosec // kickoff turn is intentionally async and detached from request cancellation
		if !l.startLifecycleGoroutine(func() {
			l.runKickoffTurn(kickoffCtx, sessionID, mailbox, bootDir, req, binding.Provider, eventCh, kickoffPayload)
		}) {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), sessionShutdownTimeout)
			defer stopCancel()
			_ = l.sessions.Stop(stopCtx, sessionID)
			return execution.AgentLaunchResult{}, errors.New("launcher is closing")
		}
	}

	result := execution.AgentLaunchResult{
		SessionID: sessionID,
		Mailbox:   mailbox,
		Handles: map[string]any{
			"logical_agent_id": req.LogicalAgentID,
			"provider":         binding.Provider,
			"runtime":          runtimeSetting(binding.Runtime),
			"session_urn":      sessionURN(cfg.Authority, sessionID),
			"workdir":          projectDir,
			"workspace_dir":    workspaceDir,
			"boot_dir":         bootDir,
		},
	}
	return result, nil
}

func buildSessionLaunch(ctx context.Context, dataDir string, cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest, sessionID, mailbox, projectDir string, adapter provider.CLIAdapter, mcp agentlaunch.MCPSpec, admission launchartifacts.Admission) (sessionshim.SessionLaunch, string, string, error) {
	workspaceDir := filepath.Join(dataDir, "agents", "sessions", sessionID)
	return buildAgentkitSessionLaunch(ctx, dataDir, cfg, req, sessionID, mailbox, projectDir, workspaceDir, adapter, mcp, admission)
}

func buildAgentkitSessionLaunch(ctx context.Context, dataDir string, cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest, sessionID, mailbox, projectDir, workspaceDir string, adapter provider.CLIAdapter, mcp agentlaunch.MCPSpec, admission launchartifacts.Admission) (launchResult sessionshim.SessionLaunch, bootRoot string, workspaceRoot string, err error) {
	bootPrompt, bootContent, _, err := renderBootArtifacts(dataDir, cfg, bootRenderContext{
		SessionID:      sessionID,
		SessionURN:     sessionURN(cfg.Authority, sessionID),
		MailboxURN:     mailbox,
		ProjectDir:     projectDir,
		BlueprintPath:  req.BlueprintPath,
		LaunchID:       req.LaunchID,
		LogicalAgentID: req.LogicalAgentID,
		PromptAppend:   req.PromptAppend,
		Metadata:       req.Metadata,
		InjectedFiles:  req.Injection.NativeFiles,
	})
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", err
	}
	plan, err := buildLaunchPlan(cfg, req, projectDir, workspaceDir, bootPrompt, bootContent, mcp)
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", err
	}
	compiled, err := launcherpkg.Compile(ctx, plan)
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("compile launch plan: %w", err)
	}
	_, ok := adapter.(provider.BootDirProvider)
	if !ok {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("adapter %q does not support bootdir planting", adapter.Name())
	}
	prepared, custody, err := launchartifacts.Prepare(ctx, compiled, admission)
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", err
	}
	defer func() { err = errors.Join(err, custody.Close()) }()
	_, _, extraFiles, err := renderBootArtifacts(dataDir, cfg, bootRenderContext{
		SessionID:      sessionID,
		SessionURN:     sessionURN(cfg.Authority, sessionID),
		MailboxURN:     mailbox,
		ProjectDir:     projectDir,
		BlueprintPath:  req.BlueprintPath,
		BootDir:        prepared.PlantedBootDir,
		LaunchID:       req.LaunchID,
		LogicalAgentID: req.LogicalAgentID,
		PromptAppend:   req.PromptAppend,
		Metadata:       req.Metadata,
		InjectedFiles:  req.Injection.NativeFiles,
	})
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", err
	}
	if plantErr := plantCanonicalArtifacts(ctx, prepared, adapter, extraFiles, custody.Authorize); plantErr != nil {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("prepare launch: %w", plantErr)
	}
	if mkdirErr := os.MkdirAll(filepath.Join(prepared.WorkspaceDir, "logs"), 0o750); mkdirErr != nil {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("ensure workspace dir: %w", mkdirErr)
	}
	if mkdirErr := os.MkdirAll(filepath.Join(prepared.PlantedBootDir, replyOutboxRelDir), 0o750); mkdirErr != nil {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("ensure reply outbox dir: %w", mkdirErr)
	}
	sessionLaunch, err := sessionshim.ToSessionLaunch(prepared)
	if err != nil {
		return sessionshim.SessionLaunch{}, "", "", fmt.Errorf("build session launch: %w", err)
	}
	sessionLaunch.Options.Env = mergeEnv(cfg.Env, envMapToKV(prepared.Env))
	sessionLaunch.Options.LogPath = filepath.Join(prepared.WorkspaceDir, "logs", "session.log")
	sessionLaunch.Options.Env = prependEnvPath(sessionLaunch.Options.Env, prepared.PlantedBootDir)
	return sessionLaunch, prepared.PlantedBootDir, prepared.WorkspaceDir, nil
}

func launchKickoffPayload(adapter provider.CLIAdapter) []byte {
	if _, ok := adapter.(provider.BootDirProvider); ok {
		return []byte("Boot @./boot.md")
	}
	return nil
}

func (l *Launcher) runKickoffTurn(ctx context.Context, sessionID, mailbox, bootDir string, req execution.AgentLaunchRequest, providerName string, eventCh <-chan llmtypes.StreamEvent, payload []byte) {
	replySubstrate := strings.TrimSpace(anyString(req.Metadata["reply_substrate"]))
	correlationID := strings.TrimSpace(anyString(req.Metadata["correlation_id"]))
	disableFallbackReply := anyBool(req.Metadata["disable_fallback_reply"])
	outboxDir := filepath.Join(bootDir, replyOutboxRelDir)

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
		watchCtx, watchCancel := context.WithTimeout(ctx, replyOutboxWatchWindow)
		if !l.startLifecycleGoroutine(func() {
			defer watchCancel()
			l.watchReplyOutbox(watchCtx, outboxDir)
		}) {
			watchCancel()
		}
	}

	sendErr := l.sendTurn(ctx, sessionID, providerName, string(payload))
	finalCtx, finalCancel := context.WithTimeout(ctx, 2*time.Second)
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
	sendCtx, sendCancel := context.WithTimeout(ctx, 5*time.Second)
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
			Runtime:  runtimes.ModeJSONRPCStdio,
		})
	case info.Caps.StreamingStdio:
		framed, err := turn.Frame(text, turn.Options{
			Provider: providerName,
			Runtime:  runtimes.ModeStreamingStdio,
		})
		if err != nil {
			return fmt.Errorf("frame streaming-stdio turn: %w", err)
		}
		return l.sessions.SendInput(sessionID, framed)
	default:
		return l.sessions.SendInput(sessionID, []byte(text))
	}
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
	case binding.Provider == "claude" && binding.Runtime == runtimes.ModeSubprocessPerTurn:
		a := provider.NewClaudeAdapter()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "claude" && binding.Runtime == runtimes.ModeStreamingStdio:
		a := provider.NewClaudeAdapterStreamingStdio()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{StreamingStdio: true, ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "claude" && binding.Runtime == runtimes.ModePTY:
		a := provider.NewClaudeAdapterPTY()
		a.AdditionalDirectories = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{PTY: true, Resize: true, ProviderSessionID: true, BinaryRequired: true}
	case binding.Provider == "codex" && binding.Runtime == runtimes.ModeSubprocessPerTurn:
		a := provider.NewCodexAdapter()
		a.WritableRoots = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{BinaryRequired: true}
	case binding.Provider == "codex" && binding.Runtime == runtimes.ModeJSONRPCStdio:
		a := provider.NewCodexAdapterAppServer()
		a.WritableRoots = []string{projectDir}
		adapter = a
		caps = agentsessions.Capabilities{JsonRpcStdio: true, BinaryRequired: true}
	case binding.Provider == "opencode" && binding.Runtime == runtimes.ModeSubprocessPerTurn:
		a := provider.NewOpencodeAdapter()
		a.Dir = projectDir
		adapter = a
		caps = agentsessions.Capabilities{BinaryRequired: true}
	case binding.Provider == "opencode" && binding.Runtime == runtimes.ModeHTTPSSE:
		a := provider.NewOpencodeAdapterServeHTTP()
		a.Dir = projectDir
		adapter = a
		caps = agentsessions.Capabilities{ServeHTTP: true, BinaryRequired: true}
	default:
		return nil, agentsessions.Capabilities{}, &UnsupportedRuntimeError{Provider: binding.Provider, Runtime: string(binding.Runtime)}
	}

	return &scopedCLIAdapter{
		inner:    adapter,
		binary:   cfg.Command,
		baseArgs: append([]string(nil), cfg.Args...),
	}, caps, nil
}

// ErrRuntimeNotSupported is matched (errors.Is) by every
// *UnsupportedRuntimeError.
var ErrRuntimeNotSupported = errors.New("runtime not supported by Hadron launcher yet")

// UnsupportedRuntimeError refuses a provider/runtime pair the launcher has no
// adapter for. It replaces the generic subprocess fallback, which started the
// binary with argv [prompt] and parsed none of its output.
type UnsupportedRuntimeError struct {
	Provider string
	Runtime  string
	Cause    error
}

func (e *UnsupportedRuntimeError) Error() string {
	msg := fmt.Sprintf("runtime %s/%s not supported by Hadron launcher yet", e.Provider, e.Runtime)
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *UnsupportedRuntimeError) Is(target error) bool { return target == ErrRuntimeNotSupported }

func (e *UnsupportedRuntimeError) Unwrap() error { return e.Cause }

func buildLaunchPlan(cfg settings.AgentSubstrateSettings, req execution.AgentLaunchRequest, projectDir, workspaceDir, bootPrompt, bootContent string, mcp agentlaunch.MCPSpec) (agentlaunch.LaunchPlan, error) {
	nativeFiles := make([]agentlaunch.NativeFile, 0, len(req.Injection.NativeFiles))
	if cfg.Boot.PlantNativeFiles {
		for _, file := range req.Injection.NativeFiles {
			nativeFiles = append(nativeFiles, agentlaunch.NativeFile{
				Kind:    agentlaunch.NativeFileRaw,
				RelPath: file.RelPath,
				Content: file.Source,
				Mode:    0o644,
			})
		}
	}
	projectID := sanitizeIDPart(filepath.Base(projectDir))
	if projectID == "" {
		projectID = "project"
	}
	agentID := sanitizeIDPart(req.LogicalAgentID)
	if agentID == "" {
		agentID = sanitizeIDPart(req.LaunchID)
	}
	if agentID == "" {
		agentID = "agent"
	}
	binding, err := runtimebind.Resolve(runtimebind.Request{Provider: cfg.Provider, RequestedRuntime: runtimeMode(cfg.Runtime), Posture: runtimePosture(cfg.Runtime), AllowPTY: true})
	if err != nil {
		return agentlaunch.LaunchPlan{}, err
	}
	posture := permission.Mode("")
	if binding.Provider == "claude" {
		posture = permission.ModeDefault
	}
	return agentlaunch.LaunchPlan{
		Project: agentlaunch.ProjectSpec{
			ID:   projectID,
			Name: filepath.Base(projectDir),
			Root: projectDir,
		},
		Agent: agentlaunch.AgentSpec{
			ID:   agentID,
			Name: req.LogicalAgentID,
		},
		Provider: agentlaunch.ProviderSpec{
			ID:         binding.Provider,
			Binary:     cfg.Command,
			Env:        mapsClone(cfg.Env),
			Permission: posture,
		},
		Runtime: binding.Runtime,
		MCP:     mcp,
		Workspace: agentlaunch.WorkspaceSpec{
			Mode:         agentlaunch.WorkspaceFresh,
			WorkspaceDir: workspaceDir,
			Workdir:      projectDir,
		},
		BootProfile: agentlaunch.BootProfileRef{
			Inline: &agentlaunch.BootProfileInline{
				BootPrompt:  bootPrompt,
				BootContent: bootContent,
				BootMode:    agentlaunch.BootModePlanted,
			},
		},
		Injection: agentlaunch.InjectionSpec{

			NativeFiles: nativeFiles,
		},
		Mode: agentlaunch.LaunchBackground,
		Metadata: agentlaunch.Metadata{
			Labels: map[string]string{
				"substrate": req.Substrate,
				"launch_id": req.LaunchID,
			},
			Annotations: map[string]string{
				"blueprint_path":   req.BlueprintPath,
				"logical_agent_id": req.LogicalAgentID,
			},
		},
	}, nil
}

// mcpSpecFromSettings carries the settings' stdio MCP servers into the launch
// plan, sorted by name. INERT TODAY: agentkit's providerplant never copies
// LaunchPlan.MCP.Servers into go-providers' PlantContext.MCPServers, and only
// the codex renderer reads that field; claude and opencode ignore it. The
// libs are to plant it (CW-20260930-0136, W4a-c). agentlaunch.MCPServerSpec
// has no URL field, so URL servers are skipped with a warning (W4c).
func mcpSpecFromSettings(servers map[string]settings.MCPServerSettings) agentlaunch.MCPSpec {
	names := make([]string, 0, len(servers))
	for name := range servers {
		names = append(names, name)
	}
	sort.Strings(names)
	var spec agentlaunch.MCPSpec
	for _, name := range names {
		srv := servers[name]
		if srv.Command == "" {
			log.Printf("agentsubstrate: mcp server %q skipped: only stdio servers can be planted (transport=%q)", name, srv.Transport)
			continue
		}
		spec.Servers = append(spec.Servers, agentlaunch.MCPServerSpec{
			Name:    name,
			Command: srv.Command,
			Args:    append([]string(nil), srv.Args...),
			Env:     mapsClone(srv.Env),
		})
	}
	return spec
}

func mapsClone(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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

func normalizedProviderName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "generic"
	}
	return name
}

func mergeEnv(extra map[string]string, appended []string) []string {
	// Never hand a launched agent the operator's credential.
	env := localauth.ScrubEnv(os.Environ())
	for k, v := range extra {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	env = append(env, appended...)
	return env
}

func envMapToKV(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}
	return out
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
