package agentsubstrate

import (
	"context"
	"fmt"
	"strings"

	claudenative "github.com/hollis-labs/substrate/harness/adapters/claude/nativefiles"
	codexnative "github.com/hollis-labs/substrate/harness/adapters/codex/nativefiles"
	layout "github.com/hollis-labs/substrate/harness/adapters/layout/plan"
	native "github.com/hollis-labs/substrate/harness/adapters/nativefiles"
	"github.com/hollis-labs/substrate/harness/adapters/provider"
	"github.com/hollis-labs/substrate/harness/adapters/registry"
	"github.com/hollis-labs/substrate/harness/agentlaunch"
	"github.com/hollis-labs/substrate/harness/agentlaunch/planting"
	"github.com/hollis-labs/substrate/harness/interception/permission"
	"github.com/hollis-labs/substrate/harness/workspace/materialize"
	"github.com/hollis-labs/substrate/harness/workspace/materialize/artifact"
	"github.com/hollis-labs/substrate/harness/workspace/render"
	"github.com/hollis-labs/substrate/llm-core/contracts/runtimes"
)

// plantCanonicalArtifacts uses runtime projection solely for the launch
// convention. The canonical renderer owns managed content; credentials remain
// preparation requirements and never become empty managed placeholders.
func plantCanonicalArtifacts(ctx context.Context, prepared *agentlaunch.PreparedLaunch, adapter provider.CLIAdapter, extraFiles []bootFile, authorize agentlaunch.ArtifactAuthorizer) error {
	scoped, ok := adapter.(*scopedCLIAdapter)
	if !ok {
		return fmt.Errorf("scoped launch adapter required")
	}
	projector, ok := scoped.inner.(provider.ProjectionProvider)
	if !ok {
		return fmt.Errorf("adapter %q lacks supported runtime projection", adapter.Name())
	}
	pc := planting.PlantContextFor(prepared)
	pc.AgentName = prepared.Compiled.Plan.Agent.ID
	projection, err := projector.ProviderProjection(pc, provider.ProjectionOptions{})
	if err != nil {
		return err
	}
	plan := prepared.Compiled.Plan
	req := render.Request{Provider: runtimes.ID(plan.Provider.ID), Layer: layout.Boot, Mode: plan.Runtime, Agent: plan.Agent.ID, Roots: map[layout.Root]string{layout.RootBoot: prepared.PlantedBootDir, layout.RootProject: plan.Project.Root}}
	for _, field := range []layout.Field{layout.Instructions, layout.Kickoff, layout.Settings, layout.MCP} {
		resolution, resolveErr := layout.Resolve(layout.Request{Key: layout.Key{Provider: req.Provider, Layer: req.Layer, Mode: req.Mode, Field: field}, Requirement: layout.Required, Components: map[string]string{"agent": req.Agent}})
		if resolveErr != nil {
			return resolveErr
		}
		content := render.Content{}
		switch field {
		case layout.Instructions:
			content.Body = []byte(prepared.BootPrompt)
		case layout.Kickoff:
			content.Body = []byte(prepared.BootContent)
		default: // Native settings and MCP are supplied through typed slots below.
		}
		req.Inputs = append(req.Inputs, render.Input{Resolved: resolution, Content: content})
	}
	for _, server := range pc.MCPServers {
		binding := native.Server{Name: server.Name, HTTPURL: server.HTTPURL, Command: server.Command, Args: append([]string(nil), server.Args...)}
		for _, variable := range server.Env {
			key, value, ok := strings.Cut(variable, "=")
			if !ok {
				return fmt.Errorf("invalid MCP environment binding")
			}
			binding.Env = append(binding.Env, native.Variable{Name: key, Value: value})
		}
		req.Native.Servers = append(req.Native.Servers, binding)
	}
	switch a := scoped.inner.(type) {
	case *provider.CodexAdapter:
		// Preserve the old Hadron headless adapter defaults, explicitly as native
		// host settings. They do not confer artifact or credential authority.
		approval, sandbox := a.ApprovalPolicy, a.SandboxMode
		if approval == "" {
			approval = "never"
		}
		if sandbox == "" {
			sandbox = "workspace-write"
		}
		req.Native.Codex = codexnative.ConfigInput{ApprovalPolicy: approval, SandboxMode: sandbox, WritableRoots: append([]string(nil), a.WritableRoots...)}
	case *provider.ClaudeAdapter:
		doc, documentErr := a.SettingsDocument()
		if documentErr != nil {
			return documentErr
		}
		req.Native.Claude = claudenative.SettingsInput{}
		for key, value := range doc {
			if key == "permissions" {
				continue
			} // The posture renderer owns native permission policy; project access stays in launch argv.
			req.Native.Claude.Slots = append(req.Native.Claude.Slots, native.Slot{Key: key, Value: value})
		}
	case *provider.OpencodeAdapter:
	// Canonical settings contain the existing explicit MCP bindings.
	default:
		return fmt.Errorf("adapter %q native settings migration unsupported", adapter.Name())
	}
	if plan.Provider.Permission != "" {
		resolution, resolveErr := layout.Resolve(layout.Request{Key: layout.Key{Provider: req.Provider, Layer: req.Layer, Mode: req.Mode, Field: layout.Permissions}, Requirement: layout.Required, Posture: plan.Provider.Permission, LookupPosture: func(id runtimes.ID, posture permission.Mode, mode runtimes.Mode) error {
			desc, ok := registry.Lookup(string(id))
			if !ok {
				return fmt.Errorf("unknown provider posture")
			}
			_, postureErr := desc.PostureFor(posture, mode)
			return postureErr
		}})
		if resolveErr != nil {
			return resolveErr
		}
		req.Inputs = append(req.Inputs, render.Input{Resolved: resolution})
	}
	for _, f := range plan.Injection.NativeFiles {
		req.Overlays = append(req.Overlays, artifact.Entry{Path: f.RelPath, Kind: artifact.EntryFile, Mode: f.Mode, Bytes: []byte(f.Content)})
	}
	for path, body := range plan.Injection.BootDirOverlay {
		req.Overlays = append(req.Overlays, artifact.Entry{Path: path, Kind: artifact.EntryFile, Mode: 0644, Bytes: []byte(body)})
	}
	for _, file := range extraFiles {
		req.Overlays = append(req.Overlays, artifact.Entry{Path: file.RelPath, Kind: artifact.EntryFile, Mode: file.Mode, Bytes: []byte(file.Content)})
	}
	rendered, err := render.Render(req)
	if err != nil {
		return err
	}
	roots := provider.ProjectionRoots{ProjectRoot: plan.Project.Root, BootRoot: prepared.PlantedBootDir, ConfigRoot: prepared.PlantedBootDir, StateRoot: prepared.WorkspaceDir, CWD: rendered.Binding.CWD}
	extraArgs := append([]string(nil), scoped.baseArgs...)
	extraArgs = append(extraArgs, plan.Provider.Flags...)
	extraArgs = append(extraArgs, plan.Injection.Args...)
	if plan.Provider.Permission != "" {
		desc, ok := registry.Lookup(plan.Provider.ID)
		if !ok {
			return fmt.Errorf("provider posture not registered")
		}
		posture, postureErr := desc.PostureFor(plan.Provider.Permission, plan.Runtime)
		if postureErr != nil {
			return postureErr
		}
		extraArgs = append(posture.Args, extraArgs...)
		if prepared.Env == nil {
			prepared.Env = make(map[string]string)
		}
		for key, value := range posture.Env {
			prepared.Env[key] = value
		}
	}
	prepared.Launch = &agentlaunch.TurnTemplate{Convention: projection.Launch, Roots: roots, ExtraArgs: extraArgs}
	binding, err := projection.Launch.ResolveTurn(roots, provider.TurnInput{Prompt: "Boot @./boot.md"}, prepared.Launch.ExtraArgs)
	if err != nil {
		return err
	}
	if _, err = agentlaunch.MaterializeArtifacts(ctx, agentlaunch.ArtifactMaterializationRequest{TargetRoot: prepared.PlantedBootDir, Roots: agentlaunch.ExecutionRoots{BootRoot: prepared.PlantedBootDir, ProjectRoot: plan.Project.Root}, Artifacts: rendered.Tree, Operation: materialize.OperationReconcile, Generation: prepared.Compiled.Provenance.PlanHash, Authorize: authorize}); err != nil {
		return err
	}
	binary := plan.Provider.Binary
	if binary == "" {
		binary = prepared.Compiled.ResolvedProviderBinary
	}
	prepared.Argv = append([]string{binary}, binding.Argv...)
	prepared.Workdir = rendered.Binding.CWD
	if prepared.Env == nil {
		prepared.Env = make(map[string]string)
	}
	updatedEnv := make(map[string]string)
	for _, entry := range provider.ApplyEnvDeltas(envMapToKV(prepared.Env), binding.Env) {
		key, value, _ := strings.Cut(entry, "=")
		updatedEnv[key] = value
	}
	prepared.Env = updatedEnv
	for key, value := range rendered.Binding.Environment {
		prepared.Env[key] = value
	}
	return nil
}
