# 01 - Package Boundaries

## Architectural Change

Move Hadron's workflow semantics out of app-internal packages and into a
reusable Go package family. Hadron app/service then imports the engine as a
host, the same way Nanite, Torque, and other applications can.

This changes the architecture from:

```text
hadrond -> internal/blueprint + internal/execution + internal/pipeline
```

to:

```text
Hadron app/service -> workflow engine core -> host-supplied stores, adapters, timers, policy
Nanite/Torque/etc -> workflow engine core -> product-owned adapters and policy
```

The engine core must be usable without starting `hadrond`, opening Hadron's
SQLite store, importing Hadron `internal/*`, or depending on HTTP, MCP, A2A,
Wails, or concrete provider SDKs.

## Current Shape

The current implementation puts meaningful workflow behavior under
`internal/`:

- [`internal/blueprint/blueprint.go`](../../../internal/blueprint/blueprint.go)
  defines the blueprint model, validation, imports, and render-time template
  behavior.
- [`internal/execution/manager.go`](../../../internal/execution/manager.go)
  executes blueprint sections and steps sequentially through a worker pool.
- [`internal/pipeline`](../../../internal/pipeline) owns the only DAG behavior,
  pipeline-level dependency ordering, and stage output capture.
- [`internal/persistence`](../../../internal/persistence) is a concrete SQLite
  implementation for Hadron app/service state.
- [`internal/mcpadapter`](../../../internal/mcpadapter) exposes Hadron daemon
  capabilities as MCP tools.

Those are valid app implementation packages, but they are not consumable
engine contracts for Nanite, Torque, or embedded Go callers.

## Target Package Families

Names below are illustrative. Boundary and import direction are authoritative.

```text
workflow/
  graph          graph IR, node definitions, source maps, schema metadata
  compile        validation, source loaders, import resolution, plan digesting
  values         typed values, artifact references, expression bindings
  runtime        ready queue, state machine, retry, cancellation, replay
  wait           generic suspend/resume/wait contracts
  stepkind       registry interfaces and conformance fixtures
  conformance    shared black-box tests for hosts and stores

workflow/adapters/
  cmd            command executor adapter
  http           HTTP executor adapter
  mcp            MCP client executor adapter
  llm            provider/harness-backed LLM executor adapters
  script         script/transform executors
  gate           gate/checkpoint domain package over wait contracts
  sqlite         optional storage adapter, if shared enough to extract
  telemetry      OTel/event adapter helpers

hadron/internal/
  appworkflow    Hadron host binding over workflow core
  api            HTTP surface over Hadron application services
  mcpadapter     MCP server surface over Hadron application services
  registry       Hadron registry, pack, pin, discovery, provenance
  scheduler      Hadron activation binding to go-scheduler
  trigger        Hadron webhook/file/event trigger sources
  ui             Wails/backend/frontend presentation over daemon APIs
```

## Import Rules

Engine core may import:

- Go standard library;
- narrow shared Hollis Labs libraries only when the abstraction is universal;
- schema, validation, expression, and test libraries selected by architecture
  decision.

Engine core must not import:

- `github.com/hollis-labs/hadron/internal/...`;
- Hadron daemon, API, MCP, A2A, CLI, registry, Wails, or settings packages;
- concrete SQLite persistence code;
- Nanite, Torque, Tether, or Cerberus application packages;
- concrete model/provider SDKs;
- transport-specific MCP or HTTP server packages.

Hadron app/service may import:

- workflow core;
- optional adapters;
- Hadron app packages;
- concrete stores, registries, transports, and UI dependencies.

Adapters may import:

- relevant shared libraries such as `go-messaging`, `go-scheduler`,
  `go-sandbox`, `go-mcp`, `agentkit`, `go-providers`, or `go-llm-types`;
- concrete SDKs for the capability they bind;
- workflow core contracts.

Adapters must not leak their concrete dependencies into core types unless an
architecture decision promotes that dependency to a universal contract.

## Host API Shape

The host supplies policy, storage, executors, timers, telemetry, and identity.
The core supplies validation and runtime mechanics.

```go
package workflowhost

type Host struct {
	Definitions DefinitionResolver
	Store       StateStore
	Executors   stepkind.Registry
	Timers      ActivationScheduler
	Policy      PolicyEvaluator
	Artifacts   ArtifactStore
	Telemetry   EventSink
	Clock       Clock
}

func RunWorkflow(ctx context.Context, h Host, ref DefinitionRef, input map[string]any) (RunHandle, error) {
	source, err := h.Definitions.Resolve(ctx, ref)
	if err != nil {
		return RunHandle{}, err
	}
	plan, err := compile.Compile(ctx, source, compile.Options{
		Definitions: h.Definitions,
		StepKinds:   h.Executors,
		Policy:      h.Policy,
	})
	if err != nil {
		return RunHandle{}, err
	}
	bound, err := runtime.Bind(ctx, plan, runtime.BindOptions{
		Input:    input,
		Policy:   h.Policy,
		Artifacts: h.Artifacts,
	})
	if err != nil {
		return RunHandle{}, err
	}
	return runtime.Start(ctx, h.Store, h.Executors, h.Timers, bound)
}
```

The exact package names and API signatures are open decisions. The dependency
direction is not.

## Hadron Host Binding

Hadron app/service binds the core to:

- SQLite-backed operational state;
- local daemon lifecycle;
- registry and package resolution;
- schedules, triggers, webhooks, file watchers, one-shot callbacks, and TTL
  cleanup;
- HTTP, CLI, MCP, A2A, and desktop surfaces;
- app settings, run scopes, credentials references, telemetry, and diagnostics.

That binding belongs under Hadron app packages. It is not engine core.

## Conformance Boundary

The shared engine needs a conformance suite that any host or adapter can run.
At minimum it should cover:

- graph validation and cycle rejection;
- archived blueprint and pipeline rewrite/reference fixtures;
- typed value binding and unresolved-reference failures;
- ready-queue scheduling and skip/failure semantics;
- retry, cancellation, timeout, and recovery behavior;
- suspend/resume and wait-correlation behavior;
- state-store idempotency and crash-recovery invariants;
- step-kind metadata validation.

Conformance tests prevent Hadron, Nanite, and Torque from drifting into
separate workflow engines again.

## Decision Needed

- Public module and package names.
- Whether the first extraction is in this repository or a shared library repo.
- Which dependencies are acceptable in engine core.
- Whether SQLite storage is Hadron-only, shared adapter, or both.
- Minimum conformance suite that gates host adoption.
