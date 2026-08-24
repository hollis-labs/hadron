# 08 - Hadron App Service Surfaces

## Architectural Change

Hadron app/service becomes the reference host and operator surface over the
engine core. It owns product experience, local-first daemon operation, registry,
exposure, and transport contracts. It does not own a second workflow semantic
model.

```text
CLI ----+
HTTP ---+
MCP ----+--> Hadron application services --> workflow core
A2A ----+
UI -----+
```

## Current Strengths To Preserve

The desired future state should retain the current product center:

- daemon-owned orchestration and persistence;
- CLI and desktop as daemon clients;
- schedules and triggers dispatch through shared execution;
- pipeline stages as ordinary run records with additional orchestration state;
- registry indexing, hashing, pack/unpack, pinning, and discovery;
- MCP adapter and A2A skill exposure;
- run events, telemetry, and diagnostics;
- local message and human gate operational records.

Existing ADRs remain aligned with Hadron-as-reference-host:

- [`ADR 0001`](../adr/0001-daemon-owns-orchestration-and-persistence.md)
- [`ADR 0002`](../adr/0002-cli-and-desktop-are-daemon-clients.md)
- [`ADR 0003`](../adr/0003-blueprint-execution-trigger-schedule-model.md)
- [`ADR 0004`](../adr/0004-pipeline-orchestration-model.md)
- [`ADR 0005`](../adr/0005-wails-layering-frontend-backend-contract.md)

Future ADRs may supersede details, but the app/service remains valuable.

## Hadron Host Responsibilities

Hadron app/service owns:

- daemon lifecycle, health, readiness, graceful shutdown, and recovery;
- app-owned persistence implementation and migrations;
- definition registry, pack, pin, digest, provenance, cache, and discovery;
- activation registration APIs for schedules, triggers, callbacks, and events;
- run inspection, diagnostics, event streams, replay views, and telemetry;
- app settings, run scopes, execution target binding, and policy selection;
- MCP server surface for agents;
- A2A task/skill surface;
- HTTP API and CLI commands;
- desktop canvas and operator UI;
- workflow exposure profiles;
- agent-builds-tools flywheel.

The app/service consumes workflow core and adapters. It must not introduce
alternate execution semantics in a transport handler or UI page.

## MCP Server Surface

Hadron's MCP server exposes Hadron workflow capabilities. The engine core does
not depend on MCP.

Current MCP pointers:

- [`internal/mcpadapter/adapter.go`](../../../internal/mcpadapter/adapter.go)
  stores token, scopes, and a session ID.
- [`internal/mcpadapter/server_surface.go`](../../../internal/mcpadapter/server_surface.go)
  maps tool names to read-only/destructive/idempotent annotations.
- [`internal/mcpadapter/tools.go`](../../../internal/mcpadapter/tools.go)
  registers the current `hadron_*` meta-tools.

Target exposure model:

```yaml
profile: nanite-reviewer
principal: agent:nanite/reviewer
namespaces: [torque, git]
pin:
  - torque/task-bulk-create@sha256:...
deny_effects: [destructive]
max_direct_tools: 24
search_scope: namespaces
lazy_load: true
```

Tiers:

- **Meta-tools:** default for unknown callers; search, inspect, validate, run
  by reference, subscribe/inspect run, submit gate/message.
- **Pinned tools:** selected workflows appear as first-class MCP tools with
  compact input/output schemas.
- **Discoverable tools:** agents search and load schemas into a session when
  needed.
- **Hidden/denied:** unavailable to the principal/profile.

Effect metadata drives MCP annotations and confirmation policy.

## A2A Surface

Current A2A skill generation starts from blueprints:

- [`internal/agentcard/agentcard.go`](../../../internal/agentcard/agentcard.go)

Target A2A contracts derive from registry entries, input schemas, output
schemas, effect classifications, and definition provenance. Task-to-run
correlation should be durable in Hadron run state, not transport-local memory.

## HTTP And CLI

The HTTP API and CLI should expose the same semantic path:

```text
resolve definition -> compile/validate -> bind run -> start/inspect/cancel/resume
```

CLI examples in the target model:

```text
hadron workflow validate ./workflows/bulk-create.yaml
hadron workflow explain torque/task-bulk-create@1.2.0
hadron run torque/task-bulk-create@sha256:... --input tasks.json
hadron run inspect <run-id>
hadron run resume <run-id> --wait <wait-id> --payload decision.json
hadron build ./workflows/tool.yaml --as mcp-server -o bin/tool-server
```

Command names are illustrative. The architectural requirement is that every
door drives the same plan and bound-run model.

## Desktop UI And Canvas

The desktop app remains a presentation layer over daemon APIs.

Target UI architecture should render:

- graph nodes and edges from compiled IR;
- node-level positions, not only pipeline stage positions;
- typed value flow on edges;
- run state, waits, retries, attempts, and artifacts;
- source-map navigation back to the source file;
- exposure profile and registry state;
- explain/blast-radius views from effects and capabilities.

The UI must not create hidden orchestration rules that do not exist in the
engine core.

## Registry And Agent-Builds-Tools Flywheel

Hadron is the natural home for reusable workflow authoring and operation:

```text
missing capability
  -> search registry
  -> draft workflow
  -> validate and run contract tests
  -> register under namespace
  -> pin or publish
  -> expose as MCP/A2A/CLI callable tool
```

The registry remains a discovery, resolution, validation, and provenance index.
It is not the canonical authoring repository unless a source explicitly names
Hadron as the authority.

## Decisions

- Principal and exposure profile records are Hadron-local by default.
- A Tether policy adapter may become an optional authority later.
- MCP token/session resolves to a principal, and the principal resolves to an
  exposure profile.
- The compiled/offline subset starts conservative.
- Daemon-less builds support pure, read, compute, and materialize nodes that do
  not require daemon wait services.
- MCP and LLM nodes are allowed in compiled/offline mode only with explicit
  external config bindings.
- Gates, messages, and callback waits require a remote daemon binding or are
  rejected at build time.

## Decision Needed

- Exposure profile storage shape under Hadron-local defaults.
- Direct MCP tool budget and lazy loading mechanics.
- A2A task-to-run persistence contract.
- CLI command shape for plan/explain/build/resume.
- Which app services own compile caching and plan inspection.
- Desktop canvas source of truth for node positions and run-value display.
