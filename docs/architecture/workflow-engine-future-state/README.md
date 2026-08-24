# Hadron Workflow Engine Future-State Architecture

**Status:** architecture working set
**Scope:** target architecture only; no implementation sequence

This folder expands
[`HADRON_DESIRED_FUTURE_STATE.md`](../HADRON_DESIRED_FUTURE_STATE.md) into
area-focused architecture notes. The purpose is to define the architectural
changes Hadron needs before a planner turns the work into tasks.

## Architecture Thesis

Hadron becomes a library-backed product:

- a reusable Go workflow engine core owns graph semantics, validation, typed
  values, scheduling, run state, waits, and conformance;
- optional adapter packages bind step kinds, persistence, timers, telemetry,
  execution targets, LLMs, MCP, messages, scripts, gates, and artifacts;
- the Hadron app/service consumes the engine as the reference host and owns
  daemon lifecycle, registry, triggers, schedules, HTTP, MCP, A2A, CLI, UI,
  operator diagnostics, and workflow exposure;
- Nanite, Torque, Cerberus, and future apps may embed the core directly or
  call Hadron app/service when an operated workflow runtime is the right
  product boundary.

The load-bearing change is that CLI, HTTP, MCP, A2A, UI, embedded Go, and
compiled/offline forms all drive one semantic graph model. Transports translate
identity, auth, streaming, and result shape. They do not define separate
workflow semantics.

## Documentation Set

- [01 - Package Boundaries](01-package-boundaries.md): reusable engine,
  adapters, Hadron host packages, and import rules.
- [02 - Graph IR And Source Formats](02-graph-ir-source-formats.md): one graph
  IR, blueprint/pipeline lowering, source maps, and call semantics.
- [03 - Values, Expressions, And Artifacts](03-values-expressions-artifacts.md):
  typed outputs, expression context, artifact references, and `::set-output`
  compatibility.
- [04 - Execution State And Scheduler](04-execution-state-scheduler.md):
  ready-queue scheduling, node invocation state, retries, fan-out, and replay.
- [05 - Waits, Gates, And Callbacks](05-waits-gates-callbacks.md): durable
  suspend/resume, generic wait records, gates, messages, timers, and callbacks.
- [06 - Step Kinds And Executors](06-step-kinds-executors.md): step-kind
  registry, executor contracts, LLM and agent execution, and effect metadata.
- [07 - Activation And Run Binding](07-activation-run-binding.md): definition
  references, execution plans, bound runs, schedules, triggers, timers, and
  activation semantics.
- [08 - Hadron App Service Surfaces](08-hadron-app-service-surfaces.md):
  daemon, CLI, HTTP, MCP, A2A, UI, registry, and exposure profiles.
- [09 - Consumer Boundaries And Extraction](09-consumer-boundaries-extraction.md):
  Nanite, Torque, Cerberus, shared libraries, and embed-vs-call rules.
- [10 - Migration, Compatibility, And Safety](10-migration-safety-compatibility.md):
  archive/rewrite stance for old examples, unsafe legacy behavior, redaction,
  and safety invariants.
- [11 - Open Decision Register](11-open-decision-register.md): accepted
  architecture decisions plus remaining low-level decisions that need owner
  review before planning.

## Source Material

Primary local sources:

- [`../HADRON_DESIRED_FUTURE_STATE.md`](../HADRON_DESIRED_FUTURE_STATE.md)
- [`../../workflow-engine-direction.md`](../../workflow-engine-direction.md)
- [`../../workflow-engine-target-capabilities.md`](../../workflow-engine-target-capabilities.md)
- [`../ARCHITECTURE.md`](../ARCHITECTURE.md)
- [`../adr`](../adr)

Current code evidence:

- [`internal/blueprint/blueprint.go`](../../../internal/blueprint/blueprint.go)
- [`internal/execution`](../../../internal/execution)
- [`internal/pipeline`](../../../internal/pipeline)
- [`internal/persistence`](../../../internal/persistence)
- [`internal/mcpadapter`](../../../internal/mcpadapter)
- [`internal/scheduler`](../../../internal/scheduler)
- [`internal/trigger`](../../../internal/trigger)

Sibling-app evidence called out by the source docs:

- `apps/nanite/internal/agentworkflow`
- `apps/nanite/internal/service/workflow_engine.go`
- Torque and Cerberus integration points described in the future-state draft

## Scope Rules

These docs intentionally do not define phases, estimates, task breakdowns,
migration order, or staffing. They describe the target architecture and the
decisions that must be made before implementation planning.

ADRs remain the durable place for accepted decisions. The entries in
[11 - Open Decision Register](11-open-decision-register.md) are placeholders,
not accepted ADRs.

## Non-Negotiable Constraints

- The engine core is embeddable and does not require `hadrond`.
- Hadron app/service continues to exist as the reference runtime and operator
  surface.
- Hadron defines one graph-native workflow source format over the graph IR.
  Current blueprint and pipeline examples are historical/reference material, not
  compatibility constraints.
- Effects, idempotency, retryability, cancellation, output shape, and required
  capabilities are first-class metadata.
- Runtime state is durable by contract. Waiting nodes release workers.
- Typed values and artifact references are the data plane. Logs are not.
- Product semantics stay in products. Shared mechanics move to libraries.
