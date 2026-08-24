# Hadron Desired Future-State Architecture

**Status:** rough draft for architecture discussion
**Date:** 2026-08-23
**Scope:** desired future state only; this is not an implementation sequence

This document captures the target architecture for Hadron as both a reusable
workflow-engine package family and a local-first Hadron app/service. It pulls
forward the useful parts of the current Hadron direction docs, the Nanite
workflow engine that was rebuilt from Hadron ideas, the portfolio sweep
findings, and external workflow-engine prior art.

The sharp language:

> Hadron owns the reference app/service/runtime for workflow execution, while
> the workflow engine core is a reusable Go library consumed by Hadron, Nanite,
> Torque, and other apps.

That distinction is load-bearing. Nanite and Torque are legitimate consumers
of Hadron's engine library, not necessarily of the Hadron app/service. Hadron
the app/service remains useful for agents and users who want to build, expose,
operate, and reuse workflows through CLI, HTTP, MCP, A2A, and UI surfaces.

## Source material

Local sources:

- [`../workflow-engine-target-capabilities.md`](../workflow-engine-target-capabilities.md)
- [`../workflow-engine-direction.md`](../workflow-engine-direction.md)
- [`../../../../docs/portfolio-sweep/hadron.md`](../../../../docs/portfolio-sweep/hadron.md)
- [`../../../../docs/portfolio-sweep/00-cross-app-duplication-map.md`](../../../../docs/portfolio-sweep/00-cross-app-duplication-map.md)
- [`../../../../docs/portfolio-sweep/06-harvest-and-cleanup-program.md`](../../../../docs/portfolio-sweep/06-harvest-and-cleanup-program.md)
- [`../../../../docs/drafts/engineering-principles.md`](../../../../docs/drafts/engineering-principles.md)
- [`../../../nanite/internal/agentworkflow`](../../../nanite/internal/agentworkflow)
- [`../../../nanite/internal/service/workflow_engine.go`](../../../nanite/internal/service/workflow_engine.go)

External workflow-engine references:

- [Temporal Workflows](https://docs.temporal.io/workflows) and [Temporal workflow message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing)
- [Argo Workflows DAGs](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/) and [core concepts](https://argo-workflows.readthedocs.io/en/latest/workflow-concepts/)
- [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
- [Conductor tasks](https://conductor-oss.github.io/conductor/devguide/concepts/tasks.html)
- [Prefect introduction](https://docs.prefect.io/v3/get-started)
- [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview) and [interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [Dagger basics](https://docs.dagger.io/0.21.4/getting-started/quickstarts/basics/)
- [Open Workflow Specification](https://github.com/open-workflow-specification/specification)

These are references, not templates to copy. Hadron's product boundary is
different: local-first, embeddable, Go-native, workflow/tool-building oriented,
LLM/provider agnostic, and able to run as either an app/service or an embedded
engine.

## Architectural constraints

1. **The engine is embeddable.** The workflow engine must be usable as a Go
   library without starting `hadrond`, using Hadron's HTTP API, or depending
   on Hadron's UI, MCP adapter, registry, SQLite implementation, or daemon.

2. **Hadron the app/service continues to exist.** The app/service is the
   reference runtime and operator surface: daemon, CLI, HTTP, MCP, A2A, UI,
   workflow registry, triggers, schedules, run inspection, and the
   agent-builds-tools flywheel.

3. **One semantic engine, multiple doors.** CLI, HTTP, MCP, A2A, UI, embedded
   Go, and any compiled/offline shape must all drive the same graph semantics.
   Transports translate identity, auth, streaming, and result shape; they do
   not define alternate workflow semantics.

4. **Blueprint and pipeline become source formats over one graph IR.** The
   current two-language stack collapses into one typed dataflow graph. A
   pipeline stage is a `call` node with `mode: run`; an inline blueprint call
   is the same node with `mode: inline`. Existing source formats can remain as
   compatibility/sugar layers.

5. **The core is LLM/provider agnostic.** `llm` is a step kind contract, not a
   dependency on one model provider, agent framework, or agent runtime. The
   engine invokes a registered executor; consumers bind that executor to
   go-providers, agentkit, Nanite's harness, an MCP broker, or something else.

6. **Gates are not a Hadron-app-only feature.** The engine owns generic
   suspend/resume/wait semantics. Human gates/checkpoints are a domain package
   or adapter layered onto those semantics. Hadron, Nanite, and Torque must be
   able to bind their own gate stores, policies, and presentation surfaces.

7. **Shared mechanics go in libraries, product semantics stay in products.**
   Hadron should not become mandatory for Nanite's core agent runtime or
   Torque's work-item lifecycle. Likewise, Nanite and Torque should not become
   mandatory for Hadron's reference app/service.

8. **Effects are first-class.** Every node declares effect class, idempotency,
   retryability, cancellation behavior, output shape, and required
   capabilities. This drives recovery safety, MCP annotations, confirmation
   policy, blast-radius explanation, and agent exposure.

9. **Runtime state is durable by contract, not by worker occupation.** A node
   waiting on a gate, message, timer, child run, or external callback must
   release the worker and resume from persisted run state. Polling while
   holding a worker is not the target architecture.

10. **Typed values replace log scraping.** `::set-output` compatibility can
    exist as a migration shim, but control flow and downstream dataflow must
    use executor-returned typed values and artifact references.

## Current tension to resolve

Hadron today has the right product center but the wrong package shape for
external consumption:

- all meaningful packages are under `internal/`;
- blueprint execution is sequential and render-once;
- pipeline orchestration is a separate DAG over blueprint runs;
- pipeline outputs are scraped from log lines;
- `human_gate` and `message_wait` block workers while polling;
- `agent_launch` is a heavy full session launch and there is no lightweight
  `llm` node;
- `call` returns no value;
- MCP/HTTP/A2A/CLI surfaces are app/service surfaces, not an embeddable engine
  API;
- concrete storage, daemon concerns, transport concerns, and step semantics are
  too interleaved for clean package consumption.

Nanite independently rebuilt much of the missing shape:

- per-step DAG execution;
- `llm`, `tool`, `gate`, `flex`, and `loop` step kinds;
- a `WorkflowEngine` interface that sequences only;
- a `StepExecutor` interface that performs actual LLM/tool/verify work;
- capability-restricted LLM tool surfaces;
- verify as a modifier;
- durable waiting statuses and resume;
- fail-hard behavior for unresolved references;
- external engine adapters for LangGraph/CrewAI-style experiments.

The desired Hadron architecture should absorb the lessons from both without
making either application subordinate to the other.

## Prior-art lessons to adopt

### Temporal

Adopt:

- durable event history as the conceptual source of truth;
- replay/recovery discipline;
- separation between workflow orchestration and side-effecting activities;
- message categories analogous to query, async signal, and tracked update.

Do not copy wholesale:

- Temporal's determinism boundary does not require Hadron workflows to be
  authored as general-purpose code;
- Hadron needs a typed graph IR that agents and UI tools can inspect, validate,
  compile, and expose as reusable tools.

### Argo Workflows

Adopt:

- one workflow model with both sequential and DAG shapes;
- DAG dependencies as the concurrency primitive;
- templates/calls as reusable units;
- fail-fast versus run-to-completion semantics as an explicit policy;
- output parameters and artifacts as first-class dataflow concepts.

Do not copy wholesale:

- Hadron is not Kubernetes-native and should not make containers the only unit
  of execution;
- Hadron's graph nodes include HTTP, MCP, LLM, transform, script, gate, message,
  child-run, and local command executors.

### AWS Step Functions

Adopt:

- separate integration patterns for immediate request/response, run-a-job, and
  callback-token waits;
- map/parallel semantics for controlled fan-out;
- explicit retry/catch/error-handling policy;
- human approval as a callback-backed wait, not a busy worker.

Do not copy wholesale:

- Hadron should not be AWS-shaped or service-integration-only;
- the same engine must run embedded inside local applications.

### Conductor

Adopt:

- task taxonomy: system tasks, worker/custom tasks, and operators;
- a registry of task/step kinds with schemas and execution behavior;
- explicit separation between workflow task definition/configuration/execution;
- built-in wait/human/HTTP/inline/transform/LLM/MCP-style categories as useful
  vocabulary.

Do not copy wholesale:

- Hadron should not require a central JVM/service runtime for embedded
  consumers.

### Prefect

Adopt:

- portable execution: local process, container, Kubernetes, cloud/service are
  deployment choices rather than different workflow semantics;
- robust state tracking across success, failure, retry, pause, and resume;
- schedules/events/API as activation surfaces over the same run model.

Do not copy wholesale:

- Hadron needs stronger package boundaries for embedding in Go applications,
  not just portable deployment of a Python workflow runtime.

### LangGraph

Adopt:

- mixing deterministic graph steps with LLM-driven steps in one graph;
- durable interrupts/checkpoints for human-in-the-loop and other external
  waits;
- resume by stable run/thread pointer;
- idempotency requirement for side effects before a pause;
- streaming projections for interactive agent surfaces.

Do not copy wholesale:

- Hadron's core must stay provider/framework agnostic and Go-native;
- LangGraph/CrewAI/ADK/AutoGen/LangChain can be external engines or step
  executors, not the definition of Hadron's core.

### Dagger

Adopt:

- typed, composable function/module orientation;
- reusable modules callable from CLI, code, and API;
- developer experience where composition is inspectable and reusable.

Do not copy wholesale:

- Dagger's container-centric object graph is not Hadron's domain;
- Hadron's core value is durable workflow state plus app/service exposure, not
  only typed container composition.

### Open Workflow Specification

Adopt:

- the separation between a workflow DSL, validation/schema tooling, runtimes,
  and SDKs;
- event-driven and service-oriented vocabulary as useful cross-checks.

Do not copy wholesale:

- Hadron should not prematurely adopt an external DSL as its primary contract.
  The primary contract should be Hadron's graph IR plus schema, validator, and
  compatibility loaders.

## Target architecture

```text
                       +-------------------------------+
                       |        Hadron app/service      |
                       | daemon, CLI, HTTP, MCP, A2A,  |
                       | UI, registry, triggers, runs  |
                       +---------------+---------------+
                                       |
                                       v
+-------------------+       +--------------------------+       +-------------------+
| Nanite            |       | embeddable workflow core |       | Torque            |
| harness, agents,  +------>| graph IR, scheduler,     |<------+ work lifecycle,   |
| sessions, teams,  |       | state machine, waits,    |       | checkpoints,      |
| tools, reflexes   |       | values, step registry    |       | task policy       |
+-------------------+       +-------------+------------+       +-------------------+
                                           |
                                           v
                           +-------------------------------+
                           | adapters and step executors    |
                           | cmd/http/mcp/llm/script/gate/ |
                           | message/child-run/agent       |
                           +-------------------------------+
```

The core package is not a daemon. It is a set of contracts and pure-ish runtime
mechanics that can be embedded by a host process. The host supplies storage,
executors, capabilities, policy, identity, telemetry sinks, and optional
transport surfaces.

The Hadron app/service is the reference host. It binds the core to local
SQLite or shared storage abstractions, daemon lifecycle, workflow registry,
triggers, schedules, HTTP, MCP, A2A, CLI, and UI.

Nanite is another host. It binds the core to its agent harness, session
context, tool broker, permission engine, teams/flex/loop semantics, and
interactive surfaces.

Torque is another host. It binds the core to work items, task lifecycle,
checkpoint policy, escalation, scheduling policy, and task-oriented MCP/API
surfaces.

## Package boundary model

Names are illustrative; the boundary is the important part.

### Engine-core packages

The reusable engine package family should contain:

- graph IR types;
- definition validation;
- compatibility loaders for blueprint and pipeline source formats;
- topological analysis and dependency validation;
- typed value model;
- expression/interpolation rules;
- ready-queue scheduler;
- run/step state machine;
- retry/catch/continue/fail-fast policy;
- `for_each`, conditional, switch, join, and finally semantics;
- suspend/resume/wait contracts;
- child-run/call contract;
- step-kind registry interfaces;
- conformance test suite.

It should not import:

- Hadron `internal/*`;
- Hadron daemon/API/MCP/A2A/UI packages;
- concrete SQLite store code;
- Wails;
- concrete provider SDKs;
- Nanite service/store packages;
- Torque task/checkpoint packages.

### Adapter packages

Adapters can live beside the core but outside the core boundary:

- command executor adapter;
- HTTP executor adapter;
- MCP client executor adapter;
- LLM executor adapter;
- script/transform executor adapter;
- gate/checkpoint adapter;
- message adapter;
- agent-launch adapter;
- persistence adapters;
- telemetry adapters.

Adapters may depend on other Hollis Labs libraries where appropriate
(`go-messaging`, `go-sandbox`, `go-scheduler`, `go-mcp`, `agentkit`,
`go-providers`, `go-llm-types`, etc.), but those dependencies should not leak
into the core unless the abstraction is truly universal.

### Hadron app/service packages

Hadron app/service keeps:

- daemon lifecycle;
- API server;
- CLI commands;
- MCP server surface and exposure profiles;
- A2A task surface and agent cards;
- workflow registry, pack, pin, and discovery UX;
- schedules and triggers;
- app-owned persistence implementation;
- run event log and diagnostics;
- UI and flow canvas;
- app-level settings and workspace management;
- agent-builds-tools workflow.

This keeps Hadron useful as a product while allowing the engine to become
useful as infrastructure.

## Graph IR

The graph IR is the semantic center. Source formats compile into it; transports
expose it; UI renders it; tests validate it; the engine runs it.

Required concepts:

- workflow identity, version, digest, provenance, and source reference;
- inputs with schema, defaults, redaction, and documentation;
- outputs with schema and retention class;
- nodes with stable IDs, type/kind, dependencies, inputs, outputs, effect
  class, retry/catch/finally policy, timeout, concurrency key, and metadata;
- edges carrying typed values or explicit control dependencies;
- artifact references for large/binary/sensitive values;
- call nodes with `mode: inline | run`;
- node-local config schema defined by the registered step kind;
- source-map back to blueprint section/step or pipeline stage for migration,
  diagnostics, and UI display.

Blueprints and pipelines are authoring surfaces, not separate engines. They can
remain user-facing where useful, but their compiled form must be one graph.

## Step kinds

The core engine should know how to schedule and account for nodes. It should
not hard-code every domain-specific executor.

Candidate built-in/standard step kinds:

- `cmd`
- `http`
- `mcp`
- `llm`
- `agent_launch`
- `message_wait`
- `human_gate`
- `call`
- `script`
- `transform`
- `sleep`
- `wait_for`
- `emit`
- `checkpoint`

Each step kind declares:

- semantic name and version;
- config schema;
- input schema;
- output schema;
- required capabilities;
- effect class: `read | compute | materialize | mutate | destructive`;
- idempotency and retry safety;
- cancellation behavior;
- observation/progress behavior;
- whether it can suspend;
- whether it can run in embedded mode without Hadron app services.

Nanite's current `WorkflowEngine` / `StepExecutor` split is the right
direction: the engine sequences; executors perform real work. Hadron should
generalize that shape rather than baking Nanite-specific harness assumptions
into the core.

## Values, outputs, and artifacts

Hadron should move from string templates and log-scraped outputs to typed
dataflow.

Target model:

- small JSON-compatible values can flow inline between nodes;
- large, binary, or sensitive values become artifact references;
- node outputs are declared and validated;
- unresolved references fail hard at validation or binding time where possible;
- runtime reference failures fail the node with structured diagnostics;
- string interpolation remains available for authoring convenience, but it is
  not the data plane;
- `::set-output` remains only as a compatibility shim during migration.

This aligns Hadron with Argo-style output parameters/artifacts, Conductor-style
task input/output references, and the practical needs of agents that must
inspect a workflow before running it.

## Scheduler and state machine

The scheduler should be a ready-queue over graph nodes, not a level-by-level
barrier unless the graph policy requires a barrier.

Required scheduler semantics:

- dependency readiness;
- bounded worker pool;
- per-effect and per-capability concurrency limits;
- concurrency keys across runs;
- fail-fast versus run-to-completion policy;
- tolerated failure policy for fan-out;
- retry and backoff;
- timeout and cancellation;
- finally/cleanup nodes;
- crash recovery;
- resume from persisted run/step state;
- clear distinction between failed, cancelled, timed out, waiting, crashed, and
  blocked.

The core library should define the state machine and the storage interface.
Hadron, Nanite, and Torque can provide different storage implementations.

## Timed activation scheduler

This is separate from the workflow graph scheduler.

The graph scheduler decides which workflow nodes are ready to run. The timed
activation scheduler wakes something later: cron schedule, one-shot timer,
delayed retry, gate timeout, message timeout, child-run polling, loop tick, or
external wait resume.

`go-scheduler` should remain the shared timed activation substrate. Nanite's
recent scheduler work confirms that the missing pieces are not a new
product-shaped library; they are the next layer of `go-scheduler`:

- stable fire identity, such as scheduled-at/fire ID, instead of per-tick run
  IDs;
- retry, backoff, and exhaustion policy;
- per-fire attempt tracking and status;
- `on_fail` / on-exhausted hooks;
- observability hooks for fire, retry, skip, success, exhaustion, and disable;
- configurable tick cadence, batch limits, and clock;
- contract tests for compare-and-swap claim semantics.

App-specific pieces should stay out of `go-scheduler`:

- Nanite job types and producers;
- Nanite event-log category names;
- Hadron workflow registry, API, MCP, and trigger UX;
- Torque task, checkpoint, and escalation lifecycle.

The embeddable workflow engine should depend on a small timer/activation
interface rather than on Hadron's daemon or on a concrete `go-scheduler`
instance. Hadron app/service, Nanite, Torque, and other hosts can bind that
interface to `go-scheduler` or to an equivalent host runtime.

## Waits, gates, and callbacks

The core engine should model waits generically:

- a node may suspend with a wait record;
- a wait record has type, run ID, node ID, correlation ID, timeout, payload,
  resume schema, and visibility metadata;
- suspended nodes release workers;
- resumes are correlated and idempotent;
- timeouts are observable and resumable through the same state machine;
- cancellation closes outstanding waits.

Human gates are one domain built on that wait contract.

Target split:

- engine core: wait state, node suspension, timeout, cancellation, resume;
- shared gate/checkpoint package: prompt, options/schema, decision, responder,
  policy hook, correlation, escalation vocabulary;
- Hadron app/service: HTTP/MCP/CLI/UI surface and Hadron persistence adapter;
- Nanite: chat/envelope/A2A surfaces and harness policy adapter;
- Torque: checkpoint/task lifecycle, required-workflow policy, escalation, and
  task-state adapter.

This prevents Hadron's current blocking worker gate from becoming the shared
abstraction and prevents Nanite/Torque from depending on the Hadron daemon to
ask a human a question.

## LLM and agent execution

Hadron should treat LLM calls as typed workflow functions.

`llm` target shape:

- provider/model selected by binding or profile, not hard-coded in IR;
- request has system/input messages, optional context assembly flag, tool
  allowlist, output schema, budget, and timeout;
- response has typed output, raw text, tool call records, usage, stop reason,
  and audit metadata;
- tool calls are capability-restricted to the node's declared tool surface;
- `verify` is a modifier on `llm` or `tool`, not a separate engine;
- provider errors and model-decision failures are distinct in state.

`agent_launch` remains for open-ended or session-oriented work. It should not
be the only way to use AI inside a workflow.

Nanite's harness remains Nanite-owned. Hadron can bind to it through an
executor adapter when embedded in Nanite or when using Nanite as a selected
execution substrate, but the core must not import Nanite.

## Hadron app/service

Hadron app/service is the strongest place for reusable workflow authoring and
operation.

It should provide:

- registry search, pinning, versioning, digesting, packaging, and provenance;
- validation and contract tests for workflows;
- run inspection, diagnostics, telemetry, and replay views;
- MCP tools for discovery, execution, gate submission, messages, and run
  inspection;
- selective tool exposure profiles;
- HTTP and A2A surfaces over the same run model;
- schedules, webhooks, file triggers, TTL/one-shot triggers, and event
  activations;
- flow canvas and operator UI;
- agent-builds-tools loop: discover missing capability, draft workflow,
  validate/test, register, expose/pin.

The app/service should consume the engine the same way other hosts do. Its
extra responsibility is product experience and operational runtime, not a
different execution model.

## Consumer boundaries

### Nanite

Nanite embeds the engine where workflows are part of its core agent runtime.
It owns:

- agent profiles, sessions, teams, roles, skills, reflexes, loops, chat;
- context assembly and memory recall policy;
- tool broker and permission engine;
- LLM/tool/flex/loop executors;
- envelope/card/A2A presentation for waits and approvals.

Nanite may still call Hadron app/service for reusable workflows, discovery, or
tool-building workflows. It must not require Hadron service availability for
core agent-workflow execution.

### Torque

Torque embeds the engine where workflows are part of task/work execution. It
owns:

- work items, task lifecycle, priorities, scheduling policy;
- checkpoints, escalation, required-workflow policy, enforcement mode;
- task-specific MCP/API surfaces;
- task-state transitions around waits and outcomes.

Torque may still call Hadron app/service for reusable workflow catalogs or
workflow execution when service delegation is the right boundary. It must not
need the Hadron daemon for local task/checkpoint orchestration.

### Cerberus and other apps

Cerberus-style pipelines should become adapters over the shared engine rather
than another in-repo DAG. Connector actions remain app-owned tools/executors.
The shared engine should account for rollback/compensation as a design input,
because Cerberus exposes that requirement more directly than Hadron's current
blueprints do.

Other apps can choose:

- embed the engine when workflow execution is part of their core runtime;
- call Hadron app/service when they want an operated workflow runtime;
- expose their own capabilities as step executors or MCP tools.

## MCP, A2A, HTTP, CLI, and UI exposure

Hadron should expose workflow capabilities through profiles, not by flooding
every agent context with every workflow.

Target surface:

- default profile exposes meta-tools only: search, inspect, validate, run by
  reference, subscribe/inspect run, submit gate/message;
- pinned tools expose selected workflows directly;
- discoverable tools can be lazily loaded for a session;
- hidden/denied workflows do not appear for that principal/profile;
- effect classes drive read-only/destructive annotations and confirmation
  policy;
- `tools.listChanged` or equivalent notifies agents when per-session tools are
  mounted or removed;
- A2A agent cards derive from registry entries and input/output schemas;
- CLI and UI are operator surfaces over the same definitions and runs.

The MCP server surface is Hadron app/service territory. The engine core should
not depend on MCP.

## Compatibility and migration stance

Compatibility matters, but legacy behavior should not define the target.

Target stance:

- existing blueprints and pipelines are source formats with compatibility
  loaders;
- current behavior can be preserved where it is safe and useful;
- unsafe behavior gets explicit migration shims and diagnostics;
- `::set-output` is deprecated in favor of typed returns;
- scaffold-specific blueprint fields that are not workflow semantics should
  move out of the universal IR;
- pipeline stage identity becomes a run-identity property on `call`;
- current Wails UI is not the future UI architecture, but the flow canvas is
  valuable prior art.

## Non-goals

- Do not make Hadron service mandatory for Nanite or Torque core runtime use.
- Do not build a new general-purpose programming language as the workflow
  contract.
- Do not make one large package that imports every app concern.
- Do not make LLM/provider choice part of the core IR.
- Do not make MCP the internal execution boundary.
- Do not define human gates as a Hadron-only table/API.
- Do not keep worker-blocking waits as the target semantics.

## Open design questions

These are unresolved architecture questions to settle in this document or in
follow-on ADRs:

- exact graph IR schema and source map;
- exact typed value/artifact/redaction model;
- expression language and function library;
- scheduler fairness and concurrency-key semantics;
- state-store interface shape;
- wait/callback idempotency and timeout semantics;
- gate/checkpoint package boundary versus engine wait boundary;
- exact timer/activation interface shape between engine core and
  `go-scheduler`;
- whether `go-scheduler` owns generic schedule-run attempt storage interfaces
  or only contracts that app stores implement;
- rollback/compensation semantics;
- `llm` binding: direct provider adapter, Nanite harness adapter, agentkit, or
  multiple bindings behind one executor interface;
- script node runtime and sandbox limits;
- compiled/offline subset and what happens to MCP/LLM/gate nodes there;
- conformance test suite shared by Hadron, Nanite, and Torque;
- public package naming and release-cadence boundary.

## Follow-up candidates

- Extract Nanite's scheduler learnings into `hollis-labs/go-scheduler` and
  document the upgraded contract. The specific items to carry forward are
  stable fire identity, retry/backoff/exhaustion policy, per-fire attempt
  tracking, observability hooks, configurable cadence/batch/clock, and CAS
  store contract tests. Keep Nanite-specific job types, producers, event-log
  naming, and loop/reflex semantics in Nanite adapters. Update the
  `go-scheduler` docs and this Hadron architecture document once that boundary
  is settled.

## Rough architectural conclusion

Hadron should become a library-backed product:

- a reusable Go workflow engine core;
- a set of optional adapters;
- an evolved `go-scheduler` as the shared timed activation substrate;
- a Hadron app/service that hosts, exposes, and operates workflows;
- compatibility loaders for today's blueprint and pipeline definitions;
- clear embedding contracts for Nanite, Torque, Cerberus, and future apps.

The engine should be extracted around stable semantics: graph IR, validation,
typed values, scheduler, state machine, suspend/resume, and step registry.
Hadron the product should remain the place where users and agents discover,
build, test, register, expose, and operate reusable workflows.
