# 09 - Consumer Boundaries And Extraction

## Architectural Change

Hadron app/service is not mandatory infrastructure for every Hollis Labs
workflow use case. The reusable engine core is the shared substrate. Products
choose whether to embed that core or call Hadron app/service.

```text
embed engine: product owns runtime semantics and local policy
call Hadron: product delegates to an operated workflow host
expose tools: product keeps semantics and exposes capabilities to workflows
```

## Boundary Matrix

| Consumer | Owns | Uses engine for | May call Hadron app/service for |
|---|---|---|---|
| Hadron | workflow operation, registry, exposure, schedules, triggers, run inspection | reference host/runtime | not applicable |
| Nanite | agents, sessions, teams, roles, skills, context, tool broker, loops, chat | agent workflows where workflow execution is core runtime | reusable workflow catalog, workflow-building workflows, operated runs |
| Torque | work items, task lifecycle, priorities, checkpoints, escalation | required workflows/checkpoints tied to tasks | cataloged reusable workflows, delegated workflow execution |
| Cerberus | infrastructure resources, workspaces, leases, deployments | orchestration where graph semantics are app-owned | Hadron workflows that operate infrastructure or run in a workspace |
| Tether | sessions, messaging, federation, MCP gateway, identity surfaces | only if workflow runtime is needed inside Tether | message/MCP/session adapters and operated workflows |
| Vanta | memory and knowledge authority | explicit workflow read/write steps if useful | no cache delegation; Vanta remains authority |

## Embed Versus Call

Embed the engine when:

- workflow execution is part of the product's core runtime;
- the product owns policy, state transitions, and presentation;
- waiting/checkpoint behavior needs product-native state integration;
- the product must run without `hadrond`;
- the workflow is a local implementation detail.

Call Hadron app/service when:

- the caller wants an operated workflow runtime;
- registry, exposure, run inspection, schedules, triggers, or UI are useful;
- the workflow should be reusable across products;
- the caller wants durable Hadron run records and transport surfaces.

Expose product capabilities as tools/adapters when:

- the product owns the business semantics;
- workflows need to invoke the capability without absorbing its domain model;
- Hadron should record invocation facts, not own the external state.

## Nanite Boundary

Nanite should embed the engine for agent workflows while keeping Nanite-owned
semantics outside the core:

- agent profiles, sessions, teams, roles, skills, reflexes, chat;
- context assembly and memory policy;
- tool broker and permission engine;
- LLM/tool/flex/loop executors;
- chat/envelope/A2A presentation of gates and waits.

Embedding shape:

```go
engine := workflow.New(workflow.Host{
	Store:     naniteWorkflowStore,
	Executors: naniteStepKinds, // llm, tool, gate, flex, loop adapters
	Timers:    naniteScheduler,
	Policy:    nanitePermissionEngine,
	Telemetry:  naniteEvents,
})
```

Nanite may still call Hadron for reusable workflows or workflow-building
flywheel behavior. It should not require Hadron daemon availability for core
agent-workflow execution.

## Torque Boundary

Torque should embed the engine where workflows participate in work-item
lifecycle:

- task state transitions;
- required workflow policies;
- checkpoints and approvals;
- escalation;
- task-specific scheduling and priorities;
- task-oriented API/MCP surfaces.

Torque gates/checkpoints are not Hadron human-gate rows. They are Torque
domain state over the shared wait/checkpoint mechanics.

## Cerberus Boundary

Cerberus owns infrastructure resources and leases. Hadron may bind a run to a
Cerberus-provided execution target:

```text
Hadron binds run
  -> Cerberus workspace acquire
  -> WorkspaceHandle + WorkspaceLease
  -> target-aware step executors
```

The engine should account for rollback/compensation because infrastructure
operations expose that requirement strongly, but Cerberus remains the authority
for provider-native resources.

## Shared Library Extraction

Extract only stable semantics:

- graph IR and validation;
- source loaders for the graph-native workflow format;
- typed values and expressions;
- ready-queue scheduler;
- durable state machine and wait contracts;
- step-kind registry interfaces;
- conformance suite.

Do not extract:

- Hadron registry/product UX;
- Nanite agent harness and context policy;
- Torque task lifecycle;
- Cerberus provider/resource model;
- transport-specific app surfaces;
- concrete app persistence schemas unless they are intentionally adapters.

## Dependency Guardrails

The shared core must not import application packages. Application adapters may
import the shared core.

```text
allowed:
  hadron/internal/appworkflow -> workflow/runtime
  nanite/internal/agentworkflow -> workflow/runtime
  torque/internal/checkpoints -> workflow/wait

not allowed:
  workflow/runtime -> hadron/internal/persistence
  workflow/runtime -> nanite/internal/service
  workflow/runtime -> torque/internal/tasks
```

## Decision Needed

- Shared library repository and module boundary.
- Which Nanite tests become conformance fixtures.
- Whether Hadron keeps its current engine until extraction or adopts core
  incrementally in place.
- Gate/checkpoint shared package ownership.
- How loop/flex semantics relate to core `for_each`, waits, and child runs.
- Cerberus execution target contract and compensation vocabulary.
