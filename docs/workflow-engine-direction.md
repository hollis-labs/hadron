# Hadron Workflow Engine Direction

**Status:** Directional architecture draft

**Date:** 2026-08-22
**Scope:** Desired product boundaries and architecture; not an implementation plan

## Purpose

This document describes Hadron's intended place in the Hollis Labs portfolio.
It evaluates blueprints, pipelines, execution, scheduling, triggers, human
gates, agent launching, messaging, registry behavior, credentials,
observability, and extension points against the portfolio's shared engineering
boundaries.

It deliberately does not define phases, estimates, task breakdowns, migration
steps, or compatibility strategy. Existing ADRs remain historical and
implementation truth until explicitly superseded. This document provides a
target direction for a later architecture and planning session.

## Portfolio axiom

> Hollis tools own execution and operational state, but not the business
> definitions or business data they operate on.

For Hadron, this means:

- Projects, users, and publishers own blueprint and pipeline definitions,
  workflow intent, input meaning, and produced business artifacts.
- Called systems own the capabilities, resources, and provider-native state
  behind command, HTTP, MCP, message, agent, and future step adapters.
- Credential authorities own secret material and secret lifecycle.
- Hadron owns definition validation and compilation, resolved invocation
  bindings, run and step state, scheduling and trigger registrations, gates,
  cancellation, operational events, and execution provenance.

Hadron may index definitions, retain immutable execution snapshots, cache
metadata, materialize files, and temporarily carry input or output data without
becoming the authoritative owner of that content. Its durable records explain
what it executed and what happened; they do not replace the originating
project, catalog, message system, artifact store, or business application.

## Product definition

Hadron is a local-first deterministic workflow compiler and execution engine.
It:

1. Resolves externally owned workflow definitions and immutable revisions.
2. Parses, validates, and compiles those definitions into an executable plan.
3. Binds a plan to inputs, configuration, capabilities, policy, adapters, and
   an execution target.
4. Executes the bound plan with explicit lifecycle, retry, cancellation, gate,
   and failure semantics.
5. Activates workflows through direct requests, schedules, triggers, and
   parent workflow nodes without creating separate execution engines.
6. Records run state, step invocations, outputs, decisions, diagnostics, and
   provenance.
7. Exposes the same execution model through CLI, desktop, HTTP, MCP, A2A, and
   future transports.

Hadron is not the canonical authoring repository for workflow logic, a task or
sprint manager, a general agent platform, a general message broker, a secret
manager, an infrastructure control plane, or a long-term business artifact
store.

Calling Hadron the portfolio's "execution substrate" is accurate only within
this boundary: it is the reusable substrate for executing declared workflows.
It is not the mandatory execution path for every process, agent, task,
resource, or application in the portfolio.

## Architecture sketch

```text
Project repositories / publishers / operators / calling applications
          authoritative workflow definitions and business inputs
                              |
                              v
                 +--------------------------+
                 |          Hadron          |
                 |                          |
                 | definition resolver      |
                 | validator + compiler     |
                 | invocation binder        |
                 | workflow runner          |
                 | schedules + triggers     |
                 | gates + cancellation     |
                 | events + provenance      |
                 +------------+-------------+
                              |
                    immutable BoundRun
                              |
          +-------------------+--------------------+
          |                   |                    |
          v                   v                    v
    built-in executor   registered adapter   child workflow
    command / gate      MCP / agent / msg    same run model
          |                   |
          +-------------------+--------------------+
                              |
          +-------------------+-----------------------------+
          |                   |               |             |
          v                   v               v             v
   local processes       MCP services     agent/session   external APIs
                                            substrate

Credential authority -------- references / scoped grants --------^

Cerberus -------- optional ExecutionTarget + WorkspaceLease ------^
Tether ---------- optional MCP, messaging, or session adapter ----^
```

The definition remains owned by its publisher. Hadron produces a resolved,
immutable execution binding and owns the lifecycle of that invocation. Each
called substrate retains ownership of its native state and semantics.

## Definition and execution boundary

Hadron needs a hard distinction between authored intent and operational facts.

### Externally owned definitions

These are business definitions and remain authoritative outside Hadron:

- blueprint source
- pipeline source
- imported workflow source
- agent, role, or launch-profile definitions
- schedules and triggers expressed as desired project configuration
- policy declarations and required capabilities
- input and output schemas
- project-specific hooks and commands

Hadron can validate, index, package, cache, pin, and materialize these. It must
retain their source authority and provenance and must not silently turn its
index or installed copy into the canonical authoring location.

### Hadron-owned operational state

These facts exist because Hadron performs its execution contract:

- resolved definition and revision used by a run
- compiled execution plan
- bound inputs and effective non-secret configuration
- adapter and execution-target bindings
- granted capabilities and policy decisions
- run, node, attempt, wait, and cancellation state
- schedule and trigger registration state
- human-gate wait and submitted decision state
- operational output references and digests
- logs, events, diagnostics, timing, and provenance

This state must be durable enough to explain and recover Hadron's own work.
It should not duplicate authoritative state already owned by another app or
provider merely because that state is convenient to display.

## Core domain concepts

### Definition reference

A workflow invocation should begin with a stable reference rather than an
unqualified mutable file path:

```text
DefinitionRef
  authority
  kind
  logical identity
  source locator
  requested revision or version constraint
  resolved immutable digest
  provenance
```

The source may be a project file, an installed package, a remote catalog, an
embedded example, or another registered definition provider. Resolution must
produce an immutable identity for execution. A path is a locator, not a
revision.

### Workflow definition

A workflow definition expresses semantic execution intent:

- metadata and provenance
- typed inputs and outputs
- nodes and dependencies
- conditions and control flow
- failure, retry, timeout, and cancellation policy
- required capabilities and effect classifications
- references to child definitions

The current blueprint and pipeline formats may remain useful authoring forms.
They should share a common semantic model after parsing rather than grow into
independent execution systems.

### Execution plan

Validation and compilation produce an immutable plan:

```text
workflow source
    -> parse
    -> resolve imports and child definitions
    -> validate schemas, graph, capabilities, and policy
    -> normalize into ExecutionPlan
```

The plan contains resolved node identities and dependencies but no secret
values. Different source formats may compile to the same plan. Source-level
features that cannot be represented faithfully must remain explicit extensions
rather than being flattened or interpreted differently by each client.

### Bound run

A run is not merely a path plus an input map. It is the immutable release-like
binding of:

```text
BoundRun =
  ExecutionPlan digest
  + validated input binding
  + effective policy and grants
  + adapter registrations
  + execution target
  + configuration references
  + caller and provenance
```

Secret material is resolved only at the narrow execution boundary and is not
part of the persisted bound-run document. The binding should be explainable
without exposing credentials.

### Node invocation and attempt

A workflow node is definition identity. A node invocation is one occurrence in
a run. An attempt is one execution try for that invocation. Keeping these
identities separate makes retries, resumptions, diagnostics, and idempotency
reasoning explicit.

Common invocation lifecycle states should distinguish at least:

```text
Pending -> Ready -> Running -> Waiting -> Succeeded
                    |    |        |
                    |    |        +-> TimedOut
                    |    +----------> Failed
                    +---------------> Canceled

Pending / Ready --------------------> Skipped
```

Provider-specific status remains available alongside normalized lifecycle
conditions. `Running`, `Waiting`, and `Succeeded` are not interchangeable, and
an asynchronous launch succeeding does not mean the launched work completed.

### Activation registration

Schedules and triggers are activation registrations, not alternate workflow
definitions or execution engines. A registration binds an activation source to
a pinned or explicitly resolved `DefinitionRef`, input mapping, scope, and
authority.

When a project owns a declarative schedule or trigger, Hadron's durable row is
the materialized operational registration. Ad hoc registrations created through
Hadron may be authoritative within Hadron, but that authority must be explicit
rather than inferred from storage location.

### Gate

A human gate is a workflow wait condition. Hadron owns:

- the fact that the run is waiting
- allowed response shape
- timeout and resumption mechanics
- authenticated submission record
- operational provenance

The workflow owner owns what the decision means and who is authorized to make
it. Hadron must enforce a supplied authority policy; it does not become the
business approver.

### Artifact and output reference

Business artifacts should normally live in a project workspace, artifact
store, object store, or calling system. Hadron records typed references,
digests, media types, producer invocation, and retention hints.

Small structured values may be stored inline when that is part of the run
contract. Large files, arbitrary command output, prompts, model responses, and
other potentially sensitive content should not become durable Hadron data by
accident.

## Blueprint and pipeline direction

Blueprints and pipelines express different authoring scales but should compile
through one semantic path:

```text
Blueprint source -----+
                      +--> resolver/compiler --> ExecutionPlan --> runner
Pipeline source ------+
```

A blueprint is a reusable workflow unit. A pipeline is a graph that composes
workflow units with explicit dependency and data-flow bindings. A child
blueprint call and a pipeline stage are both nested plan nodes after resolution,
even if their authoring syntax remains different.

This preserves useful authoring ergonomics without duplicating lifecycle,
policy, retry, cancellation, output, or provenance behavior.

The workflow language should remain narrow. Project scaffolding, package
manager behavior, Git hosting, framework-specific fields, and similar business
semantics do not belong permanently in the universal blueprint object merely
because an early workflow needed them. They should be expressed through:

- ordinary commands where portability is not required
- typed, registered step kinds where a durable semantic contract is useful
- child workflows owned by the relevant publisher
- calls into the portfolio app that owns the capability

Executable definitions are code. They require immutable identity, provenance,
trust classification, capability review, and policy enforcement comparable to
other executable artifacts.

## Step execution and adapter direction

Hadron owns a small execution protocol; adapters own integration mechanics.

A step kind should declare:

- stable semantic name and version
- input and output schemas
- required capabilities
- effect classification
- idempotency and retry characteristics
- supported cancellation and observation behavior
- configuration schema
- executor binding requirements

The common executor lifecycle is approximately:

```text
Validate -> Prepare -> Execute / Wait -> Observe -> Finalize
                              |
                              +-------> Cancel
```

This does not mean every external capability must fit one universal resource or
provider interface. Command execution, an MCP call, an agent launch, a message
wait, and a human gate have different semantics. They share invocation,
authority, diagnostics, and lifecycle mechanics while retaining kind-specific
contracts.

Effect classification should be explicit:

```text
read | compute | materialize | mutate | destructive
```

Retry policy must account for the adapter's idempotency contract. A generic
`retry: 2` cannot safely mean the same thing for a pure read and a destructive
external mutation. Dry-run or preview is meaningful only when the selected step
executor can truthfully support it.

Hadron should prefer official SDKs, official CLIs, and standard protocols in
integration adapters. A generic command or HTTP escape hatch remains valuable,
but it should not cause Hadron to absorb the business semantics of every system
a workflow can invoke.

## Execution targets and workspaces

Hadron has two distinct concepts that must not be conflated.

### Hadron run scope

The current Hadron `workspace_id` groups runs, schedules, triggers, gates, and
pipelines. That is a logical operational namespace. A clearer long-term name
may be `RunScope`, `ProjectScope`, or another term selected during the next
architecture session.

It does not establish a filesystem, compute environment, isolation boundary,
lease, or readiness contract merely because it is called a workspace.

### Compute execution target

An execution target describes where node execution occurs:

```text
ExecutionTarget
  target identity and kind
  filesystem/workdir handle
  process/tool endpoint
  capabilities
  connectivity
  readiness conditions
  lease or expiry
  provenance
```

The local machine is one target. A Coder workspace supplied through Cerberus is
another. A future remote runner may be another. Workflow semantics should bind
to required capabilities, not directly to Coder fields or provider-specific
transport details.

If Hadron uses a Cerberus workspace:

```text
Hadron binds run
       |
       v
Cerberus workspace acquire
       |
       v
WorkspaceHandle + WorkspaceLease
       |
       v
Hadron selects target-aware step executors
```

Hadron owns the run-to-target binding and execution lifecycle. Cerberus owns the
workspace resource and lease. Coder owns provider-native workspace
infrastructure. Provisioned and Ready remain distinct.

## Agentic workflow boundary

`agent_launch`, `message_wait`, and `human_gate` are valid workflow primitives.
Their presence does not turn Hadron into a universal agent platform.

For an agent launch, Hadron owns:

- the workflow invocation and its policy
- resolution of a declared launch substrate
- ephemeral invocation inputs and injected materialization
- returned normalized handles
- correlation with later workflow nodes
- cancellation and observation required by the workflow contract

The project or caller owns the agent definition, role, prompt intent, and
business task. The selected session substrate owns its native session and
process state. When Hadron's built-in local adapter is selected, Hadron may own
that local session operationally for the lifetime of the invocation, but it
still does not own a portfolio-wide agent catalog or the agent's semantic
identity.

The current use of shared `agentkit` capabilities is aligned with this
boundary: shared libraries provide launch, boot, runtime, and session mechanics;
Hadron supplies workflow-specific binding and event semantics. Provider-specific
fields should stay out of the blueprint contract.

Nanite, Tether, Torque, and Hadron may each launch agents for different product
reasons:

- Nanite owns interactive agent construction and operation.
- Tether owns sessions explicitly launched through its session substrate.
- Torque owns agent execution tied to its work-item lifecycle.
- Hadron owns agent invocation as a node in a declared workflow.

The overlap is intentional at the product-semantics layer. Shared mechanics
belong in shared libraries; no application becomes mandatory for the others'
core use case.

## Messaging boundary

Hadron needs durable request/reply correlation so workflows can wait reliably.
A bounded local message substrate is therefore legitimate operational state.

Hadron's built-in messaging responsibility should remain workflow-scoped:

- store or retrieve callback envelopes for Hadron runs
- correlate replies to an invocation or thread
- separate durable delivery from optional wake behavior
- wait, time out, consume, and audit according to workflow policy
- adapt normalized `go-messaging` concepts to local or remote substrates

General cross-application messaging, federation, public identity discovery,
long-lived inbox products, and organization-wide delivery policy belong to
Tether. Hadron composes with Tether through a message adapter when those
capabilities are wanted, while preserving a local substrate so Hadron remains
independently useful.

Message content remains owned by sender and recipient. Hadron's custody and
retention should be limited to what the workflow execution contract requires.

## MCP, HTTP, A2A, CLI, and desktop surfaces

Hadron follows one-core-several-doors:

```text
CLI -------+
Desktop ---+
HTTP ------+--> application services --> execution model
MCP -------+
A2A -------+
```

Transport adapters translate identity, authorization, requests, results, and
streaming behavior. They do not own alternate run semantics.

Hadron's MCP server exposes Hadron workflow capabilities. Its MCP client invokes
external capabilities as a workflow step. These are distinct roles. Tether's
MCP gateway may optionally provide discovery, policy, and observability between
Hadron and upstream servers, but Hadron must still be able to call configured
MCP servers directly.

Likewise, A2A may present publisher-owned blueprints as callable skills. Hadron
does not become the semantic agent or skill publisher merely because it
materializes an agent card and executes the backing workflow. Agent-card
identity and advertised skills must retain source authority and durable task
correlation must map to Hadron's persisted run model rather than a
transport-local in-memory catalog.

## Scheduling and trigger boundary

Hadron scheduling answers one narrow question:

> When should this declared workflow be activated?

It does not own project planning, backlog priority, sprint state, or the
business reason work exists. Those remain Torque or caller concerns.

Schedules and triggers should share these properties:

- explicit owner and scope
- pinned or policy-governed definition resolution
- typed input mapping
- activation deduplication and provenance
- bounded concurrency and backpressure
- clear missed-fire, overlap, and recovery semantics
- explicit enable, disable, expiry, and deletion authority
- dispatch through the same run-binding and execution path as direct requests

Webhook bodies, file events, remote events, and schedule metadata are untrusted
inputs. Extraction and template binding must be typed and bounded before they
reach execution.

The persistent daemon is appropriate for queues, timers, triggers, waits,
subscriptions, and shared local state. The operating system or an external
service manager should supervise the daemon process. Hadron should not invent
its own machine-level daemon supervisor.

## Credentials and secrets

Hadron is not a credential authority and should not own secret lifecycle.

Definitions and settings should carry opaque references:

```text
secret://authority/path#field
```

Resolution belongs at the narrowest adapter or child-process boundary that can
use the value. Supported delivery mechanisms may include:

- delegated execution through an official credential-aware CLI
- environment injection into one child process
- ephemeral request headers constructed immediately before a call
- short-lived files or sockets scoped to one invocation
- provider-native identity or workload credentials
- time-bounded grants from an external credential authority

Hadron may validate that a reference is syntactically valid and resolvable,
record which reference and version were used, and redact all derived
observations. It should not provide general secret `Set`, `Get`, or `Delete`
operations.

Several current surfaces need to be evaluated against this direction:

- MCP, agent, and message substrate settings accept literal environment values
  and headers.
- blueprint templates can read arbitrary daemon environment variables.
- blueprint and step `env` maps can carry plaintext into child processes and
  persisted or displayed representations.
- webhook `secret_hash` currently stores the raw HMAC key required for
  verification rather than a one-way hash.
- MCP bearer credentials can be supplied through process arguments.
- command output and compatibility output markers may echo sensitive values
  into events or logs.

The target contract is reference, resolve, inject, redact, and forget. A
workflow may transport a secret to an authorized operation without Hadron
becoming its owner or durable store.

## Authority, trust, and safety

Workflow execution is Hadron's primary trust boundary.

Each run should have an authenticated caller, source authority, definition
provenance, trust classification, granted capabilities, execution target, and
effective policy. Transport-specific bearer tokens or local-process assumptions
should normalize into this common authority model.

Policy and mechanism remain distinct:

- workflow and operator policy state what may occur
- Hadron enforces invocation and lifecycle policy
- the execution target enforces filesystem, process, network, and resource
  isolation
- external services enforce their own authorization
- credential authorities decide whether secret access is granted

String-substring command deny lists are a useful guardrail, not a security
boundary. Trustworthy enforcement needs structural capability checks, resolved
path checks, sandbox or target policy, and fail-closed adapter behavior. A
declared sandbox or confirmation mode must correspond to real enforcement and
must not be presented as protective while reserved or inactive.

Plugins and executable workflow packages follow:

```text
declared capabilities -> validated request -> explicit grant -> observed use
```

Destructive actions, privilege elevation, network access, credential use, and
writes outside the run target require explicit policy. Failures should explain
the denied capability without leaking protected data.

## Definition registry and packages

The Hadron registry is a discovery, resolution, validation, and provenance
index. It is not the canonical blueprint repository.

For every indexed definition, it should be possible to explain:

- originating authority and source locator
- logical identity and definition kind
- declared version
- immutable content digest
- schema and required capabilities
- publisher and signature or trust evidence where available
- time observed and materialization location
- whether the source is currently reachable

Caching immutable content for reproducibility or offline execution is
reasonable. Such a cache remains derived and digest-addressed. A history row
that contains only a mutable file path and an old hash can prove drift but
cannot reproduce the old execution; the next architecture session should
decide the intended reproducibility guarantee.

Installed packages are materializations. Publishers own package content and
versioning. Hadron owns install validation, local binding, integrity checks,
cache state, and operational provenance.

Discovery and ranking should remain deterministic by default. An LLM may help
interpret ambiguous user intent through an optional external capability, but it
must not silently become the registry's source of truth or definition selector.

## Extension model

Hadron should support configuration-driven registration for classes of
capability that benefit from extension:

- definition sources and package resolvers
- step kinds and executors
- activation and trigger sources
- execution targets
- agent launch substrates
- message substrates
- MCP and external-service transports
- artifact stores and output resolvers
- telemetry and audit sinks
- policy evaluators

The shared Hollis Labs plugin SDK should provide common mechanics:

- manifest and identity
- discovery and registration
- lifecycle and health
- configuration schema
- capability declaration and grants
- transport and process isolation
- version negotiation
- diagnostics and conformance testing

Hadron-specific interfaces should define workflow semantics above those
mechanics. A `StepExecutor`, `TriggerSource`, or `DefinitionSource` should not
be forced into a generic portfolio-wide plugin interface that erases the
meaning Hadron needs to validate and observe it.

Extension registration should be transactional. An invalid plugin must not
leave a partially registered step kind, trigger, schema, or transport surface.
CLI, HTTP, MCP, A2A, and GUI exposure should derive from the same capability
description where practical so adapter surfaces cannot drift independently.

## Observability, audit, and retention

Hadron owns the operational journal of its execution. That journal should make
these questions answerable:

- Who invoked this run, through which surface, and under which authority?
- Which exact definition revisions and imports were executed?
- Which inputs, configuration references, grants, adapters, and target were
  bound?
- Why did each node become ready, skipped, retried, wait, fail, or cancel?
- Which external operation was attempted and what normalized result occurred?
- Which outputs and artifacts were produced, and where are they authoritative?
- Which data was redacted, truncated, expired, or deliberately not retained?

Run events should be append-only operational facts. Derived operation summaries
and UI views should be reproducible from durable state where practical.
Provider-native raw diagnostics may be retained separately with explicit
classification and tighter access.

Logs are event streams, not business output. Child stdout and stderr may be
captured and routed as operational streams, but data exchange should use typed
outputs rather than conventions embedded in log lines. Retention, redaction,
encryption, and size limits must be explicit for:

- run inputs
- stdout and stderr
- step results
- messages
- prompts and model responses
- MCP requests and results
- HTTP bodies and headers
- human-gate decisions
- materialized files and artifact references

Hadron's SQLite database is authoritative for Hadron operational state. It is
not authoritative for external workflow definitions, external messages,
provider sessions, infrastructure resources, or produced business artifacts.

## Daemon and process model

The daemon is a natural product shape because Hadron provides durable queues,
schedules, triggers, waits, cancellation, and multiple concurrent clients.

The daemon owns application-level orchestration and persistence. It should:

- recover Hadron-owned work from durable state after restart
- stop accepting new work during graceful shutdown
- cancel, detach, or recover active child operations according to explicit
  policy
- keep transport clients stateless with respect to durable run truth
- expose health, readiness, and dependency conditions

The daemon does not need to supervise its own installation. Launchd, systemd,
the Windows Service Control Manager, containers, or Cerberus registration can
own machine-level process supervision. One-shot administration such as
migrations, verification, import, repair, and registry maintenance should run
from the same release and authority boundary as the daemon.

Whether Hadron remains a single local authority or supports coordinated
multi-instance execution is a product-semantic decision. Goroutines and worker
pools provide in-process concurrency; they do not by themselves define durable
distributed work claiming or horizontal scale.

## Build, release, and run for workflows

The portfolio's build/release/run principle maps directly onto Hadron:

```text
Build
  resolve + validate + compile definitions into an immutable ExecutionPlan

Release
  bind the plan to inputs, configuration refs, policy, grants, adapters,
  and an execution target to create a BoundRun

Run
  execute the BoundRun without silently changing its definition or binding
```

This does not require every invocation to persist three heavyweight artifacts.
It requires the semantic boundary to be real and the executed revision and
effective binding to be explainable.

Backing services such as SQLite, MCP servers, message substrates, session
runtimes, artifact stores, and Cerberus should be attached through typed
references and adapters. The application should remain restartable from
durable operational state, use explicit port binding, and emit structured logs
to configured sinks.

## Portfolio composition

Hadron composes with other Hollis Labs applications through explicit contracts:

| Application | Application-owned semantics | Hadron composition |
|---|---|---|
| Torque | Work items, sprints, prioritization, task lifecycle, scheduling policy | Torque invokes a pinned Hadron workflow and correlates the run with its work item |
| Nanite | Interactive agents, sessions, teams, roles, skills, conversation experience | Nanite discovers or invokes Hadron workflows without delegating its core agent runtime |
| Tether | Optional session substrate, LLM/MCP gateways, general messaging and federation | Hadron selects Tether adapters when gateway, session, or messaging capabilities are desired |
| Cerberus | Infrastructure resources, providers, workspaces, leases, deployment materialization | Hadron invokes Cerberus operations or binds a run to a Cerberus execution target |
| Vanta | Durable memory and knowledge authority | Workflows read or write through explicit Vanta operations; Hadron does not cache Vanta as workflow truth |

Composition should occur through immutable definition references, run handles,
resource handles, message envelopes, typed outputs, and correlated events. A
shared database or duplicated internal catalog is not a composition contract.

## Current-model strengths

Several current decisions already align well with this direction:

- The daemon owns orchestration and persistence while CLI and desktop remain
  clients.
- Direct runs, schedules, triggers, and pipeline stages converge on the shared
  execution manager.
- Pipeline stages remain ordinary blueprint runs with additional orchestration
  context.
- Blueprint and pipeline parsing already provide schemas, validation, typed
  inputs, hashing, and pin verification.
- The registry indexes external files and retains locators and content hashes
  rather than presenting its database as the authoring source.
- Run events provide an append-only operational audit base.
- Agent, message, and MCP behaviors already sit behind explicit interfaces and
  substrate configuration.
- The live agent launcher uses shared `agentkit` mechanics while keeping a
  Hadron-specific `AgentLauncher` contract.
- `go-scheduler`, `go-messaging`, `go-otel`, provider adapters, MCP libraries,
  and other shared packages are reused instead of rebuilt in Hadron.
- MCP annotations and scoped mutation controls recognize that agent-facing
  execution requires explicit authority.
- Structured `agent_launch`, `message_wait`, and `human_gate` nodes model waits
  and asynchronous work more honestly than shell-command emulation.

## Current-model tensions to revisit

- The phrase "portfolio execution substrate" can imply ownership beyond
  Hadron's workflow-execution boundary.
- The blueprint model contains framework and scaffolding concerns such as PHP,
  Node, packages, Git, stubs, and tool installation that are not universal
  workflow semantics.
- Blueprints, imports, and pipelines have separate source models without one
  explicit compiled execution-plan contract.
- Runs and pipelines primarily identify mutable source paths; the exact
  immutable definition snapshot and full effective binding are not first-class
  run identity.
- Registry version history stores hashes and mutable paths but not enough
  material to reproduce an unavailable historical definition.
- `workspace_id` is a logical namespace but reads like a compute workspace,
  which will conflict with Cerberus/Coder workspace handles.
- Agent substrate configuration retains the legacy `go_agent_runtime` name
  while the live implementation has moved to shared `agentkit` packages and
  in-process session management.
- Local message APIs and storage can grow into a general messaging product
  unless their workflow-scoped boundary from Tether remains explicit.
- MCP, message, and agent settings accept literal environment variables and
  headers, and blueprint templates can read the daemon environment.
- Webhook `secret_hash` is actually raw HMAC secret custody, not a stored hash.
- MCP mutation credentials may be passed in process arguments.
- Full input JSON, message payloads, provider results, and command output can be
  retained without one cross-subsystem data-classification and redaction model.
- Compatibility `::set-output` log parsing mixes the log stream with the typed
  data plane.
- Command substring allow/deny checks, reserved confirmation behavior, and a
  reserved sandbox flag can overstate the effective enforcement boundary.
- A2A task-to-run correlation is held in memory and agent-card publisher
  identity is synthesized by Hadron, despite runs and definitions having
  durable external identities.
- Provider and step kinds are registered through hard-coded configuration
  switches rather than a capability-described extension contract.
- Some current documents still describe `go-agent-runtime` as future or current
  architecture even though live HEAD has adopted `agentkit`; directional
  decisions must be checked against the current tree, not roadmap wording.

These are architectural inputs, not a task list.

## Questions for the next architecture session

1. Should blueprint and pipeline remain separate public definition kinds that
   compile into one `ExecutionPlan`, or should their authoring model converge as
   well?
2. What is the exact `DefinitionRef` and immutable-resolution contract across
   project files, packages, installed caches, and remote catalogs?
3. What reproducibility guarantee does Hadron make when an original definition
   or imported child is changed or disappears?
4. What are the canonical `BoundRun`, node-invocation, attempt, wait, output,
   and provenance envelopes?
5. Which step semantics remain built in, which are Hadron plugins, and which
   should always be expressed as calls to another application or standard
   protocol?
6. What capability, effect, idempotency, retry, preview, and cancellation
   declarations must every executable step kind provide?
7. What replaces or clarifies the current logical `workspace_id`, and how does
   a run bind independently to a local or Cerberus-provided execution target?
8. Which agent-session state does Hadron persist and recover for a local
   `agentkit` launch versus an external Tether or other session substrate?
9. Where is the deliberate boundary between Hadron's workflow callback mailbox
   and Tether's general messaging, federation, and identity services?
10. What secret-reference, runtime delivery, and redaction contract spans
    commands, MCP, HTTP, webhooks, agents, messages, and execution targets?
11. What is authoritative versus materialized for schedules and triggers, and
    what are the overlap, missed-fire, deduplication, and recovery semantics?
12. What data-retention and artifact-reference policies apply separately to
    inputs, logs, outputs, messages, prompts, external responses, and gates?
13. What normalized caller identity, scopes, and approval authority span CLI,
    desktop, HTTP, MCP, A2A, schedules, triggers, and parent workflows?
14. Which plugin SDK mechanics are truly shared and which Hadron-specific
    conformance contracts sit above them?
