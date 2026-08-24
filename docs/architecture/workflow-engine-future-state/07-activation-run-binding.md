# 07 - Activation And Run Binding

## Architectural Change

Replace path-plus-input execution requests with an explicit build/release/run
boundary:

```text
DefinitionRef -> ExecutionPlan -> BoundRun -> Run
```

Schedules, triggers, parent workflow calls, CLI, HTTP, MCP, A2A, UI, and
embedded callers all activate the same run-binding path. They differ in how
they authenticate, map inputs, stream results, and expose state.

## Current Shape

Current app records identify mutable source paths:

- [`persistence.RunRecord`](../../../internal/persistence/records.go) stores
  `BlueprintPath`, status, and input JSON.
- [`persistence.ScheduleRecord`](../../../internal/persistence/records.go)
  stores `BlueprintPath` and `CronExpr`.
- [`persistence.TriggerRecord`](../../../internal/persistence/records.go)
  stores trigger type, path, `BlueprintPath`, extraction config, and ownership.
- [`persistence.PipelineRunRecord`](../../../internal/persistence/records.go)
  stores `PipelinePath`.

Current activation services dispatch into the shared execution path:

- [`internal/scheduler/adapter.go`](../../../internal/scheduler/adapter.go)
  maps `go-scheduler` jobs into `execution.Request`.
- [`internal/trigger/trigger.go`](../../../internal/trigger/trigger.go) maps
  webhook/file/TTL events into `execution.Request`.

That convergence is good. The missing pieces are immutable definition
resolution, effective binding, typed input mapping, activation deduplication,
and durable plan identity.

## DefinitionRef

A workflow invocation begins with a stable reference:

```go
type DefinitionRef struct {
	Authority          string
	Kind               string // blueprint | pipeline | graph | package | registry
	LogicalID          string
	Locator            string
	RequestedVersion   string
	ResolvedDigest     string
	TrustClass         string
	Provenance         Provenance
}
```

A path is a locator, not a revision. Resolution produces an immutable digest
and enough provenance to explain what was executed.

Examples:

```yaml
definition:
  authority: project
  kind: blueprint
  locator: ./workflows/release.yaml
  requested_version: main
```

```yaml
definition:
  authority: registry
  logical_id: torque/task-bulk-create
  requested_version: 1.2.0
  resolved_digest: sha256:...
```

## ExecutionPlan

Build produces an immutable plan:

```go
type ExecutionPlan struct {
	ID              string
	Digest          string
	Definition      DefinitionRef
	Graph           graph.Graph
	InputSchema      SchemaRef
	OutputSchema     SchemaRef
	RequiredCaps     []Capability
	EffectSummary    EffectSet
	SourceDigests    []SourceDigest
	CompiledAt       time.Time
	CompilerVersion  string
}
```

The plan contains no secret material. It is safe to persist, compare, inspect,
validate, and expose to operators according to policy.

## BoundRun

Release binds a plan to caller-specific execution facts:

```go
type BoundRun struct {
	ID              string
	PlanDigest      string
	InputsRef       ValueSetRef
	Caller          Principal
	Authority       AuthorityContext
	Policy          EffectivePolicy
	Grants          []Grant
	Adapters        AdapterBindings
	ExecutionTarget ExecutionTargetRef
	ConfigRefs      map[string]string
	Secrets         []SecretRef // references only, not values
	CreatedAt       time.Time
	Provenance      RunProvenance
}
```

Secret material resolves only at the adapter boundary that needs it. The bound
run should be explainable without exposing credentials.

## Activation Registration

Schedules and triggers are operational registrations over a definition
reference. They are not alternate workflow definitions.

```go
type ActivationRegistration struct {
	ID            string
	Owner         Principal
	Scope         RunScope
	Source        ActivationSource
	Definition    DefinitionRef
	InputMapping   map[string]Expression
	Policy         ActivationPolicy
	Concurrency    ActivationConcurrency
	Deduplication   DeduplicationPolicy
	Enabled        bool
	ExpiresAt      *time.Time
	CreatedAt      time.Time
}
```

Target source sugar:

```yaml
on:
  webhook:
    path: /torque/bulk-create
    extract:
      tasks: body.tasks
  schedule:
    cron: "0 6 * * *"
    overlap: Forbid
    missed_fire_deadline: 10m
  message:
    to: msg://agent/hadron/bulk-create
```

Registering this source materializes operational rows. If the project owns the
source file, the registration is derived operational state. If an operator
creates an ad hoc trigger in Hadron, Hadron may be authoritative for that
registration, but the authority must be explicit.

## Timed Activation Scheduler

The graph scheduler and timed activation scheduler are separate.

The graph scheduler decides which nodes are ready. The activation scheduler
wakes something later:

- cron fire;
- one-shot trigger;
- delayed retry;
- wait timeout;
- sleep node;
- message timeout;
- child-run observation;
- external callback TTL.

The engine core should depend on a small interface:

```go
type ActivationScheduler interface {
	Schedule(ctx context.Context, activation Activation) error
	Cancel(ctx context.Context, activationID string) error
}

type Activation struct {
	ID          string
	Kind        string // schedule | retry | wait_timeout | sleep | callback_ttl
	RunID       string
	NodeID      string
	FireAt      time.Time
	DedupKey    string
	PayloadRef  ValueSetRef
}
```

Hadron can bind that interface to `go-scheduler` plus trigger persistence.
Nanite and Torque can bind equivalent host runtimes.

## Overlap, Deduplication, And Missed Fires

Use standard vocabulary unless an architecture decision rejects it:

```yaml
overlap: Allow | Forbid | Replace
starting_deadline: 10m
catchup: false
idempotency_key: "{{ schedule.id }}:{{ fire.scheduled_at }}"
run_id_reuse: reject | allow_duplicate | terminate_existing
```

These are activation policies. They should be resolved before a bound run is
created.

## Execution Targets

Run scope and compute target are different concepts.

```go
type ExecutionTargetRef struct {
	ID           string
	Kind         string // local | cerberus_workspace | remote_runner
	Capabilities []Capability
	LeaseID      string
	Ready        bool
	Provenance   Provenance
}
```

The local machine is one target. A Cerberus/Coder workspace is another. The
workflow graph binds to required capabilities. The host selects an execution
target that satisfies them.

## Decisions

- Activation policies use standard vocabulary: `Allow`, `Forbid`, and
  `Replace`.
- Activation policy includes `starting_deadline`, `catchup`, and an explicit
  run ID reuse policy.
- `RunScope` is the target architecture term for Hadron's logical operational
  namespace.
- The target architecture does not preserve `workspace_id` as a public concept.
- Compute workspace binding belongs to `ExecutionTarget`, never to `RunScope`.
- Execution stores plan digest, source digests, and enough source
  snapshot/cache material to rerun even if original source files change or
  disappear.

## Decision Needed

- Exact `DefinitionRef`, `ExecutionPlan`, and `BoundRun` envelopes.
- Activation registration authority: project-owned versus Hadron-owned.
- Timer interface between engine core and `go-scheduler`.
- Whether `go-scheduler` owns generic attempt storage contracts or only host
  callback contracts.
- Execution target interface and Cerberus binding.
