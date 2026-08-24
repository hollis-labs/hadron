# 04 - Execution State And Scheduler

## Architectural Change

Replace sequential blueprint execution and pipeline level barriers with a
ready-queue scheduler over durable node invocation state.

The core runtime owns the run and node state machine. Hosts provide storage,
workers, timers, executors, policy, and telemetry.

## Current Shape

Blueprint execution is sequential:

- [`internal/execution/manager.go`](../../../internal/execution/manager.go)
  starts fixed workers that consume `execution.Request`.
- `executeFile` loops over sections and steps in order.
- `evaluateCondition` is a truthy-string check over pre-rendered values.
- `call` recurses into the parent execution and returns no value.

Pipeline execution is graph-shaped but level-barriered:

- [`internal/pipeline/toposort.go`](../../../internal/pipeline/toposort.go)
  returns `[][]Stage` levels.
- [`internal/pipeline/runner.go`](../../../internal/pipeline/runner.go)
  starts goroutines for a level and waits for the whole level before the next
  level can run.

There is no durable per-node table today. `runs`, `pipeline_runs`,
`pipeline_stage_runs`, and `run_events` are app-level records, not a reusable
engine state contract.

## Target Runtime State

The runtime stores run, node invocation, attempt, value, wait, and event facts.

```go
type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunWaiting   RunStatus = "waiting"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
	RunCanceled  RunStatus = "canceled"
	RunTimedOut  RunStatus = "timed_out"
	RunCrashed   RunStatus = "crashed"
)

type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepReady     StepStatus = "ready"
	StepRunning   StepStatus = "running"
	StepWaiting   StepStatus = "waiting"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
	StepCanceled  StepStatus = "canceled"
	StepTimedOut  StepStatus = "timed_out"
	StepCrashed   StepStatus = "crashed"
)

type NodeInvocation struct {
	RunID       string
	NodeID      string
	Iteration   string // empty for non-for_each nodes
	Attempt     int
	Status      StepStatus
	InputsRef   ValueSetRef
	OutputsRef  ValueSetRef
	Error        *StepError
	WakeOn       *WaitRef
	StartedAt   time.Time
	EndedAt     time.Time
	UpdatedAt   time.Time
}
```

The app may store this in SQLite. The core sees a `StateStore` interface.

```go
type StateStore interface {
	CreateRun(ctx context.Context, run BoundRun) error
	LoadRun(ctx context.Context, runID string) (RunSnapshot, error)
	ListReady(ctx context.Context, limit int) ([]NodeInvocation, error)
	ClaimNode(ctx context.Context, id NodeInvocationID, token string) (bool, error)
	MarkRunning(ctx context.Context, id NodeInvocationID, attempt int) error
	MarkWaiting(ctx context.Context, id NodeInvocationID, wait WaitRecord) error
	MarkTerminal(ctx context.Context, id NodeInvocationID, result StepResult) error
	AppendEvent(ctx context.Context, event Event) error
	SaveValues(ctx context.Context, owner ValueOwner, values map[string]Value) (ValueSetRef, error)
}
```

The exact store API is open. Required properties are durable recovery,
idempotent resume, compare-and-swap claims, and explainable state transitions.

## Ready-Queue Scheduler

The scheduler should enqueue nodes as soon as their dependencies are satisfied.
It should not wait for unrelated nodes from the same topological level.

```go
func DriveRun(ctx context.Context, run RunSnapshot) error {
	for _, node := range run.Nodes {
		if node.Status != StepPending {
			continue
		}
		if !dependenciesSatisfied(run, node) {
			continue
		}
		if !readyRuleSatisfied(run, node) {
			markSkippedOrBlocked(node)
			continue
		}
		if !ifExpressionTrue(run, node) {
			markSkipped(node)
			continue
		}
		enqueueReady(node)
	}
	return nil
}
```

Worker execution is bounded by:

- global worker pool;
- per-effect limits;
- per-capability limits;
- configured `concurrency_key` semaphores;
- per-run and per-node fan-out limits;
- host policy.

## Readiness And Failure Semantics

Default readiness should be skip-on-upstream-failure: if a dependency fails,
dependents are skipped unless they explicitly opt into failure-aware behavior.

Examples:

```yaml
- name: notify-failure
  needs: [deploy]
  ready_when: one_failed
  if: steps.deploy.status == "failed"
  mcp_call:
    server: slack
    tool: post_message
```

```yaml
finally:
  - name: cleanup-workspace
    cmd: "./scripts/cleanup.sh"
```

`finally` nodes are nodes, not shell hooks. They get retries, policy, events,
outputs, and cancellation semantics like every other node.

## Fan-Out

`for_each` expands node invocations at run time, not compile time. The plan
stays static while invocation rows represent each item.

```go
type ForEachSpec struct {
	ItemsExpression string
	MaxConcurrency  int
	Tolerate        ToleratedFailurePolicy
	ItemName        string
	IndexName       string
}
```

Fan-out produces a collected `items` output with one entry per item:

```json
{
  "items": [
    {"index": 0, "status": "succeeded", "outputs": {"id": "T-1"}},
    {"index": 1, "status": "failed", "error": {"code": "rate_limited"}}
  ]
}
```

This supports bulk workflows without hiding per-item retries and diagnostics.

## Retry, Timeout, And Recovery

Retry policy must be effect-aware:

```yaml
retry:
  attempts: 3
  backoff: exponential
  max_delay: 30s
  on: [timeout, 5xx, rate_limited]
  idempotency_key: "{{ inputs.project_id }}:{{ item.title }}"
```

Rules:

- `read` and `compute` can retry freely within policy.
- `materialize` requires target-specific cleanup or idempotent writes.
- `mutate` requires an idempotency key or explicit policy grant.
- `destructive` retries are denied unless a specific executor declares them
  safe and policy permits them.
- recovery after daemon restart treats `running` nodes according to effect and
  idempotency metadata.

Timeout taxonomy should distinguish queue wait, execution duration, external
wait duration, heartbeat absence, and schedule-to-close where those meanings
matter.

## Replay

Durable values make replay possible:

```text
hadron rerun <run-id> --from <node-id>
```

Replay reuses persisted upstream outputs, creates new invocation attempts from
the selected node, and records provenance that the run is a replay. Policy can
block replay across `mutate` or `destructive` effects unless inputs and
idempotency guarantees are explicit.

## Decisions

- Use a high-level runtime state-store interface rather than SQL-shaped APIs or
  a purely event-sourced core.
- The store contract exposes runs, node invocations, attempts, waits, values,
  events, and compare-and-swap claims.
- Hadron SQLite is one adapter behind that interface.
- Scheduler fairness is host-configurable with FIFO as the default.
- The first contract supports priority and per-run fairness hooks without
  requiring a complex scheduler on day one.
- Readiness uses Airflow-style named rules. `all_success` is the default; other
  rules include `all_done`, `one_failed`, `all_failed`, `none_failed`, and
  `always`.
- `if` is a data predicate evaluated after readiness is satisfied.
- Fan-out defaults to fail on unhandled item failure.
- Fan-out can explicitly tolerate failures by count or percentage.
- Fan-out always collects per-item status, output, and error data for downstream
  handling.

## Decision Needed

- Exact state-store method names and claim token semantics.
- Concurrency-key declaration and host configuration.
- `finally` representation and scoping.
- Fan-out expansion limits.
- Recovery behavior for previously `running` mutating nodes.
- Replay policy and provenance envelope.
