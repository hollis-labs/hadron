# Wave 03 - Scheduler, Runtime, And Wait Tasks

**Purpose:** implement durable graph execution with ready-queue scheduling,
retries, waits, resumes, and recovery.
**Primary architecture refs:** [execution state and scheduler](../../../architecture/workflow-engine-future-state/04-execution-state-scheduler.md), [waits and callbacks](../../../architecture/workflow-engine-future-state/05-waits-gates-callbacks.md), [ADR 0009](../../../architecture/adr/0009-durable-ready-queue-runtime.md), [ADR 0012](../../../architecture/adr/0012-run-scope-and-execution-target.md).

## W03-T01 - Implement Node Lifecycle State Machine

**Objective:** make node execution status durable and explicit enough for
parallel execution, waits, retries, and diagnostics.

**Concrete work:**

- Implement run and node state transitions in `workflow/runtime`.
- Support statuses for pending, ready, running, waiting, succeeded, failed,
  skipped, canceled, timed_out, crashed, and blocked, with structured reason
  codes for blocked nodes.
- Add attempt records with start time, finish time, executor metadata, error,
  and output refs.
- Validate legal transitions and emit structured events for each transition.
- Add tests for legal and illegal transition paths.

**Acceptance criteria:**

- Runtime behavior never depends on scanning log events for node state.
- Every node invocation has a durable state and attempt history.
- Transition failures are deterministic and recoverable.

**Verification:**

- `go test ./workflow/runtime/...`

## W03-T02 - Implement Durable Ready Queue And Claims

**Objective:** schedule runnable graph nodes from persisted state rather than
holding all scheduling state in memory.

**Concrete work:**

- Add ready-node discovery based on graph dependencies, node status, and store
  state.
- Implement claim/lease operations using the state-store CAS contract.
- Start with FIFO behavior and expose host hooks for priority and per-run
  fairness.
- Ensure worker crashes can leave claims recoverable after lease expiry.
- Add concurrency tests for duplicate claim prevention.

**Acceptance criteria:**

- Multiple workers cannot execute the same node attempt concurrently.
- Scheduler state survives process restart.
- FIFO is the default without preventing later host-provided fairness policy.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/conformance/...`

## W03-T03 - Implement Readiness Rules, Skip, Failure, And Timeout Propagation

**Objective:** encode graph progression semantics once in runtime instead of
duplicating behavior per surface or executor.

**Concrete work:**

- Implement `all_success`, `all_done`, `one_failed`, `all_failed`,
  `none_failed`, and `always`.
- Evaluate node `if` expressions only after readiness is satisfied.
- Mark nodes skipped when data predicates are false.
- Mark waits that exceed timeout as `timed_out`.
- Propagate failures and timeouts to dependents through readiness rules unless
  a catch/error route handles them.

**Acceptance criteria:**

- Failure, skip, and timeout behavior matches the architecture vocabulary.
- A downstream node can intentionally handle failed or timed-out upstream nodes.
- Runtime diagnostics explain why a node is not ready or was skipped.

**Verification:**

- `go test ./workflow/runtime/...`
- Conformance fixtures for each readiness rule.

## W03-T04 - Implement Retry, Backoff, Cancellation, And Fan-Out

**Objective:** provide core runtime mechanics needed by real workflows before
Hadron surfaces depend on them.

**Concrete work:**

- Implement retry attempts, backoff calculation, max attempts, retryable error
  classification, maximum delay, selected error classes, idempotency-key
  enforcement, and timeout interaction.
- Persist delayed retries as timed activations so backoff releases the worker and
  survives process restart.
- Distinguish queue/schedule-to-start, execution/start-to-close, external-wait,
  heartbeat, and schedule-to-close timeouts where executor behavior requires it.
- Implement cancellation propagation from run to nodes, waits, and child calls.
- Implement `for_each` expansion with per-item state, output, error, and
  status collection.
- Support fan-out concurrency limits.
- Default fan-out failure policy to fail on unhandled item failure, with
  count/percentage tolerance options.

**Acceptance criteria:**

- Retried nodes preserve attempt history and final typed outputs/errors.
- Canceling a run stops ready/running/waiting work according to executor
  cancellation contracts.
- Fan-out results are observable per item and as aggregate node output.

**Verification:**

- `go test ./workflow/runtime/...`
- Race/concurrency tests for fan-out claims where practical.

## W03-T05 - Implement Generic `WaitRecord` Suspend/Resume

**Objective:** make waits durable runtime records that release workers and can
be resumed idempotently.

**Concrete work:**

- Define `workflow/wait` contracts for wait kind, correlation key, timeout,
  payload ref, resume schema, resume token/URL, visibility, responder authority,
  and wake source.
- Define the small core `ActivationScheduler` interface used to schedule/cancel
  retry delays, wait timeouts, sleep nodes, callback TTLs, and child observation
  without importing `go-scheduler` or Hadron daemon types.
- Add runtime operations to mark a node waiting, resume by `wait_id`, reject
  duplicate resumes idempotently, and timeout waiting nodes.
- Support optional caller-provided idempotency keys on resume.
- Converge gate submission, message arrival, timer fire, webhook callback, child
  run completion, and typed external signal on the same validated resume path.
- Add app-service extension points for one-shot TTL triggers without making
  triggers part of core.
- Add tests for duplicate resume, late resume after timeout, and invalid token.

**Acceptance criteria:**

- Waiting nodes release workers.
- Duplicate resume calls return the existing accepted result or a structured
  already-resumed response.
- Wait records are generic; human gates and message waits layer on top.

**Verification:**

- `go test ./workflow/wait/... ./workflow/runtime/...`

## W03-T06 - Implement Crash Recovery And Replay

**Objective:** allow Hadron to restart and continue incomplete graph-native
runs from persisted state.

**Concrete work:**

- Add recovery queries for active runs, leased/running nodes, waiting nodes,
  expired leases, and due timers.
- Reconcile running nodes after process crash according to executor metadata
  and host policy.
- Rebuild ready queue from persisted graph/run state.
- Add replay/explain helpers that reconstruct run progress from state and
  events.
- Implement `rerun <run> --from <node>` semantics in the runtime service:
  create replay provenance, reuse journaled upstream values, create new
  invocation attempts from the selected node, and apply effect/idempotency
  policy before re-executing mutating work.
- Add integration tests using the SQLite adapter once W02-T05 exists.

**Acceptance criteria:**

- Restarting the daemon does not strand ready or waiting graph-native runs.
- Replayed state matches persisted run/node/value/event records.
- Replay from a node reuses upstream outputs and records its source run, node,
  plan digest, and policy decision.
- Recovery never reruns a non-idempotent completed attempt.

**Verification:**

- `go test ./workflow/runtime/... ./internal/persistence/...`
- Crash/restart integration test with a waiting node and a ready downstream
  node.

## W03-T07 - Enforce Scheduler Concurrency Resources And Run Policies

**Objective:** enforce bounded execution and resource policy consistently across
all runs rather than limiting concurrency only inside one fan-out node.

**Concrete work:**

- Add a global bounded worker pool over claimed node attempts.
- Add host-configurable semaphores for effect classes, required capabilities,
  named `concurrency_key` resources, per-run limits, and per-node fan-out limits.
- Enforce concurrency keys across runs and expose current holder/waiter state to
  diagnostics.
- Implement run completion policies for fail-fast versus run-to-completion and
  ensure tolerated fan-out failures interact predictably with both modes.
- Apply FIFO by default while exercising the priority and per-run fairness hooks
  created by W03-T02.
- Release all resource claims on success, failure, timeout, crash reconciliation,
  cancellation, and suspension.

**Acceptance criteria:**

- Two runs sharing a configured concurrency key never exceed its limit.
- Effect and capability limits are enforced independently of worker-pool size.
- Fail-fast cancels or skips remaining eligible work according to policy, while
  run-to-completion continues independent branches.
- No terminal or waiting transition leaks a semaphore or worker slot.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/conformance/...`
- Race tests for cross-run keys, cancellation, suspension, lease expiry, and
  fairness under contention.

## W03-T08 - Implement Catch, Finally, Switch, And Error-As-Data Semantics

**Objective:** complete graph-native control flow and cleanup semantics using
ordinary nodes, typed errors, and readiness rules.

**Concrete work:**

- Implement `switch` with ordered first-match arms and an optional default arm.
- Implement catch/error routes with the originating structured error and timeout
  payload in scope.
- Expose `steps.<id>.error` and fan-out item errors as typed values.
- Implement `finally` nodes that run according to declared scope after normal,
  failed, timed-out, crashed, or canceled graph completion and receive status
  context for the scope they clean up.
- Preserve `continue_on_error` as explicit policy sugar over error-as-data and
  readiness behavior.
- Give catch/finally nodes normal retry, output, event, effect, cancellation,
  and diagnostics behavior.

**Acceptance criteria:**

- Switch routing is deterministic and only selected branches become eligible.
- A failed or timed-out node can be handled without losing its typed error.
- Finally nodes execute once per declared scope and cannot be skipped by the
  default upstream-failure rule.
- Run terminal status accounts for handled errors, unhandled errors, and failed
  cleanup according to documented policy.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/compile/...`
- Conformance fixtures for switch defaulting, catch, continue-on-error, timeout
  catch, nested finally scopes, and cleanup failure.

## W03-T09 - Implement Memoization And Pinned-Value Execution

**Objective:** reuse trusted results for safe nodes and support development runs
against explicitly pinned node outputs without bypassing provenance or policy.

**Concrete work:**

- Add `memoize` policy with key expression, maximum age, output schema/digest,
  and cache provenance.
- Permit memoization by default only for `read` and `compute`; require explicit
  executor and policy approval for `materialize` and reject mutating/destructive
  nodes.
- Add runtime pinned-output bindings for named nodes, validating schema, source
  plan compatibility, digest, redaction, retention, and caller authority.
- Record cache hits and pinned values as journaled invocation outcomes with an
  origin distinct from executor-produced values.
- Ensure replay, fan-out, diagnostics, and downstream expressions treat cached
  and pinned results through the ordinary typed value contract.

**Acceptance criteria:**

- A repeated safe node with the same valid memoization key reuses its prior
  output without invoking the executor.
- Expired, schema-incompatible, unauthorized, or effect-unsafe cache entries are
  rejected or missed with structured diagnostics.
- `hadron workflow run --pin <node>=<value-ref>` can be implemented without a
  transport-specific runtime bypass.
- Run inspection distinguishes executed, memoized, replayed, and pinned results.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/values/... ./workflow/conformance/...`
- Integration fixtures for cache hit/miss/expiry, pinned values, replay, fan-out,
  and denied mutating-node memoization.
