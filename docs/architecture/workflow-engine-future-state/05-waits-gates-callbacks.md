# 05 - Waits, Gates, And Callbacks

## Architectural Change

Replace worker-blocking polling waits with generic durable suspend/resume
semantics. A waiting node persists a wait record, releases the worker, and is
resumed by a correlated activation.

Human gates, message waits, sleep timers, external callbacks, and child-run
completion all become domains over the same wait contract.

## Current Shape

`human_gate` creates a row and polls it while holding a worker:

- [`internal/execution/human_gate.go`](../../../internal/execution/human_gate.go)
  creates a `human_gates` row, emits `human_gate_waiting`, then loops until
  the row is decided or timed out.

`message_wait` polls a message source while holding a worker:

- [`internal/execution/message_wait.go`](../../../internal/execution/message_wait.go)
  calls `MessageSource.PollMessage`, emits `message_wait_poll` repeatedly, and
  returns when a matching message is found or timeout occurs.

Hadron already has useful persistence pieces:

- [`internal/persistence/migrations/0012_human_gates.sql`](../../../internal/persistence/migrations/0012_human_gates.sql)
- [`internal/persistence/migrations/0013_messages.sql`](../../../internal/persistence/migrations/0013_messages.sql)
- [`internal/persistence/migrations/0006_triggers.sql`](../../../internal/persistence/migrations/0006_triggers.sql)

The missing abstraction is a generic wait state and wake path owned by the
runtime.

## Target Wait Contract

```go
type WaitRecord struct {
	ID            string
	RunID         string
	NodeID        string
	Iteration     string
	Type          WaitType // gate | message | timer | callback | child_run | signal
	CorrelationID string
	ResumeSchema  SchemaRef
	TimeoutAt     time.Time
	PayloadRef     ValueSetRef
	Visibility    Visibility
	Status        WaitStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type ResumeRequest struct {
	WaitID        string
	CorrelationID string
	Responder     Principal
	Payload       map[string]any
	IdempotencyKey string
	ReceivedAt    time.Time
}
```

A step executor can suspend:

```go
func (e *HumanGateExecutor) Execute(ctx context.Context, inv Invocation) (StepResult, error) {
	wait := WaitRecord{
		Type:          "gate",
		CorrelationID: inv.RunID + ":" + inv.NodeID,
		ResumeSchema: e.decisionSchema(inv),
		TimeoutAt:     inv.Now().Add(inv.Config.Duration("timeout")),
		PayloadRef:     inv.StorePromptAndOptions(),
	}
	return runtime.Suspend(wait), nil
}
```

The runtime stores `StepWaiting`, writes the wait record, schedules any timeout
activation, emits an event, and releases the worker.

## Wake Sources

Every wake source should converge on one resume path:

```text
gate submit --------+
message arrival ----+
timer fire ---------+--> Resume(wait_id/correlation_id, payload) --> re-drive run
webhook callback ---+
child run terminal -+
external signal ----+
```

The resume path validates correlation, idempotency, payload schema, authority,
timeout, and cancellation state before marking the node ready to continue.

## Human Gates

Human gates are a domain package over generic waits, not an engine primitive
hard-wired to Hadron's table.

Split:

- engine core: wait state, suspend, timeout, cancel, resume, idempotency;
- gate/checkpoint package: prompt, options, decision schema, responder,
  escalation vocabulary, authority hook;
- Hadron app/service: HTTP/MCP/CLI/UI submission and Hadron persistence
  adapter;
- Nanite: chat/envelope/A2A presentation and harness policy;
- Torque: task lifecycle, checkpoint policy, required-workflow enforcement,
  escalation, and task-state integration.

Target source shape:

```yaml
- name: approve-release
  human_gate:
    environment: production
    prompt: "Release {{ inputs.version }}?"
    options:
      - id: approve
        label: Approve
      - id: reject
        label: Reject
    timeout: 24h
  outputs:
    decision:
      type: string
      enum: [approve, reject]
```

`environment` names a policy subject. Hadron enforces supplied policy; it does
not become the business approver.

## Message Waits And Callbacks

`message_wait` becomes a typed signal wait:

```yaml
- name: wait-for-review
  wait_for:
    signal: review-reply
    correlation_id: steps.launch.outputs.correlation_id
    timeout: 30m
    payload_schema:
      type: object
      required: [approved]
      properties:
        approved: { type: boolean }
```

Hadron's local message substrate remains workflow-scoped. Tether or another
message system can be an adapter when broader identity, federation, or delivery
policy is needed.

External callback waits can be represented as one-shot activations:

```yaml
- name: wait-for-provider
  wait_for:
    callback:
      path: /callbacks/provider/{{ run.id }}/{{ node.id }}
      ttl: 1h
      payload_schema: provider_result_v1
```

The app/service materializes the callback endpoint or one-shot trigger. The
core only sees a wait resume with validated payload.

## Timer Waits

Sleep and delayed retries should not occupy workers.

```yaml
- name: pause
  sleep: 15m
```

The runtime writes a timer wait and asks the host activation scheduler to wake
the node later.

## Cancellation And Timeout

Cancellation closes outstanding waits. Timeout is a state transition, not a
poll loop result.

```text
Waiting --resume accepted--> Ready/Running
Waiting --timeout---------> TimedOut or catch route
Waiting --cancel----------> Canceled
Waiting --run failed------> Canceled
```

Timeout payloads should be available to `catch` and `on_error` routes the same
way executor failures are.

## Decisions

- Core owns generic wait semantics and a `WaitRecord` contract.
- Hadron may materialize callback wakes as one-shot TTL triggers, but that is an
  app-service implementation detail, not the core wait model.
- Resume transitions are idempotent by `wait_id`, with an optional
  caller-provided idempotency key.
- Duplicate resumes return the existing accepted result or a structured
  already-resumed response.
- Generic wait semantics live in core.
- A shared gate/checkpoint package defines prompt, options, decision, and
  escalation vocabulary above the generic wait contract.
- Product policy and presentation stay product-owned.
- Wait timeout marks the node `timed_out`.
- Default timeout behavior fails dependents through readiness rules unless a
  `catch` or error route handles the timeout.

## Decision Needed

- Wait record schema and persistence location.
- Gate/checkpoint package naming.
- How authority is represented for human decisions.
- Message substrate boundary between Hadron local messages and Tether.
- Callback URL and correlation model.
