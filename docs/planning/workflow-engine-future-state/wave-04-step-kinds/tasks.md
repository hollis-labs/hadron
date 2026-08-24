# Wave 04 - Step-Kind And Executor Tasks

**Purpose:** implement the initial built-in step kinds through the registry
contract, with typed outputs and declared effects.
**Primary architecture refs:** [step kinds and executors](../../../architecture/workflow-engine-future-state/06-step-kinds-executors.md), [values and artifacts](../../../architecture/workflow-engine-future-state/03-values-expressions-artifacts.md), [ADR 0010](../../../architecture/adr/0010-step-executor-registry.md).

## W04-T01 - Harden Step-Kind Runtime Integration

**Objective:** replace hard-coded execution dispatch with a registry-driven
executor model.

**Current code refs:** `../../../../internal/execution/manager.go`,
`../../../../internal/execution/http_call.go`,
`../../../../internal/execution/mcp_call.go`.

**Concrete work:**

- Extend the W00-T06 skeleton with runtime execution context, typed result,
  retry classification, wait result, and cancellation behavior.
- Include metadata for config schema, input/output schema, effects,
  idempotency, retryability, cancellation support, and wait behavior.
- Implement registry lookup by kind and version for real adapters.
- Connect runtime dispatch to registry lookup and step-kind execution.
- Persist external-operation references and drive optional observe/progress,
  heartbeat, cancel, and finalize hooks through the same recovery-aware runtime
  path.
- Add conformance tests for duplicate registration, unknown kind, invalid
  config, missing metadata, and optional lifecycle behavior.

**Acceptance criteria:**

- Runtime executes nodes through the registry, not switch statements over
  legacy blueprint step fields.
- Step kinds can live in adapters without changing core runtime code.
- The compiler can validate node config through registered specs.

**Verification:**

- `go test ./workflow/stepkind/... ./workflow/runtime/...`

## W04-T02 - Implement `transform` Executor

**Objective:** provide a pure data transformation node for summaries, reshapes,
and derived workflow outputs.

**Concrete work:**

- Implement `transform` using the expression engine from W02-T02.
- Accept named output expressions and return typed `Value` outputs.
- Mark effects as pure/read-only.
- Reject side-effecting or unsupported operations.
- Add fixtures for maps, filters, aggregates, missing references, and type
  errors.

**Acceptance criteria:**

- Transform nodes can summarize fan-out item outputs.
- Transform execution is deterministic for a given input context.
- Errors are source-mapped and do not appear as log-scraped outputs.

**Verification:**

- `go test ./workflow/adapters/... ./workflow/values/...`
- Use the actual adapter package path selected by implementation.

## W04-T03 - Implement `cmd` Executor

**Objective:** preserve command execution as a step kind while moving outputs
to typed values and making effects explicit.

**Current code refs:** `../../../../internal/execution/manager.go`,
`../../../../internal/execution/execution.go`.

**Concrete work:**

- Implement command config validation for command, args, cwd, env refs,
  timeout, output capture, and declared effects.
- Enforce structured executable/path/cwd/capability/sandbox policy after binding;
  do not rely only on command substring allow/deny checks.
- Return typed outputs for exit code, stdout/stderr capture refs or inline
  snippets, and structured capture outputs when configured.
- Support explicit `json`, `lines`, and `kv` capture parsers with output-schema
  validation.
- Keep PTY/log streaming as operational events, not workflow data flow.
- Make `::set-output` parsing unavailable in target profile unless explicitly
  enabled for one selected node stream as a compatibility-origin capture; emit
  diagnostics and never scan the global event log.
- Add tests for success, non-zero exit, timeout, cancellation, and redaction.

**Acceptance criteria:**

- Command nodes no longer require log scanning to produce outputs.
- Command effects and idempotency expectations are declared and visible to
  policy.
- Existing command execution behavior that remains useful is reachable through
  the adapter.

**Verification:**

- `go test ./workflow/adapters/cmd/... ./workflow/runtime/...`

## W04-T04 - Implement `http` Executor

**Objective:** provide typed HTTP calls without coupling core runtime to HTTP
transport/server packages.

**Current code refs:** `../../../../internal/execution/http_call.go`.

**Concrete work:**

- Implement config validation for method, URL, headers, body, auth/secret refs,
  timeout, retry safety, and expected response schema.
- Replace the legacy local-only restriction with explicit network destination,
  redirect, credential, capability, and host policy.
- Return typed outputs for status, headers, body, parsed JSON, and artifact refs
  for large/binary bodies.
- Apply redaction to request/response events.
- Map network, protocol, and schema errors to retryable/non-retryable classes.

**Acceptance criteria:**

- HTTP executor can be registered as an adapter and used by the compiler.
- Response data is available through typed outputs.
- Secrets are resolved only at adapter boundary and are not persisted.

**Verification:**

- `go test ./workflow/adapters/http/...`
- Local test server integration tests.

## W04-T05 - Implement `mcp` Executor

**Objective:** support MCP tool calls as workflow nodes while keeping MCP
client/server dependencies out of core.

**Current code refs:** `../../../../internal/execution/mcp_call.go`,
`../../../../internal/mcpadapter`.

**Concrete work:**

- Implement config validation for server ref, tool name, arguments, timeout,
  idempotency key, and expected result shape.
- Map MCP tool annotations into effects, idempotency, and retry hints when
  available.
- Return typed outputs for structured result content, text content, artifacts,
  and tool metadata.
- Add redaction handling for arguments and results.
- Keep Hadron MCP server exposure separate from MCP client execution.

**Acceptance criteria:**

- The engine core does not import MCP packages.
- MCP executor can call a tool and return typed outputs to downstream nodes.
- Effect metadata is visible to validation and policy.

**Verification:**

- `go test ./workflow/adapters/mcp/... ./internal/mcpadapter/...`

## W04-T06 - Implement `call` Executor

**Objective:** make child workflows a first-class graph node instead of a
separate pipeline engine.

**Current code refs:** `../../../../internal/pipeline`,
`../../../../internal/execution/manager.go`.

**Concrete work:**

- Resolve `DefinitionRef` through host definition resolver.
- Support path, registry name, version/digest, and package references; record the
  resolved child digest and provenance on the parent invocation.
- Preserve import/default `with:` bindings as partial application before
  node-local call inputs are evaluated.
- Support `call.mode: inline` to execute child graph inside the parent run and
  return declared child outputs.
- Support `call.mode: run` to create child run identity with status, events,
  cancellation handle, and output refs.
- Enforce call-depth and cycle policies.
- Propagate parent closure according to explicit `cancel | abandon |
  request_cancel` policy and make asynchronous child handles available as typed
  outputs for `wait_for`.

**Acceptance criteria:**

- Pipeline-stage semantics can be represented by `call.mode: run`.
- Blueprint-call semantics can be represented by `call.mode: inline`.
- Child outputs are typed and visible through declared bindings.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/compile/...`
- Integration fixture with nested calls in both modes.

## W04-T07 - Implement Wait-Backed Executor Set

**Objective:** implement `sleep`, `wait_for`, `human_gate`, and `message_wait`
through core waits so waiting nodes do not occupy workers.

**Current code refs:** `../../../../internal/execution/human_gate.go`,
`../../../../internal/execution/message_wait.go`,
`../../../../internal/messagesubstrate`,
`../../../../internal/persistence/human_gates.go`,
`../../../../internal/persistence/messages.go`.

**Concrete work:**

- Implement `sleep` as a timer wait.
- Implement `wait_for` as a generic external condition or callback wait.
- Support typed signal, callback, event, and child-run-completion wake sources
  through `wait_for`.
- Implement `human_gate` using the shared gate/checkpoint package over
  `WaitRecord`.
- Define the shared gate/checkpoint package contract for prompt, options and
  resume schema, decision, responder, authority hook, environment/policy
  subject, correlation, and escalation vocabulary.
- Support environment-bound gates and non-blocking optional manual gates without
  embedding approver lists or product policy in workflow core.
- Implement `message_wait` through the message substrate adapter and generic
  wait resume path.
- Emit typed outputs for decision/message payload, timeout status, and resume
  metadata.

**Acceptance criteria:**

- No wait-backed executor polls while holding a worker.
- Duplicate, late, and unauthorized resumes are handled through runtime wait
  semantics.
- Gate/message presentation policy stays in Hadron-owned packages.

**Verification:**

- `go test ./workflow/wait/... ./workflow/adapters/gate/... ./internal/messagesubstrate/...`
- Integration tests for sleep, gate approval, message wake, and timeout.

## W04-T08 - Implement Node Verification Modifier

**Objective:** make verification an ordinary post-execution modifier for
eligible node kinds rather than a separate workflow engine or LLM-only special
case.

**Concrete work:**

- Define verification specs, verifier registry, typed verification result, and
  source-mapped diagnostics in adapter/runtime contracts.
- Support deterministic checks such as `no_error`, output-schema validation,
  output predicates, expected tool calls, tests, and lint where an adapter can
  provide the evidence.
- Persist literal tool-call/activity evidence so verification does not rely on
  executor or model self-report.
- Run verification after executor output is available but before the invocation
  becomes succeeded; route verification failure through retry/catch/error
  policy as a model/decision failure distinct from infrastructure failure.
- Allow later agent/LLM reviewer adapters without importing providers into core.
- Add policy and effect checks so verification cannot silently repeat unsafe
  side effects.

**Acceptance criteria:**

- `verify` can modify an MCP/tool-like node and later the `llm` node through the
  same runtime contract.
- Verification evidence and result are persisted and visible in diagnostics.
- Unknown checks, missing evidence, and unparseable reviewer results fail closed.
- Provider/executor failures and verification/model-decision failures remain
  distinct states and error codes.

**Verification:**

- `go test ./workflow/stepkind/... ./workflow/runtime/...`
- Conformance fixtures for deterministic pass/fail, expected tool-call evidence,
  retry interaction, catch routing, and fail-closed reviewer parsing.
