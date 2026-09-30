# Workflow authoring and operations

## Source and schema

A graph-native YAML document contains `workflow`, optional `on`, typed
`inputs`, typed `outputs`, `steps`, and optional `finally`. Structural objects
are closed: unsupported fields produce diagnostics instead of being ignored.
Bindings use one explicit mode—`literal`, `expression`, or `interpolation`.
`needs` is the only dependency declaration lowered directly from source; the
production resolver subsequently infers deterministic data edges and value
visibility from static step-output references. See
[go-workflow's source contract](https://github.com/hollis-labs/go-workflow/blob/v0.1.0/compile/SOURCE.md) for the complete
lowering contract.

The authoritative generated JSON Schema is
[published by go-workflow](https://github.com/hollis-labs/go-workflow/blob/v0.1.0/graph/schema/workflow.schema.json).
Hadron consumes it through `github.com/hollis-labs/go-workflow/graph/schema`;
schema generation and compatibility checks run in that module.

Do not edit the schema or copy graph DTOs into a client by hand. The HTTP
schema and TypeScript client consume the same generated graph schema.

## Production host kinds

The stock `hadrond` capability profile registers exactly:

| Kind | Effect | Behavior |
|---|---|---|
| `transform@v1` | `compute` | Evaluate deterministic expressions into declared typed outputs |
| `script@v1` | `compute` | Run bounded deterministic JavaScript in the goja sandbox |
| `sleep@v1` | `read` | Suspend on a durable timer and resume through runtime provenance |
| `wait_for@v1` | `read` | Suspend for a typed authorized callback/continuation |
| `message_wait@v1` | `read` | Suspend for a correlated typed message |
| `human_gate@v1` | `read` | Suspend for an authorized human decision |
| `call@v1` | conservative (`read` through `destructive`) | Start a child workflow as a separately identified run (`mode: run`) |

The executor registry freezes name/version/schema/effects/capabilities before a
plan is admitted. A source cannot narrow those facts. Other repository adapters
are public embeddable contracts, but are unavailable in the stock production
host and therefore fail validation instead of being advertised.

### Child workflows (`call@v1`)

A `call` node resolves its child definition through the same resolver as a
top-level start and completes immediately with the child handle (`run-id`,
`status`, `events-ref`, `cancellation`, `outputs-ref`). Collect the child's
outputs with a separate `wait_for` node:

```yaml
steps:
  - id: launch
    kind: call
    kind_version: v1
    call:
      definition: {kind: file, id: child, locator: child.workflow.yaml, version: v1}
      mode: run
    with:
      message: inputs.message
    idempotency: {mode: keyed, scope: workflow}
    outputs:
      run-id: {type: string}
      status: {type: string}
      events-ref: {type: string}
      cancellation: {type: object}
      outputs-ref: {schema: {type: [object, "null"]}}
  - id: collect
    kind_version: v1
    needs: [launch]
    wait_for:
      child_run: {input: child, fail_on_unsuccessful: true}
      timeout: 1h
      payload_schema: {type: object}
    with:
      child: steps.launch.outputs["run-id"]
```

- Only `mode: run` is supported. `mode: inline` is refused when the
  definition is validated ("inline calls are not supported by hadrond"), so an
  inline call never starts a run.
- Every call-started child passes the same gate as a top-level start before it
  is created: the child plan is validated against the production kinds, policy
  facts are computed under the root run's identity (including execution-target
  capabilities), and the decision is recorded in the policy journal under
  `child-run:<child run id>`. A child the root caller could not start directly
  fails the call step. A child whose policy asks for confirmation runs only
  when the root start was itself confirmed.
- Plans containing a call node always require confirmation to start (the
  child is unresolved at start) and report dry run as unavailable.
- The child terminal status wakes the collecting `wait_for`; only that child
  run may resume it.

### Agent steps (`agent_launch`, Tether-backed)

The compiler recognizes `agent_launch`: it lowers to `call@v1` plus a bundled
child running `agent_session@v1`. `agent_session@v1` needs a durable session
host, and the only one `hadrond` has is the Tether daemon (muxd). Without a
configured `tether_session` agent substrate, validation refuses the step with:

> agent_session@v1 is not enabled in this hadrond yet: agent launch needs a
> durable session host (CW-20260930-0227)

A directly authored `agent_session` node is refused with the same message.
Configuring a `tether_session` substrate lifts the gate: `hadrond` talks to
muxd through go-tether-client at the substrate's `endpoint`, and
`agent_session@v1` joins the production kind set above. The client connects on
demand, so `hadrond` starts even when muxd is down; agent steps then see muxd
as unreachable (below). All `tether_session` substrates must name the same
endpoint, or `hadrond` refuses to start. The local execution
target already grants `agent.session.launch`, `agent.session.observe`, and
`agent.session.cancel`; MCP-started runs still cannot use agent steps.

#### Settings

```json
{
  "agent_substrates": {
    "tether": {
      "kind": "tether_session",
      "tether": {
        "endpoint": "~/.tether/run/muxd.sock",
        "launch": "claude-default",
        "launches": {"reviewer": "claude-review"},
        "result_optional": false,
        "stop_on_result": true,
        "unreachable_timeout": "10m"
      }
    }
  }
}
```

| Field | Default | Meaning |
|---|---|---|
| `launch` | — | Tether `launch_id` for any logical agent not in `launches` |
| `launches` | — | `logical_agent_id` → Tether `launch_id` |
| `endpoint` | `~/.tether/run/muxd.sock` | muxd socket path (`~/` expanded), or `unix:`, `tcp:host:port`, `http(s)://` address |
| `result_optional` | `false` | A session that completes without a result reply succeeds with a JSON `null` result instead of failing `agent_no_result` |
| `stop_on_result` | `true` | Stop the session once its result reply is accepted |
| `unreachable_timeout` | `10m` | How long muxd may stay unreachable before the step fails `agent_host_unreachable` |

Settings validation requires `launch` or `launches` and a positive
`unreachable_timeout`. A step whose `logical_agent_id` has no launch fails
permanently (`agent_unknown_logical_agent`, carried under the step's
`agent_launch_failed` failure).

```yaml
steps:
  - id: review
    agent_launch:
      substrate: tether
      logical_agent_id: reviewer
      prompt_append: Review the change on this branch.
      wait: {timeout: 1h}
outputs:
  verdict:
    type: object
    value: steps.review.outputs.payload.result
```

#### Launch and the result contract

Each agent step creates one keyed Tether session. The Tether idempotency key is
`hadron/<run>/<node>/<iteration or ->/<k>`, where `k` is the first 16 hex
characters of sha256 of the step's idempotency key (capped at 512 bytes by
shortening the run/node/iteration parts). A retry, a lost response, or a
`hadrond` restart replays the same session instead of starting another.

Hadron appends a result contract to the step's prompt:

```text
----- BEGIN HADRON RESULT CONTRACT -----
When your task is complete, send exactly one message with the mux_message_send tool:
  kind: response
  to: msg://agent/hadron/results
  thread_id: <correlation>
  payload: {"nonce": "<nonce>", "result": <your result as a JSON value>}
Do not send the nonce anywhere else. Hadron treats the first valid reply on this thread as your step result.
----- END HADRON RESULT CONTRACT -----
```

The thread is the step's correlation (`agent:<parent run>:<node>`). The nonce
is `hex(HMAC-SHA256(secret, "hadron-agent-result-nonce/v1|" + request digest +
"|" + correlation))`, keyed by a per-Hadron 32-byte secret in
`<data dir>/agent-result-secret.key` (mode 0600, created on first use). The
request digest and correlation are both in the durable session reference, so
observation recomputes the nonce after a restart. Everything sent to Tether is
a pure function of the step's launch request and that secret, because Tether's
idempotency digest covers the prompt: any nondeterminism would turn a replay
into a 409 conflict.

Hadron reads the thread as `msg://agent/hadron/results` (`?as=`), so it sees
only turns to or from that mailbox, and the read has no delivery side effects.
A reply is the step's result only if its kind is `response`, it is addressed to
`msg://agent/hadron/results`, its payload is a JSON object with a string
`nonce` and a `result` key, and the nonce matches (constant-time compare).
Anything else is ignored and logged once, and the step keeps waiting.

**Limitation:** Tether's reply `from` is asserted by the sender and not
authenticated (same-host trust, ADR 0045; tracked with CW-20260918-0037).
Hadron logs it as `from_unverified` and never uses it. The nonce is the only
proof a reply came from the launched session, so a process that can read the
session's prompt can answer for it.

#### How a session maps to the step

Hadron reads the reply thread first, so a result reply wins over any later
session state, including a stop or a muxd restart sweep. Without a valid reply:

| Tether session | Step |
|---|---|
| `created`, `ready`, `launching`, `running` | pending (progress shows `state`, `alive`) |
| `completed` | failed `agent_no_result`, or succeeds with `null` when `result_optional` |
| `killed` | canceled `agent_session_canceled` — Hadron's own cancel, or a human stopping the session in Tether |
| `failed`, exit code -1 | failed `agent_session_lost` (muxd's restart sweep lost the process) |
| `failed`, other exit | failed `agent_session_failed` |
| not found | failed `agent_session_missing` |

These failures are not retryable. Cancelling the run stops the session.

While muxd is unreachable (connection refused, missing socket, or the client's
5-second request timeout) the step stays pending (progress
`state: unreachable`). If it stays unreachable for `unreachable_timeout`, the
step fails `agent_host_unreachable` (retryable). The timer is held in memory
and restarts from zero when `hadrond` restarts.

#### Not supported yet

- Typed inputs: an `agent_launch` with `with:` bindings, or an `agent_session`
  node binding anything other than the reserved `parent-correlation`, is
  refused at validation on a `tether_session` substrate ("agent_session typed
  inputs are not supported by the Tether session host yet"). Put the task in
  `prompt_append`.
- `agent_launch` nodes with `needs:`: go-workflow does not expose `run.id` to
  dependent-node bindings, so the generated correlation cannot bind. Use
  `agent_launch` as a root step.
- Authored `retry:` is not honored by `hadrond`. The session host replays the
  keyed create itself (3 attempts) when muxd is briefly unreachable.

### External operations

Kinds that hand work to an external system suspend it as a durable external
operation. `hadrond` reconciles pending operations every 2 seconds (batches of
100) and stops the loop with the other workflow workers on shutdown. Explicit
run cancellation cancels a suspended external step through its adapter.

Runnable examples are under [`examples/workflow/production`](../examples/workflow/production/).
The HTTP/cmd/MCP files one directory above are explicitly broader compiler or
fake-adapter conformance fixtures.

## Validate, explain, and run

```sh
hadron workflow validate <file|registry-selector> --json
hadron workflow explain <file|registry-selector> --input-json '{}' --json
hadron workflow run <file|registry-selector> \
  --run-id <id> --idempotency-key <key> --input-json '{}' --json
```

Validation cannot start a run. Explain is policy checked and returns the real
immutable plan/effect/capability/blast-radius facts. Run identity and the
idempotency key bind immutable start intent; a replay converges, while changed
definition/input/scope/target intent conflicts. Pins are immutable value refs
included in that digest and bound before any node becomes ready.

File refs retain the clean supplied path as their locator. These operational
commands accept `namespace/name@version` or
`namespace/name@sha256:<digest>`, but not the combined lifecycle grammar.
Catalog inspection and registry/exposure mutations require
`namespace/name@version#sha256:<digest>` so version and digest are bound
together; current aliases are not accepted for exact mutations.

## Durable waits and signals

Wait-capable executors persist a `WaitRecord` and yield the worker. Runtime
recovery reconstructs timers and readiness after restart. Resume proves the
wait belongs to the run, binds the authenticated responder, and checks the
expected correlation, token when present, typed payload schema, wake source,
and responder authority. Duplicate delivery converges by wait identity.

Typed signals use the same application boundary and require their idempotency
key. Gate/message MCP aliases map to the same wait contract; they do not create
transport-private lifecycle.

## Values, artifacts, and redaction

Inputs, node outputs, wait payloads, and final outputs are typed `Value`
records with visibility and provenance. Inline values are bounded; larger data
uses immutable artifact references. Rendering is an explicit projection:

- public values can be displayed;
- private values require authorized private-display access;
- secret references and secret material remain masked in every display mode;
- diagnostics, events, HTTP/MCP/A2A results, and transport errors use bounded
  safe DTOs rather than raw runtime records.

Inspect with `hadron workflow inspect <run-id> --json`. Add
`--reveal-private` only when the caller is authorized and needs it; this does
not reveal secrets.

## Registry, qualification, and exposure

The lifecycle is `search → draft → validate → scaffold → contract test →
register → package → exact registry pin → publish → exposure-profile pin`.
Contract tests are deterministic evidence tied to the exact plan. Package
responses contain safe metadata, not raw source, credentials, or mock payloads.

Three states are independent:

1. `current` is the movable catalog alias.
2. A registry-version pin qualifies one exact immutable version and is a
   publication prerequisite.
3. An exposure-profile pin makes one exact published definition directly
   visible as a tool, subject to profile generation CAS, effects, collision,
   and budget preflight.

Removing an exposure pin removes the direct tool on session reconciliation;
it does not move `current` or unqualify the registry version.

## Source activations

`on:` declarations compile into immutable plan activations. Registering a
workflow as current materializes source-owned registrations through the shared
activation service. Moving/clearing current retires the prior projection, and
fire-time admission is fenced against the movable catalog alias so a stale
durable row cannot start the wrong version.

The public external fire route accepts only payload-ingress activation kinds,
requires the authorized exact registration handle returned by lifecycle
detail, validates occurrence/receipt time, and executes as the registration's
durable principal. Schedules and timers fire internally; they are not generic
external trigger handles.

## Troubleshooting

For a fuller graph-native operator walkthrough, see
[Workflow diagnostics and recovery](workflow-diagnostics.md).

- **`unknown command` for `run`/`validate`:** use `hadron workflow run` or
  `hadron workflow validate`; legacy root commands were retired.
- **unknown step kind:** compare the plan with the six production kinds above.
  A conformance example using HTTP/cmd/MCP is not daemon-runnable.
- **policy confirmation required:** inspect `workflow explain`, then repeat with
  `--confirm` only after reviewing the exact effects and target.
- **hidden record looks missing:** intentional. Hidden and nonexistent records
  use the same safe not-found response.
- **run is waiting:** inspect the safe wait descriptor and resume using its
  exact wait/correlation/source contract and an authorized responder.
- **private output is masked:** request authorized private display; secret
  material cannot be unmasked.
- **daemon reports unavailable/recovering:** wait for host recovery. Health is
  backed by the shared workflow host rather than a hard-coded OK response.
