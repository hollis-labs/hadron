# Hadron Agent Runtime And Messaging Roadmap

**Status:** in progress  
**Date:** 2026-05-24  
**Audience:** Hadron implementers, agent substrate maintainers, workflow authors

## Why This Exists

Hadron already has first-class blueprint primitives for:

- `agent_launch`
- `message_wait`

What it does not yet have is a production-backed default daemon path for those
primitives.

The next big unlock is to make Hadron **AI-native by default** while keeping it:

- provider-agnostic
- CLI/runtime-agnostic
- PTY/tool-agnostic
- interoperable with the rest of the Hollis stack

This roadmap proposes:

1. using `go-agent-runtime` as Hadron's first real local agent launch substrate
2. keeping Hadron's public blueprint/runtime contract abstract
3. introducing an abstract messaging substrate aligned with `go-messaging`
4. treating Tether as an important first adapter and interoperability target,
   not a hard dependency

## Current State

Hadron today has:

- blueprint/schema support for `agent_launch` and `message_wait`
- execution-layer interfaces:
  - `execution.AgentLauncher`
  - `execution.MessageSource`
- good diagnostics and UI surfaces for launch/wait events
- settings surfaces for:
  - `agent_substrates`
  - `message_substrates`
- a stock-daemon-backed local launcher:
  - `agent_substrates[*].kind = "go_agent_runtime"`
  - provider/runtime binding through `go-agent-runtime/runtimebind`
  - per-session boot directory materialization with injected native files
  - normalized launch handles (`session_id`, `mailbox`, `session_urn`, etc.)
- a stock-daemon-backed local and remote message path:
  - `message_substrates[*].kind = "go_messaging"`
  - `message_substrates[*].kind = "go_messaging_http"`
  - `message_substrates[*].kind = "tether_http"`

That means the baseline production-backed daemon path now exists for both
`agent_launch` and `message_wait`.

The remaining stock-daemon work is now:

- deepen message substrate coverage beyond the first local adapter
- integrate launch boot/profile settings more fully

So the problem is no longer blueprint shape or the absence of a default daemon
path. The remaining problems are **message substrate breadth, autonomous
end-to-end proof, and deeper launch/profile integration**.

## Decision

### 1. Use `go-agent-runtime` For Local Agent Dispatch

Hadron should adopt `go-agent-runtime` as the first-party implementation behind
its `AgentLauncher` interface.

Reasoning:

- `go-agent-runtime` is explicitly a shared launch/boot/turn/session substrate,
  not an app daemon
- it is capability-based and not bound to a single provider or agent system
- it already carries the shared mechanics we want:
  - runtime/provider binding
  - bootdir planting
  - first-turn policy
  - runtime-safe turn framing
  - resume/checkpoint hint vocabulary

This lets Hadron become AI-native without turning the blueprint model into a
provider-specific launch contract.

### 2. Keep Hadron's Launch Contract Abstract

Hadron should keep its current `agent_launch` model abstract:

- `substrate`
- `launch_id`
- `logical_agent_id`
- `prompt_append`
- `injection`
- `metadata`

Do **not** add provider-specific fields like:

- `claude_session_id`
- `codex_thread_id`
- `--resume`-style flags
- provider-specific transport knobs in the blueprint step itself

Those belong in substrate configuration and adapter logic, not in the workflow
document.

### 3. Introduce An Abstract Messaging Substrate

Hadron should not require Tether for `message_wait`.

Instead, Hadron should define its own messaging substrate boundary and align it
with `go-messaging` concepts where possible:

- canonical recipient/sender addressing
- request/reply correlation
- durable inbox semantics
- optional live subscribe/wake behavior

Tether should be a first adapter, not the only adapter.

## Prior Art We Should Reuse

### `go-agent-runtime`

Relevant shared capabilities:

- `runtimebind` for provider/runtime resolution
- `bootdir` for planted native-file/task overlays
- `sessionkit` for first-turn / resume-safe session policy
- `turn` for runtime-aware turn framing
- `checkpoint` for provider session ID / resume hint vocabulary

This is a strong fit for implementing Hadron's `AgentLauncher`.

### `go-messaging`

`go-messaging` already gives us a useful shared contract:

- URN addressing: `msg://<kind>/<authority>/<id>[/<subid>]`
- typed address kinds:
  - `agent`
  - `user`
  - `service`
  - `session`
  - `workflow`
  - `group`
- message kinds:
  - `request`
  - `response`
  - `notice`
  - `status_update`
  - `handoff`
  - `escalation`
- `Store` contract
- blocking request/reply `Dispatcher`
- local-plus-federated routing via `Router`

This is the right interoperability target for Hadron messaging.

### Tether Messaging

Tether is the strongest current app-level prior art:

- durable `/messages/*` mailbox surface
- notify-and-wake behavior
- live session resolution for `msg://session/...` and `msg://agent/...`
- local/federated authority routing
- shared `go-messaging` store contract

Useful ideas to align with:

- canonical `msg://` URNs
- separate durable send from best-effort wake
- agent pull inbox versus operator/UI listing
- live subscribe as optional acceleration, not required durability

Hadron should align with these standards where it helps, while keeping its own
adapter seam.

## Architecture Direction

### Launch Side

Add a Hadron-managed launch substrate layer, for example:

```text
internal/agentsubstrate/
  launcher.go
  runtime_launcher.go
  config.go
```

Responsibilities:

- translate `execution.AgentLaunchRequest` into substrate-specific launch plans
- plant injected files/context via shared bootdir logic
- return durable handles in Hadron's normalized result shape
- surface explicit launch errors and config errors

The first implementation should use `go-agent-runtime` for local dispatch.

### Messaging Side

Add a Hadron-managed message substrate layer, for example:

```text
internal/messagesubstrate/
  source.go
  config.go
  gomsg_source.go
  tether_source.go
```

Responsibilities:

- translate Hadron wait queries into substrate polling or request/reply reads
- keep `message_wait` semantics explicit and timeout-driven
- normalize replies into `execution.Message`
- allow different implementations:
  - local `go-messaging`-backed store
  - Tether `/messages/*` client
  - future provider/service-specific adapters

Status: the stock daemon now implements:

- `message_substrates[*].kind = "go_messaging"` for local durable storage
- `message_substrates[*].kind = "go_messaging_http"` for generic remote
  go-messaging `/messages/*` peers
- `message_substrates[*].kind = "tether_http"` as the Tether-aligned alias

## Standards We Should Adopt

### Addressing

For message-capable substrates, prefer canonical `go-messaging` URNs:

```text
msg://agent/<authority>/<logical_agent_id>
msg://session/<authority>/<session_id>
msg://user/<authority>/<user_id>
msg://service/<authority>/<service_id>
msg://workflow/<authority>/<workflow_id>
```

Hadron does not need to expose the full `go-messaging` model everywhere
immediately, but new message-capable adapters should accept and emit these URNs
where they have recipient identity.

### Correlation

Hadron's current `message_wait.correlation_id` field is still useful and should
remain. Internally, adapters should prefer aligning it with one of:

- `thread_id`
- `in_reply_to`
- substrate-native correlation metadata

depending on the backing transport.

### Durable First, Wake Second

Hadron should separate:

1. durable message existence
2. best-effort wake / live delivery

This matches Tether's useful split and avoids making `message_wait` rely on a
live session or websocket/SSE path for correctness.

## Scope Boundary

### In Scope Now

- local agent launch from Hadron through a configured runtime substrate
- durable launch handles returned into workflow outputs
- message wait over an abstract message substrate
- shared config for launch and message substrates
- first Tether-aligned or `go-messaging`-aligned adapter(s)

### Out Of Scope For This Milestone

- full interactive session management in Hadron
- attach/resume UX
- long-lived turn orchestration inside Hadron
- provider-specific blueprint fields
- requiring Tether to be present for agent-native workflows

Sessions are useful, but they are not required to unlock Hadron's next phase.
The immediate value is launch + durable handle + deterministic wait.

## Proposed Config Shape

Hadron needs new settings surfaces beyond `mcp_servers`.

Suggested shape:

```json
{
  "agent_substrates": {
    "local_runtime": {
      "kind": "go_agent_runtime",
      "provider": "claude",
      "runtime": "streaming-stdio",
      "authority": "hadron",
      "working_dir_mode": "blueprint_dir",
      "allow_generic_subprocess": false,
      "boot": {
        "profile": "hadron.default",
        "callbacks_profile": "shared",
        "plant_native_files": true
      }
    }
  },
  "message_substrates": {
    "local_mailbox": {
      "kind": "go_messaging"
    },
    "tether": {
      "kind": "tether_http",
      "base_url": "http://127.0.0.1:7777",
      "headers": {
        "Authorization": "Bearer ..."
      },
      "timeout_seconds": 30,
      "authority": "agent-mux"
    }
  }
}
```

This shape is illustrative, not final, but the important part is:

- separate launch and messaging config
- explicit substrate kind
- adapter-owned details live in config, not blueprints

## Execution Model

### `agent_launch`

`agent_launch` should:

1. resolve a configured launch substrate by `substrate`
2. prepare bootdir/injection/native files
3. launch the agent
4. return normalized handles, at minimum:
   - `session_id`
   - `logical_agent_id`
   - `result_json`
5. optionally return:
   - `mailbox`
   - `mailbox_urn`
   - `provider_session_id`
   - `resume_hint`
   - any substrate-specific opaque handles

### `message_wait`

`message_wait` should:

1. resolve a configured message substrate by `substrate`
2. poll or block on a reply using:
   - recipient `to`
   - `correlation_id`
   - timeout / poll interval
3. return normalized outputs:
   - `message_id`
   - `body`
   - `body_json`

The substrate may implement this through:

- durable inbox pull
- request/reply dispatcher
- filtered list/read operations
- live subscribe plus durable fallback

## Implementation Slices

### Slice 1 — Roadmap And Settings Design

- land this roadmap
- define `agent_substrates` and `message_substrates` settings structs
- document intended first substrate kinds

### Slice 2 — `go-agent-runtime` Launch Adapter

- add `go-agent-runtime` dependency
- implement a Hadron `AgentLauncher` backed by `go-agent-runtime`
- wire it in `cmd/hadrond/main.go`
- keep result surface normalized and durable

Acceptance:

- stock daemon can run `agent_launch`
- no provider-specific blueprint changes required

### Slice 3 — Real Launch Integration Tests

- add daemon-backed integration tests for configured local launch
- verify bootdir/injection planting
- verify returned session/mailbox/resume handles

### Slice 4 — Messaging Abstraction Design

- expand or wrap the current `MessageSource` seam into a substrate-oriented
  implementation package
- preserve the current execution contract while preparing for richer backends
- decide whether `MessageQuery.To` should formally allow `msg://` URNs as the
  preferred portable shape

### Slice 5 — First Message Adapter

Pick one:

- a lightweight local `go-messaging` store/client path
- a Tether `/messages/*` client adapter

Recommendation: start with the Tether-aligned adapter if it is the shortest
path to a real end-to-end flow, but keep it behind the abstract message
substrate boundary.

### Slice 6 — `agent_launch -> message_wait` End-To-End

- launch an agent through the real launch substrate
- wait for a durable reply through the real message substrate
- capture operation diagnostics across both steps

### Slice 7 — Examples And Docs

- add narrow runnable examples
- document supported substrate kinds
- document launch handles and wait semantics

## Recommended First Adapter Order

1. `go-agent-runtime` launch adapter
2. richer remote message semantics beyond inbox pull
3. timeout unification across `mcp_call`, `agent_launch`, and remote message adapters
4. richer result/wake semantics only after the above are real

## Non-Goals

- make Hadron into a chat runtime
- require Tether for all AI-native workflows
- force `go-messaging` as a mandatory process dependency
- expose provider-specific launch/session details in blueprints
- block the launch work on full session lifecycle support

## Bottom Line

Hadron should become AI-native through **abstract capabilities with shared
substrate standards**, not through one concrete runtime.

The right implementation path is:

- `go-agent-runtime` for launch
- `go-messaging` as the messaging contract to align with
- Tether as a strong first adapter and interoperability target
- Hadron-owned abstractions at the workflow/runtime boundary

That gives us a fast path to real local agent dispatch now, while preserving
the provider/runtime/message neutrality that Hadron needs long term.
