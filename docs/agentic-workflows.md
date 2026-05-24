# Agentic Workflows With Hadron

**Status:** implementation audit and operator guide  
**Date:** 2026-05-24  
**Audience:** Hadron users, workflow authors, and agent integrators

Hadron now has first-class structured workflow steps for agent-facing work, and
the stock daemon backs all five structured step kinds in some production form.

## What Exists Today

Hadron currently supports these structured step kinds in the blueprint model:

- `http_call`
- `mcp_call`
- `message_wait`
- `agent_launch`
- `human_gate`

It also supports the pipeline controls added for longer-lived workflows:

- `defaults.stage_wait_timeout_seconds`
- `stages[].wait_timeout_seconds`
- `stages[].async`

## Production Status Matrix

| Primitive | Blueprint/schema support | Execution implementation | Backed in `hadrond` today | Notes |
|---|---|---|---|---|
| `http_call` | yes | yes | yes | local-only HTTP; loopback targets only |
| `mcp_call` | yes | yes | yes | local Hadron adapter plus configured external MCP servers |
| `human_gate` | yes | yes | yes | resumable through REST, CLI, and MCP |
| `message_wait` | yes | yes | yes | local `go_messaging` substrates are daemon-backed |
| `agent_launch` | yes | yes | yes | local `go_agent_runtime` substrates are daemon-backed |

That means the current user-facing agentic workflow surface is:

1. use `http_call` for bounded local daemon/API probes
2. use `mcp_call` for internal or external MCP tools
3. use `human_gate` for operator decisions
4. use `agent_launch` for local runtime-backed session dispatch
5. use `message_wait` with local durable mailboxes or custom remote adapters

## Recommended Current Pattern

Hadron should still own deterministic process shape:

- ordered steps
- DAG stage orchestration
- retries and timeouts
- human approval gates
- local API probes
- MCP tool calls
- audit trail and diagnostics

Agents should own judgment:

- interpretation
- correlation
- summarization
- operator-facing recommendations

Today the practical way to build a production workflow is:

- launch local task-scoped sessions through `agent_launch`
- launch judgment through `mcp_call` into an external MCP server
- gather machine state through `http_call`
- pause for approval with `human_gate`
- inspect behavior through run operation diagnostics

## What Is Usable Now

### `http_call`

Use `http_call` when the target is a local daemon or service on loopback.

Example:

```yaml
steps:
  - section: Probe
    tasks:
      - name: hadron-health
        http_call:
          method: GET
          url: "http://127.0.0.1:8095/v1/health"
          timeout_seconds: 5
```

Outputs are exposed through compatibility `::set-output` lines:

- `status_code`
- `body`
- `body_json` when the body is JSON

### `mcp_call`

Use `mcp_call` for structured calls into:

- the local Hadron MCP adapter with `server: hadron`, `local`, or `self`
- configured external MCP servers from `~/.hadron/settings.json`

Example:

```yaml
steps:
  - section: Analyze
    tasks:
      - name: hadron-health-tool
        mcp_call:
          server: hadron
          tool: hadron_health
```

External servers currently support:

- `stdio`
- `streamable_http` / `http`
- `sse`

### `human_gate`

Use `human_gate` when a run must pause for an operator decision.

Example:

```yaml
steps:
  - section: Decide
    tasks:
      - name: approve-remediation
        human_gate:
          prompt: "Approve remediation?"
          options:
            - id: approve
              label: Approve
            - id: deny
              label: Deny
          timeout_seconds: 3600
```

The resulting gate can be resolved through:

- REST:
  - `GET /v1/human-gates/{id}`
  - `POST /v1/human-gates/{id}/decision`
- CLI:
  - `hadron gate get <gate-id>`
  - `hadron gate submit <gate-id> <decision>`
- MCP:
  - `hadron_human_gate_get`
  - `hadron_human_gate_submit`

## What Still Needs Follow-Through

### `message_wait`

The default daemon now configures a local durable `MessageSource` through
`settings.json -> message_substrates`.

Current user-visible behavior in the daemon:

- a blueprint validates
- execution reaches the step
- the step resolves `substrate` through `settings.json -> message_substrates`
- local `message_substrates[*].kind = "go_messaging"` stores and polls durable
  inbox messages in Hadron's SQLite state
- the step matches by recipient URN and `correlation_id`
- the step returns `message_id`, `body`, and `body_json` through the existing
  event and `::set-output` surface

Current limitations:

- the stock daemon supports:
  - `message_substrates[*].kind = "go_messaging"` for local durable mailboxes
  - `message_substrates[*].kind = "go_messaging_http"` for generic remote
    go-messaging `/messages/*` peers
  - `message_substrates[*].kind = "tether_http"` as the Tether-aligned alias
- wake/notify delivery is not required for correctness; the current model is
  durable polling first

### `agent_launch`

The default daemon now configures a local `go_agent_runtime`-backed launcher.

Current user-visible behavior in the daemon:

- a blueprint validates
- execution reaches the step
- the step resolves `substrate` through `settings.json -> agent_substrates`
- Hadron resolves provider/runtime with `go-agent-runtime/runtimebind`
- Hadron materializes a per-session boot directory and plants injected native
  files into it
- the step returns normalized handles such as `session_id`, `mailbox`,
  `mailbox_urn`, `session_urn`, `provider`, `runtime`, `workdir`, and `boot_dir`

Current limitations:

- only `agent_substrates[*].kind = "go_agent_runtime"` is implemented
- boot profile and callbacks profile settings now render real boot content:
  - built-in `hadron.default` boot context
  - built-in `shared` callback contract and planted callback metadata files
  - file-backed custom profiles from project or Hadron state directories
- this is still a light-weight renderer, not a full external boot-profile
  catalog compiler
- local `agent_launch -> message_wait` flows now work when the launched agent
  and waiting step share a configured `go_messaging` mailbox contract
- sessions are launched and tracked, but first-class attach/resume UX remains a
  later milestone

## Observability

Every structured step kind emits step-specific events and is summarized through:

- `GET /v1/runs/{run_id}/operations`
- `hadron_run_operations`

`mcp_call` also has a dedicated summary surface:

- `GET /v1/runs/{run_id}/mcp-calls`
- `hadron_run_mcp_calls`

The desktop app now uses the operations summary instead of raw event scraping
for MCP, HTTP, wait, launch, and human-gate diagnostics.

## Suggested Workflow Shape Today

For a workflow that must work in the current daemon without extra embedding:

1. collect local state with `http_call`
2. call specialist tools with `mcp_call`
3. launch a local session with `agent_launch`
4. wait on a correlated mailbox reply with `message_wait`
5. require approval with `human_gate`
6. inspect failures and retries through run operations

## Next Backend Priorities

The audit points to this order:

1. add richer boot-profile compilation if the simple file-backed renderer proves too narrow
2. add more complete remote message surfaces beyond pull-inbox semantics
3. expand example workflows and operator docs around reply contracts
4. harden step-level docs and diagnostics around adapter configuration failures
5. add first-class session inspection/attach surfaces only if the workflow need
   remains strong after message substrates land
