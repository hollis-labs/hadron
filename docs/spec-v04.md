# Hadron Blueprint Spec v0.4

A blueprint is a YAML file that describes a sequence of tasks organized into sections.

## Minimal Example

```yaml
version: "0.4"

blueprint:
  name: hello
  title: Hello Hadron
  description: A minimal blueprint.
  author: Your Name
  tags: [example]

steps:
  - section: Greet
    tasks:
      - name: say-hello
        cmd: echo "Hello from Hadron!"
```

---

## Top-Level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | string | yes | Must be `"0.4"` |
| `blueprint` | object | yes | Blueprint metadata |
| `inputs` | list | no | Typed input definitions |
| `steps` | list | yes | Ordered list of sections |
| `imports` | list | no | Blueprint aliases for `call:` tasks |
| `hooks` | object | no | Blueprint-level lifecycle hooks |

---

## `blueprint` Block

```yaml
blueprint:
  name: my-app          # machine-readable identifier (required)
  title: My App         # human-readable title
  description: Sets up my app.
  author: Hollis Labs
  tags: [setup, app]
```

---

## `inputs` Block

Inputs are declared in the `inputs` list and referenced via `{{ .inputs.<name> }}` in task commands.

```yaml
inputs:
  - name: app_name
    label: Application Name
    description: Name of the application.
    type: string
    required: true
    pattern: "^[a-zA-Z][a-zA-Z0-9-]*$"
    min_length: 2
    max_length: 50
    prompt: "Application name?"

  - name: worker_count
    type: number
    default: 3
    min: 1
    max: 32

  - name: enable_debug
    type: boolean
    default: false

  - name: environment
    type: string
    default: development
    enum: [development, staging, production]

  - name: tags
    type: array
    items_type: string
```

### Input Types

| Type | Description | Extra fields |
|---|---|---|
| `string` | Text value | `pattern`, `min_length`, `max_length`, `enum` |
| `number` | Integer or float | `min`, `max` |
| `boolean` | `true` / `false` | — |
| `array` | List of values | `items_type` |

---

## `steps` Block

Steps is an ordered list of sections. Each section has a name and a list of tasks.

```yaml
steps:
  - section: Build
    tasks:
      - name: compile
        cmd: go build ./...

      - name: test
        cmd: go test ./...
        timeout_seconds: 120
        retry: 2
        retry_delay_seconds: 5
        continue_on_error: false
```

### Task Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Unique task identifier within section |
| `cmd` | string | — | Shell command to run (alias: `run`) |
| `call` | string | — | Import alias or path to sub-blueprint |
| `http_call` | object | — | Structured local HTTP request |
| `mcp_call` | object | — | Structured MCP tool request |
| `message_wait` | object | — | Wait for a correlated message reply |
| `agent_launch` | object | — | Launch a bounded agent and return handles |
| `human_gate` | object | — | Wait for an operator decision |
| `if` | string | — | Condition expression; skip task if falsy |
| `enabled` | bool | true | Set false to permanently skip task |
| `dir` | string | — | Working directory for the command |
| `env` | map | — | Additional environment variables |
| `timeout_seconds` | int | settings default | Max run time before timeout |
| `retry` | int | 0 | Number of retry attempts on failure |
| `retry_delay_seconds` | int | 0 | Delay between retries |
| `continue_on_error` | bool | false | Continue to next task on failure |
| `on_success` | list | — | Action hooks run on task success |
| `on_fail` | list | — | Action hooks run on task failure |

---

### HTTP Call Tasks

`http_call` is mutually exclusive with `cmd`, `run`, and `call`. Runtime
execution is currently local-only: URLs must target `localhost`, `127.0.0.1`,
`::1`, or another loopback address.

```yaml
tasks:
  - name: torque-health
    http_call:
      method: GET
      url: "http://127.0.0.1:8990/health"
      timeout_seconds: 5
      headers:
        Accept: application/json
```

The runner emits `http_call_start`, `http_call_response`, and
`http_call_error` events. It also emits compatibility `::set-output` log lines
for `status_code`, `body`, and `body_json` when the response body is JSON.
Daemon consumers can request summarized operation diagnostics from
`GET /v1/runs/{run_id}/operations`.

---

### MCP Call Tasks

`mcp_call` is mutually exclusive with `cmd`, `run`, `call`, and `http_call`.
Execution uses Hadron's internal MCP caller interface; it does not recursively
invoke Hadron's own MCP stdio adapter. The current daemon-backed implementation
supports the local Hadron adapter via `server: hadron` and the aliases
`local`/`self`. Additional server names are resolved from `settings.json`
`mcp_servers` entries and currently support `stdio`, `streamable_http` (alias:
`http`), and `sse` transports.

```yaml
tasks:
  - name: list-torque-runs
    mcp_call:
      server: torque
      tool: torque_runs_list
      arguments:
        workspace_id: "{{ .inputs.workspace_id }}"
        limit: 50
```

The runner emits `mcp_call_start`, `mcp_call_result`, and `mcp_call_error`
events. When Hadron's internal MCP caller can determine transport lifecycle
details, it also emits `mcp_call_transport`, `mcp_call_retry`, and
`mcp_call_reconnect` events. The JSON result is size-capped before being stored
in run events, and a compatibility `::set-output result_json=...` log line is
emitted. Summaries are available from `GET /v1/runs/{run_id}/operations` and
`GET /v1/runs/{run_id}/mcp-calls`.

Example external server configuration:

```json
{
  "mcp_servers": {
    "torque": {
      "transport": "stdio",
      "command": "/path/to/torque-mcp",
      "args": ["serve"],
      "env": {
        "TORQUE_TOKEN": "..."
      }
    },
    "tether": {
      "transport": "streamable_http",
      "url": "http://127.0.0.1:8991/mcp",
      "headers": {
        "Authorization": "Bearer ..."
      },
      "timeout_seconds": 30
    }
  }
}
```

---

### Message Wait Tasks

`message_wait` is mutually exclusive with other executable task kinds. It polls
a configured message source until a matching message arrives or the timeout is
reached.

Current runtime status: the stock `hadrond` process now configures a message
source through `settings.json -> message_substrates`. The first built-in
implementation is `message_substrates[*].kind = "go_messaging"`, which stores
durable messages in Hadron's SQLite state and polls by recipient URN plus
`correlation_id`. The stock daemon also supports remote HTTP-backed message
substrates via `go_messaging_http` and the Tether-aligned alias `tether_http`.
For those remote substrates, `correlation_id` is projected onto the peer's
`thread_id` inbox filter.

```yaml
tasks:
  - name: wait-for-correlator
    message_wait:
      substrate: tether
      to: "{{ .inputs.reply_mailbox }}"
      correlation_id: "{{ .inputs.correlation_id }}"
      timeout_seconds: 1800
      poll_interval_seconds: 5
```

The runner emits `message_wait_start`, `message_wait_poll`,
`message_wait_reply`, `message_wait_timeout`, and `message_wait_error` events.
It also emits compatibility `::set-output` log lines for `message_id`, `body`,
and `body_json` when a reply arrives. Summaries are available from
`GET /v1/runs/{run_id}/operations`.

The stock daemon also exposes supporting message APIs and MCP tools for local
message-capable workflows:

- REST:
  - `POST /v1/messages`
  - `GET /v1/messages/{id}`
  - `POST /v1/messages/{id}/consume`
  - `GET /v1/messages/inbox`
- MCP:
  - `hadron_message_send`
  - `hadron_message_get`
  - `hadron_message_consume`
  - `hadron_messages_inbox`

---

### Agent Launch Tasks

`agent_launch` is mutually exclusive with other executable task kinds. It
launches an agent through a configured adapter and returns durable handles
instead of waiting for agent completion.

Current runtime status: the stock `hadrond` process now configures a local
`go_agent_runtime` launcher through `settings.json -> agent_substrates`. The
step resolves a configured substrate, materializes a per-session boot
directory, plants injected native files, and returns normalized handles such as
`session_id`, `mailbox`, `mailbox_urn`, `session_urn`, `provider`, `runtime`,
`workdir`, and `boot_dir`. `agent_substrates[*].boot.profile` and
`callbacks_profile` now do real work:

- built-in `hadron.default` boot rendering includes launch metadata, nearby
  project guidance files and `.agent-ops/project.yaml` when present
- built-in `shared` callbacks rendering plants `hadron/callbacks.json` and
  `hadron/callbacks.md` into the boot directory
- custom profile names resolve from project-local or Hadron state profile files

Current limitation: only `agent_substrates[*].kind = "go_agent_runtime"` is
implemented in the stock daemon. Remote launch and message substrates remain
adapter work.

```yaml
tasks:
  - name: launch-correlator
    agent_launch:
      substrate: tether
      launch_id: torque-monitor-correlator
      logical_agent_id: torque-monitor-correlator
      prompt_append: |
        Read the injected monitor artifacts.
        Return a concise JSON finding summary.
      injection:
        native_files:
          - rel_path: context/torque-state.json
            source: "{{ .inputs.state_artifact }}"
```

The runner emits `agent_launch_start`, `agent_launch_result`, and
`agent_launch_error` events. It also emits compatibility `::set-output` log
lines for `session_id`, `mailbox`, `mailbox_urn`, and `result_json`. Summaries
are available from `GET /v1/runs/{run_id}/operations`.

---

### Human Gate Tasks

`human_gate` is mutually exclusive with other executable task kinds. It creates
a durable gate record and pauses the run until an operator submits one of the
configured option IDs or the timeout expires.

```yaml
tasks:
  - name: approve-remediation
    human_gate:
      prompt: "Approve Torque remediation?"
      options:
        - id: approve
          label: Approve
        - id: deny
          label: Deny
      timeout_seconds: 3600
```

The runner emits `human_gate_waiting`, `human_gate_decision`,
`human_gate_timeout`, and `human_gate_error` events. It emits compatibility
`::set-output` log lines for `gate_id` and `decision`. Waiting gates can be
inspected and resolved through REST (`GET /v1/human-gates/{id}`, `POST
/v1/human-gates/{id}/decision`), CLI (`hadron gate get`, `hadron gate submit`),
and MCP (`hadron_human_gate_get`, `hadron_human_gate_submit`).

---

## Template Functions

Templates use Go `text/template` syntax. The context object has:

| Expression | Description |
|---|---|
| `{{ .inputs.<name> }}` | Input value |
| `{{ .project.path }}` | Absolute path of the blueprint file |
| `{{ .project.dir }}` | Directory of the blueprint file |
| `{{ .workspace.id }}` | Current workspace ID |

---

## `if:` Conditions

The `if` field is evaluated as a shell-like condition after template rendering.
Standard truthy values: `1`, `t`, `true`, `yes`, `y`, `on`.
Standard falsy values: `0`, `f`, `false`, `no`, `n`, `off`.

```yaml
- name: warn-prod
  if: "test '{{ .inputs.environment }}' = 'production'"
  cmd: echo "WARNING: targeting production"
```

---

## `imports` Block

Import aliases allow a task to `call:` a sub-blueprint:

```yaml
imports:
  - alias: setup
    path: ./setup.yaml
    with:
      app_name: "{{ .inputs.app_name }}"

steps:
  - section: Setup
    tasks:
      - name: run-setup
        call: setup
```

---

## `hooks` Block

Blueprint-level hooks run before/after the entire blueprint or on error.

```yaml
hooks:
  before_run:
    - name: pre-check
      cmd: echo "starting"
  after_run:
    - name: notify
      cmd: echo "finished"
  on_error:
    - name: alert
      cmd: echo "something went wrong"
```

### Per-Task Hooks (`on_success` / `on_fail`)

```yaml
tasks:
  - name: deploy
    cmd: ./deploy.sh
    on_success:
      - type: cmd
        value: echo "deploy succeeded"
    on_fail:
      - type: error
        value: "deployment failed — check logs"
      - type: cmd
        value: ./rollback.sh
```

Hook types: `cmd`, `error`, `step`, `blueprint`, `call`.

---

## Validation Rules

- `version` must be `"0.4"` (or omitted — defaults to `"0.4"`)
- `blueprint.name` is required
- Each task must have exactly one executable kind: `cmd`/`run`, `call`, `http_call`, `mcp_call`, `message_wait`, `agent_launch`, or `human_gate`
- Input `pattern` must be a valid Go regex
- `enum` values must be non-empty strings
- `min_length` ≤ `max_length` (when both specified)
- `min` ≤ `max` (when both specified)

---

## Pipeline Spec

A pipeline chains multiple blueprints into ordered stages.

```yaml
meta:
  name: my-pipeline

stop_on_fail: true

defaults:
  stage_wait_timeout_seconds: 600

stages:
  - name: setup
    blueprint_path: setup.yaml
  - name: test
    blueprint_path: test.yaml
    wait_timeout_seconds: 1800
  - name: launch-agent
    blueprint_path: launch-agent.yaml
    async: true
```

| Field | Type | Default | Description |
|---|---|---|---|
| `meta.name` | string | required | Pipeline identifier |
| `stop_on_fail` | bool | true | Stop on first stage failure |
| `defaults.stage_wait_timeout_seconds` | int | 60 | Default seconds to wait for each stage blueprint run to reach a terminal state |
| `stages` | list | required | Ordered stage list |
| `stages[].name` | string | required | Stage name |
| `stages[].blueprint_path` | string | required | Path to blueprint file |
| `stages[].wait_timeout_seconds` | int | inherited | Stage-specific wait timeout override in seconds |
| `stages[].async` | bool | false | Succeed after the stage blueprint run is enqueued instead of waiting for terminal status |

Async stages persist stage outputs with at least `run_id`, `status: launched`,
and `exit_code: 0`. The pipeline stage record is marked `success` once the
underlying blueprint run is enqueued.
