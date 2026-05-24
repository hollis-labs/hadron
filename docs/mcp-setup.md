# Hadron MCP Setup

Hadron exposes a Model Context Protocol (MCP) adapter so MCP clients can
validate, run, and inspect blueprint runs without leaving the chat session.

## Starting hadrond in MCP Mode

```sh
hadrond mcp \
  -db ~/.hadron/state/hadron.db \
  -logs ~/.hadron/logs \
  -data ~/.hadron
```

The adapter communicates over stdio (JSON-RPC), as required by the MCP spec.

### With a Token (Mutating Tools)

By default, read-only tools are always available. To enable mutating tools
(enqueue runs, create schedules, etc.) provide a token and the desired scopes:

```sh
hadrond mcp \
  -token "my-secret-token" \
  -token-scopes "run.write,schedule.write,pipeline.write"
```

---

## MCP Client Integration

Add an entry to your MCP client configuration. For clients that use JSON config,
the shape typically looks like:

```json
{
  "mcpServers": {
    "hadron": {
      "command": "/path/to/bin/hadrond",
      "args": [
        "mcp",
        "-db", "/Users/<you>/.hadron/state/hadron.db",
        "-logs", "/Users/<you>/.hadron/logs",
        "-data", "/Users/<you>/.hadron",
        "-token", "your-token-here",
        "-token-scopes", "run.write,schedule.write,pipeline.write"
      ]
    }
  }
}
```

Restart your MCP client to pick up the new server.

Hadron does not ship a repo-local MCP client config file. Keep local client
configuration in your own user or workspace config.

---

## Available MCP Tools

### Read-Only Tools (no token required)

| Tool | Description |
|---|---|
| `hadron_run_operations` | Summarize run operation diagnostics across MCP, HTTP, waits, and launches |
| `hadron_run_mcp_calls` | Summarize MCP call diagnostics for a run |
| `hadron_human_gate_get` | Inspect a waiting or decided human gate |
| `hadron_validate_blueprint` | Validate a YAML blueprint; returns `{valid, error}` |
| `hadron_list_runs` | List recent blueprint runs |
| `hadron_get_run` | Get a run by ID, including status and error |
| `hadron_list_run_events` | List events for a run (log lines, task results) |
| `hadron_list_schedules` | List all schedules |
| `hadron_list_workspaces` | List all workspaces |

### Mutating Tools (require token + scope)

| Tool | Scope | Description |
|---|---|---|
| `hadron_enqueue_run` | `run.write` | Enqueue a blueprint run |
| `hadron_cancel_run` | `run.write` | Cancel an in-progress run |
| `hadron_human_gate_submit` | `human_gate.write` | Submit a decision for a waiting human gate |
| `hadron_create_schedule` | `schedule.write` | Create a new schedule |
| `hadron_update_schedule` | `schedule.write` | Enable/disable a schedule |
| `hadron_delete_schedule` | `schedule.write` | Delete a schedule |
| `hadron_enqueue_pipeline` | `pipeline.write` | Start a pipeline run |

---

## Example: Asking A Client To Validate A Blueprint

Once the MCP server is configured, you can ask your client:

> "Validate the blueprint at `examples/hello-hadron.yaml` using Hadron."

The client should call `hadron_validate_blueprint` and report back the result.

> "Run `examples/parameterized.yaml` with `app_name=demo`."

The client calls `hadron_enqueue_run` with the appropriate inputs and streams
the result back.

---

## Auth Token Setup

Generate a random token and keep it in your environment or a secrets manager:

```sh
export HADRON_TOKEN=$(openssl rand -hex 32)
```

Pass it to `hadrond mcp -token "$HADRON_TOKEN"`.

### Scopes

| Scope | Effect |
|---|---|
| `run.write` | Allow enqueueing and cancelling runs |
| `human_gate.write` | Allow submitting decisions to waiting human gates |
| `schedule.write` | Allow creating, updating, and deleting schedules |
| `pipeline.write` | Allow starting pipeline runs |

Omit a scope to make that category read-only even with a valid token.

## External MCP Servers For Blueprints

Blueprint `mcp_call` steps can target the local Hadron adapter with
`server: hadron`, or a named external server from `~/.hadron/settings.json`.
The current runtime supports:

- `stdio` for subprocess-backed MCP servers
- `streamable_http` or `http` for MCP streamable HTTP endpoints
- `sse` for legacy SSE-style MCP endpoints

Example:

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

Then a blueprint step can call:

```yaml
mcp_call:
  server: torque
  tool: torque_runs_list
  arguments:
    limit: 50
```

## Agentic Workflow Notes

For agents using Hadron as a workflow substrate today:

- `mcp_call`, `http_call`, `human_gate`, local-runtime `agent_launch`, and
  local-mailbox `message_wait` are production-usable in the stock daemon
- local message workflows use `message_substrates[*].kind = "go_messaging"`
  with `msg://` recipient URNs and correlation matching
- the message helper tools are:
  - `hadron_message_send`
  - `hadron_messages_inbox`
  - `hadron_message_get`
  - `hadron_message_consume`
- `hadron_run_operations` is the preferred inspection tool for structured step
  diagnostics instead of scraping raw run events
