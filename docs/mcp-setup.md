# Hadron MCP Setup

Hadron exposes a stdio MCP server from `hadrond mcp`. The adapter is meant to
let agents discover blueprints, inspect their input schema, enqueue runs, and
debug execution without leaving the conversation.

## Start The Server

```sh
hadrond mcp \
  -db ~/.hadron/state/hadron.db \
  -logs ~/.hadron/logs \
  -data ~/.hadron
```

Read-only tools are available without a token. Mutating tools require a token
plus the corresponding scopes:

```sh
hadrond mcp \
  -token "my-secret-token" \
  -token-scopes "run.write,run.cancel,schedule.write,pipeline.write,trigger.write,human_gate.write,message.write"
```

## MCP Client Config

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
        "-token-scopes", "run.write,run.cancel,schedule.write,pipeline.write,trigger.write,human_gate.write,message.write"
      ]
    }
  }
}
```

Restart the MCP client after updating its config.

## Recommended Agent Flow

When the client is unfamiliar with Hadron:

1. Call `hadron_skills` with no arguments.
2. Call `hadron_blueprint_broker` or `hadron_blueprint_discover`.
3. Call `hadron_blueprint_schema` for the chosen blueprint.
4. Call `hadron_run_enqueue`.
5. Use `hadron_run_operations` for structured diagnostics and `hadron_run_events` for the raw audit trail.

## Key Read-Only Tools

| Tool | Purpose |
|---|---|
| `hadron_skills` | Progressive MCP orientation and workflow guidance |
| `hadron_health` | Adapter health/status |
| `hadron_workspaces_list` / `hadron_workspace_get` | Inspect workspaces |
| `hadron_runs_list` / `hadron_run_get` | Inspect runs |
| `hadron_run_events` | Read the append-only event history |
| `hadron_run_operations` | Structured step diagnostics across MCP, HTTP, waits, and launches |
| `hadron_run_mcp_calls` | MCP-call-only diagnostic summary |
| `hadron_blueprints_list` | List blueprint files from the configured blueprint directory |
| `hadron_blueprint_broker` | Rank blueprint recommendations for a task with reasons and next steps |
| `hadron_blueprint_discover` | Rank likely-fit blueprints for a task |
| `hadron_blueprint_search` | Deterministic keyword search across blueprints |
| `hadron_blueprint_schema` | Read the agent-facing JSON input schema for one blueprint |
| `hadron_blueprint_get` | Read the raw blueprint YAML |
| `hadron_blueprint_validate` | Validate blueprint content |
| `hadron_blueprint_lint` | Lint a blueprint or pipeline file |
| `hadron_agent_card` | Generate an A2A-compatible agent card from one or all blueprints |
| `hadron_schedules_list` | Inspect schedules |
| `hadron_pipelines_list` / `hadron_pipeline_stages` / `hadron_pipeline_graph` | Inspect pipeline runs |
| `hadron_triggers_list` / `hadron_trigger_list_mine` | Inspect triggers |
| `hadron_human_gate_get` | Inspect a human decision gate |
| `hadron_messages_inbox` / `hadron_messages_list` / `hadron_messages_thread` / `hadron_message_get` | Inspect local message workflows |
| `hadron_registry_list` / `hadron_registry_search` / `hadron_registry_show` | Inspect the optional indexed blueprint registry |

## Prompts And Resources

Hadron also exposes standard MCP prompts and resources for agent orientation:

- prompts:
  - `hadron_pick_blueprint`
  - `hadron_debug_run`
- static resources:
  - `hadron://docs/mcp/start-here`
  - `hadron://docs/mcp/blueprint-discovery`
  - `hadron://docs/mcp/run-inspection`
  - `hadron://docs/mcp/message-workflows`
  - `hadron://docs/mcp/input-schema-guide`
- resource template:
  - `hadron://blueprints/{blueprint_ref}/input-schema`

These are optional convenience surfaces for MCP clients that understand prompts/resources. The core workflow remains fully available through tools alone.

## Mutating Tools And Scopes

| Tool | Scope |
|---|---|
| `hadron_workspace_create` | `workspace.write` |
| `hadron_run_enqueue` | `run.write` |
| `hadron_run_cancel` | `run.cancel` |
| `hadron_schedule_create` / `hadron_schedule_update` / `hadron_schedule_delete` | `schedule.write` |
| `hadron_pipeline_enqueue` | `pipeline.write` |
| `hadron_trigger_create` / `hadron_trigger_watch` / `hadron_trigger_delete` | `trigger.write` |
| `hadron_human_gate_submit` | `human_gate.write` |
| `hadron_message_send` / `hadron_message_consume` | `message.write` |
| `hadron_registry_index` | none today, but treat as operator-oriented |

## Example Prompts

> "Use Hadron to find a blueprint that looks like a release workflow."

Expected call path: `hadron_blueprint_broker` or `hadron_blueprint_discover`.

> "Inspect the schema for `examples/parameterized.yaml` and then run it with reasonable demo inputs."

Expected call path: `hadron_blueprint_schema` → `hadron_run_enqueue`.

> "This run failed. Use Hadron to explain which step failed and why."

Expected call path: `hadron_run_operations`, then `hadron_run_events` only if deeper raw detail is needed.

## External MCP Servers For Blueprints

Blueprint `mcp_call` steps can target the local Hadron adapter with
`server: hadron`, or a named external server from `~/.hadron/settings.json`.
Supported transports today:

- `stdio`
- `streamable_http` or `http`
- `sse`

Example settings:

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

- `mcp_call`, `http_call`, `human_gate`, local-runtime `agent_launch`, and local-mailbox `message_wait` are production-usable in the stock daemon.
- Local message workflows use `message_substrates[*].kind = "go_messaging"` with `msg://` URNs and correlation matching.
- Prefer recipient- and thread-based message reads over id-only polling when the workflow already has a stable thread identifier.
