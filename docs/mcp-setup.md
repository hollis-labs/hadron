# Hadron MCP Setup

Hadron exposes a Model Context Protocol (MCP) adapter so Claude Code (and other
MCP clients) can validate, run, and inspect blueprint runs without leaving the
chat session.

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

## Claude Code Integration

Add an entry to your `.mcp.json` (project-local) or `~/.claude/mcp.json` (global):

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

Restart Claude Code to pick up the new server.

---

## Available MCP Tools

### Read-Only Tools (no token required)

| Tool | Description |
|---|---|
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
| `hadron_create_schedule` | `schedule.write` | Create a new schedule |
| `hadron_update_schedule` | `schedule.write` | Enable/disable a schedule |
| `hadron_delete_schedule` | `schedule.write` | Delete a schedule |
| `hadron_enqueue_pipeline` | `pipeline.write` | Start a pipeline run |

---

## Example: Asking Claude to Validate a Blueprint

Once the MCP server is configured, you can ask Claude:

> "Validate the blueprint at `examples/hello-hadron.yaml` using Hadron."

Claude will call `hadron_validate_blueprint` and report back the result.

> "Run `examples/parameterized.yaml` with `app_name=demo`."

Claude calls `hadron_enqueue_run` with the appropriate inputs and streams the
result back.

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
| `schedule.write` | Allow creating, updating, and deleting schedules |
| `pipeline.write` | Allow starting pipeline runs |

Omit a scope to make that category read-only even with a valid token.
