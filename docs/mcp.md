# MCP and A2A

Agents reach Hadron three ways: MCP over HTTP (`/mcp` on the daemon), MCP over stdio (`hadron mcp`), and A2A.

## Connecting

**HTTP.** The daemon serves Streamable HTTP MCP at `http://127.0.0.1:8095/mcp`. With authentication enforcing the caller needs a bearer token with the `hadron.mcp` scope, plus the scope of each tool it calls.

```json
{ "mcpServers": { "hadron": { "type": "http", "url": "http://127.0.0.1:8095/mcp",
                              "headers": { "Authorization": "Bearer <token>" } } } }
```

**Stdio.** An agent that launches Hadron as a subprocess uses:

```json
{ "mcpServers": { "hadron": { "command": "hadron", "args": ["mcp", "--token-file", "/path/to/token"] } } }
```

The token file holds the caller's service token; it is required when authentication enforces and optional when it is off. The stdio process runs its own runtime on the same database as the daemon, so it needs the same configuration (`SUBSTRATE_CONFIG`) and working directory as the daemon.

## What an agent sees

`tools/list` is **per caller**. A caller sees:

1. The **static tools** its scopes allow.
2. One **typed tool per workflow** its exposure profile pins.

| Static tool | Scope | |
|---|---|---|
| `hadron_workflow_validate`, `_run`, `_run_inspect`, `_run_events`, `_run_cancel`, `_run_resume`, `_signal` | `hadron.run` | Operate any workflow the caller may use, by `name`, `version`, `digest` |
| `hadron_workflow_catalog_search`, `_catalog_inspect` | `hadron.run` | Search one namespace (`namespace` is required); inspect an exact version |
| `hadron_workflow_author_validate`, `_scaffold`, `_test`, `_register` | `hadron.manage` | Authoring |
| `hadron_workflow_registry_package`, `_publish`, `_pin_version`, `_unpin_version`, `_clear_current` | `hadron.manage` | Registry state |
| `hadron_workflow_exposure_inspect`, `_put_profile`, `_delete_profile`, `_pin_definition`, `_unpin_definition` | `hadron.manage` | Exposure profiles |

`hadron_workflow_run_events` takes `after` (a `next_cursor`) and `wait_ms` (up to 30000) to long-poll; `hadron_workflow_run_resume` takes `wake_source` (`callback`, `gate` or `message`).

### Typed workflow tools

A workflow that is published, or pinned by any profile, becomes a tool named `workflow_<name>` with non-alphanumerics replaced by `_` (`team/code-review` becomes `workflow_team_code-review`). Its input schema is the workflow's own, so an agent calls it with real arguments instead of a generic `inputs` object, and the arguments are checked before anything starts:

```
workflow_team_code-review({"repo": "hollis-labs/hadron", "pr": 42})
→ {"run_id": "mcp-run-…", "status": "running", ...}
```

A call starts a run and returns its id; follow it with `hadron_workflow_run_inspect` and `hadron_workflow_run_events`. A typed tool needs the `hadron.run` scope and the workflow in the caller's profile; a tool the caller does not have is hidden from the list and refused if called by name. The list follows the catalog: new versions, publications and profile edits appear without a restart (changes made through the daemon at once, changes made elsewhere within a minute). Where several versions share a name, the tool's schema comes from the catalog's current version.

### Errors

A tool failure is an MCP tool error whose content is `{"error": {"code": ...}}` with a short code (`policy_denied`, `not_found`, `invalid_request`, `confirmation_required`, ...). Messages never include request data or engine internals.

## A2A

The daemon also speaks A2A, outside `/api/` because the protocol fixes the paths. All require the `hadron.run` scope:

| | |
|---|---|
| `GET /.well-known/agent.json` | The agent card: the published workflows the caller may use, as skills with their schemas. **Not public**: it needs a token, unlike the previous Hadron's open card |
| `POST /a2a/tasks` | Submit a task (a workflow run) with an `Idempotency-Key` |
| `GET /a2a/tasks/{id}` | Read a task |
| `POST /a2a/tasks/{id}/cancel`, `/resume` | Cancel; resume a waiting task |
