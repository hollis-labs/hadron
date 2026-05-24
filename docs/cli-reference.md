# Hadron CLI Reference

The `hadron` CLI communicates with a running `hadrond` daemon over HTTP.

## Typical Flow

Most users only need four commands to get started:

```sh
hadrond serve
hadron daemon
hadron validate examples/hello-hadron.yaml
hadron run examples/hello-hadron.yaml
```

Use `hadron blueprint ...` for local file inspection without a running daemon.

## Global Flags

| Flag | Default | Description |
|---|---|---|
| `--addr` | `http://127.0.0.1:8095` | Daemon base URL |

---

## `hadron run`

Enqueue a blueprint run and stream events until completion.

```sh
hadron run <blueprint-path> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--input key=value` | — | Set an input value (repeatable) |
| `--workspace` | `default` | Workspace ID |
| `--dry-run` | `false` | Preview commands without executing |

**Examples:**

```sh
hadron run examples/hello-hadron.yaml
hadron run examples/parameterized.yaml --input app_name=myapp --input worker_count=4
hadron run examples/laravel-app.yaml --workspace production --dry-run
```

Use this when:

- you want to execute a blueprint immediately
- you want the CLI to stream the run events back to your terminal

---

## `hadron validate`

Validate a blueprint file and report errors.

```sh
hadron validate <blueprint-path>
```

Exits 0 if valid, 1 if invalid.

**Example:**

```sh
hadron validate examples/hello-hadron.yaml
# valid
```

Use this before:

- committing a new or edited blueprint
- scheduling a blueprint
- asking an agent to run a blueprint you just changed

---

## `hadron lint`

Lint blueprint files for errors.

```sh
hadron lint <path|dir> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--json` | `false` | Output machine-readable JSON |

Scans directories recursively for `*.yaml`, `*.yml`, `*.json`, `*.jsonc`.
Exits 0 if all valid, 1 if any invalid.

**Examples:**

```sh
hadron lint examples/
hadron lint examples/ --json
hadron lint examples/hello-hadron.yaml
```

---

## `hadron fmt`

Format a blueprint file to canonical YAML.

```sh
hadron fmt <path> [flags]
```

| Flag | Default | Description |
|---|---|---|
| `--write` | `false` | Write canonical YAML back to file |
| `--check` | `false` | Exit 1 if file would change (CI mode) |

Also normalises legacy field aliases:
- `condition:` → `if:`
- `continueOnError:` → `continue_on_error:`
- `retryDelay:` → `retry_delay_seconds:`

**Examples:**

```sh
hadron fmt examples/hello-hadron.yaml              # print to stdout
hadron fmt examples/hello-hadron.yaml --write      # rewrite in place
hadron fmt examples/hello-hadron.yaml --check      # CI check
```

---

## `hadron blueprint`

Local blueprint file operations (no daemon required).

### `hadron blueprint list`

```sh
hadron blueprint list [--dir <dir>]
```

Lists blueprint files in a directory and whether each is valid.

### `hadron blueprint show`

```sh
hadron blueprint show <path>
```

Prints a parsed blueprint summary (name, version, inputs, sections).

Use these when:

- you want to inspect blueprint metadata locally
- the daemon is not running yet
- you are editing or reviewing blueprint files directly

---

## `hadron schedule`

Manage schedules.

### `hadron schedule list`

```sh
hadron schedule list [--workspace <id>]
```

### `hadron schedule create`

```sh
hadron schedule create --blueprint <path> --cron <expr> [--name <name>] [--workspace <id>]
```

| Flag | Required | Description |
|---|---|---|
| `--blueprint` | yes | Blueprint path |
| `--cron` | yes | Cron expression (5-field standard) |
| `--name` | no | Human-readable schedule name |
| `--workspace` | no | Workspace ID (default: `default`) |

**Example:**

```sh
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "0 9 * * 1-5" \
  --name weekday-morning
```

### `hadron schedule enable <id>`

### `hadron schedule disable <id>`

### `hadron schedule delete <id>`

Use schedules when:

- the workflow should recur on a cron cadence
- you want the daemon to own timing and audit history

---

## `hadron pipeline`

### `hadron pipeline run`

```sh
hadron pipeline run <pipeline-path> [--workspace <id>]
```

Starts a pipeline run and returns the pipeline run ID.

Use this when a workflow is already split into several blueprints with stage
boundaries.

---

## `hadron workspace`

### `hadron workspace list`

### `hadron workspace create <name>`

---

## `hadron daemon`

Check daemon connectivity and version.

```sh
hadron daemon
# status: ok  version: 0.4.0
```

This is the fastest “is Hadron up?” check.

---

## `hadron version`

Print CLI build metadata.

```sh
hadron version
```

Example output:

```text
hadron v0.4.0
commit: abc1234
built: 2026-05-24T22:00:00Z
```

---

## `hadrond` Daemon

### `hadrond serve`

Starts the HTTP REST API server.

```sh
hadrond serve [flags]
```

| Flag | Default | Description |
|---|---|---|
| `-addr` | `127.0.0.1:8095` | Listen address |
| `-db` | `~/.hadron/state/hadron.db` | SQLite database path |
| `-logs` | `~/.hadron/logs` | Run log directory |
| `-data` | `~/.hadron` | Data directory (for settings.json) |

### `hadrond mcp`

Starts the MCP stdio adapter for MCP client integration.

```sh
hadrond mcp [flags]
```

| Flag | Default | Description |
|---|---|---|
| `-db` | `~/.hadron/state/hadron.db` | SQLite database path |
| `-logs` | `~/.hadron/logs` | Run log directory |
| `-data` | `~/.hadron` | Data directory |
| `-token` | — | Bearer token for mutating tools |
| `-token-scopes` | — | Comma-separated scopes (e.g. `run.write,pipeline.write`) |

Use `hadrond mcp` when you want an MCP client or agent to discover and run
Hadron workflows. See [mcp-setup.md](mcp-setup.md) for the actual tool model.

### `hadrond version`

Print daemon build metadata.

```sh
hadrond version
```
