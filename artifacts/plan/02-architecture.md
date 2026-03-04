---
type: architecture
id: ARCH-HAD-001
title: "Hadron System Architecture"
status: accepted
date: 2026-02-28
---

# Hadron System Architecture

## Overview

Hadron is a local-first, agent-first blueprint automation runner. It executes composable YAML/JSON/JSONC blueprints via a daemon process, exposes a REST API and MCP adapter, and provides a CLI for human and agent use. A Wails desktop GUI is planned for Phase 2.

## Phase 1 Components

```
hadron/
├── cmd/
│   ├── hadrond/        # Daemon entrypoint
│   └── hadron/         # CLI entrypoint
├── internal/
│   ├── specparse/      # YAML/JSON/JSONC parser
│   ├── blueprint/      # Spec v0.4 model, validation, template rendering
│   ├── config/         # Runtime config (ports, paths, workspace)
│   ├── persistence/    # SQLite store (runs, schedules, workspaces, events)
│   ├── execution/      # Job manager, PTY runner, safety validation
│   ├── scheduler/      # Cron engine with claim-and-update
│   ├── pipeline/       # Pipeline spec, stage runner
│   ├── api/            # HTTP REST API server (hadrond)
│   ├── mcpadapter/     # MCP server (hadron_* tools)
│   └── settings/       # settings.json (safety, execution, UI prefs)
├── examples/           # Reference blueprints (v0.4)
├── docs/               # User-facing documentation
└── artifacts/          # Plan artifacts (this directory)
    └── plan/
```

## Data Flow

```
User/Agent
    │
    ├─ CLI (`hadron run blueprint.yaml`)
    │       └─▶ REST API (hadrond :8095)
    │
    ├─ MCP client (Claude, Volon, Mentat)
    │       └─▶ MCP adapter (hadrond mcp mode)
    │
    └─ REST client (direct HTTP)
            └─▶ REST API (hadrond :8095)
                    │
                    ├─▶ execution.Manager (job queue, PTY runner)
                    ├─▶ scheduler.Engine  (cron tick, claim-and-update)
                    ├─▶ pipeline.Runner   (stage orchestration)
                    └─▶ persistence.Store (SQLite)
```

## Key Package Contracts

### `internal/specparse`
- `Unmarshal(path, data, out)` — detect format, parse YAML or JSONC (strips comments, trailing commas)
- No external state. Pure parse function.

### `internal/blueprint`
- `Blueprint` struct — full v0.4 model
- `ParseFile(path)` → `*Blueprint, error`
- `ParseBytes(data)` → `*Blueprint, error`
- `Validate(bp)` → `error`
- `NormalizeInputs(bp, values)` → `map[string]any, error`
- `BuildTemplateContext(bp, path, workspaceID, inputs)` → `map[string]any`
- `RenderForExecution(bp, ctx)` → `*Blueprint, error` (renders all template strings)
- `LoadWithImports(path)` → `*Blueprint, error` (resolves imports recursively)

### `internal/persistence`
- `Store` wraps `*sql.DB` (SQLite, single writer)
- Tables: `workspaces`, `runs`, `run_events`, `schedules`, `pipeline_runs`, `pipeline_stage_runs`
- All list queries support cursor-based pagination
- `Open(path)` runs migrations on first call

### `internal/execution`
- `Manager` — worker pool, job queue (buffered chan)
- `Request` — `{WorkspaceID, RunID, BlueprintPath, Inputs}`
- Jobs execute via PTY (`creack/pty`) for real terminal output
- Per-step: retry, timeout, condition eval, onSuccess/onFail hooks
- Safety: command allowlist/denylist, path validation (from settings)
- Events streamed to `run_events` via persistence

### `internal/scheduler`
- `Engine` — 1-second tick loop, `ListDueSchedules` → claim-and-update → `Enqueue`
- `ValidateCron(expr)` → `error`
- `NextRun(expr, from)` → `time.Time`
- Claim-and-update prevents double-firing across restarts

### `internal/pipeline`
- `Spec` — ordered stages, each points to a blueprint path + input overrides
- `Runner` — executes stages sequentially (or parallel if spec allows), persists stage runs

### `internal/api`
- Chi or stdlib HTTP router
- Endpoints:

| Method | Path | Description |
|---|---|---|
| GET | /v1/health | Daemon health + version |
| GET | /v1/workspaces | List workspaces |
| POST | /v1/workspaces | Create workspace |
| GET | /v1/workspaces/:id | Get workspace |
| GET | /v1/runs | List runs (workspace-scoped, cursor paginated) |
| POST | /v1/runs | Enqueue a run |
| GET | /v1/runs/:id | Get run |
| DELETE | /v1/runs/:id | Cancel run |
| GET | /v1/runs/:id/events | List run events |
| GET | /v1/schedules | List schedules |
| POST | /v1/schedules | Create schedule |
| GET | /v1/schedules/:id | Get schedule |
| PATCH | /v1/schedules/:id | Update (enable/disable, next_run) |
| DELETE | /v1/schedules/:id | Delete schedule |
| GET | /v1/pipelines | List pipeline runs |
| POST | /v1/pipelines | Enqueue a pipeline run |
| GET | /v1/pipelines/:id | Get pipeline run |
| GET | /v1/pipelines/:id/stages | List pipeline stages |
| POST | /v1/blueprints/validate | Validate blueprint bytes |

### `internal/mcpadapter`
- MCP server (stdio or HTTP transport)
- Tool namespace: `hadron_*`
- Scope-based auth tokens (`run.write`, `run.cancel`, `scheduler.control`, `workspace.write`, `pipeline.write`)

| Tool | Description |
|---|---|
| `hadron_health` | Daemon health |
| `hadron_workspaces_list` | List workspaces |
| `hadron_workspace_get` | Get workspace |
| `hadron_workspace_create` | Create workspace |
| `hadron_runs_list` | List runs |
| `hadron_run_get` | Get run |
| `hadron_run_enqueue` | Enqueue a blueprint run |
| `hadron_run_cancel` | Cancel a run |
| `hadron_run_events` | List run events |
| `hadron_schedules_list` | List schedules |
| `hadron_schedule_create` | Create a schedule |
| `hadron_schedule_update` | Enable/disable/update schedule |
| `hadron_pipelines_list` | List pipeline runs |
| `hadron_pipeline_enqueue` | Enqueue a pipeline run |
| `hadron_pipeline_stages` | List pipeline stages |
| `hadron_blueprint_validate` | Validate blueprint YAML/JSON |

## CLI Command Surface

```
hadron run <path> [--inputs key=val...] [--workspace id] [--dry-run]
hadron validate <path>
hadron blueprint list [--dir path]
hadron blueprint show <path>
hadron schedule list [--workspace id]
hadron schedule create --blueprint <path> --cron <expr> [--name str] [--workspace id]
hadron schedule enable <id>
hadron schedule disable <id>
hadron pipeline run <path> [--workspace id]
hadron workspace list
hadron workspace create <name>
hadron daemon status [--addr url]
hadron daemon start [--addr addr] [--db path] [--logs path]
```

## Daemon Startup

```
hadrond [--addr 127.0.0.1:8095] [--db ~/.hadron/state/hadron.db] [--logs ~/.hadron/logs/runs]

# MCP mode
hadrond mcp [--db path] [--logs path] [--token token] [--token-scopes scope1,scope2]
```

Default data dir: `~/.hadron/`

## Source Material

| Source | Use for |
|---|---|
| `reference-only/nanite-wails-starter/backend/blueprint/` | v0.4 blueprint model base |
| `reference-only/nanite-wails-starter/backend/core/` | PTY execution, job manager |
| `reference-only/nanite-wails-starter/backend/settings/` | Safety settings |
| `reference-only/nanite-wails-starter/backend/telemetry/` | Logger structure |
| `vnext-blueprint-runner/internal/specparse/` | Port as-is |
| `vnext-blueprint-runner/internal/persistence/` | Port as-is, rename module path |
| `vnext-blueprint-runner/internal/scheduler/` | Port as-is |
| `vnext-blueprint-runner/internal/pipeline/` | Port as-is |
| `vnext-blueprint-runner/internal/mcpadapter/` | Port, rename cortex_* → hadron_* |
| `vnext-blueprint-runner/internal/blueprint/` | Harvest typed inputs, hooks, imports model |
| `vnext-blueprint-runner/internal/execution/` | Harvest Request/contract model |
| `vnext-blueprint-runner/internal/api/` | Harvest route structure |

## Phase 2: Wails GUI (backlog)

- `cmd/hadron-app/` — Wails entrypoint wrapping the Phase 1 REST API
- Frontend: React + TypeScript + Tailwind v4 + Volon HUD classes
- Pages: Dashboard, Blueprint Browser, Blueprint Detail, Execution Log, Scheduled Jobs, Settings
- UX reference: `reference-only/nanite-wails-starter/frontend/`
- Theme: Volon HUD (zinc-950/900/800, system font) — NOT departure board

## Integration Points (other suite apps)

| App | Integration |
|---|---|
| Volon | Uses Hadron MCP tools for task scheduling and custom skill execution |
| Nanite | Can trigger blueprint runs via MCP for automation |
| Context Memory Service (Cortex) | Hadron run events can be published to context broker |
| Flow Builder (future) | Will use Hadron pipelines as the execution substrate |
| Mentat | Will use Hadron as a primary automation tool via MCP |
