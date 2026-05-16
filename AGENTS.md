# Hadron — Agent Orientation

## What is this and why

Hadron is a local-first, agent-first **blueprint automation runner**. It executes
YAML "blueprints" — ordered, typed, parameterized collections of shell tasks — via
a persistent local daemon (`hadrond`). The daemon exposes a REST API and an MCP
adapter so AI agents (Claude Code, Mentat, Nanite, Torque) can validate, run, and
inspect automation without leaving the conversation. Hadron is part of the Hollis
Labs suite (Nanite · Volon · Tesseract · Mentat · Hadron) and is the portfolio's
execution substrate: other systems hand it declarative blueprints and it runs them
deterministically with an append-only audit trail.

The v0.4 codebase intentionally merges two prior efforts (see ADR-0001 lineage):
v0.2's rich blueprint spec (sections, packages, git, per-task hooks, PTY) and
v0.3's clean architecture (SQLite persistence, cron scheduling, MCP, pipelines).

## Where to start — entry points

- **`cmd/hadrond/`** — the HTTP daemon. Serves the `/v1/*` REST API, runs the
  execution manager, scheduler, trigger manager, pipeline runner, and the MCP
  adapter. This is the orchestration core.
- **`cmd/hadron/`** — the CLI client. `run`, `validate`, `lint`, `schedule`,
  `pipeline`, `daemon`. A thin client over the daemon API.
- **`cmd/hadron-app/`** — the Wails v2 desktop app (React + Vite + Tailwind 4 +
  shadcn). Frontend lives in `cmd/hadron-app/frontend/`. Backed entirely by the
  daemon API.
- **`internal/`** — core packages (see Domain Concepts).
- **`docs/architecture/ARCHITECTURE.md`** — canonical high-level system map.
- **`docs/spec-v04.md`** — the blueprint spec reference.
- **`examples/*.yaml`** — reference blueprints, smallest first (`hello-hadron.yaml`).
- **`README.md`** — quick-start and command summary.

## Key domain concepts

- **Blueprint** — a YAML/JSONC document describing typed inputs and an ordered set
  of shell tasks (optionally grouped into sections). Declarative data, not code.
  Model + validation in `internal/blueprint/`; parsing in `internal/specparse/`.
- **Run** — a single execution of a blueprint. Every state change is appended to
  `run_events` (append-only audit trail).
- **Workspace** — the scoping boundary for execution; all persistence queries are
  scoped by `workspace_id`.
- **Execution manager** (`internal/execution/`) — PTY-based job manager
  (`creack/pty`); per-task retry, timeout, and condition-expression evaluation;
  safety allowlist/denylist enforcement.
- **Scheduler** (`internal/scheduler/`) — cron scheduling (`robfig/cron/v3`) using
  a claim-and-update pattern so a schedule does not double-fire across restarts.
- **Pipeline** (`internal/pipeline/`) — orchestrates multiple blueprints as
  ordered stages.
- **Triggers** — file-watch / event-driven blueprint invocation.
- **MCP adapter** (`internal/mcpadapter/`) — exposes every daemon capability as
  `mcp__hadron__*` tools for agent clients.
- **Persistence** (`internal/persistence/`) — single-writer SQLite
  (`modernc.org/sqlite`); tables: workspaces, runs, run_events, schedules,
  pipeline_runs, pipeline_stage_runs, triggers; cursor-paginated.

**Data flow:** user/agent → CLI / MCP / REST → `hadrond` → (execution + scheduler
+ pipeline) → SQLite. Daemon listens on `:8095`. Data dir is `~/.hadron/`.

## Common operations

Build and develop (from repo root):

```sh
make build      # build bin/hadrond + bin/hadron
make test       # go unit tests
make test-ui    # frontend tests (node --test)
make lint       # go vet + golangci-lint + staticcheck + errcheck + govulncheck, plus UI lint
make typecheck  # frontend tsc
make e2e        # build + tagged end-to-end tests (./test/e2e/...)
make app        # wails build of the desktop app
make app-dev    # wails dev (hot reload)
```

Run blueprints:

```sh
hadrond serve                                 # start the daemon
hadron run examples/hello-hadron.yaml          # run a blueprint
hadron validate examples/parameterized.yaml    # validate one blueprint
hadron lint examples/                          # lint a directory
hadron schedule create --blueprint <f> --cron "* * * * *" --name <n>
hadron daemon                                  # daemon status
```

MCP mode (for Claude Code / agents):

```sh
hadrond mcp -token <secret> -token-scopes run.write,schedule.write,pipeline.write
```

Add the server to `.mcp.json`; see `docs/mcp-setup.md`. Note: MCP setup docs are
known to have drifted from the live tool registry — cross-check
`internal/mcpadapter/` when wiring agents.

Deployment: the daemon runs as a launchd service supervised by **Cerberus**
(resource `hadron-daemon-service`). Cerberus builds via `make build` and runs the
synced artifact from `~/.cerberus/apps/hadron/`.

## Where to look for more

- **Architecture:** `docs/architecture/ARCHITECTURE.md`
- **ADRs:** `docs/architecture/adr/` (0001 daemon ownership, 0002 client model,
  0003 trigger/schedule model, 0004 pipeline model, 0005 Wails layering)
- **Blueprint spec:** `docs/spec-v04.md`
- **CLI reference:** `docs/cli-reference.md`
- **Safety model:** `docs/safety.md`
- **Audits:** `docs/audits/` (review-pass conventions; see `2026-05-12-go-lint-security/`)
- **Portfolio knowledge:** `~/dev/agent-os/knowledge/projects/hadron.md`
- **SoT for agents:** `.agent-ops/project.yaml`

## Status / caveats

Core runtime is beta-capable; Phase 2 (Wails GUI) is substantially complete.
Beta readiness is still conditional on: `go test -race ./...` failing in
file-watch trigger handling, MCP docs/write-path hardening drift, and mechanical
cleanup surfaced by the 2026-05-13 audit (orphaned `queue_entries` table, dormant
`stubs` capability, large-file decomposition). See the portfolio knowledge file
for the full cleanup list.
