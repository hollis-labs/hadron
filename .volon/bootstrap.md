---
iteration: 2
updated_at: 2026-02-28
status: ready
active_sprint: sprint-HAD-03d
project: hadron
---

# Hadron Bootstrap

## What is Hadron

Hadron is a local-first, agent-first blueprint automation runner. Blueprints are composable YAML/JSON/JSONC recipes (collections of particles/primitives). Hadron executes them via a daemon with a REST API, CLI, and MCP adapter.

Part of the Hollis Labs app suite: Nanite (notes), Volon (orchestration), Cortex (context memory), Hadron (automation), Mentat (cognitive partner).

## Current State

- Sprint A complete: Go module skeleton, specparse, blueprint v0.4, persistence, test suite all done
- Sprint B complete: settings, execution (PTY), scheduler, pipeline, integration tests all done
- Sprint C complete: config, api (net/http), cmd/hadrond, cmd/hadron (cobra), mcpadapter (MCP stdio), smoke tests all done
- 16/16 Sprint A+B+C tasks done — `go test ./...` passes (9 packages, all green)
- Dependencies: `gopkg.in/yaml.v3`, `github.com/mattn/go-sqlite3`, `github.com/creack/pty`, `github.com/robfig/cron/v3`, `github.com/mark3labs/mcp-go`, `github.com/spf13/cobra`

## Build Location

`blueprint-runner/hadron/` — will move to `~/Projects-apps/hadron/` when Phase 1 complete.

## Next Action

Start Sprint D — sprint-HAD-03d (Hardening + Examples + Docs):
- Error handling hardening
- Example blueprints
- Documentation

See `artifacts/plan/sprint-HAD-03d-*.md` for sprint D plan.

## Sprint Map

| Sprint | ID | Focus | Status |
|---|---|---|---|
| A | sprint-HAD-03a | Spec v0.4 + Persistence | done |
| B | sprint-HAD-03b | Execution + Scheduler | done |
| C | sprint-HAD-03c | API + CLI + MCP | done |
| D | sprint-HAD-03d | Hardening + Examples + Docs | todo |
| Backlog | — | Wails GUI (Phase 2), Agent Chat (Phase 3) | backlog |

## Key Artifacts

| File | Purpose |
|---|---|
| `artifacts/plan/00-adr-hadron.md` | All major decisions and rationale |
| `artifacts/plan/01-blueprint-spec-v04.md` | Spec v0.4 ground truth |
| `artifacts/plan/02-architecture.md` | System architecture, package contracts, API surface |
| `artifacts/plan/sprint-HAD-03a-*.md` | Sprint A plan |
| `artifacts/plan/sprint-HAD-03b-*.md` | Sprint B plan |
| `artifacts/plan/sprint-HAD-03c-*.md` | Sprint C plan |
| `artifacts/plan/sprint-HAD-03d-*.md` | Sprint D plan |
| `.volon/tasks/TASK-HAD-*.md` | All tasks |

## Source Material (read-only)

| Path | Used for |
|---|---|
| `../reference-only/nanite-wails-starter/` | UX reference, v0.2 spec, PTY execution, telemetry |
| `../reference-only/nanite-spec-v0.2/` | v0.2 schema and reference blueprint |
| `../vnext-blueprint-runner/` | v0.3 architecture: specparse, persistence, scheduler, pipeline, mcpadapter |

## Module Path

`github.com/hollis-labs/hadron`

## Daemon Defaults

- Address: `127.0.0.1:8095`
- Data dir: `~/.hadron/`
- DB: `~/.hadron/state/hadron.db`
- Logs: `~/.hadron/logs/runs/`
