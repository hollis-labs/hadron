---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Architecture

## Tech stack

- **Language:** Go 1.25
- **Module:** `github.com/hollis-labs/hadron`
- **Key dependencies:** cobra (CLI), mcp-go (MCP adapter), go-sqlite3 (persistence), robfig/cron (scheduling), Wails v2 (desktop app), creack/pty (PTY execution), yaml.v3
- **Blueprint format:** YAML with typed parameters and lifecycle hooks

## Key components

### Daemon (`cmd/hadrond`)
Persistent local service exposing REST API. Manages blueprint execution, scheduling, and run history.

### CLI (`cmd/hadron`)
Client binary for triggering runs, validation, linting, scheduling, and daemon health checks.

### Desktop App (`cmd/hadron-app`)
Wails v2 desktop application for visual blueprint management.

### Internal packages (`internal/`)
- `api` -- REST API routes and handlers
- `blueprint` -- blueprint parsing and validation
- `config` -- configuration management
- `execution` -- shell task execution engine (with PTY support)
- `mcpadapter` -- MCP server adapter for AI agent integration
- `persistence` -- SQLite-backed run history and state
- `pipeline` -- multi-blueprint pipeline orchestration
- `scheduler` -- cron-based blueprint scheduling
- `settings` -- safety settings and trust levels
- `specparse` -- blueprint spec parsing (v0.4)
- `telemetry` -- execution telemetry and reporting

## Data flow

1. Blueprint YAML loaded and validated against spec
2. Parameters resolved (defaults, user overrides, env vars)
3. Tasks executed in order with lifecycle hooks (pre/post)
4. Results persisted to SQLite; status reported via API
5. Scheduler triggers runs per cron expressions

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: cmd/, internal/, go.mod, README.md
