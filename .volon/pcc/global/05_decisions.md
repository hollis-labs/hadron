---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Architectural Decisions

No formal ADRs recorded yet. Key design decisions inferred from codebase:

## Inferred design choices

- **Local-first daemon architecture:** persistent `hadrond` process rather than one-shot CLI execution; enables scheduling, run history, and API access
- **YAML blueprint format:** typed parameters, ordered tasks, lifecycle hooks, conditions, and `continue_on_error` for flexibility
- **MCP adapter over stdio:** JSON-RPC 2.0 with token-scoped access for AI agent integration
- **SQLite persistence:** run history and state stored locally via `go-sqlite3`
- **PTY execution:** `creack/pty` for interactive command support
- **Pipeline orchestration:** multi-blueprint composition for complex workflows
- **Wails v2 desktop app:** visual blueprint management alongside CLI/daemon

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: internal/, go.mod, README.md, docs/
