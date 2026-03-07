---
type: decisions-log
project: hadron
updated_at: 2026-02-28
---

# Hadron Decisions Log

## 2026-02-28 — Architect Session (Initial)

| Decision | Choice | Rationale |
|---|---|---|
| App name | **Hadron** | Hadrons = composite particles from quarks. Blueprints = composites of primitives. Exact metaphor. |
| Phase 1 scope | CLI + daemon + MCP | CLI/API/MCP first philosophy; agents use from day 1 |
| Phase 2 scope | Wails GUI | Built on Phase 1 API; UX from nanite-wails-starter |
| Blueprint spec | **v0.4 merged** | Best of v0.2 (sections, packages, per-task hooks) + v0.3 (typed inputs, blueprint hooks, import aliases) |
| Step structure | **Sections** (v0.2 style) | Better readability, meaningful grouping, better GUI rendering |
| Persistence | **SQLite** (WAL mode) | From vnext — correct and tested |
| Scheduler | **Cron + claim-and-update** | From vnext — prevents double-fire |
| Execution | **PTY-based** | From wails-starter — real terminal feel, pty output |
| MCP namespace | **`hadron_*`** | Renamed from vnext's `cortex_*` |
| Module path | `github.com/hollis-labs/hadron` | Consistent with suite |
| Data dir | `~/.hadron/` | User home, consistent with suite |
| Port | `127.0.0.1:8095` | Local-first default |
| Agent chat | **Backlog (Phase 3)** | After core stable; MCP tools sufficient for now |
| "Cortex" name | Reserved for Context Memory Service | Not used by Hadron |

Full rationale: `artifacts/plan/00-adr-hadron.md`
