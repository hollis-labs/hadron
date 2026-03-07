---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Backlog & Priorities

## Task tracking

Hadron does not use Volon task tracking (no `agentrc.yaml` or `.agentrc/tasks/`). Development priorities are inferred from codebase state.

## Active areas

- Blueprint spec v0.4 is current (documented in `docs/spec-v04.md`)
- MCP adapter operational for AI agent integration
- Pipeline support for multi-blueprint orchestration
- Scheduler with cron expressions
- Desktop app via Wails v2

## Known integration points

- **Mentat** uses Hadron as its blueprint execution engine (`blueprints/mentat_boot.yaml`, `blueprints/clone_repos.yaml`)
- **Volon** references Hadron for task execution in executor mode
- MCP adapter enables Claude Code agents to trigger/inspect runs

## Inferred priorities

1. Blueprint spec evolution beyond v0.4
2. Safety settings refinement (trust levels per `docs/safety.md`)
3. Desktop app maturation (currently Wails v2)
4. Telemetry and run history improvements

## Known gaps

- No Volon integration for task/backlog tracking within Hadron itself
- No GitHub issues or external task tracker detected

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: README.md, docs/, internal/, examples/
