---
id: TASK-HAD-BACKLOG-002
title: "Phase 3: Embedded agent chat assistant"
status: todo
priority: C
project: hadron
sprint_id: backlog
created_at: "2026-02-28"
updated_at: "2026-02-28"
tags: [agent, chat, ai, phase3, backlog]
---

## Description

Add an embedded AI chat agent to the Hadron GUI, focused on helping users create, edit, manage, and run blueprints.

## Scope

- Chat panel integrated into the Wails GUI (similar to Nanite and Volon chat)
- Context-aware: agent knows the currently open blueprint, recent run results, installed blueprints
- Capabilities: create blueprints from description, edit step commands, explain template syntax, trigger runs, debug failures
- Backend: Claude API via `internal/agent/` package
- MCP tool access: agent can call hadron_* tools directly to inspect and act

## Entry Criteria

- Phase 2 (Wails GUI) complete and stable
- Context Memory Service (Cortex) available for agent context storage

## Notes

Pattern reference: Nanite chat (`reference-only/nanite-wails-starter/`) and Volon chat (`context-aware-context/apps/gui/src/components/ChatPanel.tsx`)
