---
id: TASK-HAD-BACKLOG-001
title: "Phase 2: Wails desktop GUI"
status: todo
priority: B
project: hadron
sprint_id: backlog
created_at: "2026-02-28"
updated_at: "2026-02-28"
tags: [gui, wails, phase2, backlog]
---

## Description

Build the Hadron desktop app as a Wails v2 application on top of the Phase 1 REST API.

## Scope

- `cmd/hadron-app/` — Wails entrypoint, wraps Phase 1 API client
- Frontend: React + TypeScript + Tailwind v4 + Volon HUD classes
- Pages: Dashboard, Blueprint Browser (folder nav + file list), Blueprint Detail, Execution Log, Scheduled Jobs, Settings
- Blueprint creation/editing: form-based wizard + raw YAML editor
- Execution real-time view: live event stream with section/task progress
- Theme: Volon HUD (zinc-950/900/800, `hud-panel`, `hud-input`, `hud-button`, `hud-table`) — NOT departure board

## UX Reference

- Page structure + keyboard navigation: `reference-only/nanite-wails-starter/frontend/`
- Design system: `context-aware-context/apps/gui/src/styles.css` (Volon HUD classes)

## Entry Criteria

- Sprint D complete
- Phase 1 API fully stable
- Volon HUD design system documented

## Notes

This is a separate sprint block (sprint-HAD-04a through 04c estimated). Do not start until Phase 1 is complete and dogfooded.
