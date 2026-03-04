---
type: adr
id: ADR-HAD-001
title: "Hadron — Naming, Architecture, and Scope Decisions"
status: accepted
date: 2026-02-28
author: architect
---

# ADR-HAD-001: Hadron — Naming, Architecture, and Scope Decisions

## Context

The blueprint-runner project had two prior versions:

- **nanite-wails-starter (v0.2):** Working Wails desktop app. Rich blueprint spec (sections, packages ecosystem, per-task hooks). Strong UX. Weak architecture (in-memory only, no real scheduler, no MCP).
- **vnext-blueprint-runner (v0.3 / "Cortex"):** Agent started from scratch instead of enhancing v0.2. Good architecture (SQLite, cron scheduler, MCP adapter, pipelines, tests) but regressed the spec (flat steps, lost packages/git/stubs, no sections) and built task management instead of blueprint management.

The goal is to combine the best of both into a new, clean project.

## Decisions

### 1. Name: Hadron

**Rationale:** Hadrons are composite particles made of quarks (fundamental particles). The metaphor is exact:
- Individual commands/primitives = quarks/particles
- Blueprints = hadrons (composite structures assembled from particles)
- The runner = the collider that fires them

Fits the naming ecosystem: Nanite (engineered nano-matter), Hadron (composite subatomic particle). CLI reads cleanly: `hadron run blueprint.yaml`.

**Rejected names:** Cortex (reserved for Context Memory Service), Forge (good but less conceptually tight), Relay, Loom.

### 2. Architecture: CLI/Daemon first, GUI second

**Phase 1 (this sprint set):** Go daemon (`hadrond`) + CLI (`hadron`) + REST API + MCP adapter. Agents can use Hadron from day 1 via MCP. Aligns with suite philosophy: CLI/API/MCP first.

**Phase 2 (later):** Wails desktop GUI (`hadron-app`) built on top of the Phase 1 API. UX sourced from nanite-wails-starter. Theme updated to Volon HUD style.

**Rationale:** The wails-starter's in-memory architecture made it impossible to add persistence, scheduling, and MCP without a full rewrite anyway. Building the core clean first gives a solid API that both the GUI and external agents consume identically.

### 3. Blueprint Spec: v0.4 (merged)

Merge v0.2 richness with v0.3 improvements. See `01-blueprint-spec-v04.md` for full spec.

**From v0.2 (keep):**
- Sections structure: `steps: [{section, tasks: [...]}]`
- Rich packages ecosystem (composer, npm, pip, brew, go)
- `project` block with vars
- `git`, `stubs`, `tools` config blocks
- Per-task `onSuccess`/`onFail` action hooks (type: cmd|error|step|blueprint|call)
- PTY-based execution (real terminal feel)
- Telemetry logger

**From v0.3 (add):**
- Blueprint-level hooks (`hooks.before_run`, `hooks.after_run`, `hooks.on_error`)
- Improved `inputs` type system (label, min/max, minLength/maxLength, pattern, items_type, enum)
- Import aliases + `with` params
- `call` step directive (invoke imported blueprint by alias)
- `if` field on steps (alongside `condition` for compat)
- Workspace concept (namespaced runs/schedules)
- `specparse` JSONC with trailing comma support
- SQLite persistence
- Cron scheduler with claim-and-update

### 4. Persistence: SQLite

Single-writer SQLite via `mattn/go-sqlite3`. Schema: workspaces, runs, run_events, schedules, pipeline_runs, pipeline_stage_runs. Cursor-based pagination on list endpoints.

### 5. MCP Tools: `hadron_*` namespace

Port vnext MCP adapter, rename all tools from `cortex_*` to `hadron_*`. Scope-based auth tokens. Full tool set: health, workspaces, runs, run_events, schedules, pipelines, blueprint validation.

### 6. Pipeline: Included in Phase 1

Pipelines (ordered stages of blueprints) are needed for Volon integration. Included in Phase 1 scope.

### 7. Agent Chat: Backlog

Defer embedded AI chat agent until core is stable. Design MCP tools to be rich enough that external agents (Volon, Mentat) can fully drive Hadron via MCP. Agent chat is a Phase 3 item.

### 8. Location

Built in `blueprint-runner/hadron/` during development. Move to `~/Projects-apps/hadron/` when ready for independent repo treatment.

## Consequences

- Orchestrators working on Hadron should treat `reference-only/` and `vnext-blueprint-runner/` as read-only source material.
- The vnext Go packages (specparse, persistence, scheduler, mcpadapter, pipeline, execution) are the primary source for porting — they are tested and architecturally sound.
- The wails-starter backend (blueprint, core, settings, telemetry) fills gaps where vnext regressed.
- The wails-starter frontend is the UX reference for Phase 2 only.
