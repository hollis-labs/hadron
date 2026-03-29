# Boot Prompt — Hadron Frontend Beta Stabilization

> Last updated: 2026-03-29

## Active Agent

`hadron-frontend` — React/Wails frontend development for the Hadron desktop app.

## What We're Doing

Migrating the Hadron frontend from legacy inline styles + custom CSS classes to **shadcn/ui components + Tailwind CSS utility classes**. Establishing a **"props down, events up"** component architecture. Preparing for beta release.

## Plan Document

Read `.agentrc/agents/frontend-beta-plan.md` for the full 5-phase plan with file-level detail.

## Current Phase

**Phase 1 — shadcn Primitives** (COMPLETE as of 2026-03-29)

All mechanical replacements done:
- 18 shadcn components installed
- ~130 `.btn`/`.hud-button` → `<Button>` with variants
- 17 `hud-modal-overlay` → `<Dialog>` / `<AlertDialog>` (Modal.tsx + ConfirmDialog.tsx deleted)
- ~20 `.badge`/`bp-badge` → `<Badge>` with custom status variants
- ~116 `hud-input`/`hud-label` → `<Input>` / `<Label>`
- 2 manual dropdowns → `<DropdownMenu>` in AppHeader
- Removed badge/button/modal/input/label CSS from theme.css and index.css
- Build passes, tsc clean

**Phase 2 — Context & Architecture** (COMPLETE as of 2026-03-29)

- Created `DaemonContext` (daemon status, address, workspace CRUD, active run timer, demo mode)
- Created `NavigationContext` (page routing, selected IDs, openRun/openBlueprint/openPipeline/openWizard/goBack)
- App.tsx slimmed from 430 → ~130 lines (just providers + keyboard handler + page router)
- Workspace creation modal moved to AppHeader (owns its own state)
- All 13 pages + AppHeader consume contexts instead of props
- Build passes, tsc clean

**Phase 3 — Page-by-Page Migration** (in progress as of 2026-03-29)

7 of 13 pages fully migrated to Tailwind (0 inline styles):
- [x] RunsPage (168 lines)
- [x] SettingsPage (198 lines)
- [x] DashboardPage (262 lines)
- [x] RunDetailPage (299 lines)
- [x] TelemetryPage (320 lines)
- [x] SchedulerPage (380 lines)
- [x] PipelineDetailPage (404 lines)

6 pages remaining (436 inline styles total):
- [ ] BlueprintsPage (660 lines, 26 inline styles)
- [ ] BlueprintDetailPage (584 lines, 56 inline styles)
- [ ] HelpPage (759 lines, 126 inline styles)
- [ ] PipelinesPage (869 lines, 66 inline styles)
- [ ] BlueprintWizardPage (1168 lines, 122 inline styles)
- [ ] FlowBuilderPage (913 lines, 38 inline styles)

**Tailwind migration patterns established** (use these for remaining pages):
- `page-header` → `flex items-center justify-between mb-6`
- `page-title` → `text-xl font-semibold text-foreground tracking-tight`
- `section` → `rounded-lg border border-border bg-card overflow-hidden`
- `section-header` → `flex items-center justify-between px-5 py-4 border-b border-border`
- `table` th → `text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50`
- `table` td → `px-5 py-3 text-sm text-muted-foreground border-t border-border`
- `col-primary` → `w-full`, `col-shrink` → `whitespace-nowrap`, `col-right` → `text-right`
- `mono` → `font-mono`
- Status colors: success=`text-blue-400`, running=`text-amber-400`, failed=`text-red-400`
- Use `cn()` for all conditional classes

After Phase 3, proceed to Phase 4 (god component decomposition) and Phase 5 (CSS file cleanup).

## Key Context

- **Stack:** React 18.2 SPA in Wails v2, Vite 5, TypeScript 5.3, Tailwind CSS v4.2.2, shadcn/ui 4.1.1 (base-nova preset)
- **Frontend path:** `cmd/hadron-app/frontend/`
- **Design:** Dark theme, zinc backgrounds, blue success / amber running / red failed / gray queued / purple canceled. Geist font. See `feedback_ui_color_palette.md` in memory.
- **No router:** Navigation is state-driven (`NavPage` union type in App.tsx)
- **Desktop app:** Runs in Wails webview, Go bindings on `window.go`

## Rules

- No new inline styles. Use Tailwind utility classes.
- No new CSS classes. Use Tailwind + shadcn semantic tokens.
- Use `cn()` for conditional classes, not template literals.
- Semantic colors only (`bg-primary`, `text-muted-foreground`), never raw hex/rgb.
- `gap-*` for spacing, `size-*` for equal dimensions.
- shadcn first — check if a component exists before writing custom markup.
- Each phase is independently shippable. The app must build and run after every change.

## Files to Read First

1. `.agentrc/agents/frontend.md` — full frontend context and audit
2. `.agentrc/agents/frontend-beta-plan.md` — detailed migration plan
3. `cmd/hadron-app/frontend/components.json` — shadcn config
4. `cmd/hadron-app/frontend/src/index.css` — Tailwind imports + @theme inline block
