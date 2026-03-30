# Boot Prompt — Hadron Frontend Polish & Remaining Migration

> Last updated: 2026-03-29

## Active Agent

`hadron-frontend` — React/Wails frontend development for the Hadron desktop app.

## What We're Doing

Design polish pass across all pages. Establishing a consistent visual language based on the **BlueprintsPage** as the golden template. Migration from legacy CSS to Tailwind/shadcn is largely complete — remaining work is style alignment, god component decomposition, and CSS cleanup.

## Plan Document

Read `.agentrc/agents/frontend-beta-plan.md` for the full 5-phase plan with file-level detail.

## Current Status

**Phase 1 — shadcn Primitives** (COMPLETE)
**Phase 2 — Context & Architecture** (COMPLETE)
**Phase 3 — Page-by-Page Migration** (COMPLETE — all 13 pages have 0 inline styles, 0 hud-* classes)

**Design Polish Pass** (in progress as of 2026-03-29)

Pages aligned to BlueprintsPage golden template:
- [x] DashboardPage — stat cards + flat rows, blue glass hover
- [x] BlueprintsPage — **golden template**: glass search, sort chips, blue hover rows, dropdown menus
- [x] PipelinesPage — flat rows, blue/yellow buttons, dropdown menus
- [x] RunsPage — flat rows, chip filters, glass search
- [x] SchedulerPage — flat rows, dropdown menus
- [x] TelemetryPage — flat rows (list + detail views), chip level filters

Pages remaining for polish:
- [ ] FlowBuilderPage — needs its own session (visual editor, several issues to address)
- [ ] BlueprintDetailPage
- [ ] BlueprintWizardPage
- [ ] PipelineDetailPage
- [ ] RunDetailPage
- [ ] SettingsPage
- [ ] HelpPage

After polish, proceed to Phase 4 (god component decomposition) and Phase 5 (CSS file cleanup).

## Design Language (established via BlueprintsPage)

**Layout patterns:**
- Page container: `flex flex-col gap-2 h-full`
- Section labels: `text-xs tracking-wider uppercase text-muted-foreground`
- No card-wrapped tables — use flat rows on transparent bg (grid shows through)

**Row pattern (flat, transparent, blue glass hover):**
```
'flex items-center gap-3 px-3 py-1.5 rounded transition-colors',
'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
'border border-transparent',
```

**Search input (glass focus effect):**
```
'h-10 border-border/60 text-sm placeholder:text-muted-foreground/50
 focus-visible:border-blue-500 focus-visible:ring-0
 dark:focus-visible:bg-blue-500/10
 focus-visible:shadow-[inset_0_0_12px_rgba(59,130,246,0.08),0_0_8px_rgba(59,130,246,0.06)]
 focus-visible:text-blue-100 transition-all'
```
Escape clears text, second Escape blurs.

**Filter/sort chips:**
```
'h-8 px-3 rounded-md text-xs font-medium transition-colors',
'border border-border/60 bg-transparent',
'hover:bg-muted/60 hover:text-foreground',
// active: 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]'
```

**Buttons:**
- Primary action: `bg-blue-500 text-white hover:bg-blue-600` (e.g. New Blueprint)
- Folder actions: `bg-yellow-500 text-yellow-950 hover:bg-yellow-600`
- Secondary actions: moved to `...` DropdownMenu with gray trigger button
- Destructive in dropdown: `className="text-red-400 focus:text-red-400"`

**Colors:**
- Folders: `text-yellow-500`
- Files/blueprints: `text-blue-400`
- Status: success=`text-blue-400`, running=`text-amber-400`, failed=`text-red-400`

## Key Context

- **Stack:** React 18.2 SPA in Wails v2, Vite 5, TypeScript 5.3, Tailwind CSS v4.2.2, shadcn/ui 4.1.1 (base-nova preset)
- **Frontend path:** `cmd/hadron-app/frontend/`
- **Design:** Dark theme, zinc backgrounds (#09090b base, #0f0f12 surface, #18181b raised, #27272a borders). Blue success / amber running / red failed / gray queued / purple canceled. Yellow-500 for folders. Text: #fafafa primary, #a1a1aa secondary, #71717a tertiary.
- **Dark mode:** `class="dark"` on `<html>`. Dark tokens in `.dark {}` block of index.css. Hadron overrides: `--primary` = blue-500, chart colors = status palette.
- **CSS note:** Do NOT add unlayered `* { margin:0; padding:0 }` — it overrides all Tailwind utilities. Tailwind v4 preflight handles resets. The `body` rule uses legacy vars for bg/color.
- **No router:** Navigation is state-driven (`NavPage` union type in App.tsx)
- **Desktop app:** Runs in Wails webview, Go bindings on `window.go`
- **Reference screenshots:** `~/Downloads/hadron-shots/` (13 screenshots from 2026-03-29)

## Rules

- No new inline styles. Use Tailwind utility classes.
- No new CSS classes. Use Tailwind + shadcn semantic tokens.
- Use `cn()` for conditional classes, not template literals.
- `gap-*` for spacing, `size-*` for equal dimensions.
- shadcn first — check if a component exists before writing custom markup.
- Use the design language patterns above (flat rows, glass search, chip filters, dropdown menus).
- Each change must build and run. Not a Next.js app — ignore "use client" suggestions.

## Files to Read First

1. `.agentrc/agents/frontend.md` — full frontend context and audit
2. `.agentrc/agents/frontend-beta-plan.md` — detailed migration plan
3. `cmd/hadron-app/frontend/components.json` — shadcn config
4. `cmd/hadron-app/frontend/src/index.css` — Tailwind imports + @theme inline block + dark mode tokens
5. `cmd/hadron-app/frontend/src/pages/BlueprintsPage.tsx` — golden template for design patterns
