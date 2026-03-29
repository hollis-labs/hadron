# Hadron Frontend Beta Stabilization Plan

> Created: 2026-03-29
> Status: In Progress
> Agent: hadron-frontend

## Goal

Migrate the Hadron frontend from legacy inline styles + custom CSS classes to shadcn/ui components + Tailwind utility classes. Establish a "props down, events up" component architecture. Prepare for beta release.

## Guiding Principles

- **One page at a time.** Never leave the app in a half-migrated state that breaks the build or UX.
- **shadcn first.** Check if a component exists before writing custom markup.
- **Semantic tokens only.** `bg-primary`, `text-muted-foreground` — never raw hex/rgb values.
- **Preserve the design.** The overall visual language (zinc backgrounds, blue/amber/red status colors, Geist font) stays. We're changing implementation, not design.
- **No new dependencies** beyond shadcn components and their sub-dependencies.
- **Each phase is independently shippable.** The app works correctly after each phase completes.

---

## Phase 1 — shadcn Primitives

**Goal:** Install core shadcn components and do mechanical replacements of the most common legacy patterns.

### 1.1 Install shadcn components

```bash
npx shadcn@latest add dialog alert-dialog badge input label table \
  dropdown-menu separator skeleton tabs card tooltip scroll-area \
  select switch checkbox popover sheet
```

Verify each component lands in `src/components/ui/` and imports resolve.

### 1.2 Replace Buttons

| Find | Replace With |
|------|-------------|
| `className="btn"` or `className="btn btn-ghost"` | `<Button variant="ghost">` |
| `className="btn btn-primary"` | `<Button>` (default variant) |
| `className="btn btn-danger"` | `<Button variant="destructive">` |
| `className="hud-button"` | `<Button variant="outline">` |
| `className="hud-button-ghost"` | `<Button variant="ghost">` |

Import: `import { Button } from '@/components/ui/button'`

**Files affected:** All 13 pages + AppHeader + App.tsx (~50+ button instances).

### 1.3 Replace Modals

| Find | Replace With |
|------|-------------|
| `hud-modal-overlay` + `hud-modal` (17 instances, 9 files) | `<Dialog>` / `<DialogContent>` |
| `<Modal>` component usage | `<Dialog>` |
| `<ConfirmDialog>` | `<AlertDialog>` |

Files with raw modal pattern:
- `App.tsx` (workspace modal)
- `BlueprintsPage.tsx` (x3)
- `BlueprintDetailPage.tsx` (x3)
- `PipelinesPage.tsx` (x3)
- `PipelineDetailPage.tsx` (x2)
- `FlowBuilderPage.tsx` (x1)
- `HelpPage.tsx` (x1)

After migration, delete `src/components/ui/Modal.tsx` and `ConfirmDialog.tsx`.

### 1.4 Replace Badges

| Find | Replace With |
|------|-------------|
| `<StatusBadge status={s} />` | `<Badge variant={statusVariant(s)}>` with custom status variants |
| `className="badge badge-success"` etc. | `<Badge variant="success">` |
| `className="bp-badge bp-badge-warn"` etc. | `<Badge variant="warning">` |

Create a `statusVariant()` helper or extend Badge variants to include `success`, `running`, `failed`, `queued`, `canceled` using the locked-in palette colors.

### 1.5 Replace Inputs

| Find | Replace With |
|------|-------------|
| `className="hud-input"` (86 refs) | `<Input>` from shadcn |
| `className="hud-label"` (33 refs) | `<Label>` or `<FieldLabel>` inside `<Field>` |

### 1.6 Replace Dropdowns

| Find | Replace With |
|------|-------------|
| Manual dropdown in AppHeader (workspace selector) | `<DropdownMenu>` |
| Manual dropdown in AppHeader (app menu) | `<DropdownMenu>` |

### 1.7 Validation Build

After all Phase 1 replacements:
- `npm run build` passes with zero errors
- Visual spot-check all 13 pages
- Dark mode toggle works
- All modals open/close correctly with Escape key
- All dropdowns have keyboard navigation

### 1.8 CSS Cleanup (Phase 1 only)

Remove from `theme.css`:
- `.btn`, `.btn-primary`, `.btn-ghost`, `.btn-danger` classes
- `.badge`, `.badge-success`, `.badge-running`, `.badge-failed`, `.badge-queued`, `.badge-canceled` classes

Remove from `index.css`:
- `.hud-button`, `.hud-button-ghost` classes
- `.hud-modal-overlay`, `.hud-modal` classes

Keep everything else for now — later phases handle the rest.

---

## Phase 2 — Context & Architecture

**Goal:** Eliminate prop drilling. Establish "props down, events up" pattern.

### 2.1 DaemonContext

```typescript
interface DaemonContextValue {
  status: 'running' | 'stopped' | 'unknown';
  address: string;
  workspaceId: string;
  workspaces: Workspace[];
  selectWorkspace: (id: string) => void;
  createWorkspace: (name: string) => Promise<void>;
}
```

Provides: daemon connection state, workspace selection.
Consumers: AppHeader, all pages that check `daemonStatus`.

### 2.2 NavigationContext

```typescript
interface NavigationContextValue {
  page: NavPage;
  navigate: (page: NavPage, context?: NavigationParams) => void;
  selectedRunId?: string;
  selectedBlueprintPath?: string;
  selectedPipelinePath?: string;
  goBack: () => void;
}
```

Provides: current page, navigation with context, back navigation.
Consumers: AppHeader (breadcrumbs), AppNav, all pages with navigation callbacks.

### 2.3 Remove Custom Events

Replace `window.dispatchEvent(new CustomEvent('hadron:refresh'))` with a `refresh` callback in context or passed as prop from App.tsx.

### 2.4 Slim Down App.tsx

After context extraction, App.tsx should be ~150 lines: provider wrappers, page router switch, keyboard shortcut handler.

---

## Phase 3 — Page-by-Page Migration

**Goal:** Convert each page from inline styles + legacy CSS to Tailwind + shadcn.

Migration order (smallest to largest, establishing patterns first):

| Order | Page | Lines | Key Patterns |
|-------|------|-------|-------------|
| 1 | RunsPage | 168 | List, filters, keyboard nav |
| 2 | SettingsPage | 198 | Forms, sections, switches |
| 3 | DashboardPage | 262 | Cards, stats, tables |
| 4 | RunDetailPage | 299 | Live data, grouped display |
| 5 | TelemetryPage | 320 | Log viewer, filters |
| 6 | SchedulerPage | 380 | CRUD, modals, forms |
| 7 | PipelineDetailPage | 404 | Read-only detail view |
| 8 | BlueprintDetailPage | 584 | Split-pane, expandable sections |
| 9 | BlueprintsPage | 660 | File browser, batch actions |
| 10 | HelpPage | 759 | Static content, tabs |

Per-page checklist:
- [ ] Replace all `style={{}}` with Tailwind classes
- [ ] Replace all `hud-*` classes with Tailwind/shadcn
- [ ] Replace all template literal classNames with `cn()`
- [ ] Use `<Table>` for data tables
- [ ] Use `<Card>` for sections/panels
- [ ] Use `<Input>` / `<Field>` for form elements
- [ ] Verify dark mode
- [ ] Build passes

---

## Phase 4 — God Component Decomposition

**Goal:** Break the three largest pages into focused sub-components.

### 4.1 BlueprintWizardPage (1168 lines)

Extract:
- `WizardShell` — step sidebar + navigation + footer
- `WizardStep<N>` — one component per step (8 total)
- `useWizardState()` — custom hook for wizard form state
- `blueprintYaml.ts` — YAML serialization/parsing (move to utils/)

### 4.2 PipelinesPage (869 lines)

Extract:
- `PipelineBrowser` — file list with search/sort
- `PipelineEditor` — inline editor form for pipeline stages
- `PipelineYamlPreview` — YAML display panel

### 4.3 FlowBuilderPage (913 lines)

Extract:
- `FlowCanvas` — React Flow wrapper with drag-and-drop
- `FlowExecutionOverlay` — live run status overlay
- `FlowYamlExport` — YAML export dialog
- `useFlowState()` — custom hook for flow graph state

---

## Phase 5 — CSS Cleanup

**Goal:** Consolidate to a single CSS file.

### 5.1 Remove theme.css

All `.badge`, `.btn`, `.table`, `.stat-card`, `.section`, `.page-header`, `.kbd`, `.mono` classes replaced by Tailwind/shadcn.

### 5.2 Remove tokens.css

All values already bridged to Tailwind via `@theme inline` in `index.css`. Verify no direct `var(--bg-base)` etc. references remain.

### 5.3 Clean index.css

Remove:
- All `hud-*` class definitions
- All `.status-*` utility classes
- All layout classes replaced by Tailwind (`.app-shell`, `.sidebar`, `.content`, etc.)
- Legacy `:root` RGB variables
- `rgb(var(--*))` patterns in flow components

Keep:
- `@import "tailwindcss"` and related imports
- `@theme inline` block
- `.dark` theme overrides
- `@layer base` resets
- Any React Flow / @xyflow overrides that can't move to Tailwind

### 5.4 Remove fonts.css

If `@fontsource-variable/geist` covers all font needs, remove the local font CSS file.

### 5.5 Final State

```
src/
  index.css          # ~100 lines: imports, @theme, dark mode, base resets, flow overrides
  lib/utils.ts       # cn()
  components/ui/     # All shadcn components
```

---

## Tracking

After each sub-task, the working session should note:
- Files changed
- Components migrated
- Legacy classes removed
- Build status (pass/fail)
- Any blockers or design decisions made

## Exit Criteria (Beta Ready)

- [ ] Zero inline `style={{}}` in TSX files
- [ ] Zero `hud-*` class references
- [ ] Zero `rgb(var(--*))` patterns
- [ ] Zero hardcoded color/spacing values in TSX
- [ ] All modals use `<Dialog>` or `<AlertDialog>` with keyboard support
- [ ] All forms use shadcn `<Field>` / `<FieldGroup>` pattern
- [ ] All buttons use `<Button>` with variants
- [ ] All tables use `<Table>`
- [ ] `cn()` used for all conditional classes
- [ ] Dark mode works on all 13 pages
- [ ] `theme.css` and `tokens.css` deleted
- [ ] `index.css` under 150 lines
- [ ] No god components over 500 lines
- [ ] App.tsx under 200 lines
- [ ] Build passes with zero TypeScript errors
