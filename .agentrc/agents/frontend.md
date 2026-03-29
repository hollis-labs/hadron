# Frontend Context — Hadron

> Project-specific frontend conventions. Loaded by the frontend agent role when working in this project.

## Current State (2026-03-29)

**UI redesign shipped, beta stabilization in progress.** Design system (tokens.css + theme.css) applied across all 13 pages. Tailwind CSS v4 and shadcn/ui 4.1.1 installed and configured but barely adopted — the codebase is a hybrid of legacy CSS classes, inline styles, and the new toolchain. The next phase is a systematic migration to shadcn components + Tailwind utility classes, with a "props down, events up" component architecture.

### Audit Summary

| Metric | Count | Severity |
|--------|-------|----------|
| Inline `style={{}}` props | 852 | Critical |
| `hud-*` legacy class refs | 185 | High |
| `rgb(var(--*))` legacy pattern | 142 | Medium |
| Unmigrated modals (`hud-modal-overlay`) | 17 in 9 files | Medium |
| Hardcoded color values in TSX | 38 | High |
| Hardcoded spacing/sizing in inline styles | 439 | Critical |
| `cn()` usage | 1 | Near-zero adoption |
| shadcn components installed | 1 (Button) | Minimal |
| Tailwind classes in TSX | ~0 | Not adopted |
| Unique legacy CSS classes | 139 across 3 files | High fragmentation |

### What's Working

- **Tailwind v4 infrastructure is solid**: `@tailwindcss/vite` plugin, `@theme inline` block mapping CSS vars to Tailwind tokens, `@custom-variant dark` for dark mode, Geist font loaded via `@fontsource-variable`.
- **shadcn config is correct**: `components.json` with `base-nova` preset, `@base-ui/react` primitives, CVA, `clsx` + `tailwind-merge`, `cn()` utility in `src/lib/utils.ts`.
- **Color palette is locked in**: Blue/amber/red on neutral zinc (see memory: `feedback_ui_color_palette.md`). Defined in `tokens.css`, bridged to Tailwind via `@theme inline` in `index.css`.
- **Dark mode ready**: `.dark` class with oklch overrides in `index.css`. shadcn semantic tokens (`bg-background`, `text-foreground`, etc.) mapped.
- **Type system is comprehensive**: 30+ interfaces in `api/types.ts` matching Go structs.
- **Hooks are production-quality**: `usePoll` (race-safe, visibility-aware), `useRunEvents` (cursor-based, deduped).

### What's Broken

- **852 inline styles**: Nearly every component uses `style={{}}` for layout, spacing, colors, and sizing. This defeats the design system and blocks theming/dark mode.
- **Zero Tailwind adoption in components**: Tailwind is imported but no TSX file uses Tailwind classes. All styling is inline or legacy CSS.
- **1 shadcn component installed** (Button), and it's **not used** — pages still use `.btn` / `.btn-primary` / `.btn-ghost` CSS classes.
- **Two parallel CSS systems**: Legacy `hud-*` classes (47 definitions) and newer `theme.css` classes (32 definitions) coexist with no migration path.
- **`cn()` used exactly once**: The shadcn class-merging utility is essentially dead code.
- **Modal system fragmented**: `Modal.tsx` exists but 9 files still use raw `hud-modal-overlay` divs. No keyboard support (Escape), no focus trap.
- **God components**: `BlueprintWizardPage` (1168 lines), `FlowBuilderPage` (913 lines), `PipelinesPage` (869 lines).
- **Prop drilling**: `App.tsx` passes 8-12 props to layout components and 4-7 to each page. `daemonStatus`, `workspaceId`, and `onOpenRun` tunnel through 6+ levels.
- **No form abstraction**: All forms use raw `useState` with manual validation. Checkbox/input/select patterns duplicated across SettingsPage, SchedulerPage, BlueprintWizardPage.

## Stack

- **Framework:** React 18.2 (SPA, no Next.js)
- **Build:** Vite 5.0 with `@vitejs/plugin-react` + `@tailwindcss/vite`
- **Language:** TypeScript 5.3 (strict mode, `noEmit`)
- **Styling:** Tailwind CSS v4.2.2 (installed, not yet adopted in components)
- **Components:** shadcn/ui 4.1.1 with `base-nova` preset, `@base-ui/react` primitives
- **Utilities:** `cn()` via `clsx` + `tailwind-merge`, CVA for variants
- **Typography:** Geist Variable (via @fontsource-variable), Inter (legacy), JetBrains Mono (legacy)
- **Icons:** lucide-react 0.546
- **Toasts:** sonner 2.0.7
- **Flow diagrams:** @xyflow/react 12.10.1
- **Desktop shell:** Wails v2 (Go backend + webview frontend)
- **Module type:** ESM (`"type": "module"`)
- **Dev port:** 34116, proxies `/v1` to daemon at `127.0.0.1:8095`

No router library. Navigation is state-driven (`NavPage` union type, `useState` in `App.tsx`).

## Project Structure

```
cmd/hadron-app/frontend/
├── index.html
├── package.json
├── components.json             # shadcn config (base-nova, @base-ui/react)
├── tsconfig.json               # @/* → ./src/*
├── vite.config.ts              # tailwindcss vite plugin, proxy to daemon
└── src/
    ├── main.tsx                # ReactDOM entrypoint (imports index.css)
    ├── App.tsx                 # Root component — shell layout + all state (430 lines)
    ├── tokens.css              # Design tokens — palette, type scale, spacing, radii (74 lines)
    ├── index.css               # Tailwind imports + legacy CSS + @theme inline block (1050 lines)
    ├── theme.css               # Shared component classes — badge, btn, table, stat (318 lines)
    ├── vite-env.d.ts
    ├── lib/
    │   └── utils.ts            # cn() utility (clsx + tailwind-merge)
    ├── api/
    │   ├── types.ts            # All TypeScript interfaces (296 lines)
    │   └── client.ts           # API layer: REST fetch + Wails Go bindings (335 lines)
    ├── hooks/
    │   ├── usePoll.ts          # Generic polling hook (87 lines)
    │   └── useRunEvents.ts     # Streaming run events with cursor pagination (58 lines)
    ├── utils/
    │   ├── format.ts           # Duration/date formatters (74 lines)
    │   ├── path.ts             # shortPath, basename (11 lines)
    │   ├── string.ts           # unquote (4 lines)
    │   └── yaml.ts             # parsePipelineYaml (137 lines)
    ├── demo/
    │   ├── demoMode.ts         # Global demo toggle
    │   └── data.ts             # Mock data for demo mode
    ├── components/
    │   ├── layout/
    │   │   ├── AppHeader.tsx   # Top bar: breadcrumbs, dropdowns, daemon status (231 lines)
    │   │   ├── AppNav.tsx      # 52px icon-rail sidebar (67 lines)
    │   │   └── AppFooter.tsx   # Keyboard hint bar (58 lines)
    │   ├── ui/
    │   │   ├── button.tsx      # shadcn Button (CVA + @base-ui) — NOT USED IN APP
    │   │   ├── StatusBadge.tsx # Pill badge with dot (33 lines)
    │   │   ├── EmptyState.tsx  # Centered placeholder (23 lines)
    │   │   ├── Spinner.tsx     # SVG spinner with INLINE keyframes (21 lines)
    │   │   ├── Modal.tsx       # Overlay wrapper — no keyboard, no focus trap (21 lines)
    │   │   ├── ConfirmDialog.tsx # Confirm/delete dialog (30 lines)
    │   │   ├── PageHeader.tsx  # Page header with back button (27 lines)
    │   │   ├── CronBuilder.tsx # Cron expression editor (274 lines)
    │   │   └── RunInputsModal.tsx # Blueprint input form modal (164 lines)
    │   └── flow/
    │       ├── StageNode.tsx       # React Flow node (86 lines)
    │       ├── ConditionalEdge.tsx  # React Flow edge (96 lines)
    │       ├── NodePalette.tsx      # Drag-and-drop palette (134 lines)
    │       └── StagePropertyPanel.tsx # Stage property editor (269 lines)
    └── pages/
        ├── DashboardPage.tsx       # Stats + recent runs (262 lines)
        ├── BlueprintsPage.tsx      # File browser + batch ops (660 lines)
        ├── BlueprintDetailPage.tsx  # Split-pane viewer (584 lines)
        ├── BlueprintWizardPage.tsx  # 8-step wizard (1168 lines) ← GOD COMPONENT
        ├── PipelinesPage.tsx       # Pipeline browser + editor (869 lines) ← GOD COMPONENT
        ├── PipelineDetailPage.tsx   # Pipeline viewer (404 lines)
        ├── FlowBuilderPage.tsx     # Visual editor (913 lines) ← GOD COMPONENT
        ├── RunsPage.tsx            # Run list + filters (168 lines)
        ├── RunDetailPage.tsx       # Live run monitor (299 lines)
        ├── SchedulerPage.tsx       # Cron schedule CRUD (380 lines)
        ├── TelemetryPage.tsx       # Log viewer (320 lines)
        ├── SettingsPage.tsx        # Settings form (198 lines)
        └── HelpPage.tsx            # Shortcuts + docs (759 lines)
```

Total frontend: ~11,327 lines across 38 files (~8,959 TS/TSX + ~2,368 CSS).

## Architecture Target

### Props Down, Events Up

All data flows down through props. All mutations flow up through typed callback props. No custom DOM events (`hadron:refresh`), no direct state mutation across component boundaries.

```
App.tsx (shell)
├── Context providers (DaemonContext, NavigationContext)
├── AppHeader — reads context, emits onNavigate/onWorkspaceChange
├── AppNav — reads context, emits onNavigate
├── <Page> — receives data props, emits typed event callbacks
│   └── shadcn components — receives props, emits onChange/onAction
└── AppFooter — reads context
```

### shadcn Component Mapping

| Current Custom | Replace With | Priority |
|----------------|-------------|----------|
| `.btn` / `.btn-primary` / `.btn-ghost` | `<Button variant="...">` | P0 |
| `hud-modal-overlay` + `hud-modal` | `<Dialog>` | P0 |
| `ConfirmDialog.tsx` | `<AlertDialog>` | P0 |
| `.badge` / `.badge-success` etc. | `<Badge variant="...">` | P0 |
| `hud-input` | `<Input>` | P1 |
| `hud-label` | `<FieldLabel>` inside `<Field>` | P1 |
| `hud-table` / `.table` | `<Table>` | P1 |
| Inline dropdown menus (AppHeader) | `<DropdownMenu>` | P1 |
| `EmptyState.tsx` | `<Empty>` (shadcn) | P2 |
| `Spinner.tsx` | `<Spinner>` (shadcn) | P2 |
| `PageHeader.tsx` | Custom but Tailwind-styled | P2 |
| `.stat-card` / `.stat-grid` | `<Card>` composition | P2 |
| `.section` / `.section-header` | `<Card>` with `<CardHeader>` | P2 |
| Settings form | `<FieldGroup>` + `<Field>` + `<Switch>` / `<Input>` | P2 |
| Tabs (HelpPage) | `<Tabs>` + `<TabsList>` + `<TabsTrigger>` | P3 |
| Keyboard hints | `<Kbd>` (shadcn) or custom with Tailwind | P3 |

### Styling Rules (Beta Phase)

1. **No new inline styles.** Use Tailwind utility classes via `className`.
2. **No new CSS classes.** Use Tailwind + shadcn semantic tokens.
3. **Use `cn()` for conditional classes.** Not template literals.
4. **Semantic colors only.** `bg-primary`, `text-muted-foreground`, `border-destructive` — never raw hex/rgb.
5. **`gap-*` for spacing.** Not `space-y-*` or inline margin/padding.
6. **`size-*` for equal dimensions.** Not `w-* h-*`.
7. **Dark mode via semantic tokens.** No manual `dark:` overrides.

## CSS Architecture

### File Hierarchy (current)

```
main.tsx imports:
  1. ./assets/fonts/fonts.css     # Local font files (may be redundant with @fontsource)
  2. ./tokens.css                 # Design tokens (palette, spacing, radii, typography)
  3. ./index.css                  # Tailwind + legacy CSS + @theme inline + dark mode
  4. ./theme.css                  # Component classes (.badge, .btn, .table, .stat-card)
```

### File Hierarchy (target)

```
main.tsx imports:
  1. ./index.css                  # Tailwind + @theme inline + dark mode + base resets
```

All tokens mapped through `@theme inline` block. All component styling via Tailwind classes on shadcn components. `tokens.css` and `theme.css` removed once migration complete.

### Token Bridge (current, in index.css)

The `@theme inline` block maps CSS variables to Tailwind's semantic token system:
- `--color-background`, `--color-foreground`, `--color-primary`, etc. → `bg-background`, `text-foreground`, `bg-primary`
- `--color-destructive` → `bg-destructive`, `text-destructive`
- `--color-muted` → `bg-muted`, `text-muted-foreground`
- `--color-sidebar-*` variants for nav
- `--radius-sm` through `--radius-4xl` calculated from base `--radius`
- `--font-sans` = Geist Variable

Dark mode: `.dark` class on `<html>` overrides all oklch values.

## Patterns in Use

### Navigation
State-based in `App.tsx` via `useState<NavPage>('dashboard')`. No URL routing. Pages rendered conditionally. Navigation context (selected IDs, paths) held as separate state in `App.tsx` and passed as props.

### Data Fetching
- **REST API:** `api/client.ts` typed async functions wrapping `fetch()`.
- **Wails Go bindings:** `window.go.main.App` methods for file system, settings, preferences.
- **Polling:** `usePoll<T>()` with visibility-aware pausing.
- **Streaming:** `useRunEvents()` with cursor-based pagination.
- **Demo mode:** Global toggle swaps API responses transparently.

### Forms (current — to be migrated)
- Local `useState` for form state. No form library.
- Manual validation in submit handlers.
- Should migrate to shadcn `FieldGroup` + `Field` pattern.

### Modals (current — to be migrated)
- `Modal.tsx` wraps overlay + stopPropagation. No Escape key, no focus trap.
- 9 files still use raw `hud-modal-overlay`.
- Should migrate to shadcn `Dialog` / `AlertDialog` / `Sheet`.

## Anti-Patterns (Active)

### God Components (3 files)
- `BlueprintWizardPage.tsx` (1168 lines): 8 wizard steps, YAML serialization, autosave, form state.
- `FlowBuilderPage.tsx` (913 lines): React Flow + drag-and-drop + execution overlay + YAML export.
- `PipelinesPage.tsx` (869 lines): File browser + inline editor + YAML serializer.

### Prop Drilling (12+ props through App.tsx)
- `daemonStatus` passed to nearly every page.
- `workspaceId` passed to 5+ pages.
- `onOpenRun` callback passed to 6 pages.
- **Fix:** Extract `DaemonContext` and `NavigationContext`.

### Mixed Styling (3 systems active)
- Legacy `hud-*` classes (185 refs)
- Theme classes `.btn`, `.badge`, `.table` (theme.css)
- Inline `style={{}}` (852 occurrences)
- Tailwind classes (near zero)
- **Fix:** Migrate to Tailwind + shadcn, remove legacy CSS incrementally.

### Custom Events
- `window.dispatchEvent(new CustomEvent('hadron:refresh'))` for keyboard shortcuts.
- **Fix:** Move to React callback props or context.

## Migration Phases (Beta Stabilization)

### Phase 1 — Foundation (shadcn primitives)
Install and adopt core shadcn components: `dialog`, `alert-dialog`, `badge`, `input`, `table`, `dropdown-menu`, `separator`, `skeleton`, `tabs`, `card`, `tooltip`, `scroll-area`.

Replace all `hud-modal-overlay` with `<Dialog>`. Replace `ConfirmDialog` with `<AlertDialog>`. Replace `.btn` with `<Button>`. Replace `.badge` with `<Badge>`.

### Phase 2 — Context & Architecture
Extract `DaemonContext` (status, address, workspace). Extract `NavigationContext` (page, selected IDs, callbacks). Remove prop drilling from App.tsx. Remove custom DOM events.

### Phase 3 — Page Migration (one page at a time)
Convert each page from inline styles + legacy CSS to Tailwind classes + shadcn components. Start with smallest pages (RunsPage, SettingsPage) to establish patterns, then tackle larger ones.

### Phase 4 — God Component Decomposition
Break `BlueprintWizardPage` into step sub-components. Break `PipelinesPage` into browser + editor. Break `FlowBuilderPage` into canvas + overlay + export modules.

### Phase 5 — CSS Cleanup
Remove `theme.css` once all consumers migrated. Remove `hud-*` classes from `index.css`. Remove `tokens.css` once all values live in `@theme inline`. Consolidate to single `index.css`.

## Reference Implementations

### 1. `usePoll<T>()` — `src/hooks/usePoll.ts`
Well-structured generic polling hook: typed generics, stable fetcher ref, race condition handling, visibility-aware pausing.

### 2. `StatusBadge` — `src/components/ui/StatusBadge.tsx`
Clean data-driven UI primitive. Will be replaced by shadcn `<Badge>` with custom status variants.

### 3. `AppNav` — `src/components/layout/AppNav.tsx`
Data-driven navigation: declarative arrays, parent highlighting, exported `NavPage` type.

### 4. `button.tsx` — `src/components/ui/button.tsx`
Reference shadcn component: CVA variants, @base-ui primitive, semantic Tailwind classes. Target pattern for all components.

## Notes

- This is a **Wails v2 desktop app**, not a web app. Frontend runs inside a native webview.
- TypeScript is used well. `types.ts` is comprehensive with matching Go struct types.
- The locked-in color palette (blue success, amber running, red failed) must be preserved through migration.
- No tests exist in the frontend. No linting beyond TypeScript.
- Demo mode is clean — transparent API response swap without UI conditionals.
- The `usePoll` and `useRunEvents` hooks are production-quality.
