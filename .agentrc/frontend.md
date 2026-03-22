# Frontend Context — Hadron

> Project-specific frontend conventions. Loaded by the frontend agent role when working in this project.

## Current State (2026-03-22)

**UI redesign complete (Phases 0–5).** New design system applied across all 13 pages. Builds and runs in Wails. Polish pass needed.

### Known Issues for Next Session
- **Font size too small in Wails desktop app** — likely Google Fonts not loading in the webview. May need to bundle Inter + JetBrains Mono as local font files in `public/` or `assets/`. Fall back gracefully.
- **Remaining legacy CSS** — ~5 files still have `rgb(var(--*))` references (flow components), ~17 files still reference `hud-input`/`hud-label` from legacy CSS. Low priority, functional.
- **Remaining inline modals** — BlueprintsPage, BlueprintDetailPage, PipelinesPage, PipelineDetailPage, FlowBuilderPage still use raw `hud-modal-overlay` pattern instead of shared `<Modal>` component. Functional but should migrate.
- **Additional polish TBD** — user will identify specific items in next session.

## Stack

- **Framework:** React 18.2 (SPA, no Next.js)
- **Build:** Vite 5.0 with `@vitejs/plugin-react`
- **Language:** TypeScript 5.3 (strict mode, `noEmit`)
- **Styling:** CSS with design token system — `tokens.css` (palette), `theme.css` (shared classes), `index.css` (legacy + layout)
- **Typography:** Inter (UI), JetBrains Mono (data/code) — loaded via Google Fonts, may need local bundling for Wails
- **Icons:** lucide-react 0.546
- **Toasts:** sonner 2.0.7
- **Flow diagrams:** @xyflow/react 12.10.1
- **Desktop shell:** Wails v2 (Go backend + webview frontend)
- **Module type:** ESM (`"type": "module"`)
- **Dev port:** 34116, proxies `/v1` to daemon at `127.0.0.1:8095`

No router library is used. Navigation is state-driven (`NavPage` union type, `useState` in `App.tsx`).

## Project Structure

```
cmd/hadron-app/frontend/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
└── src/
    ├── main.tsx                    # ReactDOM entrypoint (imports tokens.css → index.css → theme.css)
    ├── App.tsx                     # Root component — row layout: Nav | column(Header, Content, Footer)
    ├── tokens.css                  # NEW: Design tokens (palette, type scale, spacing, radii)
    ├── index.css                   # Legacy + layout CSS (sidebar, header, nav-item, content grid)
    ├── theme.css                   # NEW: Shared component classes (badge, btn, table, stat-card, section, footer)
    ├── vite-env.d.ts
    ├── api/
    │   ├── types.ts                # All TypeScript interfaces (297 lines)
    │   └── client.ts               # API layer: REST fetch + Wails Go bindings (336 lines)
    ├── hooks/
    │   ├── usePoll.ts              # Generic polling hook with visibility-aware timer (87 lines)
    │   └── useRunEvents.ts         # Streaming run events with cursor pagination (58 lines)
    ├── utils/                      # NEW: Shared utilities
    │   ├── format.ts               # formatDuration, formatRunDuration, formatMs, formatTime, formatDate, etc.
    │   ├── path.ts                 # shortPath, basename
    │   ├── string.ts               # unquote
    │   └── yaml.ts                 # parsePipelineYaml (shared parser for all pipeline pages)
    ├── demo/
    │   ├── demoMode.ts             # Global demo toggle with subscriber pattern (15 lines)
    │   └── data.ts                 # Mock data for demo mode
    ├── components/
    │   ├── layout/
    │   │   ├── AppHeader.tsx       # Top bar: breadcrumbs, daemon status, workspace selector, menu
    │   │   ├── AppNav.tsx          # 52px icon-rail sidebar with tooltips
    │   │   └── AppFooter.tsx       # Keyboard hint bar with kbd badges
    │   ├── ui/
    │   │   ├── StatusBadge.tsx     # Pill badge with dot (badge + badge-{status} classes)
    │   │   ├── EmptyState.tsx      # Centered empty placeholder
    │   │   ├── Spinner.tsx         # SVG loading spinner
    │   │   ├── Modal.tsx           # NEW: Shared modal overlay wrapper
    │   │   ├── ConfirmDialog.tsx   # NEW: Shared confirm/delete dialog
    │   │   ├── PageHeader.tsx      # NEW: Shared page header with back button
    │   │   ├── CronBuilder.tsx     # Interactive cron expression editor (274 lines)
    │   │   └── RunInputsModal.tsx  # Modal form for blueprint input parameters (uses Modal)
    │   └── flow/
    │       ├── StageNode.tsx       # React Flow custom node for pipeline stages (86 lines)
    │       ├── ConditionalEdge.tsx # React Flow custom edge with condition labels (96 lines)
    │       ├── NodePalette.tsx     # Drag-and-drop blueprint palette (134 lines)
    │       └── StagePropertyPanel.tsx # Side panel for editing stage properties (269 lines)
    └── pages/
        ├── DashboardPage.tsx       # Stats grid, recent runs table, activity timeline (325 lines)
        ├── BlueprintsPage.tsx      # File browser with batch operations (660 lines)
        ├── BlueprintDetailPage.tsx  # Split-pane blueprint viewer (584 lines)
        ├── BlueprintWizardPage.tsx  # Multi-step blueprint creation wizard (1168 lines)
        ├── PipelinesPage.tsx       # Pipeline browser + inline editor (1000 lines)
        ├── PipelineDetailPage.tsx   # Pipeline viewer with stage timeline (483 lines)
        ├── FlowBuilderPage.tsx     # Visual pipeline editor with React Flow (1021 lines)
        ├── RunsPage.tsx            # Run list with filters (189 lines)
        ├── RunDetailPage.tsx       # Live run monitor with grouped events (312 lines)
        ├── SchedulerPage.tsx       # Cron schedule CRUD (561 lines)
        ├── TelemetryPage.tsx       # Structured log viewer (392 lines)
        ├── SettingsPage.tsx        # Settings form (205 lines)
        └── HelpPage.tsx            # Keyboard shortcuts + docs (759 lines)
```

Total frontend: ~9,580 lines across 35 files.

## Component Inventory

### Layout (3 components — all reusable)
| Component | File | Purpose |
|-----------|------|---------|
| `AppHeader` | `components/layout/AppHeader.tsx` | Top bar with logo, breadcrumb nav, workspace dropdown, daemon status dot, elapsed timer, demo mode toggle, settings menu |
| `AppNav` | `components/layout/AppNav.tsx` | Sidebar nav, data-driven from `NAV_ITEMS` array |
| `AppFooter` | `components/layout/AppFooter.tsx` | Contextual keyboard shortcut hints per page |

### UI Primitives (5 components — all reusable)
| Component | File | Purpose |
|-----------|------|---------|
| `StatusBadge` | `components/ui/StatusBadge.tsx` | Colored status label (success/running/failed/queued/canceled) |
| `EmptyState` | `components/ui/EmptyState.tsx` | Centered placeholder text for empty lists |
| `Spinner` | `components/ui/Spinner.tsx` | Inline SVG loading spinner |
| `CronBuilder` | `components/ui/CronBuilder.tsx` | Interactive 5-field cron expression editor with presets, validation, and human-readable description |
| `RunInputsModal` | `components/ui/RunInputsModal.tsx` | Dynamic form modal for blueprint input parameters, supports string/number/boolean/array/enum types |

### Flow Components (4 components — domain-specific, reusable within flow builder)
| Component | File | Purpose |
|-----------|------|---------|
| `StageNode` | `components/flow/StageNode.tsx` | Custom React Flow node showing pipeline stage with status dot, name, blueprint path, condition badge |
| `ConditionalEdge` | `components/flow/ConditionalEdge.tsx` | Custom React Flow edge with dashed style for conditional connections and inline condition labels |
| `NodePalette` | `components/flow/NodePalette.tsx` | Draggable blueprint list panel for adding stages to the flow canvas |
| `StagePropertyPanel` | `components/flow/StagePropertyPanel.tsx` | Side panel for editing a selected stage's properties (name, path, condition, inputs, outputs) |

### Pages (13 components — page-specific)
| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `DashboardPage` | `pages/DashboardPage.tsx` | 325 | Stats grid, recent runs table, 24h activity bar chart, per-blueprint stats |
| `BlueprintsPage` | `pages/BlueprintsPage.tsx` | 660 | File browser with search, sort, batch select, validate, run, move, copy, archive, delete |
| `BlueprintDetailPage` | `pages/BlueprintDetailPage.tsx` | 584 | Split-pane: left=metadata/inputs/imports/hooks, right=section/task timeline |
| `BlueprintWizardPage` | `pages/BlueprintWizardPage.tsx` | 1168 | 8-step form wizard for creating/editing blueprints with YAML preview |
| `PipelinesPage` | `pages/PipelinesPage.tsx` | 1000 | Pipeline file browser + inline pipeline editor with stage CRUD |
| `PipelineDetailPage` | `pages/PipelineDetailPage.tsx` | 483 | Read-only pipeline viewer with collapsible stage details |
| `FlowBuilderPage` | `pages/FlowBuilderPage.tsx` | 1021 | Visual pipeline editor: React Flow canvas, drag-and-drop from palette, stage property editing, YAML export, live execution overlay |
| `RunsPage` | `pages/RunsPage.tsx` | 189 | Run list with status filter chips and text search |
| `RunDetailPage` | `pages/RunDetailPage.tsx` | 312 | Live run monitoring: grouped task view with expandable logs, progress bar, raw event view |
| `SchedulerPage` | `pages/SchedulerPage.tsx` | 561 | Schedule CRUD with cron builder integration, edit modal, one-time schedule support |
| `TelemetryPage` | `pages/TelemetryPage.tsx` | 392 | Structured log viewer: run list + detail view with level/text filters, error boundary wrapper |
| `SettingsPage` | `pages/SettingsPage.tsx` | 205 | Settings form: general, execution, safety, and telemetry sections |
| `HelpPage` | `pages/HelpPage.tsx` | 759 | Static help content with keyboard shortcuts and feature docs |

## Patterns in Use

### Navigation
All routing is state-based in `App.tsx` via `useState<NavPage>('dashboard')`. No URL-based routing. Pages are conditionally rendered with `{phase === 'xxx' && <Page />}`. Navigation context (selected run ID, blueprint path, etc.) is held as separate state variables in `App.tsx` and passed as props.

### Data Fetching
- **REST API:** `api/client.ts` provides typed async functions wrapping `fetch()` against the hadrond daemon. A generic `apiFetch<T>()` helper handles JSON serialization and error extraction.
- **Wails Go bindings:** `window.go.main.App` methods for file system operations, blueprint parsing, settings, and preferences. Each function checks for binding availability and falls back gracefully.
- **Polling:** `usePoll<T>()` hook provides interval-based data refresh with visibility-aware pausing (stops when tab is hidden). Used by Dashboard, Runs, Schedules, and Pipelines pages.
- **Streaming events:** `useRunEvents()` hook handles cursor-based pagination for run events with continuous polling during active runs and a final catch-up fetch after completion.
- **Demo mode:** Global boolean toggle (`demo/demoMode.ts`) causes API functions to return mock data from `demo/data.ts` instead of hitting the daemon.

### Styling
- **Design tokens:** `tokens.css` defines the full palette using hex values (e.g., `--bg-base: #09090b`, `--status-success: #3b82f6`). Neutral zinc backgrounds, blue/amber/red status colors.
- **Theme classes:** `theme.css` provides shared component classes: `.badge`, `.btn`, `.table`, `.stat-card`, `.section`, `.footer`, `.kbd`, `.mono`, plus column utilities (`.col-shrink`, `.col-right`, `.col-primary`).
- **Legacy CSS:** `index.css` still contains old `hud-*` classes and RGB-triplet `:root` vars (used by `hud-input`, `hud-label`, flow components). These work but should migrate to new tokens over time.
- **Layout:** Row-first shell (sidebar | main column). 52px icon-rail sidebar, 48px header, 30px footer. Content area has a faint 24px grid background.
- **Typography:** Inter for UI, JetBrains Mono for data. Loaded via Google Fonts (may need local bundling for Wails offline).
- **Inline styles:** Reduced significantly from original but still present. Dynamic values and layout-specific styles remain inline.
- **No CSS modules or scoped CSS** — all styles are global across three CSS files.

### Forms
- Local `useState` for form state. No form library (no react-hook-form, no formik).
- Validation is manual — checking required fields in submit handlers with inline error state.
- Form state objects are often defined as interfaces (`NewScheduleForm`, `PipelineForm`, `WizardBlueprint`) with explicit empty defaults.

### Modals
- Modals use the `hud-modal-overlay` + `hud-modal` CSS classes directly in page components.
- Pattern: boolean state toggle (`showModal` / `deleteConfirm`) with inline rendering via `{showModal && <div className="hud-modal-overlay">...}`.
- No shared `Modal` component — each page reimplements the overlay/close/stop-propagation pattern.

### Keyboard Navigation
- Global keyboard handler in `App.tsx` via `useCallback` + `addEventListener('keydown')`.
- Per-page arrow key navigation implemented in `BlueprintsPage` and `RunsPage` with `focusIndex` state and ref-based scroll-into-view.
- Custom event `hadron:refresh` dispatched on `R` key, consumed by pages via `window.addEventListener`.

### State Management
- No external state library (no Redux, Zustand, Jotai, etc.).
- All state lives in component-local `useState` or is lifted to `App.tsx` and passed via props.
- Workspace selection, daemon status, and active run timer are managed at the `App.tsx` level.

### Component Composition
- Functional components only, no class components (except `TelemetryErrorBoundary`).
- Named exports for all components.
- `memo()` used for React Flow nodes and edges.
- Props interfaces defined inline above each component.

## Anti-Patterns Found

### God Components (4 files, high severity)
- **`pages/BlueprintWizardPage.tsx` (1168 lines):** Single component containing all 8 wizard steps, YAML serialization/parsing, autosave logic, form state for metadata/project/env/packages/inputs/steps/git/stubs/imports/hooks. Should be decomposed into step sub-components and a separate YAML serializer module.
- **`pages/FlowBuilderPage.tsx` (1021 lines):** Combines pipeline YAML parsing, React Flow state management, drag-and-drop handling, live execution overlay, YAML export, and stage property editing. The YAML parser is duplicated from `PipelineDetailPage.tsx`.
- **`pages/PipelinesPage.tsx` (1000 lines):** File browser + inline pipeline editor + YAML serializer + YAML parser all in one component. The YAML parser is duplicated from `PipelineDetailPage.tsx` and `FlowBuilderPage.tsx`.
- **`pages/BlueprintsPage.tsx` (660 lines):** File browser with 15+ state variables, batch operations, 3 separate modal dialogs, keyboard navigation, and metadata caching all in one component.

### Duplicated Code (high severity)
- **`formatDuration()`** is reimplemented in 4 separate files: `DashboardPage.tsx:15`, `RunsPage.tsx:17`, `RunDetailPage.tsx:18`, `PipelinesPage.tsx:17`. Each has slightly different signatures.
- **`shortPath()`** is reimplemented in 5 separate files: `DashboardPage.tsx:78`, `RunsPage.tsx:31`, `RunDetailPage.tsx:27`, `SchedulerPage.tsx:22`, `PipelinesPage.tsx:30`.
- **`formatTime()`** is reimplemented in 3 files: `DashboardPage.tsx:74`, `RunsPage.tsx:27`, `PipelinesPage.tsx:25`. A variant `formatTimestamp()` also exists in `TelemetryPage.tsx:61`.
- **`parsePipelineYaml()`** is implemented 2 times: `PipelinesPage.tsx:90`, `PipelineDetailPage.tsx:41`. Each is ~50-80 lines of near-identical indentation-aware YAML parsing.
- **`unquote()`** is defined in 3 files: `PipelinesPage.tsx:85`, `PipelineDetailPage.tsx:37`, and `FlowBuilderPage.tsx:52`.
- **Confirmation modal pattern** is reimplemented ~8 times across pages (batch delete, single delete, run confirmation). Same overlay + panel + cancel/confirm button layout each time.
- **Action menu dropdown** is reimplemented identically in `BlueprintDetailPage.tsx:182` and `PipelineDetailPage.tsx:253`.

### Inline Styles Overuse (medium severity)
Nearly all layout, spacing, and color decisions are made via inline `style={{}}` objects rather than CSS classes. This makes the codebase harder to maintain, creates verbose JSX, and prevents hover/focus state styling (which must be done in CSS anyway). Examples:
- `App.tsx:372-405` — workspace modal with 10+ inline style objects
- `AppHeader.tsx:91-281` — almost every element has an inline style object
- `BlueprintDetailPage.tsx` — entire task timeline rendered with inline styles

### Missing Shared Modal Component (medium severity)
Every modal reimplements the same pattern: `<div className="hud-modal-overlay" onClick={close}><div className="hud-modal" onClick={e => e.stopPropagation()}>`. This should be a reusable `<Modal>` component. Found in: `App.tsx`, `BlueprintsPage.tsx` (x3), `BlueprintDetailPage.tsx` (x3), `SchedulerPage.tsx` (x3), `PipelineDetailPage.tsx`, `RunInputsModal.tsx`.

### Prop Tunneling (medium severity)
- `daemonStatus` is passed from `App.tsx` through to nearly every page component, though most only use it to enable/disable a run button.
- `workspaceId` is passed to `BlueprintsPage`, `BlueprintDetailPage`, `PipelinesPage`, `PipelineDetailPage`, and `FlowBuilderPage`.
- `onOpenRun` callback is passed from `App.tsx` to 6 different page components.

These could benefit from a lightweight context provider.

### `any` Usage (low severity, contained)
Only one occurrence in the entire codebase:
- `api/client.ts:129` — `const go: any = (window as any).go?.main?.App;` — justified since Wails injects Go bindings at runtime with no TypeScript definitions. An eslint-disable comment is present.

### Hardcoded Values (low severity)
- Version string `"v0.4.0"` appears in both `AppHeader.tsx:275` and `AppFooter.tsx:54`. Should be a constant or imported from `package.json`.
- Polling intervals are hardcoded: `3000` (runs), `5000` (schedules), `2000` (run detail), `1500` (run events). Could be constants in a config file.
- Daemon default address `127.0.0.1:8095` appears in `App.tsx:62` and `client.ts:9`.

### No `<select>` Styling (low severity)
The custom `hud-input` class is applied to `<select>` elements (e.g., `BlueprintsPage.tsx` sort dropdown), but the dropdown options inherit the OS default appearance, which clashes with the dark HUD theme.

## Reference Implementations

### 1. `usePoll<T>()` hook — `src/hooks/usePoll.ts`
A well-structured generic polling hook that demonstrates good practices:
- Properly typed with generics
- Uses `useRef` to keep the fetcher reference stable without restarting timers
- Handles race conditions with a counter (`counterRef`)
- Automatically pauses when the tab is hidden and resumes on visibility change
- Returns a clean `{ data, loading, error, refresh }` interface

### 2. `StatusBadge` — `src/components/ui/StatusBadge.tsx`
A clean, focused UI primitive:
- Single responsibility: maps a status string to a styled label
- Typed props interface
- Handles edge cases (unknown status, British vs. American spelling of "cancelled")
- Small enough to reason about entirely (39 lines)

### 3. `AppNav` — `src/components/layout/AppNav.tsx`
A data-driven navigation component:
- Declarative `NAV_ITEMS` array defines all nav entries
- Active state logic handles parent highlighting for sub-pages (e.g., `runDetail` highlights `runs`)
- Exported `NavPage` type is the source of truth for navigation throughout the app
- Clean separation: no business logic, just UI rendering from data

## Recommendations

### Priority 1 — Extract Shared Utilities
1. Create `src/utils/format.ts` with `formatDuration()`, `shortPath()`, `formatTime()`, `formatMs()`. Remove all 12+ duplicate implementations across pages.
2. Create `src/utils/yaml.ts` with the shared `parsePipelineYaml()` and `unquote()` functions. Remove the 3 duplicate implementations.
3. Move hardcoded constants (version, polling intervals, daemon address) to `src/constants.ts`.

### Priority 2 — Extract Shared UI Components
1. Create a `<Modal>` component wrapping the overlay + panel + stopPropagation pattern. Replace 10+ inline reimplementations.
2. Create a `<ConfirmDialog>` component for the repeated delete/run confirmation pattern.
3. Create a `<PageHeader>` component for the repeated back-button + title + action-buttons pattern.
4. Create a `<DropdownMenu>` component to replace the manually-managed action menus in `BlueprintDetailPage` and `PipelineDetailPage`.

### Priority 3 — Decompose God Components
1. Break `BlueprintWizardPage.tsx` into step sub-components: `WizardMetadata`, `WizardProject`, `WizardEnv`, `WizardPackages`, `WizardInputs`, `WizardSteps`, `WizardAdvanced`, `WizardReview`. Extract YAML serialization to a utility.
2. Break `PipelinesPage.tsx` into `PipelineBrowser` (file list) and `PipelineEditor` (inline editor form) components.
3. Break `FlowBuilderPage.tsx` — extract the execution overlay panel and the YAML export logic into separate modules.

### Priority 4 — Reduce Inline Styles
1. Move repeated inline style patterns to CSS classes in `index.css`. Target the most common patterns first: flex containers, gap/padding, font-size/color overrides.
2. Consider adopting a utility-first approach (Tailwind or custom utility classes) to replace inline styles without adding a build dependency.

### Priority 5 — Add Context for Cross-Cutting State
1. Create a `DaemonContext` to provide `daemonStatus`, `daemonAddr`, and `workspaceId` instead of prop-tunneling through every page.
2. Consider a `NavigationContext` to encapsulate the page state, selected IDs, and navigation callbacks currently spread across 10+ `useState` calls in `App.tsx`.

### Priority 6 — Quality of Life
1. Add a URL-based router (even a lightweight one like `wouter`) so browser back/forward work and deep links are possible.
2. Extract the CSS design tokens into a tokens file or CSS custom property documentation so the design system is discoverable.
3. Add `React.StrictMode` to `main.tsx` to catch potential issues early.

## Notes

- The codebase is a Wails v2 desktop app, not a web app. The frontend runs inside a native webview, which is why there is no router and some browser APIs (like Wails Go bindings on `window.go`) are used directly.
- TypeScript is used well overall. The `types.ts` file is comprehensive with matching Go struct types. The only `any` usage is justified (Wails runtime injection).
- The design system is cohesive — the "HUD" aesthetic (dark theme, mono font, uppercase labels, status colors) is applied consistently. The CSS token system using raw RGB values is unconventional but works well for alpha composition.
- Demo mode is a clean implementation that transparently swaps API responses without conditional logic in the UI layer.
- The `usePoll` and `useRunEvents` hooks are production-quality with proper race condition handling and cleanup.
- No tests exist in the frontend. No linting configuration beyond what TypeScript provides (`noUnusedLocals` and `noUnusedParameters` are both set to `false`).
- The app imports no CSS framework and has zero runtime CSS dependencies — all 865 lines of CSS are handwritten. This is impressive for consistency but increases maintenance burden.
