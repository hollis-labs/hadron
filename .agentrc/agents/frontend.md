# Frontend Context — Hadron

> Project-specific frontend conventions. Loaded by the frontend agent role when working in this project.

## Current State (2026-03-22)

**UI redesign shipped.** New design system (tokens.css + theme.css) applied across all 13 pages. Shared utilities extracted. Shared UI components (Modal, ConfirmDialog, PageHeader) created. Fonts bundled locally (Inter + JetBrains Mono WOFF2). Builds and runs in Wails.

### Known Issues
- **Styling system needs overhaul** — The CSS architecture is fragile and caused a multi-hour debugging session. Problems found:
  - Legacy `:root` vars in `index.css` were silently overwriting `tokens.css` vars (e.g., `--font-mono` was `Share Tech Mono` instead of `JetBrains Mono`). Fixed but other conflicts may exist.
  - ~230 inline `fontSize` styles in TSX components override CSS classes, making the design system ineffective. Bulk-replaced with token vars via sed but the approach is brittle.
  - 6 duplicate class names between `index.css` and `theme.css` caused silent style conflicts (`.page-title`, `.stat-card`, `.stat-label`, `.stat-value`, `.page-header`, `.footer-hint`). Removed legacy duplicates but more may exist.
  - ~182 inline `padding` declarations in TSX components; some override CSS class padding with tighter values.
  - Token scale was originally too small for the app's density of usage (xs=11px, sm=12px, md=13px). Bumped to xs=14px, sm=16px, md=17px, base=18px, lg=20px. Needs fine-tuning on all pages.
  - **User requested Tailwind CSS** — the project should migrate to Tailwind to replace the fragile hand-rolled CSS system. This was the original intent.
- **Legacy CSS classes** — `hud-input`/`hud-label` still used in 16 files (~119 occurrences). Functional but should migrate.
- **Legacy CSS vars** — `rgb(var(--*))` references remain in 6 files (flow components, CronBuilder, index.css). Should migrate to hex tokens.
- **Incomplete Modal adoption** — `<Modal>` component exists but only used by SchedulerPage, ConfirmDialog, and RunInputsModal. 7 files still use raw `hud-modal-overlay` pattern.
- **Full style audit needed** — After the token scale bump, all 13 pages need visual review to check sizing, spacing, and layout. The scale change affects everything globally.

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
    ├── App.tsx                     # Root component — row layout: Nav | column(Header, Content, Footer) (430 lines)
    ├── tokens.css                  # Design tokens — palette, type scale, spacing, radii (73 lines)
    ├── index.css                   # Legacy + layout CSS — sidebar, header, nav-item, content grid (978 lines)
    ├── theme.css                   # Shared component classes — badge, btn, table, stat-card, section, footer (317 lines)
    ├── vite-env.d.ts
    ├── api/
    │   ├── types.ts                # All TypeScript interfaces (296 lines)
    │   └── client.ts               # API layer: REST fetch + Wails Go bindings (335 lines)
    ├── hooks/
    │   ├── usePoll.ts              # Generic polling hook with visibility-aware timer (87 lines)
    │   └── useRunEvents.ts         # Streaming run events with cursor pagination (58 lines)
    ├── utils/
    │   ├── format.ts               # formatDuration, formatRunDuration, formatMs, formatTime, formatDate, etc. (74 lines)
    │   ├── path.ts                 # shortPath, basename (11 lines)
    │   ├── string.ts               # unquote (4 lines)
    │   └── yaml.ts                 # parsePipelineYaml — shared parser for all pipeline pages (137 lines)
    ├── demo/
    │   ├── demoMode.ts             # Global demo toggle with subscriber pattern
    │   └── data.ts                 # Mock data for demo mode
    ├── components/
    │   ├── layout/
    │   │   ├── AppHeader.tsx       # Top bar: breadcrumbs, daemon status, workspace selector, menu (231 lines)
    │   │   ├── AppNav.tsx          # 52px icon-rail sidebar with tooltips (67 lines)
    │   │   └── AppFooter.tsx       # Keyboard hint bar with kbd badges (58 lines)
    │   ├── ui/
    │   │   ├── StatusBadge.tsx     # Pill badge with dot — badge + badge-{status} classes (33 lines)
    │   │   ├── EmptyState.tsx      # Centered empty placeholder (23 lines)
    │   │   ├── Spinner.tsx         # SVG loading spinner (21 lines)
    │   │   ├── Modal.tsx           # Shared modal overlay wrapper (21 lines)
    │   │   ├── ConfirmDialog.tsx   # Shared confirm/delete dialog — uses Modal (30 lines)
    │   │   ├── PageHeader.tsx      # Shared page header with back button (27 lines)
    │   │   ├── CronBuilder.tsx     # Interactive cron expression editor (274 lines)
    │   │   └── RunInputsModal.tsx  # Modal form for blueprint input parameters — uses Modal (164 lines)
    │   └── flow/
    │       ├── StageNode.tsx       # React Flow custom node for pipeline stages (86 lines)
    │       ├── ConditionalEdge.tsx # React Flow custom edge with condition labels (96 lines)
    │       ├── NodePalette.tsx     # Drag-and-drop blueprint palette (134 lines)
    │       └── StagePropertyPanel.tsx # Side panel for editing stage properties (269 lines)
    └── pages/
        ├── DashboardPage.tsx       # Stats grid, recent runs table, activity timeline (262 lines)
        ├── BlueprintsPage.tsx      # File browser with batch operations (660 lines)
        ├── BlueprintDetailPage.tsx  # Split-pane blueprint viewer (584 lines)
        ├── BlueprintWizardPage.tsx  # Multi-step blueprint creation wizard (1168 lines)
        ├── PipelinesPage.tsx       # Pipeline browser + inline editor (869 lines)
        ├── PipelineDetailPage.tsx   # Pipeline viewer with stage timeline (404 lines)
        ├── FlowBuilderPage.tsx     # Visual pipeline editor with React Flow (913 lines)
        ├── RunsPage.tsx            # Run list with filters (168 lines)
        ├── RunDetailPage.tsx       # Live run monitor with grouped events (299 lines)
        ├── SchedulerPage.tsx       # Cron schedule CRUD (380 lines)
        ├── TelemetryPage.tsx       # Structured log viewer (320 lines)
        ├── SettingsPage.tsx        # Settings form (198 lines)
        └── HelpPage.tsx            # Keyboard shortcuts + docs (759 lines)
```

Total frontend: ~11,327 lines across 38 files (~8,959 TS/TSX + ~1,368 CSS + demo/config).

## Component Inventory

### Layout (3 components — all reusable)
| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `AppHeader` | `components/layout/AppHeader.tsx` | 231 | Top bar with logo, breadcrumb nav, workspace dropdown, daemon status dot, elapsed timer, demo mode toggle, settings menu |
| `AppNav` | `components/layout/AppNav.tsx` | 67 | Sidebar nav, data-driven from `NAV_ITEMS` array |
| `AppFooter` | `components/layout/AppFooter.tsx` | 58 | Contextual keyboard shortcut hints per page |

### UI Primitives (8 components — all reusable)
| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `StatusBadge` | `components/ui/StatusBadge.tsx` | 33 | Colored status label (success/running/failed/queued/canceled) |
| `EmptyState` | `components/ui/EmptyState.tsx` | 23 | Centered placeholder text for empty lists |
| `Spinner` | `components/ui/Spinner.tsx` | 21 | Inline SVG loading spinner |
| `Modal` | `components/ui/Modal.tsx` | 21 | Shared modal overlay with click-outside-to-close |
| `ConfirmDialog` | `components/ui/ConfirmDialog.tsx` | 30 | Shared confirm/delete dialog (wraps Modal) |
| `PageHeader` | `components/ui/PageHeader.tsx` | 27 | Shared page header with back button |
| `CronBuilder` | `components/ui/CronBuilder.tsx` | 274 | Interactive 5-field cron expression editor with presets, validation, and human-readable description |
| `RunInputsModal` | `components/ui/RunInputsModal.tsx` | 164 | Dynamic form modal for blueprint input parameters, supports string/number/boolean/array/enum types |

### Flow Components (4 components — domain-specific, reusable within flow builder)
| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `StageNode` | `components/flow/StageNode.tsx` | 86 | Custom React Flow node showing pipeline stage with status dot, name, blueprint path, condition badge |
| `ConditionalEdge` | `components/flow/ConditionalEdge.tsx` | 96 | Custom React Flow edge with dashed style for conditional connections and inline condition labels |
| `NodePalette` | `components/flow/NodePalette.tsx` | 134 | Draggable blueprint list panel for adding stages to the flow canvas |
| `StagePropertyPanel` | `components/flow/StagePropertyPanel.tsx` | 269 | Side panel for editing a selected stage's properties (name, path, condition, inputs, outputs) |

### Pages (13 components — page-specific)
| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `DashboardPage` | `pages/DashboardPage.tsx` | 262 | Stats grid, recent runs table, 24h activity bar chart, per-blueprint stats |
| `BlueprintsPage` | `pages/BlueprintsPage.tsx` | 660 | File browser with search, sort, batch select, validate, run, move, copy, archive, delete |
| `BlueprintDetailPage` | `pages/BlueprintDetailPage.tsx` | 584 | Split-pane: left=metadata/inputs/imports/hooks, right=section/task timeline |
| `BlueprintWizardPage` | `pages/BlueprintWizardPage.tsx` | 1168 | 8-step form wizard for creating/editing blueprints with YAML preview |
| `PipelinesPage` | `pages/PipelinesPage.tsx` | 869 | Pipeline file browser + inline pipeline editor with stage CRUD |
| `PipelineDetailPage` | `pages/PipelineDetailPage.tsx` | 404 | Read-only pipeline viewer with collapsible stage details |
| `FlowBuilderPage` | `pages/FlowBuilderPage.tsx` | 913 | Visual pipeline editor: React Flow canvas, drag-and-drop from palette, stage property editing, YAML export, live execution overlay |
| `RunsPage` | `pages/RunsPage.tsx` | 168 | Run list with status filter chips and text search |
| `RunDetailPage` | `pages/RunDetailPage.tsx` | 299 | Live run monitoring: grouped task view with expandable logs, progress bar, raw event view |
| `SchedulerPage` | `pages/SchedulerPage.tsx` | 380 | Schedule CRUD with cron builder integration, edit modal, one-time schedule support |
| `TelemetryPage` | `pages/TelemetryPage.tsx` | 320 | Structured log viewer: run list + detail view with level/text filters, error boundary wrapper |
| `SettingsPage` | `pages/SettingsPage.tsx` | 198 | Settings form: general, execution, safety, and telemetry sections |
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
- Shared `<Modal>` component exists (`components/ui/Modal.tsx`) wrapping overlay + stopPropagation pattern.
- Adopted by `SchedulerPage`, `ConfirmDialog`, and `RunInputsModal`.
- 7 files still use raw `hud-modal-overlay` pattern inline — migration incomplete.

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

## Anti-Patterns (Remaining)

### God Components (3 files, high severity)
- **`pages/BlueprintWizardPage.tsx` (1168 lines):** Single component containing all 8 wizard steps, YAML serialization/parsing, autosave logic, form state for metadata/project/env/packages/inputs/steps/git/stubs/imports/hooks. Should be decomposed into step sub-components and a separate YAML serializer module.
- **`pages/FlowBuilderPage.tsx` (913 lines):** Combines React Flow state management, drag-and-drop handling, live execution overlay, YAML export, and stage property editing.
- **`pages/PipelinesPage.tsx` (869 lines):** File browser + inline pipeline editor + YAML serializer all in one component.

### Incomplete Modal Migration (medium severity)
`<Modal>` component exists but 7 files still use raw `hud-modal-overlay` pattern. Should migrate: BlueprintsPage (x3), BlueprintDetailPage (x3), PipelinesPage, PipelineDetailPage, FlowBuilderPage, HelpPage, App.tsx.

### Inline Styles Overuse (medium severity)
Layout, spacing, and color decisions still made via inline `style={{}}` objects in many components. Prevents hover/focus styling and creates verbose JSX.

### Prop Tunneling (medium severity)
- `daemonStatus` is passed from `App.tsx` through to nearly every page component, though most only use it to enable/disable a run button.
- `workspaceId` is passed to `BlueprintsPage`, `BlueprintDetailPage`, `PipelinesPage`, `PipelineDetailPage`, and `FlowBuilderPage`.
- `onOpenRun` callback is passed from `App.tsx` to 6 different page components.

### Legacy CSS (low severity, functional)
- `hud-input`/`hud-label` classes used in 16 files (~119 occurrences). Work fine but don't follow new token system.
- `rgb(var(--*))` vars in 6 files (flow components, CronBuilder, index.css). Should migrate to hex tokens.

### Hardcoded Values (low severity)
- Version string `"v0.4.0"` in `AppHeader.tsx` and `AppFooter.tsx`.
- Polling intervals hardcoded: `3000`, `5000`, `2000`, `1500`.
- Daemon default address `127.0.0.1:8095` in `App.tsx` and `client.ts`.

## Recommendations

### Priority 1 — Migrate Remaining Modals to `<Modal>` Component
Adopt the shared `<Modal>` in the 7 files still using raw `hud-modal-overlay`. Low effort, high consistency win.

### Priority 2 — Decompose God Components
1. Break `BlueprintWizardPage.tsx` into step sub-components with a shared wizard state context.
2. Break `PipelinesPage.tsx` into `PipelineBrowser` (file list) and `PipelineEditor` (inline editor form).
3. Break `FlowBuilderPage.tsx` — extract the execution overlay panel and YAML export logic.

### Priority 3 — Migrate to Tailwind CSS
User requested Tailwind. The hand-rolled CSS system (tokens.css + theme.css + index.css + 230+ inline styles) is fragile and hard to maintain. Migration plan:
1. Install Tailwind CSS + PostCSS + autoprefixer.
2. Map design tokens to `tailwind.config` (colors, spacing, typography, radii).
3. Migrate `theme.css` component classes to Tailwind utility classes.
4. Replace inline `style={{}}` objects in TSX with Tailwind classes.
5. Remove legacy `hud-*` classes and `rgb(var(--*))` references.
6. Remove `index.css` legacy `:root` vars once all consumers are migrated.

### Priority 4 — Full Visual Audit
After the token scale bump (xs=14, sm=16, md=17, base=18, lg=20), all 13 pages need visual review. Check stat cards, tables, forms, modals, wizard steps, flow builder, and all page layouts for overflow or spacing issues.

### Priority 5 — Add Context for Cross-Cutting State
1. Create a `DaemonContext` to provide `daemonStatus`, `daemonAddr`, and `workspaceId` instead of prop-tunneling.
2. Consider a `NavigationContext` for page state, selected IDs, and navigation callbacks.

### Priority 6 — Extract Constants
Move version string, polling intervals, and daemon address to a shared `constants.ts`.

## Reference Implementations

### 1. `usePoll<T>()` hook — `src/hooks/usePoll.ts`
Well-structured generic polling hook: typed generics, stable fetcher ref, race condition handling via counter, visibility-aware pausing, clean `{ data, loading, error, refresh }` interface.

### 2. `StatusBadge` — `src/components/ui/StatusBadge.tsx`
Clean, focused UI primitive: single responsibility, typed props, edge case handling (unknown status, British/American "cancelled"), 33 lines.

### 3. `AppNav` — `src/components/layout/AppNav.tsx`
Data-driven navigation: declarative `NAV_ITEMS` array, parent highlighting for sub-pages, exported `NavPage` type as source of truth, no business logic.

## Notes

- The codebase is a Wails v2 desktop app, not a web app. The frontend runs inside a native webview, which is why there is no router and Wails Go bindings on `window.go` are used directly.
- TypeScript is used well overall. The `types.ts` file is comprehensive with matching Go struct types. The only `any` usage is justified (Wails runtime injection).
- The design system is cohesive — dark theme with neutral zinc backgrounds, blue/amber/red status colors, Inter + JetBrains Mono typography. Applied consistently across all 13 pages.
- Demo mode is a clean implementation that transparently swaps API responses without conditional logic in the UI layer.
- The `usePoll` and `useRunEvents` hooks are production-quality with proper race condition handling and cleanup.
- No tests exist in the frontend. No linting configuration beyond what TypeScript provides.
- All ~1,368 lines of CSS are handwritten across three files (tokens.css, theme.css, index.css). No CSS framework or runtime CSS dependencies.
