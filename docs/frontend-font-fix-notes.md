# Frontend Font Size Issue — Session Notes (2026-03-22)

## Problem

Font size in the app is noticeably smaller than the golden mockup (`cmd/hadron-app/frontend/mockups/color-f.html`). Affects both the Wails desktop build and the Vite dev server. All text elements are smaller — stat values, table cells, section headers, labels, etc.

## Root Causes Found

### 1. Legacy `:root` vars overwriting tokens (FIXED)

`index.css` loads AFTER `tokens.css` and had its own `:root` block that silently overwrote key token vars:
- `--font-mono` was set to `'Share Tech Mono'` — a much smaller/narrower font than the intended `'JetBrains Mono'`
- `--radius-sm` and `--radius-lg` were also overwritten

**Fix:** Removed `--font-mono`, `--font-body`, and `--radius-*` from `index.css` `:root`. Only `tokens.css` now defines these.

### 2. Duplicate CSS class names (FIXED)

6 class names defined in both `index.css` (legacy) and `theme.css` (new):
- `.page-title` — legacy had green color, uppercase, smaller font
- `.stat-card`, `.stat-label`, `.stat-value` — different padding, colors, sizes
- `.page-header`, `.footer-hint` — different spacing

Properties set in `index.css` but not overridden by `theme.css` bled through (e.g., `text-transform: uppercase`, green color).

**Fix:** Removed the 6 duplicate blocks from `index.css`.

### 3. Table padding mismatch (FIXED)

`.table td/th` padding was `var(--space-2) var(--space-4)` (8px 16px) in theme.css but the mockup uses `var(--space-3) var(--space-5)` (12px 20px). The compressed rows made text appear smaller.

**Fix:** Updated theme.css table padding to match mockup.

### 4. ~230 inline fontSize + ~182 inline padding in TSX (PARTIALLY FIXED)

Components set `fontSize` and `padding` inline via `style={{}}`, overriding CSS classes. Inline styles always win over class-based styles, making the design system ineffective.

**Fix:** Bulk-replaced ~230 inline `fontSize` values with token vars via sed. Padding not yet addressed.

### 5. Token scale too small (FIXED — needs fine-tuning)

The original token scale (xs=11px, sm=12px, md=13px, base=14px) was too tight. Despite matching the mockup's CSS values pixel-for-pixel, the app LOOKED smaller because:
- The app uses xs/sm/md tokens on 230+ elements (every table cell, label, button, badge)
- The mockup only uses them selectively
- On high-DPI displays the small sizes feel even more compressed

User confirmed: disabling `--text-xs/sm/md` in DevTools made fonts look "closer but not right."

**Fix:** Bumped token scale to xs=14px, sm=16px, md=17px, base=18px, lg=20px. User confirmed "much better" but needs full page audit.

## Other Changes Made

### Font bundling
- Downloaded Inter (variable) + JetBrains Mono (regular, medium) as WOFF2 files
- Created `src/assets/fonts/fonts.css` with `@font-face` declarations
- Imported in `main.tsx` before tokens.css
- Removed Google Fonts `<link>` from `index.html`

### Body alignment with mockup
- `html { font-size: 16px }` — locks rem baseline
- Body: `font-size: var(--text-base)`, `line-height: 1.5`

### Grid background
- Darkened grid lines from `var(--border-subtle)` (#1c1c1f) to `#141416` — user confirmed good

## Outstanding Work

1. **Migrate to Tailwind CSS** — user specifically requested Tailwind. The hand-rolled CSS (3 files + 400+ inline styles) is fragile.
2. **Full visual audit** — the token scale bump affects all 13 pages globally. Need to check each page for overflow, spacing, and layout issues.
3. **Remove remaining inline styles** — 182 inline padding declarations still override CSS classes.
4. **Legacy CSS cleanup** — `hud-input`/`hud-label` (119 occurrences), `rgb(var(--*))` (6 files).
5. **Modal migration** — 7 files still use raw `hud-modal-overlay` instead of `<Modal>` component.

## Files Modified

- `index.html` — removed Google Fonts links
- `src/main.tsx` — added fonts.css import
- `src/assets/fonts/` — NEW: fonts.css + 3 WOFF2 font files
- `src/tokens.css` — bumped token scale (xs=14, sm=16, md=17, base=18, lg=20)
- `src/index.css` — removed legacy `:root` var overrides, removed duplicate classes, html baseline, body alignment, hardcoded font-size replacements, darkened grid
- `src/theme.css` — fixed table td/th padding to match mockup
- `src/pages/*.tsx` — ~230 inline fontSize replacements via sed
- `src/components/**/*.tsx` — inline fontSize replacements via sed
- `src/pages/DashboardPage.tsx` — canary test added/removed
