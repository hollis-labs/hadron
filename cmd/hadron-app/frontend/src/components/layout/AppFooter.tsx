import type { NavPage } from './AppNav';

interface AppFooterProps {
  phase?: NavPage;
}

const HINTS: Partial<Record<NavPage, { key: string; desc: string }[]>> = {
  dashboard: [
    { key: 'R', desc: 'refresh' },
  ],
  blueprints: [
    { key: 'R', desc: 'refresh' },
    { key: 'N', desc: 'new blueprint' },
    { key: 'Esc', desc: 'go up' },
  ],
  blueprintDetail: [
    { key: 'Esc', desc: 'back to list' },
  ],
  blueprintWizard: [
    { key: 'Esc', desc: 'back to list' },
  ],
  runs: [
    { key: 'R', desc: 'refresh' },
  ],
  runDetail: [
    { key: 'Esc', desc: 'back to log' },
    { key: 'R', desc: 'refresh' },
  ],
  schedules: [
    { key: 'R', desc: 'refresh' },
  ],
  pipelines: [
    { key: 'R', desc: 'refresh' },
  ],
  telemetry: [
    { key: 'R', desc: 'refresh' },
    { key: 'Esc', desc: 'back to list' },
  ],
};

export function AppFooter({ phase }: AppFooterProps) {
  const hints = phase ? HINTS[phase] ?? [] : [];
  return (
    <footer className="app-footer">
      {hints.map(({ key, desc }) => (
        <span key={key + desc} className="footer-hint">
          <kbd>{key}</kbd> {desc}
        </span>
      ))}
      <span className="footer-hint" style={{ marginLeft: 'auto' }}>
        <kbd>?</kbd> help
      </span>
      <span className="footer-hint" style={{ color: 'rgb(var(--muted))' }}>
        Hadron v0.4.0 · Hollis Labs
      </span>
    </footer>
  );
}
