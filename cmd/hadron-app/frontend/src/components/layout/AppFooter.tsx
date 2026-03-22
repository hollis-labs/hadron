import type { NavPage } from './AppNav';

interface AppFooterProps {
  phase?: NavPage;
}

const HINTS: Partial<Record<NavPage, { key: string; desc: string }[]>> = {
  dashboard: [
    { key: 'R', desc: 'Refresh' },
  ],
  blueprints: [
    { key: 'R', desc: 'Refresh' },
    { key: 'N', desc: 'New blueprint' },
    { key: '↑↓', desc: 'Navigate' },
    { key: 'Space', desc: 'Select' },
    { key: 'Enter', desc: 'Open' },
  ],
  blueprintDetail: [
    { key: 'Esc', desc: 'Back' },
  ],
  blueprintWizard: [
    { key: 'Esc', desc: 'Back' },
  ],
  runs: [
    { key: 'R', desc: 'Refresh' },
    { key: '↑↓', desc: 'Navigate' },
    { key: 'Enter', desc: 'Open' },
  ],
  runDetail: [
    { key: 'Esc', desc: 'Back' },
    { key: 'R', desc: 'Refresh' },
  ],
  schedules: [
    { key: 'R', desc: 'Refresh' },
  ],
  pipelines: [
    { key: 'R', desc: 'Refresh' },
  ],
  telemetry: [
    { key: 'R', desc: 'Refresh' },
    { key: 'Esc', desc: 'Back' },
  ],
};

export function AppFooter({ phase }: AppFooterProps) {
  const hints = phase ? HINTS[phase] ?? [] : [];
  return (
    <footer className="footer">
      {hints.map(({ key, desc }) => (
        <span key={key + desc} className="footer-hint">
          <span className="kbd">{key}</span> {desc}
        </span>
      ))}
      <div className="footer-spacer" />
      <span className="footer-meta">hadron v0.4.0</span>
    </footer>
  );
}
