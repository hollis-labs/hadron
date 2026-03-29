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
    <footer className="h-[30px] flex items-center px-6 border-t border-border bg-card gap-4 shrink-0">
      {hints.map(({ key, desc }) => (
        <span key={key + desc} className="flex items-center gap-1 text-xs text-muted-foreground">
          <span className="inline-flex items-center justify-center min-w-[18px] h-[18px] px-1 bg-muted border border-border rounded text-[10px] font-mono text-muted-foreground leading-none">{key}</span> {desc}
        </span>
      ))}
      <div className="flex-1" />
      <span className="text-xs font-mono text-muted-foreground">hadron v0.4.0</span>
    </footer>
  );
}
