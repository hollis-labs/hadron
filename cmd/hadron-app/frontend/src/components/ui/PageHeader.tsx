import type { ReactNode } from 'react';

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  onBack?: () => void;
  actions?: ReactNode;
}

export function PageHeader({ title, subtitle, onBack, actions }: PageHeaderProps) {
  return (
    <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 'var(--space-5)' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
        {onBack && (
          <button className="btn btn-ghost" onClick={onBack} style={{ padding: '4px 6px' }}>
            ←
          </button>
        )}
        <div>
          <div style={{ fontSize: 'var(--text-xl)', fontWeight: 600 }}>{title}</div>
          {subtitle && <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>{subtitle}</div>}
        </div>
      </div>
      {actions && <div style={{ display: 'flex', gap: 'var(--space-2)' }}>{actions}</div>}
    </div>
  );
}
