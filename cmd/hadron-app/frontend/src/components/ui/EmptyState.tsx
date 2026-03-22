interface EmptyStateProps {
  message: string;
  sub?: string;
}

export function EmptyState({ message, sub }: EmptyStateProps) {
  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      padding: 'var(--space-12) var(--space-4)',
      gap: 'var(--space-2)',
      color: 'var(--text-tertiary)',
    }}>
      <span style={{ fontSize: 'var(--text-base)', fontWeight: 500 }}>
        {message}
      </span>
      {sub && <span style={{ fontSize: 'var(--text-sm)' }}>{sub}</span>}
    </div>
  );
}
