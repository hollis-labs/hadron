interface EmptyStateProps {
  message: string;
  sub?: string;
}

export function EmptyState({ message, sub }: EmptyStateProps) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '3rem 1rem',
        gap: '0.5rem',
        color: 'rgb(var(--muted))',
      }}
    >
      <span style={{ fontSize: '0.85rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
        {message}
      </span>
      {sub && <span style={{ fontSize: '0.75rem' }}>{sub}</span>}
    </div>
  );
}
