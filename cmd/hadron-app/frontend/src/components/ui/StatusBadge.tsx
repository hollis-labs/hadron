interface StatusBadgeProps {
  status: string;
}

const STATUS_LABELS: Record<string, string> = {
  success: 'SUCCESS',
  running: 'RUNNING',
  failed: 'FAILED',
  queued: 'QUEUED',
  canceled: 'CANCELED',
  cancelled: 'CANCELED',
};

export function StatusBadge({ status }: StatusBadgeProps) {
  const label = STATUS_LABELS[status] ?? status.toUpperCase();
  const cls =
    status === 'success'
      ? 'status-success'
      : status === 'running'
      ? 'status-running pulse-running'
      : status === 'failed'
      ? 'status-failed'
      : status === 'queued'
      ? 'status-queued'
      : 'status-canceled';

  return (
    <span
      className={cls}
      style={{
        fontSize: '0.7rem',
        letterSpacing: '0.1em',
        fontWeight: 700,
      }}
    >
      {label}
    </span>
  );
}
