interface StatusBadgeProps {
  status: string;
}

const STATUS_LABELS: Record<string, string> = {
  success: 'Success',
  running: 'Running',
  failed: 'Failed',
  queued: 'Queued',
  canceled: 'Canceled',
  cancelled: 'Canceled',
};

const STATUS_CLASS: Record<string, string> = {
  success: 'badge-success',
  running: 'badge-running',
  failed: 'badge-failed',
  queued: 'badge-queued',
  canceled: 'badge-canceled',
  cancelled: 'badge-canceled',
};

export function StatusBadge({ status }: StatusBadgeProps) {
  const label = STATUS_LABELS[status] ?? status.charAt(0).toUpperCase() + status.slice(1);
  const cls = STATUS_CLASS[status] ?? 'badge-queued';

  return (
    <span className={`badge ${cls}`}>
      <span className="badge-dot" />
      {label}
    </span>
  );
}
