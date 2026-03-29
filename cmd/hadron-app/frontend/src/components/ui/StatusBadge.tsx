import { Badge } from './badge'
import type { VariantProps } from 'class-variance-authority'
import { badgeVariants } from './badge'

type StatusVariant = Extract<
  NonNullable<VariantProps<typeof badgeVariants>['variant']>,
  'success' | 'running' | 'failed' | 'queued' | 'canceled'
>

const STATUS_VARIANT: Record<string, StatusVariant> = {
  success: 'success',
  running: 'running',
  failed: 'failed',
  queued: 'queued',
  canceled: 'canceled',
  cancelled: 'canceled',
}

const STATUS_LABELS: Record<string, string> = {
  success: 'Success',
  running: 'Running',
  failed: 'Failed',
  queued: 'Queued',
  canceled: 'Canceled',
  cancelled: 'Canceled',
}

const DOT_COLORS: Record<StatusVariant, string> = {
  success: 'bg-blue-400',
  running: 'bg-amber-400',
  failed: 'bg-red-400',
  queued: 'bg-zinc-400',
  canceled: 'bg-purple-400',
}

interface StatusBadgeProps {
  status: string
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const variant = STATUS_VARIANT[status] ?? 'queued'
  const label = STATUS_LABELS[status] ?? status.charAt(0).toUpperCase() + status.slice(1)

  return (
    <Badge variant={variant}>
      <span className={`size-1.5 rounded-full ${DOT_COLORS[variant]}`} />
      {label}
    </Badge>
  )
}
