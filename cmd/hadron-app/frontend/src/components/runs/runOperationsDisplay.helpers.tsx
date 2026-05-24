export function operationFilterKey(kind: string): string {
  return kind || 'all'
}

export function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

export function renderHighlightedText(value: string, query: string): React.ReactNode {
  if (!query) return value
  const trimmed = query.trim()
  if (!trimmed) return value
  const pattern = new RegExp(`(${escapeRegExp(trimmed)})`, 'ig')
  const parts = value.split(pattern)
  if (parts.length === 1) return value
  const normalized = trimmed.toLowerCase()
  return parts.map((part, index) => (
    part.toLowerCase() === normalized ? (
      <mark key={`${part}-${index}`} className="rounded bg-amber-500/20 px-0.5 text-foreground">
        {part}
      </mark>
    ) : (
      <span key={`${part}-${index}`}>{part}</span>
    )
  ))
}
