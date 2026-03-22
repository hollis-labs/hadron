/** Return the last two path segments (e.g. "deploy/api-gateway"). */
export function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
}

/** Return the filename portion of a path. */
export function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}
