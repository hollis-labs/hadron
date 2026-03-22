/** Strip surrounding quotes from a YAML value. */
export function unquote(v: string): string {
  return v.trim().replace(/^["']/, '').replace(/["']$/, '');
}
