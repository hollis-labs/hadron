import type { WorkflowResumeRequest } from '../../api/types';

function normalizeForCanonicalJSON(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeForCanonicalJSON);
  if (value !== null && typeof value === 'object') {
    const input = value as Record<string, unknown>;
    return Object.fromEntries(Object.keys(input).sort().map(key => [key, normalizeForCanonicalJSON(input[key])]));
  }
  if (typeof value === 'number' && (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new Error('Resume payload numbers must be finite and lossless JSON values.');
  }
  return value;
}

export function canonicalJSONStringify(value: unknown): string {
  return JSON.stringify(normalizeForCanonicalJSON(value));
}

function valueType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

async function sha256(input: string): Promise<string> {
  const encoded = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest('SHA-256', encoded);
  const hex = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
  return `sha256:${hex}`;
}

export async function buildInlineResumeValue(
  payloadJSON: string,
  waitId: string,
): Promise<WorkflowResumeRequest['payload']> {
  const parsed = JSON.parse(payloadJSON) as unknown;
  const canonical = canonicalJSONStringify(parsed);
  return {
    type: valueType(parsed),
    inline: parsed,
    producer: { kind: 'desktop-resume', reference: waitId, output: 'resume' },
    media_type: 'application/json',
    digest: await sha256(canonical),
    redaction: 'private',
    retention: 'run',
  };
}
