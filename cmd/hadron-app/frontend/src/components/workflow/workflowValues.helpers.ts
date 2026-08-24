import type { WorkflowRenderedValue } from '../../api/types';

export const REDACTED_VALUE = '[REDACTED]';

export interface WorkflowValuePreview {
  text: string;
  masked: boolean;
  truncated: boolean;
  artifact?: {
    store: string;
    digest: string;
    mediaType: string;
    sizeBytes: number;
  };
}

export function workflowValuePreview(value: WorkflowRenderedValue, maximum = 240): WorkflowValuePreview {
  if (value.masked || value.redaction === 'secret' || value.payload === REDACTED_VALUE) {
    return { text: REDACTED_VALUE, masked: true, truncated: false };
  }
  if (value.type === 'artifact' && isRecord(value.payload)) {
    return {
      text: 'Artifact metadata',
      masked: false,
      truncated: false,
      artifact: {
        store: stringField(value.payload, 'store'),
        digest: stringField(value.payload, 'digest'),
        mediaType: stringField(value.payload, 'media_type') || value.media_type,
        sizeBytes: numberField(value.payload, 'size_bytes'),
      },
    };
  }
  let text: string;
  try {
    text = typeof value.payload === 'string' ? value.payload : JSON.stringify(value.payload) ?? '[UNAVAILABLE]';
  } catch {
    text = '[UNAVAILABLE]';
  }
  if (text.length <= maximum) return { text, masked: false, truncated: false };
  return { text: `${text.slice(0, maximum)}…`, masked: false, truncated: true };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function stringField(record: Record<string, unknown>, key: string): string {
  return typeof record[key] === 'string' ? record[key] : '';
}

function numberField(record: Record<string, unknown>, key: string): number {
  const value = record[key];
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}
