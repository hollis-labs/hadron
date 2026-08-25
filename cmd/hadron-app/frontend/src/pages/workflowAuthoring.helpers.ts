export function parseWorkflowInputs(raw: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(raw || '{}');
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== 'object') {
    throw new Error('Inputs must be a JSON object');
  }
  return parsed as Record<string, unknown>;
}

export function createWorkflowRunID(prefix: string, uuid: string): string {
  return `${prefix}-${uuid}`;
}
