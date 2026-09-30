import assert from 'node:assert/strict';
import test from 'node:test';

import { APIError, apiFetch, onUnauthorized, setAPIBaseURL } from './http';

function respond(status: number, body: unknown) {
  const calls: RequestInit[] = [];
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    calls.push(init ?? {});
    return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
  }) as typeof fetch;
  return calls;
}

test('a 401 notifies sign-in listeners and throws an APIError with the status', async () => {
  setAPIBaseURL('');
  respond(401, { error: 'operator credential required' });
  let notified = 0;
  const unsubscribe = onUnauthorized(() => { notified += 1; });
  await assert.rejects(apiFetch('/v1/workspaces'), (error: unknown) => error instanceof APIError && error.status === 401);
  assert.equal(notified, 1);
  unsubscribe();
  await assert.rejects(apiFetch('/v1/workspaces'));
  assert.equal(notified, 1, 'unsubscribed listeners are not called');
});

test('other failures do not ask for sign-in', async () => {
  respond(403, { code: 'policy_denied' });
  let notified = 0;
  const unsubscribe = onUnauthorized(() => { notified += 1; });
  await assert.rejects(apiFetch('/v1/workflows/runs'), (error: unknown) => error instanceof APIError && error.status === 403);
  assert.equal(notified, 0);
  unsubscribe();
});

test('requests send same-origin credentials so the session cookie travels', async () => {
  const calls = respond(200, { items: [] });
  await apiFetch('/v1/workspaces');
  assert.equal(calls[0]?.credentials, 'same-origin');
});
