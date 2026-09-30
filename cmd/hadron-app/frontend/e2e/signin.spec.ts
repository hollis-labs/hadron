import { expect, test } from '@playwright/test';

import { operatorToken, signInPath } from './signIn';

test('an unauthenticated browser is told to sign in and cannot read workspaces', async ({ page }) => {
  const workspaces = page.waitForResponse(response => response.url().endsWith('/v1/workspaces'));
  await page.goto('/');
  expect((await workspaces).status()).toBe(401);
  await expect(page.getByTestId('sign-in-required')).toBeVisible();
  await expect(page.getByTestId('sign-in-required')).toContainText('hadron ui');
});

test('a one-time sign-in link opens an HttpOnly session and works exactly once', async ({ page, context }) => {
  const loginPath = await signInPath(page);
  expect(loginPath).not.toContain(operatorToken());

  const workspaces = page.waitForResponse(response => response.url().endsWith('/v1/workspaces'));
  await page.goto(loginPath);
  await expect(page).toHaveURL(/\/$/);
  expect((await workspaces).status()).toBe(200);
  await expect(page.getByTestId('sign-in-required')).toHaveCount(0);

  const session = (await context.cookies()).find(cookie => cookie.name === 'hadron_session');
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe('Strict');

  // The same link cannot open a second session.
  const reused = await page.request.get(loginPath, { maxRedirects: 0 });
  expect(reused.status()).toBe(401);
});

// A real browser's same-origin POST carries Origin, which the daemon
// requires for cookie-authenticated state changes; foreign or missing
// sources are refused (covered by the Go tests, since a browser will not
// forge them).
test('the signed-in page can change state with a same-origin request', async ({ page }) => {
  await page.goto(await signInPath(page));
  const status = await page.evaluate(async () => {
    const response = await fetch('/v1/workspaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: 'e2e-signed-in', name: 'e2e-signed-in' }),
    });
    return response.status;
  });
  expect([200, 201]).toContain(status);
});
