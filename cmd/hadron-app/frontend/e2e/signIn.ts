import fs from 'node:fs';
import path from 'node:path';
import { expect, type Page } from '@playwright/test';

/** The operator token hadrond created in the Playwright runtime's data dir. */
export function operatorToken(): string {
  const runtime = process.env.HADRON_PLAYWRIGHT_RUNTIME;
  if (!runtime) throw new Error('HADRON_PLAYWRIGHT_RUNTIME is not set');
  return fs.readFileSync(path.join(runtime, 'data', 'operator.token'), 'utf8').trim();
}

/** Requests a single-use sign-in link with the token, as `hadron ui` does. */
export async function signInPath(page: Page): Promise<string> {
  const response = await page.request.post('/v1/auth/code', {
    headers: { Authorization: `Bearer ${operatorToken()}` },
  });
  expect(response.status()).toBe(200);
  const issued = (await response.json()) as { login_path: string };
  expect(issued.login_path).toMatch(/^\/auth\/login\?code=/);
  return issued.login_path;
}

/** Signs the browser in through the one-time code exchange. */
export async function signIn(page: Page) {
  await page.goto(await signInPath(page));
  await expect(page).toHaveURL(/\/$/);
}
