import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { defineConfig, devices } from '@playwright/test';

const repositoryRoot = path.resolve(import.meta.dirname, '../../..');
const runtimeRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'hadron-playwright-')));
process.on('exit', () => fs.rmSync(runtimeRoot, { force: true, recursive: true }));

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  reporter: 'line',
  use: {
    baseURL: 'http://127.0.0.1:18095',
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command:
      'make frontend-build && go run ./cmd/hadrond serve --addr 127.0.0.1:18095 --db "$HADRON_PLAYWRIGHT_RUNTIME/state.db" --logs "$HADRON_PLAYWRIGHT_RUNTIME/logs" --data "$HADRON_PLAYWRIGHT_RUNTIME/data"',
    cwd: repositoryRoot,
    env: { HADRON_PLAYWRIGHT_RUNTIME: runtimeRoot },
    url: 'http://127.0.0.1:18095/v1/health',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
