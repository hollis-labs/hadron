import path from 'node:path';
import { defineConfig, devices } from '@playwright/test';

const repositoryRoot = path.resolve(import.meta.dirname, '../../..');

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
    command: 'make frontend-build && go run ./cmd/hadrond serve --addr 127.0.0.1:18095 --db /tmp/hadron-playwright/state.db --logs /tmp/hadron-playwright/logs --data /tmp/hadron-playwright/data',
    cwd: repositoryRoot,
    url: 'http://127.0.0.1:18095/v1/health',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
