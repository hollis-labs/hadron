import { expect, test, type Page, type Request } from '@playwright/test';

import { getDemoWorkflowDiagnostic } from '../src/demo/workflowData';

async function openKnownRun(page: Page, runID: string) {
  await page.getByRole('button', { name: 'Runs' }).click();
  await page.getByLabel('Workflow run ID').fill(runID);
  await page.getByRole('button', { name: 'Inspect' }).click();
  await expect(page.getByText(runID, { exact: true })).toBeVisible();
}

test('edits the xyflow draft and keeps registry, exposure, and persistence gaps explicit', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Workflow Graph' }).click();
  await expect(page.getByRole('heading', { name: 'Resolve, validate, and run through hadrond' })).toBeVisible();
  await expect(page.locator('.workflow-author-node')).toHaveCount(2);
  await page.getByRole('button', { name: 'Add layout note' }).click();
  await expect(page.locator('.workflow-author-node')).toHaveCount(3);
  await expect(page.getByText(/Registry publication, exposure mutation, and source persistence remain unavailable/)).toBeVisible();
});

test('inspects an opaque waiting run and resumes a credentialless gate through HTTP', async ({ page }) => {
  const runID = 'source/workflow-waiting';
  const diagnostic = getDemoWorkflowDiagnostic(runID);
  let resumeRequest: Request | undefined;

  await page.route('**/v1/workflows/runs/**/inspect', route => route.fulfill({ json: diagnostic }));
  await page.route('**/v1/workflows/runs/**/resume', route => {
    resumeRequest = route.request();
    return route.fulfill({ json: { outcome: 'applied', wait: { id: 'wait-release-approval', status: 'resumed' } } });
  });
  await page.goto('/');
  await openKnownRun(page, runID);

  await expect(page.getByRole('region', { name: 'Workflow execution graph' })).toBeVisible();
  await expect(page.getByText('Release approval', { exact: true }).first()).toBeVisible();
  await page.getByRole('button', { name: 'Resume wait' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog.getByText(/does not use a one-time token/)).toBeVisible();
  await expect(dialog.getByLabel('One-time token')).toHaveCount(0);
  await dialog.getByLabel('Correlation').fill('release-approval');
  await dialog.getByRole('button', { name: 'Resume wait' }).click();
  await expect(page.getByText('Wait resume accepted')).toBeVisible();

  expect(resumeRequest).toBeDefined();
  expect(resumeRequest?.url()).toContain('/v1/workflows/runs/source%2Fworkflow-waiting/resume');
  expect(resumeRequest?.headers()['idempotency-key']).toBeTruthy();
  expect(resumeRequest?.postDataJSON()).toMatchObject({
    run_id: runID,
    wait_id: 'wait-release-approval',
    correlation: 'release-approval',
    token: '',
    wake_source: 'gate',
  });
});

test('shows registry and exposure facts and replays a completed run through HTTP', async ({ page }) => {
  const runID = 'source/workflow-completed';
  let rerunRequest: Request | undefined;

  await page.route('**/v1/workflows/runs/**/inspect', route => {
    const requestedRunID = decodeURIComponent(route.request().url().split('/runs/')[1].split('/inspect')[0]);
    return route.fulfill({ json: getDemoWorkflowDiagnostic(requestedRunID) });
  });
  await page.route('**/v1/workflows/runs/**/rerun', route => {
    rerunRequest = route.request();
    return route.fulfill({
      json: {
        outcome: 'applied',
        run: { id: 'workflow-completed-replay' },
        provenance: { source_run_id: runID, from_node_id: 'receive' },
      },
    });
  });
  await page.goto('/');
  await openKnownRun(page, runID);

  await expect(page.getByLabel('Workflow run inspector').getByText('release/publish', { exact: true })).toBeVisible();
  await expect(page.getByText('release-operators', { exact: true })).toBeVisible();
  await page.getByText('Receive release', { exact: true }).click();
  await page.getByRole('button', { name: 'Replay from node' }).click();
  await expect(page.getByRole('alertdialog')).toContainText('Replay from receive?');
  await page.getByRole('button', { name: 'Create replay' }).click();
  await expect(page.getByText('Replay created from receive')).toBeVisible();

  expect(rerunRequest).toBeDefined();
  expect(rerunRequest?.url()).toContain('/v1/workflows/runs/source%2Fworkflow-completed/rerun');
  expect(rerunRequest?.headers()['idempotency-key']).toBeTruthy();
  expect(rerunRequest?.postDataJSON()).toMatchObject({ source_run_id: runID, from_node_id: 'receive' });
});
