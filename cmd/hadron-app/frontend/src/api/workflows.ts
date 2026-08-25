import { isDemoMode } from '../demo/demoMode';
import { getDemoWorkflowDiagnostic } from '../demo/workflowData';
import { apiFetch } from './http';
import type {
  WorkflowDefinitionRef,
  WorkflowGraphDiagnostic,
  WorkflowRerunResult,
  WorkflowResumeRequest,
  WorkflowResumeResult,
  WorkflowStartResult,
  WorkflowValidateResult,
} from './types';

const browserWorkflowIdentity = { source_authority: 'http' } as const;

export async function validateWorkflow(definition: WorkflowDefinitionRef): Promise<WorkflowValidateResult> {
  return apiFetch<WorkflowValidateResult>('/v1/workflows/validate', {
    method: 'POST',
    body: JSON.stringify({ definition, identity: browserWorkflowIdentity }),
  });
}

export async function explainWorkflow(request: {
  run_id: string;
  definition: WorkflowDefinitionRef;
  inputs?: Record<string, unknown>;
  confirmed?: boolean;
  idempotency_key: string;
}): Promise<WorkflowStartResult> {
  return apiFetch<WorkflowStartResult>('/v1/workflows/explain', {
    method: 'POST',
    headers: { 'Idempotency-Key': request.idempotency_key },
    body: JSON.stringify({ ...request, identity: browserWorkflowIdentity }),
  });
}

export async function runWorkflow(request: {
  run_id: string;
  definition: WorkflowDefinitionRef;
  inputs?: Record<string, unknown>;
  confirmed?: boolean;
  dry_run?: boolean;
  idempotency_key: string;
}): Promise<WorkflowStartResult> {
  return apiFetch<WorkflowStartResult>('/v1/workflows/runs', {
    method: 'POST',
    headers: { 'Idempotency-Key': request.idempotency_key },
    body: JSON.stringify({ ...request, identity: browserWorkflowIdentity }),
  });
}

export async function inspectWorkflowRun(runId: string): Promise<WorkflowGraphDiagnostic> {
  if (isDemoMode()) return getDemoWorkflowDiagnostic(runId);
  return apiFetch<WorkflowGraphDiagnostic>(`/v1/workflows/runs/${encodeURIComponent(runId)}/inspect`, {
    method: 'POST',
    body: JSON.stringify({
      run_id: runId,
      identity: browserWorkflowIdentity,
      node_limit: 500,
      attempt_limit: 100,
      event_limit: 1000,
      value_limit: 1000,
      resource_limit: 500,
      activation_limit: 200,
    }),
  });
}

export async function cancelWorkflowRun(runId: string, idempotencyKey: string): Promise<void> {
  if (isDemoMode()) return;
  await apiFetch(`/v1/workflows/runs/${encodeURIComponent(runId)}/cancel`, {
    method: 'POST',
    headers: { 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify({
      run_id: runId,
      identity: browserWorkflowIdentity,
      idempotency_key: idempotencyKey,
      reason: 'operator requested cancellation from browser UI',
    }),
  });
}

export async function resumeWorkflowWait(request: Omit<WorkflowResumeRequest, 'identity'>): Promise<WorkflowResumeResult> {
  if (isDemoMode()) {
    return { outcome: 'applied', wait: { id: request.wait_id, status: 'resolved' } };
  }
  return apiFetch<WorkflowResumeResult>(`/v1/workflows/runs/${encodeURIComponent(request.run_id)}/resume`, {
    method: 'POST',
    headers: { 'Idempotency-Key': request.idempotency_key },
    body: JSON.stringify({ ...request, identity: browserWorkflowIdentity }),
  });
}

export async function rerunWorkflowRun(sourceRunId: string, fromNodeId: string, runId: string, idempotencyKey: string): Promise<WorkflowRerunResult> {
  return apiFetch<WorkflowRerunResult>(`/v1/workflows/runs/${encodeURIComponent(sourceRunId)}/rerun`, {
    method: 'POST',
    headers: { 'Idempotency-Key': idempotencyKey },
    body: JSON.stringify({
      source_run_id: sourceRunId,
      run_id: runId,
      from_node_id: fromNodeId,
      idempotency_key: idempotencyKey,
      identity: browserWorkflowIdentity,
    }),
  });
}
