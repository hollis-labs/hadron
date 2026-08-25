import { isDemoMode } from '../demo/demoMode';
import { getDemoWorkflowDiagnostic } from '../demo/workflowData';
import { HadronWorkflowClient } from './generated/workflow';
import { apiFetch } from './http';
import type {
  WorkflowCatalogSearchResult,
  WorkflowDefinitionRef,
  WorkflowExposureProfile,
  WorkflowGraphDiagnostic,
  WorkflowRerunResult,
  WorkflowResumeRequest,
  WorkflowResumeResult,
  WorkflowStartResult,
  WorkflowValidateResult,
  WorkflowVersionDetail,
} from './types';

const browserWorkflowIdentity = { source_authority: 'http' } as const;
const workflowClient = new HadronWorkflowClient(apiFetch);

export async function validateWorkflow(definition: WorkflowDefinitionRef): Promise<WorkflowValidateResult> {
  return workflowClient.validateWorkflow({ definition, identity: browserWorkflowIdentity });
}

export async function explainWorkflow(request: {
  run_id: string;
  definition: WorkflowDefinitionRef;
  inputs?: Record<string, unknown>;
  confirmed?: boolean;
  idempotency_key: string;
}): Promise<WorkflowStartResult> {
  return workflowClient.explainWorkflow({ ...request, identity: browserWorkflowIdentity });
}

export async function runWorkflow(request: {
  run_id: string;
  definition: WorkflowDefinitionRef;
  inputs?: Record<string, unknown>;
  confirmed?: boolean;
  dry_run?: boolean;
  idempotency_key: string;
}): Promise<WorkflowStartResult> {
  return workflowClient.runWorkflow({ ...request, identity: browserWorkflowIdentity });
}

export async function inspectWorkflowRun(runId: string): Promise<WorkflowGraphDiagnostic> {
  if (isDemoMode()) return getDemoWorkflowDiagnostic(runId);
  return workflowClient.inspectWorkflowRun({
    run_id: runId,
    identity: browserWorkflowIdentity,
    node_limit: 500,
    attempt_limit: 100,
    event_limit: 1000,
    value_limit: 1000,
    resource_limit: 500,
    activation_limit: 200,
  });
}

export async function cancelWorkflowRun(runId: string, idempotencyKey: string): Promise<void> {
  if (isDemoMode()) return;
  await workflowClient.cancelWorkflowRun({
    run_id: runId,
    identity: browserWorkflowIdentity,
    idempotency_key: idempotencyKey,
    reason: 'operator requested cancellation from browser UI',
  });
}

export async function resumeWorkflowWait(request: Omit<WorkflowResumeRequest, 'identity'>): Promise<WorkflowResumeResult> {
  if (isDemoMode()) {
    return { outcome: 'applied', wait: { id: request.wait_id, status: 'resolved' } };
  }
  return workflowClient.resumeWorkflowRun({ ...request, identity: browserWorkflowIdentity });
}

export async function rerunWorkflowRun(sourceRunId: string, fromNodeId: string, runId: string, idempotencyKey: string): Promise<WorkflowRerunResult> {
  return workflowClient.rerunWorkflow({
    source_run_id: sourceRunId,
    run_id: runId,
    from_node_id: fromNodeId,
    idempotency_key: idempotencyKey,
    identity: browserWorkflowIdentity,
  });
}

export async function searchWorkflowCatalog(
  query: string,
  namespace = '',
  limit = 20,
): Promise<WorkflowCatalogSearchResult> {
  return workflowClient.searchWorkflowCatalog({
    query,
    namespace,
    limit,
    identity: browserWorkflowIdentity,
  });
}

export async function inspectWorkflowVersion(
  definition: WorkflowDefinitionRef,
): Promise<WorkflowVersionDetail> {
  return workflowClient.inspectWorkflowVersion({ definition, identity: browserWorkflowIdentity });
}

export async function inspectWorkflowExposure(profileId: string): Promise<WorkflowExposureProfile> {
  return workflowClient.inspectWorkflowExposure({
    profile_id: profileId,
    identity: browserWorkflowIdentity,
  });
}
