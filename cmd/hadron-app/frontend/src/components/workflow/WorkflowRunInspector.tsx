import { useEffect, useState } from 'react';
import { AlertTriangle, Braces, ChevronLeft, ChevronRight, ExternalLink, GitBranch, LockKeyhole, RotateCcw, ShieldCheck, TimerReset, Users } from 'lucide-react';

import type { WorkflowGraphDiagnostic, WorkflowNodeDiagnostic, WorkflowValueSetDiagnostic, WorkflowValueSetRef } from '../../api/types';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { StatusBadge } from '../ui/StatusBadge';
import { findValueSet, invocationKey, selectPrimaryInvocation, sortWorkflowInvocations, sourceLabel } from './workflowGraph.helpers';
import { workflowValuePreview } from './workflowValues.helpers';

interface WorkflowRunInspectorProps {
  result: WorkflowGraphDiagnostic;
  selectedNodeId: string | null;
  onResume: (node: WorkflowNodeDiagnostic) => void;
}

function shortDigest(value?: string): string {
  if (!value) return 'Unavailable';
  return value.length > 20 ? `${value.slice(0, 13)}…${value.slice(-6)}` : value;
}

function resourceLabel(resource: { kind: string; name?: string; node_id?: string }): string {
  return [resource.kind, resource.name || resource.node_id].filter(Boolean).join(':');
}

function ValueSetView({ result, valueRef, label }: { result: WorkflowGraphDiagnostic; valueRef?: WorkflowValueSetRef; label: string }) {
  if (!valueRef) return null;
  const set = findValueSet(result, valueRef);
  return (
    <section className="workflow-inspector-section">
      <div className="workflow-section-heading"><Braces size={12} /> {label}</div>
      {!set ? (
        <div className="workflow-unavailable">Value data was omitted or is unavailable from this bounded response.</div>
      ) : (
        <ValueRows set={set} />
      )}
    </section>
  );
}

function ValueRows({ set }: { set: WorkflowValueSetDiagnostic }) {
  return (
    <div className="workflow-value-list">
      {Object.entries(set.values).sort(([left], [right]) => left.localeCompare(right)).map(([name, value]) => {
        const preview = workflowValuePreview(value);
        return (
          <div className="workflow-value-row" key={name}>
            <div className="workflow-value-name">
              <code>{name}</code>
              <span>{value.type}</span>
              {preview.masked && <LockKeyhole size={11} aria-label="Masked" />}
            </div>
            {preview.artifact ? (
              <dl className="workflow-artifact-facts">
                <div><dt>Store</dt><dd>{preview.artifact.store || 'Unavailable'}</dd></div>
                <div><dt>Media</dt><dd>{preview.artifact.mediaType || 'Unavailable'}</dd></div>
                <div><dt>Size</dt><dd>{preview.artifact.sizeBytes.toLocaleString()} B</dd></div>
                <div><dt>Digest</dt><dd title={preview.artifact.digest}>{shortDigest(preview.artifact.digest)}</dd></div>
              </dl>
            ) : (
              <code className={preview.masked ? 'workflow-value-preview is-masked' : 'workflow-value-preview'}>{preview.text}</code>
            )}
          </div>
        );
      })}
    </div>
  );
}

function PolicyFacts({ result }: { result: WorkflowGraphDiagnostic }) {
  const policy = result.start_policy;
  return (
    <section className="workflow-inspector-section">
      <div className="workflow-section-heading"><ShieldCheck size={12} /> Start policy</div>
      {!policy ? (
        <div className="workflow-unavailable">Start policy facts are unavailable from this host.</div>
      ) : (
        <>
          <div className="workflow-chip-row">
            {policy.declared_effects.map(effect => <Badge key={effect} variant={effect === 'destructive' || effect === 'mutate' ? 'failed' : 'outline'}>{effect}</Badge>)}
          </div>
          <dl className="workflow-fact-grid">
            <div><dt>Decision</dt><dd>{policy.decision}</dd></div>
            <div><dt>Dry run</dt><dd>{policy.dry_run_available ? 'Available' : 'Unavailable'}</dd></div>
            <div><dt>Confirmation</dt><dd>{policy.confirmation_advised ? 'Advised' : 'Not advised'}</dd></div>
            <div><dt>Exposure</dt><dd>{policy.exposure_ref || 'Not recorded'}</dd></div>
          </dl>
          <div className="workflow-capability-list">
            <span>Required capabilities</span>
            <code>{policy.required_capabilities?.join(' · ') || 'None declared'}</code>
          </div>
          <div className="workflow-blast-list">
            {Object.entries(policy.blast_radius).sort(([left], [right]) => left.localeCompare(right)).map(([effect, count]) => (
              <div key={effect}><span>{effect}</span><strong>{count}</strong></div>
            ))}
          </div>
        </>
      )}
    </section>
  );
}

function ExecutionFacts({ result }: { result: WorkflowGraphDiagnostic }) {
  const decisions = result.control.decisions ?? [];
  const resources = result.resources;
  const concurrencyAvailable = result.capabilities.concurrency_state === true;
  const controlAvailable = result.capabilities.control_decisions === true;
  return (
    <>
      <section className="workflow-inspector-section">
        <div className="workflow-section-heading"><GitBranch size={12} /> Control decisions</div>
        {!controlAvailable ? (
          <div className="workflow-unavailable">Catch, switch, and finalizer decisions are unavailable from this host.</div>
        ) : decisions.length === 0 && !result.control.terminal_intent ? (
          <div className="workflow-empty-compact">No catch or switch route has been selected.</div>
        ) : (
          <div className="workflow-decision-list">
            {decisions.map((decision, index) => (
              <div key={`${decision.source.node_id}:${decision.source.iteration ?? ''}:${decision.kind}:${index}`}>
                <span>{decision.kind}</span>
                <strong>{decision.outcome}</strong>
                <code>{decision.source.node_id} → {decision.targets?.map(target => target.node_id).join(', ') || 'no target'}</code>
              </div>
            ))}
            {result.control.terminal_intent?.finalizers?.map(finalizer => (
              <div key={`finally:${finalizer.invocation.node_id}:${finalizer.order}`}>
                <span>finally</span>
                <strong>{result.control.terminal_intent?.status}</strong>
                <code>{finalizer.invocation.node_id} · order {finalizer.order}</code>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="workflow-inspector-section">
        <div className="workflow-section-heading"><Users size={12} /> Concurrency resources</div>
        {!concurrencyAvailable || !resources ? (
          <div className="workflow-unavailable">Concurrency ownership and queue state are unavailable from this host.</div>
        ) : resources.holders.length === 0 && resources.waiters.length === 0 ? (
          <div className="workflow-empty-compact">No resource holders or queued invocations.</div>
        ) : (
          <div className="workflow-resource-list">
            {resources.holders.map((holder, index) => (
              <div key={`holder:${holder.invocation.node_id}:${index}`}>
                <span>held</span>
                <code>{resourceLabel(holder.resource)}</code>
                <strong>{holder.invocation.node_id} · {holder.units}u</strong>
              </div>
            ))}
            {resources.waiters.map((waiter, index) => (
              <div key={`waiter:${waiter.invocation.node_id}:${index}`}>
                <span>queued</span>
                <code>{waiter.blocked.map(resourceLabel).join(', ')}</code>
                <strong>{waiter.invocation.node_id}</strong>
              </div>
            ))}
          </div>
        )}
      </section>
    </>
  );
}

export function WorkflowRunInspector({ result, selectedNodeId, onResume }: WorkflowRunInspectorProps) {
  const definition = result.plan.nodes.find(node => node.id === selectedNodeId);
  const invocations = sortWorkflowInvocations(result.nodes.filter(node => node.id.node_id === selectedNodeId));
  const [selectedInvocationKey, setSelectedInvocationKey] = useState<string | null>(null);
  useEffect(() => setSelectedInvocationKey(null), [selectedNodeId]);
  const primary = selectPrimaryInvocation(invocations);
  const selected = invocations.find(node => invocationKey(node) === selectedInvocationKey) ?? primary;
  const selectedInvocationIndex = selected ? invocations.findIndex(node => invocationKey(node) === invocationKey(selected)) : -1;
  return (
    <aside className="workflow-run-inspector" aria-label="Workflow run inspector">
      <section className="workflow-inspector-section workflow-run-identity">
        <div className="workflow-section-heading"><GitBranch size={12} /> Run binding</div>
        <div className="workflow-definition-name">{result.plan.definition.id || result.plan.id}</div>
        <div className="workflow-definition-ref">
          <span>{result.plan.definition.authority || 'source'}</span>
          <code>{result.plan.definition.version || result.plan.version}</code>
        </div>
        <dl className="workflow-fact-grid">
          <div><dt>Registry digest</dt><dd title={result.plan.definition.digest}>{shortDigest(result.plan.definition.digest)}</dd></div>
          <div><dt>Plan digest</dt><dd title={result.plan.digest}>{shortDigest(result.plan.digest)}</dd></div>
          <div><dt>Generation</dt><dd>{result.run.generation}</dd></div>
          <div><dt>Nodes</dt><dd>{result.plan.nodes.length}</dd></div>
        </dl>
        {result.replay && (
          <div className="workflow-replay-fact"><RotateCcw size={12} /> Replayed from <code>{result.replay.source_run_id}</code> at <code>{result.replay.from_node_id}</code></div>
        )}
      </section>

      {definition && selected ? (
        <>
          <section className="workflow-inspector-section workflow-selected-node">
            <div className="workflow-section-heading">Selected node</div>
            <div className="workflow-selected-title">
              <div><strong>{definition.display_name || definition.id}</strong><code>{definition.id}</code></div>
              <StatusBadge status={selected.status} />
            </div>
            {invocations.length > 1 && (
              <div className="workflow-invocation-nav" aria-label="Node invocations">
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Previous invocation"
                  disabled={selectedInvocationIndex <= 0}
                  onClick={() => setSelectedInvocationKey(invocationKey(invocations[selectedInvocationIndex - 1]))}
                ><ChevronLeft size={12} /></Button>
                <code>Invocation {selectedInvocationIndex + 1}/{invocations.length}{selected.id.iteration !== undefined ? ` · iteration ${selected.id.iteration}` : ''}</code>
                <Button
                  variant="outline"
                  size="icon-sm"
                  aria-label="Next invocation"
                  disabled={selectedInvocationIndex >= invocations.length - 1}
                  onClick={() => setSelectedInvocationKey(invocationKey(invocations[selectedInvocationIndex + 1]))}
                ><ChevronRight size={12} /></Button>
              </div>
            )}
            <div className="workflow-chip-row">
              <Badge variant="outline">{definition.kind}</Badge>
              {definition.declared_effects?.map(effect => <Badge key={effect} variant={effect === 'destructive' || effect === 'mutate' ? 'failed' : 'secondary'}>{effect}</Badge>)}
              {definition.finally && <Badge variant="canceled">finally</Badge>}
            </div>
            <div className="workflow-source-link" title={sourceLabel(selected.source || definition.source)}>
              <ExternalLink size={11} /> <code>{sourceLabel(selected.source || definition.source)}</code>
            </div>
            <div className="workflow-explanation">
              <span>{selected.explanation.code}</span>
              <p>{selected.explanation.masked ? '[REDACTED]' : selected.explanation.message}</p>
            </div>
          </section>

          {selected.wait && (
            <section className="workflow-inspector-section workflow-wait-card">
              <div className="workflow-section-heading"><TimerReset size={12} /> Durable wait</div>
              <dl className="workflow-fact-grid">
                <div><dt>Kind</dt><dd>{selected.wait.kind}</dd></div>
                <div><dt>Status</dt><dd>{selected.wait.status}</dd></div>
                <div><dt>Wake source</dt><dd>{selected.wait.wake_source}</dd></div>
                <div><dt>Visibility</dt><dd>{selected.wait.visibility}</dd></div>
              </dl>
              {selected.wait.deadline && <div className="workflow-deadline">Deadline · {new Date(selected.wait.deadline).toLocaleString()}</div>}
              {selected.wait.status === 'open' && <Button onClick={() => onResume(selected)}><LockKeyhole size={12} /> Resume wait</Button>}
            </section>
          )}

          <section className="workflow-inspector-section">
            <div className="workflow-section-heading"><RotateCcw size={12} /> Attempts and retry</div>
            <div className="workflow-attempt-summary">
              <strong>{selected.latest_attempt ?? 0}</strong>
              <span>of {definition.retry?.attempts ?? Math.max(selected.latest_attempt ?? 0, 1)} attempts</span>
              <code>{definition.retry?.strategy || 'no declared backoff'}</code>
            </div>
            {selected.attempts_truncated && <div className="workflow-omission"><AlertTriangle size={11} /> Attempt history truncated</div>}
            <div className="workflow-attempt-list">
              {(selected.attempts ?? []).map(attempt => (
                <div key={attempt.number}>
                  <span>#{attempt.number}</span>
                  <StatusBadge status={attempt.status} />
                  <code>{attempt.failure?.code || attempt.executor.kind}</code>
                  {attempt.failure && <p>{attempt.failure.masked ? '[REDACTED]' : attempt.failure.message}</p>}
                </div>
              ))}
            </div>
          </section>

          {selected.pin && (
            <section className="workflow-inspector-section">
              <div className="workflow-section-heading"><GitBranch size={12} /> Pinned output</div>
              <div className="workflow-replay-fact">From <code>{selected.pin.source.node_id}</code> · {selected.pin.policy_code}</div>
            </section>
          )}

          <ValueSetView result={result} valueRef={selected.inputs} label="Typed inputs" />
          <ValueSetView result={result} valueRef={selected.outputs} label="Redacted outputs" />
          <ValueSetView result={result} valueRef={selected.wait?.payload} label="Wait payload" />
        </>
      ) : (
        <section className="workflow-inspector-section workflow-unavailable">
          Select a graph node to inspect its durable attempts, waits, values, and source location.
        </section>
      )}

      <ExecutionFacts result={result} />
      <PolicyFacts result={result} />
    </aside>
  );
}

export function WorkflowValueLedger({ result }: { result: WorkflowGraphDiagnostic }) {
  return (
    <section className="workflow-detail-panel">
      <header><div><span>Typed value ledger</span><strong>{result.values?.length ?? 0} sets</strong></div><p>Shared redacted representation; secret and private markers are preserved.</p></header>
      <div className="workflow-ledger-scroll">
        {(result.values ?? []).length === 0 ? (
          <div className="workflow-empty-state">No rendered value sets are available for this run.</div>
        ) : result.values?.map(set => (
          <article className="workflow-ledger-set" key={`${set.ref.id}:${set.ref.digest}`}>
            <div className="workflow-ledger-role"><code>{set.roles.join(' · ')}</code><span>{Object.keys(set.values).length} values</span></div>
            <ValueRows set={set} />
          </article>
        ))}
      </div>
    </section>
  );
}
