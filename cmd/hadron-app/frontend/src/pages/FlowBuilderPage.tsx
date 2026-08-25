import { useCallback, useMemo, useState } from 'react';
import {
  addEdge,
  Background,
  BackgroundVariant,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  type Connection,
  type Edge,
  type Node,
  type NodeProps,
  useEdgesState,
  useNodesState,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { AlertTriangle, CheckCircle2, GitBranch, Play, Plus, Search, ShieldAlert } from 'lucide-react';
import { toast } from 'sonner';

import { explainWorkflow, runWorkflow, validateWorkflow } from '@/api/workflows';
import type { WorkflowDefinitionRef, WorkflowValidateResult } from '@/api/types';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useDaemon } from '@/contexts/DaemonContext';
import { useNavigation } from '@/contexts/NavigationContext';
import { createWorkflowRunID, parseWorkflowInputs } from './workflowAuthoring.helpers';
import './workflowRun.css';

type DraftNodeData = { label: string; kind: string };

function DraftNode({ data, selected }: NodeProps<Node<DraftNodeData>>) {
  return (
    <div className={`workflow-author-node${selected ? ' is-selected' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <span>{data.kind}</span>
      <strong>{data.label}</strong>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

const nodeTypes = { workflowDraft: DraftNode };
const initialNodes: Node<DraftNodeData>[] = [
  { id: 'input', type: 'workflowDraft', position: { x: 80, y: 130 }, data: { label: 'source reference', kind: 'definition' } },
  { id: 'inspect', type: 'workflowDraft', position: { x: 390, y: 130 }, data: { label: 'durable run', kind: 'runtime' } },
];
const initialEdges: Edge[] = [{ id: 'input-inspect', source: 'input', target: 'inspect', label: 'validate · run' }];

function createRunID(prefix: string): string {
  return createWorkflowRunID(prefix, crypto.randomUUID());
}

export function FlowBuilderPage() {
  const daemon = useDaemon();
  const navigation = useNavigation();
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [definition, setDefinition] = useState<WorkflowDefinitionRef>({ kind: 'file', id: '', locator: '' });
  const [inputs, setInputs] = useState('{}');
  const [confirmed, setConfirmed] = useState(false);
  const [busy, setBusy] = useState<'validate' | 'explain' | 'run' | null>(null);
  const [validation, setValidation] = useState<WorkflowValidateResult>();
  const [runLookup, setRunLookup] = useState('');

  const sourceReady = definition.id.trim() !== '' && (definition.kind === 'registry' || definition.locator?.trim() !== '');
  const normalizedDefinition = useMemo<WorkflowDefinitionRef>(() => ({
    kind: definition.kind,
    id: definition.id.trim(),
    ...(definition.authority?.trim() ? { authority: definition.authority.trim() } : {}),
    ...(definition.locator?.trim() ? { locator: definition.locator.trim() } : {}),
    ...(definition.version?.trim() ? { version: definition.version.trim() } : {}),
    ...(definition.digest?.trim() ? { digest: definition.digest.trim() } : {}),
  }), [definition]);

  const updateDefinition = (field: keyof WorkflowDefinitionRef, value: string) => {
    setDefinition(current => ({ ...current, [field]: value }));
    setValidation(undefined);
  };

  const onConnect = useCallback((connection: Connection) => {
    setEdges(current => addEdge({ ...connection, type: 'default', label: 'draft control' }, current));
  }, [setEdges]);

  const addDraftNode = () => {
    const ordinal = nodes.length + 1;
    setNodes(current => [...current, {
      id: `draft-${crypto.randomUUID()}`,
      type: 'workflowDraft',
      position: { x: 180 + ordinal * 34, y: 250 + (ordinal % 3) * 90 },
      data: { label: `layout note ${ordinal}`, kind: 'draft only' },
    }]);
  };

  const validate = async () => {
    setBusy('validate');
    try {
      const result = await validateWorkflow(normalizedDefinition);
      setValidation(result);
      if (result.plan) toast.success('Definition resolved and validated');
      else toast.error('Definition returned diagnostics');
    } catch (error) {
      setValidation(undefined);
      toast.error(`Validation unavailable: ${error instanceof Error ? error.message : 'request failed'}`);
    } finally {
      setBusy(null);
    }
  };

  const explain = async () => {
    setBusy('explain');
    try {
      const requestID = createRunID('explain');
      const result = await explainWorkflow({
        run_id: requestID,
        definition: normalizedDefinition,
        inputs: parseWorkflowInputs(inputs),
        confirmed,
        idempotency_key: requestID,
      });
      toast.success(result.decision?.reason || 'Dry-run policy explanation completed');
    } catch (error) {
      toast.error(`Explain unavailable: ${error instanceof Error ? error.message : 'request failed'}`);
    } finally {
      setBusy(null);
    }
  };

  const run = async () => {
    setBusy('run');
    try {
      const requestID = createRunID('ui');
      const result = await runWorkflow({
        run_id: requestID,
        definition: normalizedDefinition,
        inputs: parseWorkflowInputs(inputs),
        confirmed,
        idempotency_key: requestID,
      });
      const runID = result.run?.id || requestID;
      toast.success('Workflow start accepted');
      navigation.openRun(runID);
    } catch (error) {
      toast.error(`Run unavailable: ${error instanceof Error ? error.message : 'request failed'}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="workflow-author-page">
      <header className="workflow-author-header">
        <div>
          <span>Graph-native workflow source</span>
          <h1>Resolve, validate, and run through hadrond</h1>
        </div>
        <div className={`workflow-transport-state is-${daemon.status}`}>
          <i /> {daemon.status} · {daemon.address}
        </div>
      </header>

      <div className="workflow-author-grid">
        <aside className="workflow-source-panel" aria-label="Workflow source reference">
          <div className="workflow-panel-kicker">Immutable definition</div>
          <Label htmlFor="workflow-kind">Source kind</Label>
          <select id="workflow-kind" value={definition.kind} onChange={event => updateDefinition('kind', event.target.value)}>
            <option value="file">Authorized file</option>
            <option value="registry">Registry identity</option>
            <option value="package">Workflow package</option>
          </select>
          <Label htmlFor="workflow-id">Workflow ID</Label>
          <Input id="workflow-id" value={definition.id} onChange={event => updateDefinition('id', event.target.value)} placeholder="deploy-service" />
          <Label htmlFor="workflow-locator">Locator</Label>
          <Input id="workflow-locator" value={definition.locator ?? ''} onChange={event => updateDefinition('locator', event.target.value)} placeholder="workflows/deploy.workflow.yaml" />
          <div className="workflow-source-columns">
            <div><Label htmlFor="workflow-version">Version</Label><Input id="workflow-version" value={definition.version ?? ''} onChange={event => updateDefinition('version', event.target.value)} placeholder="1.0.0" /></div>
            <div><Label htmlFor="workflow-authority">Authority</Label><Input id="workflow-authority" value={definition.authority ?? ''} onChange={event => updateDefinition('authority', event.target.value)} placeholder="local" /></div>
          </div>
          <Label htmlFor="workflow-digest">Pinned digest</Label>
          <Input id="workflow-digest" value={definition.digest ?? ''} onChange={event => updateDefinition('digest', event.target.value)} placeholder="sha256:… (optional)" className="font-mono" />
          <Label htmlFor="workflow-inputs">Typed inputs (JSON)</Label>
          <textarea id="workflow-inputs" value={inputs} onChange={event => setInputs(event.target.value)} spellCheck={false} />
          <label className="workflow-confirm-row">
            <input type="checkbox" checked={confirmed} onChange={event => setConfirmed(event.target.checked)} />
            Confirm consequential effects when policy requires it
          </label>
          <div className="workflow-source-actions">
            <Button variant="outline" disabled={!sourceReady || busy !== null} onClick={validate}><CheckCircle2 size={13} /> {busy === 'validate' ? 'Validating…' : 'Validate'}</Button>
            <Button variant="outline" disabled={!sourceReady || busy !== null} onClick={explain}><ShieldAlert size={13} /> {busy === 'explain' ? 'Explaining…' : 'Dry-run facts'}</Button>
            <Button disabled={!sourceReady || daemon.status !== 'running' || busy !== null} onClick={run}><Play size={13} /> {busy === 'run' ? 'Starting…' : 'Run workflow'}</Button>
          </div>
          {validation && (
            <div className={`workflow-validation-result${validation.plan ? ' is-valid' : ' is-invalid'}`} role="status">
              {validation.plan ? <CheckCircle2 size={15} /> : <AlertTriangle size={15} />}
              <div>
                <strong>{validation.plan ? 'Pinned plan ready' : 'Definition needs attention'}</strong>
                {validation.plan && <code>{validation.plan.digest}</code>}
                {validation.diagnostics.map(item => <span key={`${item.code}-${item.message}`}>{item.code}: {item.message}</span>)}
              </div>
            </div>
          )}
        </aside>

        <section className="workflow-author-canvas" aria-label="Workflow graph layout draft">
          <header>
            <div><GitBranch size={14} /><span>Canvas continuity</span><strong>xyflow layout workspace</strong></div>
            <Button variant="ghost" size="sm" onClick={addDraftNode}><Plus size={12} /> Add layout note</Button>
          </header>
          <div className="workflow-author-canvas-body">
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              nodeTypes={nodeTypes}
              fitView
              minZoom={0.25}
              maxZoom={2}
              deleteKeyCode={['Backspace', 'Delete']}
              proOptions={{ hideAttribution: true }}
            >
              <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--border-default)" />
              <MiniMap nodeColor="var(--border-strong)" maskColor="rgba(9, 9, 11, 0.72)" />
              <Controls showInteractive={false} />
            </ReactFlow>
          </div>
          <footer>
            <AlertTriangle size={13} />
            <span>Layout edits are an in-browser draft only. Registry publication, exposure mutation, and source persistence remain unavailable until their shared daemon contracts land; this client does not fabricate them.</span>
          </footer>
        </section>
      </div>

      <section className="workflow-run-lookup" aria-label="Open a durable workflow run">
        <div><Search size={15} /><span>Known run ID</span><strong>Inspect the bounded graph diagnostic directly</strong></div>
        <Input value={runLookup} onChange={event => setRunLookup(event.target.value)} placeholder="workflow run ID" className="font-mono" />
        <Button variant="outline" disabled={!runLookup.trim()} onClick={() => navigation.openRun(runLookup.trim())}>Inspect run</Button>
      </section>
    </div>
  );
}
