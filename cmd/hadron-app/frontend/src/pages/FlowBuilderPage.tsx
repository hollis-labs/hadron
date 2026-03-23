import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { toast } from 'sonner';
import { ChevronLeft, Save, Copy, Code, X, Play, ChevronDown, ChevronUp, Search, FileCode, Plus } from 'lucide-react';
import {
  ReactFlow,
  ReactFlowProvider,
  MiniMap,
  Controls,
  Background,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
  useReactFlow,
  addEdge,
  type Node,
  type Edge,
  type Connection,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { StageNode, type StageNodeData } from '../components/flow/StageNode';
import { ConditionalEdge, type StageEdgeData } from '../components/flow/ConditionalEdge';
import { StagePropertyPanel } from '../components/flow/StagePropertyPanel';
import { NodePalette, type PaletteDragData } from '../components/flow/NodePalette';
import { readBlueprintFile, saveBlueprintFile, createBlueprintFile, openDirectoryDialog, enqueuePipeline, getPipelineStages, listRunEvents, getBlueprintDir, listFilesInDir } from '../api/client';
import type { PipelineStage, RunEvent } from '../api/types';
import { Spinner } from '../components/ui/Spinner';

interface FlowBuilderPageProps {
  path?: string | null;
  onBack: () => void;
  daemonStatus?: string;
  workspaceId?: string;
}

import { parsePipelineYaml, type ParsedPipelineStage } from '../utils/yaml';

interface ParsedPipeline {
  name: string;
  stages: ParsedPipelineStage[];
}

function parsePipelineForFlow(content: string): ParsedPipeline | null {
  const raw = parsePipelineYaml(content);
  if (!raw) return null;
  return { name: raw.name, stages: raw.stages };
}

// ── Convert parsed pipeline to React Flow nodes/edges ─────────────────

const NODE_WIDTH = 240;
const NODE_SPACING_Y = 100;

function pipelineToFlow(pipeline: ParsedPipeline): { nodes: Node[]; edges: Edge[] } {
  const nodes: Node[] = [];
  const edges: Edge[] = [];
  const hasPositions = pipeline.stages.some(s => s.position);
  const stageNameMap = new Map<string, number>();
  pipeline.stages.forEach((s, i) => stageNameMap.set(s.name, i));

  // Build dependency graph to detect roots
  const hasDeps = new Set<number>();
  pipeline.stages.forEach((s, i) => {
    if (s.depends_on) {
      for (const dep of s.depends_on) {
        const depIdx = stageNameMap.get(dep);
        if (depIdx !== undefined) hasDeps.add(i);
      }
    }
  });

  pipeline.stages.forEach((stage, i) => {
    const isRoot = !hasDeps.has(i) && i === 0;

    // Position: use saved positions (v2) or auto-layout vertically (v1)
    let position: { x: number; y: number };
    if (hasPositions && stage.position) {
      position = stage.position;
    } else {
      position = { x: NODE_WIDTH, y: i * (80 + NODE_SPACING_Y) };
    }

    const nodeData: StageNodeData = {
      label: stage.name || `Stage ${i + 1}`,
      blueprintPath: stage.blueprint_path,
      status: 'idle',
      condition: stage.condition || undefined,
      isStart: isRoot,
      inputs: Object.keys(stage.inputs).length > 0 ? stage.inputs : undefined,
      outputs: stage.outputs,
    };

    nodes.push({
      id: `stage-${i}`,
      type: 'stageNode',
      position,
      data: nodeData,
    });

    // Edge data: carry condition from target stage for conditional styling
    const edgeData: StageEdgeData = {
      condition: stage.condition || undefined,
      status: 'idle',
    };

    // Edges: from depends_on if v2, otherwise linear chain
    if (stage.depends_on && stage.depends_on.length > 0) {
      for (const dep of stage.depends_on) {
        const depIdx = stageNameMap.get(dep);
        if (depIdx !== undefined) {
          edges.push({
            id: `e-${depIdx}-${i}`,
            source: `stage-${depIdx}`,
            target: `stage-${i}`,
            type: 'conditional',
            data: edgeData,
          });
        }
      }
    } else if (i > 0) {
      // v1 linear: connect to previous stage
      edges.push({
        id: `e-${i - 1}-${i}`,
        source: `stage-${i - 1}`,
        target: `stage-${i}`,
        type: 'conditional',
        data: edgeData,
      });
    }
  });

  return { nodes, edges };
}

// ── Serialize canvas state to pipeline v2 YAML ───────────────────────

function serializeCanvasToYaml(
  pipelineName: string,
  nodes: Node[],
  edges: Edge[],
): string {
  // Build reverse edge map: target → source names (for depends_on)
  const nodeIdToName = new Map<string, string>();
  nodes.forEach(n => {
    const d = n.data as StageNodeData;
    nodeIdToName.set(n.id, d.label);
  });

  const dependsOnMap = new Map<string, string[]>();
  edges.forEach(e => {
    const sourceName = nodeIdToName.get(e.source);
    if (!sourceName) return;
    const existing = dependsOnMap.get(e.target) ?? [];
    existing.push(sourceName);
    dependsOnMap.set(e.target, existing);
  });

  let yaml = `meta:\n  name: "${pipelineName}"\n\nstages:\n`;

  nodes.forEach(n => {
    const d = n.data as StageNodeData;
    yaml += `  - name: "${d.label}"\n`;
    yaml += `    blueprint_path: "${d.blueprintPath}"\n`;

    if (d.condition) {
      yaml += `    if: "${d.condition}"\n`;
    }

    // depends_on (derived from edges)
    const deps = dependsOnMap.get(n.id);
    if (deps && deps.length > 0) {
      yaml += `    depends_on:\n`;
      for (const dep of deps) {
        yaml += `      - "${dep}"\n`;
      }
    }

    // position (v2)
    yaml += `    position:\n`;
    yaml += `      x: ${Math.round(n.position.x)}\n`;
    yaml += `      y: ${Math.round(n.position.y)}\n`;

    // inputs
    const inputs = (d.inputs ?? {}) as Record<string, string>;
    const inputKeys = Object.keys(inputs);
    if (inputKeys.length > 0) {
      yaml += `    inputs:\n`;
      for (const key of inputKeys) {
        yaml += `      ${key}: "${inputs[key]}"\n`;
      }
    }

    // outputs
    if (d.outputs && d.outputs.length > 0) {
      yaml += `    outputs:\n`;
      for (const out of d.outputs) {
        yaml += `      - "${out}"\n`;
      }
    }

    yaml += `\n`;
  });

  return yaml.trimEnd() + '\n';
}

// ── Component ─────────────────────────────────────────────────────────

const nodeTypes = { stageNode: StageNode };
const edgeTypes = { conditional: ConditionalEdge };

// ── Landing page — Open / New dialog ──────────────────────────────────

function FlowBuilderLanding({ onOpen, onBack }: { onOpen: (path: string) => void; onBack: () => void }) {
  const [files, setFiles] = useState<{ name: string; path: string }[]>([]);
  const [search, setSearch] = useState('');
  const [loadingFiles, setLoadingFiles] = useState(true);

  // Recursively scan for YAML files in blueprint dir
  useEffect(() => {
    setLoadingFiles(true);
    getBlueprintDir()
      .then(async (rootDir) => {
        if (!rootDir) return;
        const all: { name: string; path: string }[] = [];

        // Scan pipelines/ subdirectory (all YAML files are pipelines there)
        // Plus scan other dirs for files containing "pipeline" in name
        const queue: { dir: string; depth: number; isPipelineDir: boolean }[] = [
          { dir: rootDir, depth: 0, isPipelineDir: false },
        ];
        while (queue.length > 0) {
          const { dir, depth, isPipelineDir } = queue.shift()!;
          try {
            const entries = await listFilesInDir(dir);
            if (!entries) continue;
            for (const e of entries) {
              if (e.isDir && depth < 3) {
                // Mark pipelines/ subtree so all YAML files within are included
                const inPipelines = isPipelineDir || /pipelines?$/i.test(e.name);
                queue.push({ dir: e.path, depth: depth + 1, isPipelineDir: inPipelines });
              } else if (!e.isDir && /\.(yaml|yml)$/i.test(e.name)) {
                if (!isPipelineDir && !e.name.toLowerCase().includes('pipeline')) continue;
                const rel = e.path.startsWith(rootDir)
                  ? e.path.slice(rootDir.length + 1).replace(/\.(yaml|yml)$/i, '')
                  : e.name.replace(/\.(yaml|yml)$/i, '');
                all.push({ name: rel, path: e.path });
              }
            }
          } catch { /* skip inaccessible dirs */ }
        }
        setFiles(all);
      })
      .catch(() => {})
      .finally(() => setLoadingFiles(false));
  }, []);

  const filtered = search
    ? files.filter(f => f.name.toLowerCase().includes(search.toLowerCase()))
    : files;

  const handleNew = () => {
    // Open with a blank canvas — use a temp name, user will Save As
    onOpen('__new__');
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div className="page-header" style={{ gap: '0.5rem', flexShrink: 0 }}>
        <span className="page-title">Flow Builder</span>
      </div>

      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <div style={{
          background: 'var(--bg-surface)', border: '1px solid var(--border-default)',
          borderRadius: 'var(--radius-lg)', padding: '1.5rem', width: '380px',
        }}>
          <div style={{ fontSize: 'var(--text-xs)', textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--text-tertiary)', marginBottom: '0.75rem' }}>
            Open Pipeline
          </div>

          {/* Search / combobox */}
          <div style={{ position: 'relative', marginBottom: '0.5rem' }}>
            <Search
              size={12}
              style={{
                position: 'absolute', left: '0.5rem', top: '50%', transform: 'translateY(-50%)',
                color: 'var(--text-tertiary)', pointerEvents: 'none',
              }}
            />
            <input
              className="hud-input"
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search pipelines..."
              autoFocus
              style={{
                width: '100%', boxSizing: 'border-box', fontSize: 'var(--text-md)',
                padding: '0.4rem 0.6rem 0.4rem 1.6rem',
              }}
            />
          </div>

          {/* File list */}
          <div style={{
            maxHeight: '220px', overflow: 'auto', marginBottom: '0.75rem',
            border: '1px solid var(--border-default)', borderRadius: 'var(--radius-sm)',
          }}>
            {loadingFiles ? (
              <div style={{ padding: '0.75rem', textAlign: 'center', color: 'var(--text-tertiary)', fontSize: 'var(--text-sm)' }}>Loading...</div>
            ) : filtered.length === 0 ? (
              <div style={{ padding: '0.75rem', textAlign: 'center', color: 'var(--text-tertiary)', fontSize: 'var(--text-sm)' }}>
                {files.length === 0 ? 'No pipeline files found' : 'No matches'}
              </div>
            ) : (
              filtered.map(f => (
                <button
                  key={f.path}
                  onClick={() => onOpen(f.path)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: '0.4rem', width: '100%',
                    padding: '0.4rem 0.6rem', background: 'none', border: 'none',
                    borderBottom: '1px solid rgba(var(--border) / 0.5)',
                    color: 'var(--text-primary)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                    fontSize: 'var(--text-sm)', textAlign: 'left', transition: 'background 0.1s',
                  }}
                  onMouseOver={e => (e.currentTarget.style.background = 'rgba(var(--panel2) / 0.8)')}
                  onMouseOut={e => (e.currentTarget.style.background = 'none')}
                >
                  <FileCode size={13} style={{ color: 'var(--accent)', flexShrink: 0 }} />
                  {f.name}
                </button>
              ))
            )}
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: '0.4rem' }}>
            <button
              className="btn btn-primary"
              onClick={handleNew}
              style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.3rem' }}
            >
              <Plus size={12} /> New Pipeline
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// Wrapper: landing page when no path, or canvas when path is set
export function FlowBuilderPage(props: FlowBuilderPageProps) {
  const [activePath, setActivePath] = useState<string | null>(props.path ?? null);

  // Sync from parent when path prop changes
  useEffect(() => {
    if (props.path) setActivePath(props.path);
  }, [props.path]);

  if (!activePath) {
    return <FlowBuilderLanding onOpen={setActivePath} onBack={props.onBack} />;
  }

  return (
    <ReactFlowProvider>
      <FlowBuilderInner {...props} path={activePath} onBack={() => setActivePath(null)} />
    </ReactFlowProvider>
  );
}

function FlowBuilderInner({ path: pathProp, onBack, daemonStatus = 'stopped', workspaceId = 'default' }: FlowBuilderPageProps) {
  const path = pathProp!; // guaranteed non-null by wrapper
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pipelineName, setPipelineName] = useState('');
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [showYamlPreview, setShowYamlPreview] = useState(false);
  const { screenToFlowPosition } = useReactFlow();

  // ── Execution state ──────────────────────────────────────────────
  const [runId, setRunId] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [runEvents, setRunEvents] = useState<RunEvent[]>([]);
  const [showRunLog, setShowRunLog] = useState(false);
  const runLogRef = useRef<HTMLDivElement>(null);

  // Derive selected node from state
  const selectedNode = selectedNodeId ? nodes.find(n => n.id === selectedNodeId) : null;
  const selectedData = selectedNode?.data as StageNodeData | undefined;

  // Live YAML preview — recomputed when nodes/edges change
  const yamlPreview = useMemo(
    () => serializeCanvasToYaml(pipelineName, nodes, edges),
    [pipelineName, nodes, edges],
  );

  // Handle node selection changes
  const onSelectionChange = useCallback(({ nodes: sel }: { nodes: Node[] }) => {
    if (sel.length === 1) {
      setSelectedNodeId(sel[0].id);
    } else {
      setSelectedNodeId(null);
    }
  }, []);

  // Update node data from property panel
  const handleUpdateNodeData = useCallback((nodeId: string, partial: Partial<StageNodeData>) => {
    setNodes(nds => nds.map(n => {
      if (n.id !== nodeId) return n;
      return { ...n, data: { ...n.data, ...partial } };
    }));
  }, [setNodes]);

  // Delete node and all connected edges
  const handleDeleteNode = useCallback((nodeId: string) => {
    setNodes(nds => nds.filter(n => n.id !== nodeId));
    setEdges(eds => eds.filter(e => e.source !== nodeId && e.target !== nodeId));
    setSelectedNodeId(null);
    toast.success('Stage deleted');
  }, [setNodes, setEdges]);

  // Handle new edge creation from dragging between handles
  const onConnect = useCallback(
    (connection: Connection) => {
      const newEdge: Edge = {
        ...connection,
        id: `e-${connection.source}-${connection.target}`,
        type: 'conditional',
        data: { status: 'idle' } satisfies StageEdgeData,
      };
      setEdges((eds) => addEdge(newEdge, eds));
      toast.success('Dependency added');
    },
    [setEdges],
  );

  // Generate unique stage name
  const nextStageName = useCallback(() => {
    const existing = new Set(nodes.map(n => (n.data as StageNodeData).label));
    let i = nodes.length + 1;
    let name = `stage-${i}`;
    while (existing.has(name)) { i++; name = `stage-${i}`; }
    return name;
  }, [nodes]);

  // Add a blank node at viewport center
  const handleAddBlankNode = useCallback(() => {
    const id = `stage-${Date.now()}`;
    const position = screenToFlowPosition({ x: window.innerWidth / 2, y: window.innerHeight / 2 });
    const newNode: Node = {
      id,
      type: 'stageNode',
      position,
      data: {
        label: nextStageName(),
        blueprintPath: '',
        status: 'idle',
      } satisfies StageNodeData,
    };
    setNodes(nds => [...nds, newNode]);
    setSelectedNodeId(id);
  }, [setNodes, screenToFlowPosition, nextStageName]);

  // Drag over — allow drop
  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'move';
  }, []);

  // Drop from palette — create node at drop position
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    const raw = e.dataTransfer.getData('application/reactflow');
    if (!raw) return;
    try {
      const dragData: PaletteDragData = JSON.parse(raw);
      if (dragData.type !== 'stageNode') return;
      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY });
      const id = `stage-${Date.now()}`;
      const newNode: Node = {
        id,
        type: 'stageNode',
        position,
        data: {
          label: dragData.label || nextStageName(),
          blueprintPath: dragData.blueprintPath,
          status: 'idle',
        } satisfies StageNodeData,
      };
      setNodes(nds => [...nds, newNode]);
      setSelectedNodeId(id);
    } catch { /* ignore invalid drag data */ }
  }, [setNodes, screenToFlowPosition, nextStageName]);

  // Save to current file
  const handleSave = useCallback(async () => {
    setSaving(true);
    try {
      const yaml = serializeCanvasToYaml(pipelineName, nodes, edges);
      await saveBlueprintFile(path, yaml);
      toast.success('Pipeline saved');
    } catch (err) {
      toast.error(`Save failed: ${err}`);
    } finally {
      setSaving(false);
    }
  }, [pipelineName, nodes, edges, path]);

  // Save As — pick directory, create new file
  const handleSaveAs = useCallback(async () => {
    const dir = await openDirectoryDialog();
    if (!dir) return;
    setSaving(true);
    try {
      const yaml = serializeCanvasToYaml(pipelineName, nodes, edges);
      const slug = pipelineName.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '');
      const filename = `${slug || 'pipeline'}.yaml`;
      await createBlueprintFile(dir, filename, yaml);
      toast.success(`Saved as ${filename}`);
    } catch (err) {
      toast.error(`Save As failed: ${err}`);
    } finally {
      setSaving(false);
    }
  }, [pipelineName, nodes, edges]);

  // Copy YAML to clipboard
  const handleCopyYaml = useCallback(() => {
    navigator.clipboard.writeText(yamlPreview).then(() => toast.success('Copied to clipboard'));
  }, [yamlPreview]);

  // ── Execution: run pipeline and poll for status ─────────────────

  // Map stage status from API to StageNodeData status
  const mapStageStatus = (s: string): 'idle' | 'running' | 'success' | 'failed' | 'skipped' => {
    if (s === 'running') return 'running';
    if (s === 'success') return 'success';
    if (s === 'failed') return 'failed';
    if (s === 'skipped') return 'skipped';
    return 'idle';
  };

  // Update node statuses from pipeline stage data
  const applyStageStatuses = useCallback((stages: PipelineStage[]) => {
    const statusMap = new Map<string, string>();
    stages.forEach(s => statusMap.set(s.stage_name, s.status));

    setNodes(nds => nds.map(n => {
      const d = n.data as StageNodeData;
      const apiStatus = statusMap.get(d.label);
      const newStatus = apiStatus ? mapStageStatus(apiStatus) : d.status;
      if (newStatus === d.status) return n;
      return { ...n, data: { ...n.data, status: newStatus } };
    }));

    // Update edge statuses based on source node
    setEdges(eds => eds.map(e => {
      const sourceStatus = statusMap.get(
        (nodes.find(n => n.id === e.source)?.data as StageNodeData)?.label ?? ''
      );
      const edgeStatus = sourceStatus ? mapStageStatus(sourceStatus) : 'idle';
      const currentStatus = (e.data as StageEdgeData | undefined)?.status ?? 'idle';
      if (edgeStatus === currentStatus) return e;
      return { ...e, data: { ...e.data, status: edgeStatus } };
    }));
  }, [setNodes, setEdges, nodes]);

  // Run pipeline
  const handleRun = useCallback(async () => {
    if (daemonStatus !== 'running') return;
    // Save first to ensure the file on disk matches the canvas
    try {
      const yaml = serializeCanvasToYaml(pipelineName, nodes, edges);
      await saveBlueprintFile(path, yaml);
    } catch (err) {
      toast.error(`Save before run failed: ${err}`);
      return;
    }

    setRunning(true);
    setShowRunLog(true);
    setRunEvents([]);
    // Reset all nodes to idle
    setNodes(nds => nds.map(n => ({ ...n, data: { ...n.data, status: 'idle' } })));

    try {
      const pipeline = await enqueuePipeline({ pipeline_path: path, workspace_id: workspaceId });
      setRunId(pipeline.id);
      toast.success('Pipeline started');
    } catch (err) {
      toast.error(`Failed to start pipeline: ${err}`);
      setRunning(false);
    }
  }, [daemonStatus, pipelineName, nodes, edges, path, workspaceId, setNodes]);

  // Poll stage statuses + run events while running
  useEffect(() => {
    if (!runId || !running) return;
    let cancelled = false;

    const poll = async () => {
      try {
        // Poll stages
        const res = await getPipelineStages(runId);
        if (cancelled) return;
        const stages = res.items ?? [];
        applyStageStatuses(stages);

        // Poll run events (for the individual stage runs)
        // Use the pipeline run's stage run_ids to get events
        const allEvents: RunEvent[] = [];
        for (const stage of stages) {
          if (stage.run_id) {
            try {
              const evtRes = await listRunEvents(stage.run_id, { limit: 50 });
              if (cancelled) return;
              allEvents.push(...(evtRes.items ?? []));
            } catch { /* stage may not have events yet */ }
          }
        }
        allEvents.sort((a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime());
        setRunEvents(allEvents);

        // Auto-scroll run log
        if (runLogRef.current) {
          runLogRef.current.scrollTop = runLogRef.current.scrollHeight;
        }

        // Check if pipeline is done (all stages terminal)
        const allTerminal = stages.length > 0 && stages.every(s =>
          s.status === 'success' || s.status === 'failed' || s.status === 'skipped'
        );
        if (allTerminal) {
          setRunning(false);
          const failed = stages.filter(s => s.status === 'failed');
          if (failed.length > 0) {
            toast.error(`Pipeline completed — ${failed.length} stage(s) failed`);
          } else {
            toast.success('Pipeline completed successfully');
          }
        }
      } catch { /* network error — keep polling */ }
    };

    poll();
    const timer = setInterval(poll, 2000);
    return () => { cancelled = true; clearInterval(timer); };
  }, [runId, running, applyStageStatuses]);

  useEffect(() => {
    // New blank pipeline — skip file load
    if (path === '__new__') {
      setPipelineName('New Pipeline');
      setNodes([]);
      setEdges([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    readBlueprintFile(path)
      .then(content => {
        const parsed = parsePipelineForFlow(content);
        if (!parsed) throw new Error('Failed to parse pipeline YAML');
        setPipelineName(parsed.name || basename(path));
        const { nodes: n, edges: e } = pipelineToFlow(parsed);
        setNodes(n);
        setEdges(e);
      })
      .catch(err => {
        setError(err.message || 'Failed to load pipeline');
        toast.error('Failed to load pipeline');
      })
      .finally(() => setLoading(false));
  }, [path]); // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Loading...</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Error</span>
        </div>
        <div style={{ color: 'var(--status-failed)', fontSize: 'var(--text-md)', padding: '0.75rem', background: 'var(--status-failed-bg)', borderRadius: '4px', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
          {error}
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Header */}
      <div className="page-header" style={{ gap: '0.5rem', flexShrink: 0 }}>
        <span className="page-title">Flow Builder</span>
        <span className="mono" style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>
          {pipelineName}
        </span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: '0.35rem' }}>
          <button
            className="btn btn-ghost"
            onClick={() => setShowYamlPreview(!showYamlPreview)}
            style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}
            title="YAML Preview"
          >
            <Code size={12} /> YAML
          </button>
          <button
            className="btn btn-ghost"
            onClick={handleSaveAs}
            disabled={saving}
            style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}
          >
            Save As
          </button>
          <button
            className="btn btn-primary"
            onClick={handleSave}
            disabled={saving}
            style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}
          >
            <Save size={12} /> {saving ? 'Saving...' : 'Save'}
          </button>
          <button
            className="btn btn-primary"
            onClick={handleRun}
            disabled={daemonStatus !== 'running' || running}
            style={{
              display: 'flex', alignItems: 'center', gap: '0.3rem',
              borderColor: running ? 'rgba(var(--warn) / 0.5)' : 'rgba(59, 130, 246, 0.5)',
              color: running ? 'var(--status-running)' : 'var(--status-success)',
            }}
          >
            <Play size={12} /> {running ? 'Running...' : 'Run'}
          </button>
        </div>
      </div>

      {/* Palette + Canvas + Property Panel */}
      <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
        <NodePalette onAddBlankNode={handleAddBlankNode} />

        <div style={{ flex: 1, borderRadius: 'var(--radius-md)', overflow: 'hidden', border: '1px solid var(--border-default)' }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onSelectionChange={onSelectionChange}
            onDragOver={onDragOver}
            onDrop={onDrop}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            defaultEdgeOptions={{ type: 'conditional' }}
            deleteKeyCode={['Backspace', 'Delete']}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            minZoom={0.2}
            maxZoom={2}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="var(--border-default)" />
            <MiniMap
              nodeColor="var(--border-default)"
              maskColor="rgba(0, 0, 0, 0.6)"
              style={{ borderRadius: 'var(--radius-md)', border: '1px solid var(--border-default)' }}
            />
            <Controls
              showInteractive={false}
              style={{ borderRadius: 'var(--radius-md)', border: '1px solid var(--border-default)' }}
            />
          </ReactFlow>
        </div>

        {/* Property Panel — shown when a node is selected */}
        {selectedNode && selectedData && (
          <StagePropertyPanel
            nodeId={selectedNode.id}
            data={selectedData}
            onUpdate={handleUpdateNodeData}
            onDelete={handleDeleteNode}
            onClose={() => setSelectedNodeId(null)}
          />
        )}
      </div>

      {/* Run Log Panel — collapsible bottom panel */}
      {(showRunLog || runEvents.length > 0) && (
        <div style={{
          flexShrink: 0,
          borderTop: '1px solid var(--border-default)',
          background: 'var(--bg-surface)',
          display: 'flex',
          flexDirection: 'column',
          maxHeight: showRunLog ? '200px' : '28px',
          transition: 'max-height 0.2s ease',
          overflow: 'hidden',
        }}>
          {/* Toggle bar */}
          <button
            onClick={() => setShowRunLog(!showRunLog)}
            style={{
              display: 'flex', alignItems: 'center', gap: '0.4rem',
              padding: '0.3rem 0.75rem', background: 'none', border: 'none',
              color: 'var(--text-tertiary)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
              fontSize: 'var(--text-xs)', textTransform: 'uppercase', letterSpacing: '0.1em',
              flexShrink: 0,
            }}
          >
            {showRunLog ? <ChevronDown size={12} /> : <ChevronUp size={12} />}
            Run Log ({runEvents.length} events)
            {running && (
              <span className="pulse-running" style={{ color: 'var(--status-running)', marginLeft: '0.3rem' }}>
                running
              </span>
            )}
          </button>

          {/* Log content */}
          {showRunLog && (
            <div
              ref={runLogRef}
              style={{
                flex: 1, overflow: 'auto', padding: '0 0.75rem 0.5rem',
                fontFamily: "'Courier New', monospace", fontSize: 'var(--text-sm)',
                lineHeight: '1.5', color: 'var(--text-tertiary)',
              }}
            >
              {runEvents.length === 0 ? (
                <div style={{ padding: '0.5rem 0', fontStyle: 'italic' }}>
                  {running ? 'Waiting for events...' : 'No events recorded'}
                </div>
              ) : (
                runEvents.map((evt, i) => (
                  <div key={i} className="event-row">
                    <span className="event-type" style={{
                      color: evt.event_type === 'error' ? 'var(--status-failed)'
                        : evt.event_type === 'step_end' ? 'var(--status-success)'
                        : undefined,
                    }}>
                      {evt.step_name ?? evt.event_type}
                    </span>
                    <span className="event-msg">{evt.message}</span>
                  </div>
                ))
              )}
            </div>
          )}
        </div>
      )}

      {/* YAML Preview Modal */}
      {showYamlPreview && (
        <div className="hud-modal-overlay" onClick={() => setShowYamlPreview(false)}>
          <div
            className="hud-modal"
            onClick={e => e.stopPropagation()}
            style={{ maxWidth: '900px', width: '90vw', maxHeight: '85vh', display: 'flex', flexDirection: 'column' }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
              <span style={{ fontSize: 'var(--text-sm)', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'var(--text-tertiary)' }}>
                Pipeline YAML Preview (live)
              </span>
              <div style={{ display: 'flex', gap: '0.4rem' }}>
                <button className="btn btn-ghost" onClick={handleCopyYaml} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: 'var(--text-sm)' }}>
                  <Copy size={12} /> Copy
                </button>
                <button className="btn btn-ghost" onClick={() => setShowYamlPreview(false)} style={{ padding: '0.25rem' }}>
                  <X size={15} />
                </button>
              </div>
            </div>
            <pre style={{
              flex: 1, overflow: 'auto', padding: '0.75rem',
              background: 'var(--bg-base)', borderRadius: '4px',
              border: '1px solid var(--border-default)',
              fontSize: 'var(--text-sm)', lineHeight: '1.6', whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}>
              {yamlPreview}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}

function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}
