import { useState, useEffect, useCallback, useMemo } from 'react';
import { toast } from 'sonner';
import { ChevronLeft, Save, Copy, Code, Play, ChevronDown, ChevronUp } from 'lucide-react';
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
import { StageNode, type StageNodeData } from '@/components/flow/StageNode';
import { ConditionalEdge, type StageEdgeData } from '@/components/flow/ConditionalEdge';
import { StagePropertyPanel } from '@/components/flow/StagePropertyPanel';
import { NodePalette, type PaletteDragData } from '@/components/flow/NodePalette';
import { FlowBuilderLanding } from '@/components/flow/FlowBuilderLanding';
import { readBlueprintFile, saveBlueprintFile, createBlueprintFile, openDirectoryDialog } from '@/api/client';
import { Spinner } from '@/components/ui/Spinner';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { useDaemon } from '@/contexts/DaemonContext';
import { useNavigation } from '@/contexts/NavigationContext';
import { cn } from '@/lib/utils';
import { parsePipelineForFlow, pipelineToFlow, serializeCanvasToYaml } from '@/utils/flowSerializer';
import { useFlowExecution } from '@/hooks/useFlowExecution';

interface FlowBuilderPageProps {
  path?: string | null;
  onBack: () => void;
  daemonStatus?: string;
  workspaceId?: string;
}

// ── Component ─────────────────────────────────────────────────────────

const nodeTypes = { stageNode: StageNode };
const edgeTypes = { conditional: ConditionalEdge };

// Wrapper: landing page when no path, or canvas when path is set
export function FlowBuilderPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const pathFromNav = nav.selectedPipelinePath;
  const [activePath, setActivePath] = useState<string | null>(pathFromNav ?? null);

  // Sync from parent when path prop changes
  useEffect(() => {
    if (pathFromNav) setActivePath(pathFromNav);
  }, [pathFromNav]);

  if (!activePath) {
    return <FlowBuilderLanding onOpen={setActivePath} />;
  }

  return (
    <ReactFlowProvider>
      <FlowBuilderInner path={activePath} onBack={() => setActivePath(null)} daemonStatus={daemon.status} workspaceId={daemon.workspaceId} />
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

  // ── Execution state (extracted hook) ─────────────────────────────
  const {
    running,
    runEvents,
    showRunLog,
    setShowRunLog,
    runLogRef,
    handleRun,
  } = useFlowExecution({
    path,
    daemonStatus,
    workspaceId,
    pipelineName,
    nodes,
    edges,
    setNodes,
    setEdges,
  });

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

  // Load pipeline from file
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
      <div className="flex flex-col h-full gap-3">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={onBack}>
            <ChevronLeft size={13} /> Back
          </Button>
          <span className="text-xl font-semibold text-foreground tracking-tight">Loading...</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col h-full gap-3">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={onBack}>
            <ChevronLeft size={13} /> Back
          </Button>
          <span className="text-xl font-semibold text-foreground tracking-tight">Error</span>
        </div>
        <div className="text-red-400 text-sm p-3 bg-red-400/10 rounded border border-red-400/30">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 gap-2 shrink-0">
        <span className="text-xl font-semibold text-foreground tracking-tight">Flow Builder</span>
        <span className="font-mono text-sm text-muted-foreground">
          {pipelineName}
        </span>
        <div className="ml-auto flex gap-1">
          <Button
            variant="ghost"
            onClick={() => setShowYamlPreview(!showYamlPreview)}
            title="YAML Preview"
          >
            <Code size={12} /> YAML
          </Button>
          <Button
            variant="ghost"
            onClick={handleSaveAs}
            disabled={saving}
          >
            Save As
          </Button>
          <Button
            onClick={handleSave}
            disabled={saving}
            className="border-blue-500/50 text-blue-400"
          >
            <Save size={12} /> {saving ? 'Saving...' : 'Save'}
          </Button>
          <Button
            onClick={handleRun}
            disabled={daemonStatus !== 'running' || running}
            className={cn(
              running ? 'border-amber-500/50 text-amber-400' : 'border-blue-500/50 text-blue-400'
            )}
          >
            <Play size={12} /> {running ? 'Running...' : 'Run'}
          </Button>
        </div>
      </div>

      {/* Palette + Canvas + Property Panel */}
      <div className="flex-1 flex overflow-hidden">
        <NodePalette onAddBlankNode={handleAddBlankNode} />

        <div className="flex-1 rounded-lg overflow-hidden border border-border">
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
              className="rounded-lg border border-border"
            />
            <Controls
              showInteractive={false}
              className="rounded-lg border border-border"
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
        <div className={cn(
          'shrink-0 border-t border-border bg-card flex flex-col overflow-hidden transition-[max-height] duration-200 ease-in-out',
          showRunLog ? 'max-h-[200px]' : 'max-h-7'
        )}>
          {/* Toggle bar */}
          <button
            onClick={() => setShowRunLog(!showRunLog)}
            className="flex items-center gap-1.5 px-3 py-1 bg-transparent border-none text-muted-foreground cursor-pointer font-mono text-xs uppercase tracking-[0.1em] shrink-0"
          >
            {showRunLog ? <ChevronDown size={12} /> : <ChevronUp size={12} />}
            Run Log ({runEvents.length} events)
            {running && (
              <span className="pulse-running text-amber-400 ml-1">
                running
              </span>
            )}
          </button>

          {/* Log content */}
          {showRunLog && (
            <div
              ref={runLogRef as React.RefObject<HTMLDivElement>}
              className="flex-1 overflow-auto px-3 pb-2 font-mono text-sm leading-relaxed text-muted-foreground"
            >
              {runEvents.length === 0 ? (
                <div className="py-2 italic">
                  {running ? 'Waiting for events...' : 'No events recorded'}
                </div>
              ) : (
                runEvents.map((evt, i) => (
                  <div key={i} className="event-row">
                    <span className={cn(
                      'event-type',
                      evt.event_type === 'error' && 'text-red-400',
                      evt.event_type === 'step_end' && 'text-blue-400',
                    )}>
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
      <Dialog open={showYamlPreview} onOpenChange={(open) => { if (!open) setShowYamlPreview(false); }}>
        <DialogContent className="sm:max-w-[900px] w-[90vw] max-h-[85vh] flex flex-col">
          <DialogHeader>
            <div className="flex items-center justify-between">
              <DialogTitle className="text-sm uppercase tracking-[0.1em] text-muted-foreground">
                Pipeline YAML Preview (live)
              </DialogTitle>
              <div className="flex gap-1.5">
                <Button variant="ghost" size="sm" onClick={handleCopyYaml}>
                  <Copy size={12} /> Copy
                </Button>
              </div>
            </div>
          </DialogHeader>
          <pre className="flex-1 overflow-auto p-3 bg-background rounded border border-border text-sm leading-relaxed whitespace-pre-wrap break-all">
            {yamlPreview}
          </pre>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}
