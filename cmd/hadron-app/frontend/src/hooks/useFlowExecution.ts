import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';
import type { Node, Edge } from '@xyflow/react';
import type { StageNodeData } from '@/components/flow/StageNode';
import type { StageEdgeData } from '@/components/flow/ConditionalEdge';
import type { PipelineStage, RunEvent } from '@/api/types';
import { enqueuePipeline, getPipelineStages, listRunEvents, saveBlueprintFile } from '@/api/client';
import { serializeCanvasToYaml } from '@/utils/flowSerializer';

interface UseFlowExecutionArgs {
  path: string;
  daemonStatus: string;
  workspaceId: string;
  pipelineName: string;
  nodes: Node[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
}

export interface FlowExecutionState {
  runId: string | null;
  running: boolean;
  runEvents: RunEvent[];
  showRunLog: boolean;
  setShowRunLog: React.Dispatch<React.SetStateAction<boolean>>;
  runLogRef: React.RefObject<HTMLDivElement | null>;
  handleRun: () => Promise<void>;
}

// Map stage status from API to StageNodeData status
function mapStageStatus(s: string): 'idle' | 'running' | 'success' | 'failed' | 'skipped' {
  if (s === 'running') return 'running';
  if (s === 'success') return 'success';
  if (s === 'failed') return 'failed';
  if (s === 'skipped') return 'skipped';
  return 'idle';
}

export function useFlowExecution({
  path,
  daemonStatus,
  workspaceId,
  pipelineName,
  nodes,
  edges,
  setNodes,
  setEdges,
}: UseFlowExecutionArgs): FlowExecutionState {
  const [runId, setRunId] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [runEvents, setRunEvents] = useState<RunEvent[]>([]);
  const [showRunLog, setShowRunLog] = useState(false);
  const runLogRef = useRef<HTMLDivElement>(null);

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

  return {
    runId,
    running,
    runEvents,
    showRunLog,
    setShowRunLog,
    runLogRef,
    handleRun,
  };
}
