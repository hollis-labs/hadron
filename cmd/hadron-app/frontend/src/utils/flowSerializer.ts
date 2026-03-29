import type { Node, Edge } from '@xyflow/react';
import type { StageNodeData } from '@/components/flow/StageNode';
import type { StageEdgeData } from '@/components/flow/ConditionalEdge';
import { parsePipelineYaml, type ParsedPipelineStage } from '@/utils/yaml';

// ── Types ────────────────────────────────────────────────────────────

export interface ParsedPipeline {
  name: string;
  stages: ParsedPipelineStage[];
}

// ── Constants ────────────────────────────────────────────────────────

export const NODE_WIDTH = 240;
export const NODE_SPACING_Y = 100;

// ── Parse pipeline YAML into flow-friendly shape ─────────────────────

export function parsePipelineForFlow(content: string): ParsedPipeline | null {
  const raw = parsePipelineYaml(content);
  if (!raw) return null;
  return { name: raw.name, stages: raw.stages };
}

// ── Convert parsed pipeline to React Flow nodes/edges ────────────────

export function pipelineToFlow(pipeline: ParsedPipeline): { nodes: Node[]; edges: Edge[] } {
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

// ── Serialize canvas state to pipeline v2 YAML ──────────────────────

export function serializeCanvasToYaml(
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
