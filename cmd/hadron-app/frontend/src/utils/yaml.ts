import { unquote } from './string';

// ── Generic parsed pipeline shape ────────────────────────────────────

export interface ParsedPipelineStage {
  name: string;
  blueprint_path: string;
  condition: string;
  inputs: Record<string, string>;
  depends_on?: string[];
  position?: { x: number; y: number };
  outputs?: string[];
}

export interface ParsedPipelineRaw {
  name: string;
  stop_on_fail: boolean;
  stages: ParsedPipelineStage[];
  inputs: Record<string, string>;
}

/**
 * Indentation-aware YAML parser for pipeline spec files.
 * Returns a generic shape that callers can map to their specific types.
 */
export function parsePipelineYaml(content: string): ParsedPipelineRaw | null {
  try {
    const result: ParsedPipelineRaw = { name: '', stop_on_fail: true, stages: [], inputs: {} };
    const lines = content.split('\n');

    type Section = 'none' | 'meta' | 'stages' | 'inputs';
    let section: Section = 'none';
    let currentStage: ParsedPipelineStage | null = null;
    let inStageInputs = false;
    let inDependsOn = false;
    let inPosition = false;
    let inOutputs = false;

    for (const line of lines) {
      if (line.trim() === '' || line.trim().startsWith('#')) continue;
      const indent = line.search(/\S/);
      const trimmed = line.trim();

      // Top-level keys (indent 0)
      if (indent === 0) {
        if (currentStage) { result.stages.push(currentStage); currentStage = null; }
        inStageInputs = false; inDependsOn = false; inPosition = false; inOutputs = false;

        if (trimmed === 'meta:') { section = 'meta'; continue; }
        if (trimmed.startsWith('stop_on_fail:')) {
          section = 'none';
          result.stop_on_fail = trimmed.includes('true');
          continue;
        }
        if (trimmed === 'stages:') { section = 'stages'; continue; }
        if (trimmed === 'inputs:') { section = 'inputs'; continue; }
        section = 'none'; continue;
      }

      // meta: block (indent 2+)
      if (section === 'meta' && indent >= 2) {
        if (trimmed.startsWith('name:')) result.name = unquote(trimmed.slice(5));
        continue;
      }

      // stages: block
      if (section === 'stages') {
        // New stage item: "  - name: ..."
        if (trimmed.startsWith('- ')) {
          if (currentStage) result.stages.push(currentStage);
          inStageInputs = false; inDependsOn = false; inPosition = false; inOutputs = false;
          currentStage = { name: '', blueprint_path: '', condition: '', inputs: {} };
          const after = trimmed.slice(2).trim();
          if (after.startsWith('name:')) currentStage.name = unquote(after.slice(5));
          continue;
        }

        // Stage properties (indent 4+)
        if (currentStage && indent >= 4) {
          // Sub-blocks at indent 6+
          if (inStageInputs && indent >= 6) {
            const ci = trimmed.indexOf(':');
            if (ci > 0) currentStage.inputs[trimmed.slice(0, ci).trim()] = unquote(trimmed.slice(ci + 1));
            continue;
          }
          if (inDependsOn && indent >= 6) {
            if (trimmed.startsWith('- ')) {
              if (!currentStage.depends_on) currentStage.depends_on = [];
              currentStage.depends_on.push(unquote(trimmed.slice(2)));
            }
            continue;
          }
          if (inPosition && indent >= 6) {
            if (!currentStage.position) currentStage.position = { x: 0, y: 0 };
            if (trimmed.startsWith('x:')) currentStage.position.x = parseFloat(unquote(trimmed.slice(2))) || 0;
            if (trimmed.startsWith('y:')) currentStage.position.y = parseFloat(unquote(trimmed.slice(2))) || 0;
            continue;
          }
          if (inOutputs && indent >= 6) {
            if (trimmed.startsWith('- ')) {
              if (!currentStage.outputs) currentStage.outputs = [];
              currentStage.outputs.push(unquote(trimmed.slice(2)));
            }
            continue;
          }

          // Reset sub-block flags at indent 4
          inStageInputs = false; inDependsOn = false; inPosition = false; inOutputs = false;

          if (trimmed.startsWith('blueprint_path:')) currentStage.blueprint_path = unquote(trimmed.slice(15));
          else if (trimmed.startsWith('name:')) currentStage.name = unquote(trimmed.slice(5));
          else if (trimmed.startsWith('if:')) currentStage.condition = unquote(trimmed.slice(3));
          else if (trimmed === 'inputs:') inStageInputs = true;
          else if (trimmed === 'depends_on:') inDependsOn = true;
          else if (trimmed === 'position:') inPosition = true;
          else if (trimmed === 'outputs:') inOutputs = true;
          continue;
        }
        continue;
      }

      // Top-level inputs: block (indent 2+)
      if (section === 'inputs' && indent >= 2) {
        const ci = trimmed.indexOf(':');
        if (ci > 0) {
          result.inputs[trimmed.slice(0, ci).trim()] = unquote(trimmed.slice(ci + 1));
        }
        continue;
      }
    }

    if (currentStage) result.stages.push(currentStage);
    return result;
  } catch {
    return null;
  }
}
