import { parsePipelineYaml } from '@/utils/yaml';

// Pipeline editor types
export interface PipelineStageForm {
  name: string;
  blueprint_path: string;
  inputs: Record<string, string>;
  condition: string; // if: field
}

export interface PipelineForm {
  name: string;
  stop_on_fail: boolean;
  stages: PipelineStageForm[];
  inputs: Record<string, string>; // top-level pipeline inputs
}

export const EMPTY_STAGE: PipelineStageForm = { name: '', blueprint_path: '', inputs: {}, condition: '' };
export const EMPTY_FORM: PipelineForm = { name: '', stop_on_fail: true, stages: [{ ...EMPTY_STAGE }], inputs: {} };

// Simple YAML serializer for pipeline spec
export function serializePipeline(form: PipelineForm): string {
  let yaml = `meta:\n  name: "${form.name}"\n\n`;
  yaml += `stop_on_fail: ${form.stop_on_fail}\n\n`;
  yaml += `stages:\n`;
  for (const stage of form.stages) {
    yaml += `  - name: "${stage.name}"\n`;
    yaml += `    blueprint_path: "${stage.blueprint_path}"\n`;
    if (stage.condition.trim()) {
      yaml += `    if: "${stage.condition}"\n`;
    }
    const inputKeys = Object.keys(stage.inputs);
    if (inputKeys.length > 0) {
      yaml += `    inputs:\n`;
      for (const key of inputKeys) {
        yaml += `      ${key}: "${stage.inputs[key]}"\n`;
      }
    }
    yaml += `\n`;
  }
  // Top-level pipeline inputs
  const topInputKeys = Object.keys(form.inputs);
  if (topInputKeys.length > 0) {
    yaml += `inputs:\n`;
    for (const key of topInputKeys) {
      yaml += `  ${key}: "${form.inputs[key]}"\n`;
    }
  }
  return yaml.trimEnd() + '\n';
}

export function parsePipelineToForm(content: string): PipelineForm | null {
  const raw = parsePipelineYaml(content);
  if (!raw) return null;
  const form: PipelineForm = {
    name: raw.name,
    stop_on_fail: raw.stop_on_fail,
    stages: raw.stages.map(s => ({
      name: s.name,
      blueprint_path: s.blueprint_path,
      inputs: s.inputs,
      condition: s.condition,
    })),
    inputs: raw.inputs,
  };
  if (form.stages.length === 0) form.stages.push({ ...EMPTY_STAGE });
  return form;
}
