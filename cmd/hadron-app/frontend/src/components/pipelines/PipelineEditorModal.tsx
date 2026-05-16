import { useState } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { FolderOpen, X, Plus, ArrowUp, ArrowDown, Layers } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { createBlueprintFile, saveBlueprintFile, selectBlueprintFile } from '@/api/client';
import { cn } from '@/lib/utils';
import type { PipelineForm, PipelineStageForm } from '@/utils/pipelineYaml';
import { EMPTY_STAGE, serializePipeline } from '@/utils/pipelineYaml';

interface PipelineEditorModalProps {
  mode: 'create' | 'edit' | null;
  initialForm: PipelineForm;
  editorPath: string | null;
  currentDir: string;
  onClose: () => void;
  onSaved: () => void;
}

export function PipelineEditorModal({ mode, initialForm, editorPath, currentDir, onClose, onSaved }: PipelineEditorModalProps) {
  const [form, setForm] = useState<PipelineForm>(initialForm);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Stage management helpers
  const addStage = () => {
    setForm(f => ({ ...f, stages: [...f.stages, { ...EMPTY_STAGE }] }));
  };

  const removeStage = (index: number) => {
    setForm(f => ({ ...f, stages: f.stages.filter((_, i) => i !== index) }));
  };

  const updateStage = (index: number, field: keyof PipelineStageForm, value: string) => {
    setForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => i === index ? { ...s, [field]: value } : s),
    }));
  };

  const moveStage = (index: number, direction: -1 | 1) => {
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= form.stages.length) return;
    setForm(f => {
      const stages = [...f.stages];
      [stages[index], stages[newIndex]] = [stages[newIndex], stages[index]];
      return { ...f, stages };
    });
  };

  const browseStagePath = async (index: number) => {
    const path = await selectBlueprintFile();
    if (path) updateStage(index, 'blueprint_path', path);
  };

  // Stage input management
  const addStageInput = (stageIndex: number) => {
    setForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        let k = '';
        let n = 0;
        while (k in s.inputs) { n++; k = `key${n}`; }
        return { ...s, inputs: { ...s.inputs, [k]: '' } };
      }),
    }));
  };

  const updateStageInput = (stageIndex: number, oldKey: string, newKey: string, value: string) => {
    setForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        const entries = Object.entries(s.inputs).map(([k, v]) =>
          k === oldKey ? [newKey, value] : [k, v]
        );
        return { ...s, inputs: Object.fromEntries(entries) };
      }),
    }));
  };

  const removeStageInput = (stageIndex: number, key: string) => {
    setForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        const rest = { ...s.inputs };
        delete rest[key];
        return { ...s, inputs: rest };
      }),
    }));
  };

  // Top-level pipeline input management
  const addPipelineInput = () => {
    setForm(f => {
      let k = '';
      let n = 0;
      while (k in f.inputs) { n++; k = `key${n}`; }
      return { ...f, inputs: { ...f.inputs, [k]: '' } };
    });
  };

  const updatePipelineInput = (oldKey: string, newKey: string, value: string) => {
    setForm(f => {
      const entries = Object.entries(f.inputs).map(([k, v]) =>
        k === oldKey ? [newKey, value] : [k, v]
      );
      return { ...f, inputs: Object.fromEntries(entries) };
    });
  };

  const removePipelineInput = (key: string) => {
    setForm(f => {
      const rest = { ...f.inputs };
      delete rest[key];
      return { ...f, inputs: rest };
    });
  };

  const handleSave = async () => {
    if (!form.name.trim()) { setError('Pipeline name is required'); return; }
    const validStages = form.stages.filter(s => s.name.trim() && s.blueprint_path.trim());
    if (validStages.length === 0) { setError('At least one stage with name and blueprint path is required'); return; }
    setError(null);
    setSaving(true);
    try {
      const yaml = serializePipeline({ ...form, stages: validStages });
      if (mode === 'create') {
        const slug = form.name.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '');
        const filename = `${slug}.yaml`;
        await createBlueprintFile(currentDir, filename, yaml);
        toast.success(`Pipeline "${filename}" created`);
      } else if (editorPath) {
        await saveBlueprintFile(editorPath, yaml);
        toast.success('Pipeline updated');
      }
      onClose();
      onSaved();
    } catch (err) {
      toast.error(`Save failed: ${err}`);
      setError(String(err));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={!!mode} onOpenChange={(open) => { if (!open) onClose(); }}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <span className="text-blue-400"><Layers size={16} /></span>
            {mode === 'create' ? 'New Pipeline' : 'Edit Pipeline'}
          </DialogTitle>
        </DialogHeader>

        {/* Pipeline name */}
        <div className="mb-3">
          <Label>Pipeline Name</Label>
          <Input
            placeholder="e.g. deploy-staging"
            value={form.name}
            onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
            className="w-full"
            autoFocus
          />
        </div>

        {/* Stop on fail */}
        <div className="mb-3 flex items-center gap-2">
          <input
            type="checkbox"
            id="pl-stop-on-fail"
            checked={form.stop_on_fail}
            onChange={e => setForm(f => ({ ...f, stop_on_fail: e.target.checked }))}
          />
          <label htmlFor="pl-stop-on-fail" className="text-sm text-foreground cursor-pointer">
            Stop on first failure
          </label>
        </div>

        {/* Stages */}
        <div className="mb-3">
          <div className="flex items-center justify-between mb-1.5">
            <Label className="m-0">Stages</Label>
            <Button
              type="button"
              variant="ghost"
              size="xs"
              onClick={addStage}
            >
              <Plus size={12} /> Add Stage
            </Button>
          </div>

          <div className="flex flex-col gap-2">
            {form.stages.map((stage, i) => (
              <div
                key={i}
                className="p-2 bg-muted rounded border border-border"
              >
                <div className="flex items-center gap-1.5 mb-1.5">
                  <span className="text-xs text-muted-foreground font-semibold w-6 text-center">
                    {i + 1}
                  </span>
                  <Input
                    placeholder="Stage name"
                    value={stage.name}
                    onChange={e => updateStage(i, 'name', e.target.value)}
                    className="flex-1 text-sm px-1.5 py-1"
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => moveStage(i, -1)}
                    disabled={i === 0}
                    title="Move up"
                  >
                    <ArrowUp size={12} />
                  </Button>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => moveStage(i, 1)}
                    disabled={i === form.stages.length - 1}
                    title="Move down"
                  >
                    <ArrowDown size={12} />
                  </Button>
                  {form.stages.length > 1 && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      onClick={() => removeStage(i)}
                      className="text-red-400"
                      title="Remove stage"
                    >
                      <X size={12} />
                    </Button>
                  )}
                </div>
                <div className="flex gap-1 ml-8">
                  <Input
                    placeholder="Blueprint path"
                    value={stage.blueprint_path}
                    onChange={e => updateStage(i, 'blueprint_path', e.target.value)}
                    className="flex-1 text-sm px-1.5 py-1 font-mono"
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => browseStagePath(i)}
                    className="whitespace-nowrap"
                  >
                    <FolderOpen size={11} />
                  </Button>
                </div>
                {/* Condition (if:) */}
                <div className="flex gap-1 ml-8 mt-1">
                  <Input
                    placeholder="if: condition (optional)"
                    value={stage.condition}
                    onChange={e => setForm(f => ({
                      ...f,
                      stages: f.stages.map((s, si) => si === i ? { ...s, condition: e.target.value } : s),
                    }))}
                    className={cn('flex-1 text-sm px-1.5 py-1 font-mono', stage.condition && 'text-amber-400')}
                  />
                </div>
                {/* Stage inputs */}
                <div className="ml-8 mt-1">
                  {Object.keys(stage.inputs).length > 0 && (
                    <div className="flex flex-col gap-0.5 mb-0.5">
                      {Object.entries(stage.inputs).map(([key, val], ki) => (
                        <div key={ki} className="flex gap-1 items-center">
                          <Input
                            placeholder="key"
                            value={key}
                            onChange={e => updateStageInput(i, key, e.target.value, val)}
                            className="w-[35%] text-sm px-1.5 py-1 font-mono"
                          />
                          <span className="text-muted-foreground text-xs">=</span>
                          <Input
                            placeholder="value"
                            value={val}
                            onChange={e => updateStageInput(i, key, key, e.target.value)}
                            className="flex-1 text-sm px-1.5 py-1 font-mono"
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            onClick={() => removeStageInput(i, key)}
                            className="text-red-400"
                          >
                            <X size={10} />
                          </Button>
                        </div>
                      ))}
                    </div>
                  )}
                  <Button
                    type="button"
                    variant="ghost"
                    size="xs"
                    onClick={() => addStageInput(i)}
                    className="text-muted-foreground"
                  >
                    <Plus size={10} /> Input
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Top-level pipeline inputs */}
        <div className="mb-3">
          <div className="flex items-center justify-between mb-1.5">
            <Label className="m-0">Pipeline Inputs</Label>
            <Button
              type="button"
              variant="ghost"
              size="xs"
              onClick={addPipelineInput}
            >
              <Plus size={12} /> Add Input
            </Button>
          </div>
          {Object.keys(form.inputs).length > 0 ? (
            <div className="flex flex-col gap-1">
              {Object.entries(form.inputs).map(([key, val], ki) => (
                <div key={ki} className="flex gap-1 items-center">
                  <Input
                    placeholder="key"
                    value={key}
                    onChange={e => updatePipelineInput(key, e.target.value, val)}
                    className="w-[35%] text-sm px-1.5 py-1 font-mono"
                  />
                  <span className="text-muted-foreground text-sm">=</span>
                  <Input
                    placeholder="value / default"
                    value={val}
                    onChange={e => updatePipelineInput(key, key, e.target.value)}
                    className="flex-1 text-sm px-1.5 py-1 font-mono"
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removePipelineInput(key)}
                    className="text-red-400"
                  >
                    <X size={12} />
                  </Button>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground italic">
              No pipeline-level inputs defined
            </div>
          )}
        </div>

        {/* Error */}
        {error && (
          <div className="text-red-400 text-sm mb-3 px-2.5 py-1.5 bg-red-500/10 rounded border border-red-500/30">
            {error}
          </div>
        )}

        <DialogFooter>
          <Button variant="ghost" onClick={onClose}>
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={saving}
            className="border-blue-500/50 text-blue-400"
          >
            {saving ? 'Saving...' : mode === 'create' ? 'Create' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
