import { useState } from 'react';
import { X, Trash2, Plus, FolderOpen } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import type { StageNodeData } from './StageNode';
import { selectBlueprintFile } from '../../api/client';

interface StagePropertyPanelProps {
  nodeId: string;
  data: StageNodeData;
  onUpdate: (nodeId: string, data: Partial<StageNodeData>) => void;
  onDelete: (nodeId: string) => void;
  onClose: () => void;
}

export function StagePropertyPanel({ nodeId, data, onUpdate, onDelete, onClose }: StagePropertyPanelProps) {
  const [deleteConfirm, setDeleteConfirm] = useState(false);

  // ── Input key-value management ──────────────────────────────────────

  // Inputs stored as Record<string, string> on the node data
  // We use a stable key list derived from Object.keys
  const inputs = (data.inputs ?? {}) as Record<string, string>;
  const inputEntries = Object.entries(inputs);

  const updateInput = (oldKey: string, newKey: string, value: string) => {
    const entries = Object.entries(inputs).map(([k, v]) =>
      k === oldKey ? [newKey, value] : [k, v]
    );
    onUpdate(nodeId, { inputs: Object.fromEntries(entries) });
  };

  const addInput = () => {
    let key = 'key';
    let n = 0;
    while (key in inputs) { n++; key = `key${n}`; }
    onUpdate(nodeId, { inputs: { ...inputs, [key]: '' } });
  };

  const removeInput = (key: string) => {
    const { [key]: _, ...rest } = inputs;
    onUpdate(nodeId, { inputs: rest });
  };

  // ── Output management ──────────────────────────────────────────────

  const outputs = data.outputs ?? [];

  const addOutput = () => {
    let name = 'output';
    let n = 0;
    while (outputs.includes(name)) { n++; name = `output${n}`; }
    onUpdate(nodeId, { outputs: [...outputs, name] });
  };

  const updateOutput = (index: number, value: string) => {
    const next = [...outputs];
    next[index] = value;
    onUpdate(nodeId, { outputs: next });
  };

  const removeOutput = (index: number) => {
    onUpdate(nodeId, { outputs: outputs.filter((_, i) => i !== index) });
  };

  // ── Browse for blueprint path ──────────────────────────────────────

  const browsePath = async () => {
    const path = await selectBlueprintFile();
    if (path) onUpdate(nodeId, { blueprintPath: path });
  };

  return (
    <div className="stage-property-panel">
      {/* Header */}
      <div className="stage-panel-header">
        <span className="stage-panel-title">Stage Properties</span>
        <Button variant="ghost" size="icon-sm" onClick={onClose}>
          <X size={14} />
        </Button>
      </div>

      <div className="stage-panel-body">
        {/* Stage Name */}
        <div className="stage-panel-field">
          <Label>Name</Label>
          <Input
            value={data.label}
            onChange={e => onUpdate(nodeId, { label: e.target.value })}
            style={{ width: '100%', boxSizing: 'border-box', fontSize: 'var(--text-md)', padding: '0.3rem 0.5rem' }}
          />
        </div>

        {/* Blueprint Path */}
        <div className="stage-panel-field">
          <Label>Blueprint Path</Label>
          <div style={{ display: 'flex', gap: '0.3rem' }}>
            <Input
              value={data.blueprintPath}
              onChange={e => onUpdate(nodeId, { blueprintPath: e.target.value })}
              style={{ flex: 1, fontSize: 'var(--text-sm)', padding: '0.3rem 0.5rem', fontFamily: 'monospace' }}
              placeholder="/path/to/blueprint.yaml"
            />
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={browsePath}
              title="Browse"
            >
              <FolderOpen size={12} />
            </Button>
          </div>
        </div>

        {/* Condition */}
        <div className="stage-panel-field">
          <Label>Condition (if:)</Label>
          <Input
            value={data.condition ?? ''}
            onChange={e => onUpdate(nodeId, { condition: e.target.value || undefined })}
            style={{
              width: '100%', boxSizing: 'border-box', fontSize: 'var(--text-sm)',
              padding: '0.3rem 0.5rem', fontFamily: 'monospace',
              color: data.condition ? 'rgb(var(--warn))' : undefined,
            }}
            placeholder="e.g. stages.build.status == 'success'"
          />
        </div>

        {/* Start node toggle */}
        <div className="stage-panel-field" style={{ flexDirection: 'row', alignItems: 'center', gap: '0.5rem' }}>
          <input
            type="checkbox"
            id={`start-${nodeId}`}
            checked={data.isStart ?? false}
            onChange={e => onUpdate(nodeId, { isStart: e.target.checked })}
            style={{ accentColor: 'rgb(var(--ok))' }}
          />
          <label htmlFor={`start-${nodeId}`} style={{ fontSize: 'var(--text-sm)', color: 'rgb(var(--text))', cursor: 'pointer' }}>
            Start node (no input handle)
          </label>
        </div>

        {/* Divider */}
        <div style={{ borderTop: '1px solid rgb(var(--border))', margin: '0.5rem 0' }} />

        {/* Inputs */}
        <div className="stage-panel-field">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Label style={{ margin: 0 }}>Inputs ({inputEntries.length})</Label>
            <Button
              variant="ghost"
              size="xs"
              onClick={addInput}
            >
              <Plus size={10} /> Add
            </Button>
          </div>
          {inputEntries.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.2rem', marginTop: '0.3rem' }}>
              {inputEntries.map(([key, val], i) => (
                <div key={i} style={{ display: 'flex', gap: '0.2rem', alignItems: 'center' }}>
                  <Input
                    value={key}
                    onChange={e => updateInput(key, e.target.value, val)}
                    style={{ width: '38%', fontSize: 'var(--text-xs)', padding: '0.2rem 0.3rem', fontFamily: 'monospace' }}
                    placeholder="key"
                  />
                  <span style={{ color: 'rgb(var(--muted))', fontSize: 'var(--text-xs)' }}>=</span>
                  <Input
                    value={val}
                    onChange={e => updateInput(key, key, e.target.value)}
                    style={{ flex: 1, fontSize: 'var(--text-xs)', padding: '0.2rem 0.3rem', fontFamily: 'monospace' }}
                    placeholder="value"
                  />
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removeInput(key)}
                    style={{ color: 'rgb(var(--danger))' }}
                  >
                    <X size={10} />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Outputs */}
        <div className="stage-panel-field">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Label style={{ margin: 0 }}>Outputs ({outputs.length})</Label>
            <Button
              variant="ghost"
              size="xs"
              onClick={addOutput}
            >
              <Plus size={10} /> Add
            </Button>
          </div>
          {outputs.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.2rem', marginTop: '0.3rem' }}>
              {outputs.map((out, i) => (
                <div key={i} style={{ display: 'flex', gap: '0.2rem', alignItems: 'center' }}>
                  <Input
                    value={out}
                    onChange={e => updateOutput(i, e.target.value)}
                    style={{ flex: 1, fontSize: 'var(--text-xs)', padding: '0.2rem 0.3rem', fontFamily: 'monospace' }}
                    placeholder="output name"
                  />
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    onClick={() => removeOutput(i)}
                    style={{ color: 'rgb(var(--danger))' }}
                  >
                    <X size={10} />
                  </Button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Divider */}
        <div style={{ borderTop: '1px solid rgb(var(--border))', margin: '0.5rem 0' }} />

        {/* Delete */}
        {!deleteConfirm ? (
          <Button
            variant="outline"
            onClick={() => setDeleteConfirm(true)}
            style={{
              width: '100%', justifyContent: 'center',
              borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))',
            }}
          >
            <Trash2 size={12} /> Delete Stage
          </Button>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
            <div style={{ fontSize: 'var(--text-sm)', color: 'rgb(var(--danger))' }}>
              Delete this stage and all connected edges?
            </div>
            <div style={{ display: 'flex', gap: '0.3rem' }}>
              <Button
                variant="ghost"
                onClick={() => setDeleteConfirm(false)}
                style={{ flex: 1 }}
              >
                Cancel
              </Button>
              <Button
                variant="outline"
                onClick={() => { onDelete(nodeId); setDeleteConfirm(false); }}
                style={{ flex: 1, borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' }}
              >
                Confirm
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
