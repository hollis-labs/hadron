import { useState } from 'react';
import { BlueprintInput, FileEntry } from '../../api/types';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface RunInputsModalProps {
  entry: FileEntry;
  inputs: BlueprintInput[];
  onConfirm: (values: Record<string, unknown>, dryRun: boolean) => void;
  onCancel: () => void;
}

function initValues(inputs: BlueprintInput[]): Record<string, string> {
  const vals: Record<string, string> = {};
  for (const inp of inputs) {
    vals[inp.name] = inp.default ?? '';
  }
  return vals;
}

export function RunInputsModal({ entry, inputs, onConfirm, onCancel }: RunInputsModalProps) {
  const [values, setValues] = useState<Record<string, string>>(() => initValues(inputs));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [dryRun, setDryRun] = useState(false);

  const set = (name: string, value: string) => {
    setValues(prev => ({ ...prev, [name]: value }));
    setErrors(prev => { const next = { ...prev }; delete next[name]; return next; });
  };

  const handleSubmit = () => {
    const newErrors: Record<string, string> = {};
    for (const inp of inputs) {
      if (inp.required && !values[inp.name]) {
        newErrors[inp.name] = 'Required';
      }
    }
    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    const coerced: Record<string, unknown> = {};
    for (const inp of inputs) {
      const raw = values[inp.name];
      if (raw === '' || raw === undefined) continue;
      switch (inp.type) {
        case 'number':
          coerced[inp.name] = parseFloat(raw);
          break;
        case 'boolean':
          coerced[inp.name] = raw === 'true';
          break;
        case 'array':
          coerced[inp.name] = raw.split(',').map(s => s.trim()).filter(Boolean);
          break;
        default:
          coerced[inp.name] = raw;
      }
    }
    onConfirm(coerced, dryRun);
  };

  return (
    <Dialog open onOpenChange={(open) => { if (!open) onCancel(); }}>
      <DialogContent className="sm:max-w-md">
        {/* Header */}
        <DialogHeader>
          <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', letterSpacing: '0.04em', textTransform: 'uppercase' }}>Run with Inputs</div>
          <DialogTitle className="font-mono text-sm text-foreground mt-0.5">{entry.name}</DialogTitle>
        </DialogHeader>

        {/* Fields */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
          {inputs.map(inp => (
            <div key={inp.name}>
              <label style={{ display: 'flex', alignItems: 'baseline', gap: 'var(--space-2)', fontSize: 'var(--text-sm)', color: 'var(--text-primary)', marginBottom: 'var(--space-1)' }}>
                <span className="font-mono">{inp.label || inp.name}</span>
                {inp.required && <span style={{ color: 'var(--status-failed)', fontSize: 'var(--text-xs)' }}>required</span>}
                {inp.description && (
                  <span style={{ color: 'var(--text-tertiary)', fontSize: 'var(--text-xs)' }}>— {inp.description}</span>
                )}
              </label>

              {inp.type === 'boolean' ? (
                <select
                  className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
                  value={values[inp.name]}
                  onChange={e => set(inp.name, e.target.value)}
                >
                  <option value="">— select —</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              ) : inp.enum && inp.enum.length > 0 ? (
                <select
                  className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
                  value={values[inp.name]}
                  onChange={e => set(inp.name, e.target.value)}
                >
                  <option value="">— select —</option>
                  {inp.enum.map(opt => (
                    <option key={opt} value={opt}>{opt}</option>
                  ))}
                </select>
              ) : inp.type === 'number' ? (
                <Input
                  type="number"
                  value={values[inp.name]}
                  onChange={e => set(inp.name, e.target.value)}
                  placeholder={inp.default || '0'}
                />
              ) : inp.type === 'array' ? (
                <Input
                  type="text"
                  value={values[inp.name]}
                  onChange={e => set(inp.name, e.target.value)}
                  placeholder="comma-separated values"
                />
              ) : (
                <Input
                  type="text"
                  value={values[inp.name]}
                  onChange={e => set(inp.name, e.target.value)}
                  placeholder={inp.default || ''}
                />
              )}

              {errors[inp.name] && (
                <div style={{ fontSize: 'var(--text-xs)', color: 'var(--status-failed)', marginTop: '4px' }}>{errors[inp.name]}</div>
              )}
            </div>
          ))}

          {/* Dry run toggle */}
          <label style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', cursor: 'pointer', marginTop: 'var(--space-2)' }}>
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => setDryRun(e.target.checked)}
              style={{ accentColor: 'var(--accent)' }}
            />
            <span style={{ fontSize: 'var(--text-sm)', fontWeight: 500 }}>Dry run</span>
            <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)' }}>
              (validate without executing)
            </span>
          </label>
        </div>

        {/* Footer */}
        <DialogFooter>
          <Button variant="ghost" onClick={onCancel}>Cancel</Button>
          <Button onClick={handleSubmit}>{dryRun ? 'Dry Run' : 'Run'}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
