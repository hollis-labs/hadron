import { useState } from 'react';
import { BlueprintInput, FileEntry } from '../../api/types';
import { X } from 'lucide-react';
import { Modal } from './Modal';

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
    <Modal onClose={onCancel} maxWidth="480px">
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: 'var(--space-5)', borderBottom: '1px solid var(--border-subtle)' }}>
        <div>
          <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', letterSpacing: '0.04em', textTransform: 'uppercase' }}>Run with Inputs</div>
          <div className="mono" style={{ fontSize: 'var(--text-md)', color: 'var(--text-primary)', marginTop: '2px' }}>{entry.name}</div>
        </div>
        <button className="btn btn-ghost" onClick={onCancel} style={{ padding: '4px' }}>
          <X size={15} />
        </button>
      </div>

      {/* Fields */}
      <div style={{ padding: 'var(--space-5)', display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
        {inputs.map(inp => (
          <div key={inp.name}>
            <label style={{ display: 'flex', alignItems: 'baseline', gap: 'var(--space-2)', fontSize: 'var(--text-sm)', color: 'var(--text-primary)', marginBottom: 'var(--space-1)' }}>
              <span className="mono">{inp.label || inp.name}</span>
              {inp.required && <span style={{ color: 'var(--status-failed)', fontSize: 'var(--text-xs)' }}>required</span>}
              {inp.description && (
                <span style={{ color: 'var(--text-tertiary)', fontSize: 'var(--text-xs)' }}>— {inp.description}</span>
              )}
            </label>

            {inp.type === 'boolean' ? (
              <select
                className="hud-input"
                value={values[inp.name]}
                onChange={e => set(inp.name, e.target.value)}
              >
                <option value="">— select —</option>
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
            ) : inp.enum && inp.enum.length > 0 ? (
              <select
                className="hud-input"
                value={values[inp.name]}
                onChange={e => set(inp.name, e.target.value)}
              >
                <option value="">— select —</option>
                {inp.enum.map(opt => (
                  <option key={opt} value={opt}>{opt}</option>
                ))}
              </select>
            ) : inp.type === 'number' ? (
              <input
                className="hud-input"
                type="number"
                value={values[inp.name]}
                onChange={e => set(inp.name, e.target.value)}
                placeholder={inp.default || '0'}
              />
            ) : inp.type === 'array' ? (
              <input
                className="hud-input"
                type="text"
                value={values[inp.name]}
                onChange={e => set(inp.name, e.target.value)}
                placeholder="comma-separated values"
              />
            ) : (
              <input
                className="hud-input"
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
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 'var(--space-2)', padding: 'var(--space-4) var(--space-5)', borderTop: '1px solid var(--border-subtle)' }}>
        <button className="btn btn-ghost" onClick={onCancel}>Cancel</button>
        <button className="btn btn-primary" onClick={handleSubmit}>{dryRun ? 'Dry Run' : 'Run'}</button>
      </div>
    </Modal>
  );
}
