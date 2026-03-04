import { useState } from 'react';
import { BlueprintInput, FileEntry } from '../../api/types';
import { X } from 'lucide-react';

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
    <div className="hud-modal-overlay" onClick={onCancel}>
      <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '480px', width: '100%' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
          <div>
            <div style={{ fontSize: '0.75rem', color: 'rgb(var(--muted))', letterSpacing: '0.08em', textTransform: 'uppercase' }}>Run with Inputs</div>
            <div style={{ fontSize: '0.85rem', color: 'rgb(var(--text))', fontFamily: 'monospace', marginTop: '0.2rem' }}>{entry.name}</div>
          </div>
          <button className="hud-button-ghost" onClick={onCancel} style={{ padding: '0.25rem' }}>
            <X size={15} />
          </button>
        </div>

        {/* Fields */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
          {inputs.map(inp => (
            <div key={inp.name}>
              <label style={{ display: 'flex', alignItems: 'baseline', gap: '0.4rem', fontSize: '0.75rem', color: 'rgb(var(--text))', marginBottom: '0.3rem' }}>
                <span style={{ fontFamily: 'monospace' }}>{inp.label || inp.name}</span>
                {inp.required && <span style={{ color: 'rgb(var(--danger))', fontSize: '0.65rem' }}>required</span>}
                {inp.description && (
                  <span style={{ color: 'rgb(var(--muted))', fontSize: '0.68rem', fontFamily: 'inherit' }}>— {inp.description}</span>
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
                <div style={{ fontSize: '0.68rem', color: 'rgb(var(--danger))', marginTop: '0.2rem' }}>{errors[inp.name]}</div>
              )}
            </div>
          ))}
        </div>

        {/* Dry run toggle */}
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', marginTop: '1rem' }}>
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            style={{ accentColor: 'rgb(var(--ok))' }}
          />
          <span className="hud-label">Dry run</span>
          <span style={{ fontSize: '11px', color: 'rgb(var(--muted))' }}>
            (validate without executing)
          </span>
        </label>

        {/* Footer */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem', marginTop: '0.75rem' }}>
          <button className="hud-button-ghost" onClick={onCancel}>Cancel</button>
          <button className="hud-button" onClick={handleSubmit}>{dryRun ? 'Dry Run' : 'Run'}</button>
        </div>
      </div>
    </div>
  );
}
