import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { getSettings, saveSettings, selectDirectoryDialog, getBlueprintDir, setBlueprintDir } from '../api/client';
import { Spinner } from '../components/ui/Spinner';
import { FolderOpen, X } from 'lucide-react';
import type { HadronSettings } from '../api/types';

export function SettingsPage() {
  const [settings, setSettings] = useState<HadronSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [defaultBpDir, setDefaultBpDir] = useState('');

  useEffect(() => {
    setLoading(true);
    Promise.all([
      getSettings().then(s => { setSettings(s); setError(null); }).catch(err => setError(String(err))),
      getBlueprintDir().then(v => { if (v) setDefaultBpDir(v); }).catch(() => {}),
    ]).finally(() => setLoading(false));
  }, []);

  const handleSave = async () => {
    if (!settings) return;
    setSaving(true);
    try {
      await saveSettings(settings);
      await setBlueprintDir(defaultBpDir);
      toast.success('Settings saved');
    } catch (err: unknown) {
      toast.error(`Save failed: ${err}`);
    } finally {
      setSaving(false);
    }
  };

  const handleBrowseDefaultDir = async () => {
    const dir = await selectDirectoryDialog();
    if (dir) setDefaultBpDir(dir);
  };

  const updateExecution = (field: string, value: unknown) => {
    setSettings(prev => prev ? { ...prev, execution: { ...prev.execution, [field]: value } } : prev);
  };

  const updateSafety = (field: string, value: unknown) => {
    setSettings(prev => prev ? { ...prev, safety: { ...prev.safety, [field]: value } } : prev);
  };

  const updateTelemetry = (field: string, value: unknown) => {
    setSettings(prev => prev ? { ...prev, telemetry: { ...prev.telemetry, [field]: value } } : prev);
  };

  if (loading) {
    return (
      <div>
        <div className="page-header">
          <div className="page-title">Settings</div>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !settings) {
    return (
      <div>
        <div className="page-header">
          <div className="page-title">Settings</div>
        </div>
        <div className="section" style={{ padding: 'var(--space-4) var(--space-5)', color: 'var(--status-failed)', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
          {error || 'Failed to load settings'}
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-title">Settings</div>
        <button className="btn btn-primary" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving...' : 'Save'}
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)', maxWidth: 640 }}>
        {/* General */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">General</span>
          </div>
          <div style={{ padding: 'var(--space-4) var(--space-5)' }}>
            <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, color: 'var(--text-primary)', marginBottom: 'var(--space-2)' }}>
              Default Blueprint Directory
            </label>
            <div style={{ display: 'flex', gap: 'var(--space-2)', alignItems: 'center' }}>
              <input
                className="hud-input"
                type="text"
                value={defaultBpDir}
                onChange={e => setDefaultBpDir(e.target.value)}
                placeholder="None — uses last opened folder"
                style={{ flex: 1, fontFamily: 'var(--font-mono)', fontSize: 'var(--text-sm)' }}
              />
              <button className="btn btn-ghost" onClick={handleBrowseDefaultDir}>
                <FolderOpen size={13} /> Browse
              </button>
              {defaultBpDir && (
                <button className="btn btn-ghost" onClick={() => setDefaultBpDir('')} style={{ padding: '4px 6px' }}>
                  <X size={13} />
                </button>
              )}
            </div>
            <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 'var(--space-1)' }}>
              Blueprints page will open this folder automatically on launch.
            </div>
          </div>
        </div>

        {/* Execution */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">Execution</span>
          </div>
          <div style={{ padding: 'var(--space-4) var(--space-5)', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-4)' }}>
            <div>
              <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Workers</label>
              <input className="hud-input" type="number" min="1" max="16"
                value={settings.execution.workers}
                onChange={e => updateExecution('workers', parseInt(e.target.value) || 1)} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Max Concurrent Jobs</label>
              <input className="hud-input" type="number" min="1" max="32"
                value={settings.execution.maxConcurrentJobs}
                onChange={e => updateExecution('maxConcurrentJobs', parseInt(e.target.value) || 1)} />
            </div>
            <div>
              <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Default Timeout (seconds)</label>
              <input className="hud-input" type="number" min="0"
                value={settings.execution.defaultTimeout}
                onChange={e => updateExecution('defaultTimeout', parseInt(e.target.value) || 0)} />
            </div>
          </div>
        </div>

        {/* Safety */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">Safety</span>
          </div>
          <div style={{ padding: 'var(--space-4) var(--space-5)', display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
            {[
              { key: 'requireConfirmation', label: 'Require confirmation before runs' },
              { key: 'dryRunByDefault', label: 'Dry run by default' },
              { key: 'blockSudo', label: 'Block sudo commands' },
              { key: 'sandboxMode', label: 'Sandbox mode', sub: '(restrict execution to allowed dirs/commands)' },
            ].map(({ key, label, sub }) => (
              <label key={key} style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', cursor: 'pointer' }}>
                <input type="checkbox" checked={(settings.safety as Record<string, boolean>)[key]}
                  onChange={e => updateSafety(key, e.target.checked)}
                  style={{ accentColor: 'var(--accent)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>{label}</span>
                {sub && <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)' }}>{sub}</span>}
              </label>
            ))}
          </div>
        </div>

        {/* Telemetry */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">Telemetry</span>
          </div>
          <div style={{ padding: 'var(--space-4) var(--space-5)', display: 'flex', flexDirection: 'column', gap: 'var(--space-3)' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.telemetry.enabled}
                onChange={e => updateTelemetry('enabled', e.target.checked)}
                style={{ accentColor: 'var(--accent)' }} />
              <span style={{ fontSize: 'var(--text-md)' }}>Enable telemetry logging</span>
            </label>
            <div style={{ maxWidth: 200 }}>
              <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Retain logs (days)</label>
              <input className="hud-input" type="number" min="1" max="365"
                value={settings.telemetry.retainDays}
                onChange={e => updateTelemetry('retainDays', parseInt(e.target.value) || 7)} />
            </div>
          </div>
        </div>

        <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)' }}>
          Settings are stored in ~/.hadron/settings.json. Changes require saving and may need a daemon restart to take effect.
        </div>
      </div>
    </div>
  );
}
