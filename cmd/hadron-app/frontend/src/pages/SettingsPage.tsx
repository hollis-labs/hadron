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
          <span className="page-title">Settings</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !settings) {
    return (
      <div>
        <div className="page-header">
          <span className="page-title">Settings</span>
        </div>
        <div style={{ color: 'rgb(var(--danger))', fontSize: '0.8rem', padding: '0.75rem', background: 'rgba(var(--danger) / 0.1)', borderRadius: '4px', border: '1px solid rgba(var(--danger) / 0.3)' }}>
          {error || 'Failed to load settings'}
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <span className="page-title">Settings</span>
        <button
          className="hud-button"
          onClick={handleSave}
          disabled={saving}
          style={{ marginLeft: 'auto', borderColor: 'rgba(var(--ok) / 0.5)', color: 'rgb(var(--ok))' }}
        >
          {saving ? 'Saving...' : 'Save'}
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem', maxWidth: '640px' }}>
        {/* General */}
        <div className="hud-panel" style={{ padding: '1rem' }}>
          <div className="bp-meta-section-title" style={{ marginBottom: '0.75rem' }}>General</div>
          <div className="wizard-field">
            <label>Default Blueprint Directory</label>
            <div style={{ display: 'flex', gap: '0.4rem', alignItems: 'center' }}>
              <input
                className="hud-input"
                type="text"
                value={defaultBpDir}
                onChange={e => setDefaultBpDir(e.target.value)}
                placeholder="None — uses last opened folder"
                style={{ flex: 1, fontFamily: 'monospace', fontSize: '0.78rem' }}
              />
              <button className="hud-button-ghost" onClick={handleBrowseDefaultDir} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', flexShrink: 0 }}>
                <FolderOpen size={13} /> Browse
              </button>
              {defaultBpDir && (
                <button className="hud-button-ghost" onClick={() => setDefaultBpDir('')} style={{ padding: '0.3rem', flexShrink: 0 }}>
                  <X size={13} />
                </button>
              )}
            </div>
            <div style={{ fontSize: '0.68rem', color: 'rgb(var(--muted))', marginTop: '0.25rem' }}>
              Blueprints page will open this folder automatically on launch.
            </div>
          </div>
        </div>

        {/* Execution Settings */}
        <div className="hud-panel" style={{ padding: '1rem' }}>
          <div className="bp-meta-section-title" style={{ marginBottom: '0.75rem' }}>Execution</div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
            <div className="wizard-field">
              <label>Workers</label>
              <input className="hud-input" type="number" min="1" max="16"
                value={settings.execution.workers}
                onChange={e => updateExecution('workers', parseInt(e.target.value) || 1)} />
            </div>
            <div className="wizard-field">
              <label>Max Concurrent Jobs</label>
              <input className="hud-input" type="number" min="1" max="32"
                value={settings.execution.maxConcurrentJobs}
                onChange={e => updateExecution('maxConcurrentJobs', parseInt(e.target.value) || 1)} />
            </div>
            <div className="wizard-field">
              <label>Default Timeout (seconds)</label>
              <input className="hud-input" type="number" min="0"
                value={settings.execution.defaultTimeout}
                onChange={e => updateExecution('defaultTimeout', parseInt(e.target.value) || 0)} />
            </div>
          </div>
        </div>

        {/* Safety Settings */}
        <div className="hud-panel" style={{ padding: '1rem' }}>
          <div className="bp-meta-section-title" style={{ marginBottom: '0.75rem' }}>Safety</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.safety.requireConfirmation}
                onChange={e => updateSafety('requireConfirmation', e.target.checked)}
                style={{ accentColor: 'rgb(var(--ok))' }} />
              <span style={{ fontSize: '0.82rem' }}>Require confirmation before runs</span>
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.safety.dryRunByDefault}
                onChange={e => updateSafety('dryRunByDefault', e.target.checked)}
                style={{ accentColor: 'rgb(var(--ok))' }} />
              <span style={{ fontSize: '0.82rem' }}>Dry run by default</span>
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.safety.blockSudo}
                onChange={e => updateSafety('blockSudo', e.target.checked)}
                style={{ accentColor: 'rgb(var(--ok))' }} />
              <span style={{ fontSize: '0.82rem' }}>Block sudo commands</span>
            </label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.safety.sandboxMode}
                onChange={e => updateSafety('sandboxMode', e.target.checked)}
                style={{ accentColor: 'rgb(var(--ok))' }} />
              <span style={{ fontSize: '0.82rem' }}>Sandbox mode</span>
              <span style={{ fontSize: '0.68rem', color: 'rgb(var(--muted))' }}>(restrict execution to allowed dirs/commands)</span>
            </label>
          </div>
        </div>

        {/* Telemetry Settings */}
        <div className="hud-panel" style={{ padding: '1rem' }}>
          <div className="bp-meta-section-title" style={{ marginBottom: '0.75rem' }}>Telemetry</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input type="checkbox" checked={settings.telemetry.enabled}
                onChange={e => updateTelemetry('enabled', e.target.checked)}
                style={{ accentColor: 'rgb(var(--ok))' }} />
              <span style={{ fontSize: '0.82rem' }}>Enable telemetry logging</span>
            </label>
            <div className="wizard-field" style={{ maxWidth: '200px' }}>
              <label>Retain logs (days)</label>
              <input className="hud-input" type="number" min="1" max="365"
                value={settings.telemetry.retainDays}
                onChange={e => updateTelemetry('retainDays', parseInt(e.target.value) || 7)} />
            </div>
          </div>
        </div>

        {/* Info */}
        <div style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))' }}>
          Settings are stored in ~/.hadron/settings.json. Changes require saving and may need a daemon restart to take effect.
        </div>
      </div>
    </div>
  );
}
