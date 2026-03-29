import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { getSettings, saveSettings, selectDirectoryDialog, getBlueprintDir, setBlueprintDir } from '../api/client';
import { Spinner } from '../components/ui/Spinner';
import { FolderOpen, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
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
        <div className="flex items-center justify-between mb-6">
          <div className="text-xl font-semibold text-foreground tracking-tight">Settings</div>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !settings) {
    return (
      <div>
        <div className="flex items-center justify-between mb-6">
          <div className="text-xl font-semibold text-foreground tracking-tight">Settings</div>
        </div>
        <div className="rounded-lg border border-red-500/30 bg-card overflow-hidden px-5 py-4 text-destructive">
          {error || 'Failed to load settings'}
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div className="text-xl font-semibold text-foreground tracking-tight">Settings</div>
        <Button onClick={handleSave} disabled={saving}>
          {saving ? 'Saving...' : 'Save'}
        </Button>
      </div>

      <div className="flex flex-col gap-4 max-w-[640px]">
        {/* General */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">General</span>
          </div>
          <div className="px-5 py-4">
            <label className="block text-sm font-medium text-foreground mb-2">
              Default Blueprint Directory
            </label>
            <div className="flex gap-2 items-center">
              <Input
                type="text"
                value={defaultBpDir}
                onChange={e => setDefaultBpDir(e.target.value)}
                placeholder="None — uses last opened folder"
                className="flex-1 font-mono text-sm"
              />
              <Button variant="ghost" onClick={handleBrowseDefaultDir}>
                <FolderOpen size={13} /> Browse
              </Button>
              {defaultBpDir && (
                <Button variant="ghost" size="xs" onClick={() => setDefaultBpDir('')}>
                  <X size={13} />
                </Button>
              )}
            </div>
            <div className="text-xs text-muted-foreground mt-1">
              Blueprints page will open this folder automatically on launch.
            </div>
          </div>
        </div>

        {/* Execution */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">Execution</span>
          </div>
          <div className="px-5 py-4 grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">Workers</label>
              <Input type="number" min="1" max="16"
                value={settings.execution.workers}
                onChange={e => updateExecution('workers', parseInt(e.target.value) || 1)} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Max Concurrent Jobs</label>
              <Input type="number" min="1" max="32"
                value={settings.execution.maxConcurrentJobs}
                onChange={e => updateExecution('maxConcurrentJobs', parseInt(e.target.value) || 1)} />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Default Timeout (seconds)</label>
              <Input type="number" min="0"
                value={settings.execution.defaultTimeout}
                onChange={e => updateExecution('defaultTimeout', parseInt(e.target.value) || 0)} />
            </div>
          </div>
        </div>

        {/* Safety */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">Safety</span>
          </div>
          <div className="px-5 py-4 flex flex-col gap-3">
            {[
              { key: 'requireConfirmation', label: 'Require confirmation before runs' },
              { key: 'dryRunByDefault', label: 'Dry run by default' },
              { key: 'blockSudo', label: 'Block sudo commands' },
              { key: 'sandboxMode', label: 'Sandbox mode', sub: '(restrict execution to allowed dirs/commands)' },
            ].map(({ key, label, sub }) => (
              <label key={key} className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={(settings.safety as Record<string, boolean>)[key]}
                  onChange={e => updateSafety(key, e.target.checked)}
                  className="accent-primary" />
                <span className="text-sm">{label}</span>
                {sub && <span className="text-xs text-muted-foreground">{sub}</span>}
              </label>
            ))}
          </div>
        </div>

        {/* Telemetry */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">Telemetry</span>
          </div>
          <div className="px-5 py-4 flex flex-col gap-3">
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={settings.telemetry.enabled}
                onChange={e => updateTelemetry('enabled', e.target.checked)}
                className="accent-primary" />
              <span className="text-sm">Enable telemetry logging</span>
            </label>
            <div className="max-w-[200px]">
              <label className="block text-sm font-medium mb-1">Retain logs (days)</label>
              <Input type="number" min="1" max="365"
                value={settings.telemetry.retainDays}
                onChange={e => updateTelemetry('retainDays', parseInt(e.target.value) || 7)} />
            </div>
          </div>
        </div>

        <div className="text-xs text-muted-foreground">
          Settings are stored in ~/.hadron/settings.json. Changes require saving and may need a daemon restart to take effect.
        </div>
      </div>
    </div>
  );
}
