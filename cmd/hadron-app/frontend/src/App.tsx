import { useState, useEffect, useCallback } from 'react';
import { toast, Toaster } from 'sonner';
import { AppHeader } from './components/layout/AppHeader';
import { AppNav, NavPage } from './components/layout/AppNav';
import { AppFooter } from './components/layout/AppFooter';
import { DashboardPage } from './pages/DashboardPage';
import { BlueprintsPage } from './pages/BlueprintsPage';
import { BlueprintDetailPage } from './pages/BlueprintDetailPage';
import { BlueprintWizardPage } from './pages/BlueprintWizardPage';
import { RunsPage } from './pages/RunsPage';
import { RunDetailPage } from './pages/RunDetailPage';
import { SchedulerPage } from './pages/SchedulerPage';
import { PipelinesPage } from './pages/PipelinesPage';
import { SettingsPage } from './pages/SettingsPage';
import { TelemetryPage } from './pages/TelemetryPage';
import { HelpPage } from './pages/HelpPage';
import { setBaseURL, listWorkspaces, createWorkspace, getPreference, setPreference, listRuns } from './api/client';
import { isDemoMode, setDemoMode } from './demo/demoMode';
import type { Workspace } from './api/types';

const PAGE_TITLES: Record<NavPage, string> = {
  dashboard: 'Dashboard',
  blueprints: 'Blueprint Browser',
  blueprintDetail: 'Blueprint Detail',
  blueprintWizard: 'Blueprint Wizard',
  pipelines: 'Pipelines',
  runs: 'Run Log',
  runDetail: 'Run Detail',
  schedules: 'Schedules',
  telemetry: 'Telemetry',
  settings: 'Settings',
  help: 'Help',
};

// Wails runtime events are exposed on window.runtime in v2
declare global {
  interface Window {
    runtime?: {
      EventsOn: (event: string, callback: (data: unknown) => void) => () => void;
    };
    go?: {
      main?: {
        App?: {
          GetDaemonAddr: () => Promise<string>;
          GetDaemonStatus: () => Promise<string>;
        };
      };
    };
  }
}

export default function App() {
  const [phase, setPhase] = useState<NavPage>('dashboard');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedBlueprintPath, setSelectedBlueprintPath] = useState<string | null>(null);
  const [wizardEditPath, setWizardEditPath] = useState<string | null>(null);
  const [daemonAddr, setDaemonAddr] = useState('127.0.0.1:8095');
  const [daemonStatus, setDaemonStatus] = useState<string>('stopped');
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState('default');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [activeRunStartedAt, setActiveRunStartedAt] = useState<string | null>(null);
  const [showWorkspaceModal, setShowWorkspaceModal] = useState(false);
  const [wsFormId, setWsFormId] = useState('');
  const [wsFormName, setWsFormName] = useState('');
  const [wsCreating, setWsCreating] = useState(false);
  const [demoEnabled, setDemoEnabled] = useState(isDemoMode());

  const handleToggleDemo = () => {
    const next = !demoEnabled;
    setDemoEnabled(next);
    setDemoMode(next);
    if (next) {
      // In demo mode, fake a running daemon
      setDaemonStatus('running');
    }
  };

  // On mount: grab daemon addr from Go binding and listen for status events
  useEffect(() => {
    const app = window.go?.main?.App;
    if (app?.GetDaemonAddr) {
      app.GetDaemonAddr().then(addr => {
        if (addr) {
          setDaemonAddr(addr);
          setBaseURL(`http://${addr}`);
        }
      });
      app.GetDaemonStatus?.().then(status => {
        if (status) setDaemonStatus(status);
      });
    } else {
      // Dev mode: assume daemon is up at default addr
      setDaemonStatus('running');
    }

    // Subscribe to daemon:status events via Wails runtime
    const unsubscribe = window.runtime?.EventsOn('daemon:status', (data: unknown) => {
      const payload = data as { status: string; addr?: string };
      setDaemonStatus(payload.status);
      if (payload.addr) {
        setDaemonAddr(payload.addr);
        setBaseURL(`http://${payload.addr}`);
      }
    });

    return () => {
      if (typeof unsubscribe === 'function') unsubscribe();
    };
  }, []);

  // When daemon comes up: fetch workspaces and restore last workspace from prefs
  useEffect(() => {
    if (daemonStatus !== 'running') return;
    listWorkspaces().then(res => setWorkspaces(res.items ?? [])).catch(() => {});
    getPreference('lastWorkspaceId').then(id => { if (id) setSelectedWorkspaceId(id); }).catch(() => {});
  }, [daemonStatus]);

  // Poll for active (running) runs to show timer in header
  useEffect(() => {
    if (daemonStatus !== 'running') { setActiveRunStartedAt(null); return; }
    let cancelled = false;
    const poll = () => {
      listRuns({ limit: 20 }).then(res => {
        if (cancelled) return;
        const running = (res.items ?? []).find(r => r.status === 'running');
        setActiveRunStartedAt(running?.started_at ?? null);
      }).catch(() => {});
    };
    poll();
    const timer = setInterval(poll, 3000);
    return () => { cancelled = true; clearInterval(timer); };
  }, [daemonStatus]);

  const handleSelectWorkspace = (id: string) => {
    setSelectedWorkspaceId(id);
    setPreference('lastWorkspaceId', id);
  };

  const handleCreateWorkspace = () => {
    setWsFormId('');
    setWsFormName('');
    setShowWorkspaceModal(true);
  };

  const handleSubmitWorkspace = async () => {
    const id = wsFormId.trim();
    if (!id) return;
    const name = wsFormName.trim() || id;
    setWsCreating(true);
    try {
      await createWorkspace(id, name);
      const res = await listWorkspaces();
      setWorkspaces(res.items ?? []);
      handleSelectWorkspace(id);
      toast.success('Workspace created');
      setShowWorkspaceModal(false);
    } catch (err) {
      toast.error(`Failed to create workspace: ${err}`);
    } finally {
      setWsCreating(false);
    }
  };

  const openRunDetail = (runId: string) => {
    setSelectedRunId(runId);
    setPhase('runDetail');
  };

  const openBlueprintDetail = (path: string) => {
    setSelectedBlueprintPath(path);
    setPhase('blueprintDetail');
  };

  const openWizard = (editPath: string | null = null) => {
    setWizardEditPath(editPath);
    setPhase('blueprintWizard');
  };

  const handleNav = (page: NavPage) => {
    if (page !== 'runDetail') {
      setPhase(page);
    }
  };

  // Keyboard navigation
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    // Don't handle if a modal overlay is open or input is focused
    if (document.querySelector('.hud-modal-overlay')) return;
    const tag = (e.target as HTMLElement)?.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

    if (e.key === 'Escape') {
      if (phase === 'runDetail') { setPhase('runs'); e.preventDefault(); }
      else if (phase === 'blueprintDetail') { setPhase('blueprints'); e.preventDefault(); }
      else if (phase === 'blueprintWizard') { setPhase('blueprints'); e.preventDefault(); }
    }

    // R — refresh (dispatch custom event that pages can listen for)
    if (e.key === 'r' && !e.metaKey && !e.ctrlKey) {
      window.dispatchEvent(new CustomEvent('hadron:refresh'));
    }

    // N — new blueprint (only on blueprints page)
    if (e.key === 'n' && !e.metaKey && !e.ctrlKey && phase === 'blueprints') {
      openWizard();
      e.preventDefault();
    }

    // ? — help
    if (e.key === '?' && !e.metaKey && !e.ctrlKey) {
      setPhase('help');
      e.preventDefault();
    }
  }, [phase]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  return (
    <div className="app-shell">
      <AppHeader
        page={PAGE_TITLES[phase]}
        phase={phase}
        daemonStatus={daemonStatus}
        daemonAddr={daemonAddr}
        workspaces={workspaces}
        selectedWorkspaceId={selectedWorkspaceId}
        onSelectWorkspace={handleSelectWorkspace}
        onCreateWorkspace={handleCreateWorkspace}
        onNavigate={handleNav}
        activeRunStartedAt={activeRunStartedAt}
        demoMode={demoEnabled}
        onToggleDemo={handleToggleDemo}
      />

      <div className="app-body">
        <AppNav current={phase} onNavigate={handleNav} />

        <main className="app-content">
          {phase === 'dashboard' && (
            <DashboardPage
              daemonStatus={daemonStatus}
              daemonAddr={daemonAddr}
              onOpenRun={openRunDetail}
            />
          )}
          {phase === 'blueprints' && (
            <BlueprintsPage
              daemonStatus={daemonStatus}
              workspaceId={selectedWorkspaceId}
              onRunCreated={id => {
                openRunDetail(id);
              }}
              onOpenBlueprint={openBlueprintDetail}
              onNewBlueprint={() => openWizard()}
              onBatchRunComplete={() => setPhase('runs')}
            />
          )}
          {phase === 'blueprintDetail' && selectedBlueprintPath && (
            <BlueprintDetailPage
              path={selectedBlueprintPath}
              onBack={() => setPhase('blueprints')}
              onOpenRun={openRunDetail}
              onEditBlueprint={(p) => openWizard(p)}
              daemonStatus={daemonStatus}
              workspaceId={selectedWorkspaceId}
            />
          )}
          {phase === 'blueprintWizard' && (
            <BlueprintWizardPage
              editPath={wizardEditPath}
              onBack={() => setPhase('blueprints')}
            />
          )}
          {phase === 'pipelines' && (
            <PipelinesPage
              daemonStatus={daemonStatus}
              workspaceId={selectedWorkspaceId}
              onOpenRun={openRunDetail}
            />
          )}
          {phase === 'runs' && (
            <RunsPage
              daemonStatus={daemonStatus}
              onOpenRun={openRunDetail}
            />
          )}
          {phase === 'schedules' && (
            <SchedulerPage
              daemonStatus={daemonStatus}
            />
          )}
          {phase === 'telemetry' && (
            <TelemetryPage
              onOpenRun={openRunDetail}
            />
          )}
          {phase === 'settings' && (
            <SettingsPage />
          )}
          {phase === 'help' && (
            <HelpPage />
          )}
          {phase === 'runDetail' && selectedRunId && (
            <RunDetailPage
              runId={selectedRunId}
              onBack={() => setPhase('runs')}
            />
          )}
        </main>
      </div>

      <AppFooter phase={phase} />

      {/* Workspace creation modal */}
      {showWorkspaceModal && (
        <div className="hud-modal-overlay" onClick={() => setShowWorkspaceModal(false)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '1rem', fontWeight: 600, fontSize: '0.9rem', letterSpacing: '0.05em' }}>New Workspace</div>
              <div style={{ marginBottom: '0.75rem' }}>
                <label className="hud-label">Workspace ID</label>
                <input
                  className="hud-input"
                  placeholder="my-workspace"
                  value={wsFormId}
                  onChange={e => setWsFormId(e.target.value.replace(/[^a-zA-Z0-9-]/g, ''))}
                  style={{ width: '100%', boxSizing: 'border-box' }}
                  autoFocus
                />
                <div style={{ fontSize: '0.68rem', color: 'rgb(var(--muted))', marginTop: '0.2rem' }}>
                  Letters, numbers, and hyphens only
                </div>
              </div>
              <div style={{ marginBottom: '1rem' }}>
                <label className="hud-label">Display Name <span style={{ color: 'rgb(var(--muted))' }}>(optional)</span></label>
                <input
                  className="hud-input"
                  placeholder="My Workspace"
                  value={wsFormName}
                  onChange={e => setWsFormName(e.target.value)}
                  style={{ width: '100%', boxSizing: 'border-box' }}
                />
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="hud-button-ghost" onClick={() => setShowWorkspaceModal(false)}>Cancel</button>
                <button
                  className="hud-button"
                  onClick={handleSubmitWorkspace}
                  disabled={!wsFormId.trim() || wsCreating}
                  style={{ borderColor: 'rgba(var(--ok) / 0.5)', color: 'rgb(var(--ok))' }}
                >
                  {wsCreating ? 'Creating...' : 'Create'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <Toaster
        position="bottom-right"
        toastOptions={{
          style: {
            background: 'rgb(var(--panel2))',
            border: '1px solid rgb(var(--border))',
            color: 'rgb(var(--text))',
            fontFamily: "'Share Tech Mono', monospace",
            fontSize: '13px',
          },
        }}
        theme="dark"
      />
    </div>
  );
}
