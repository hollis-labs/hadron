import { useState, useRef, useEffect } from 'react';
import { ChevronDown, ChevronLeft, ChevronRight, Plus, FlaskConical, Folder, MoreVertical } from 'lucide-react';
import type { Workspace } from '../../api/types';
import type { NavPage } from './AppNav';

interface Breadcrumb { label: string; page?: NavPage; }

function getBreadcrumbs(phase: NavPage): Breadcrumb[] {
  switch (phase) {
    case 'blueprintDetail': return [{ label: 'Blueprints', page: 'blueprints' }, { label: 'Detail' }];
    case 'blueprintWizard': return [{ label: 'Blueprints', page: 'blueprints' }, { label: 'Wizard' }];
    case 'runDetail': return [{ label: 'Runs', page: 'runs' }, { label: 'Detail' }];
    case 'pipelineDetail': return [{ label: 'Pipelines', page: 'pipelines' }, { label: 'Detail' }];
    case 'flowBuilder': return [{ label: 'Pipelines', page: 'pipelines' }, { label: 'Flow Builder' }];
    default: return [];
  }
}

interface AppHeaderProps {
  page: string;
  phase: NavPage;
  daemonStatus: string;
  daemonAddr: string;
  workspaces: Workspace[];
  selectedWorkspaceId: string;
  onSelectWorkspace: (id: string) => void;
  onCreateWorkspace: () => void;
  onNavigate: (page: NavPage) => void;
  activeRunStartedAt?: string | null;
  demoMode?: boolean;
  onToggleDemo?: () => void;
}

function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState('');
  useEffect(() => {
    const update = () => {
      const ms = Date.now() - new Date(startedAt).getTime();
      if (ms < 1000) setElapsed('0s');
      else if (ms < 60000) setElapsed(`${Math.floor(ms / 1000)}s`);
      else setElapsed(`${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`);
    };
    update();
    const timer = setInterval(update, 1000);
    return () => clearInterval(timer);
  }, [startedAt]);
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px', fontSize: 'var(--text-sm)', color: 'var(--status-running)' }}>
      <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', background: 'var(--status-running)', animation: 'badge-pulse 1.8s ease-in-out infinite' }} />
      <span className="mono">{elapsed}</span>
    </span>
  );
}

export function AppHeader({
  page,
  phase,
  daemonStatus,
  daemonAddr,
  workspaces,
  selectedWorkspaceId,
  onSelectWorkspace,
  onCreateWorkspace,
  onNavigate,
  activeRunStartedAt,
  demoMode,
  onToggleDemo,
}: AppHeaderProps) {
  const breadcrumbs = getBreadcrumbs(phase);
  const hasBack = breadcrumbs.length > 0;
  const [wsOpen, setWsOpen] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const wsRef = useRef<HTMLDivElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!wsOpen && !menuOpen) return;
    const handler = (e: MouseEvent) => {
      if (wsOpen && wsRef.current && !wsRef.current.contains(e.target as Node)) setWsOpen(false);
      if (menuOpen && menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [wsOpen, menuOpen]);

  return (
    <header className="header">
      {/* Back button for detail pages */}
      {hasBack && (
        <button
          className="btn-ghost btn"
          onClick={() => breadcrumbs[0].page && onNavigate(breadcrumbs[0].page)}
          style={{ padding: '4px 6px' }}
        >
          <ChevronLeft size={16} />
        </button>
      )}

      {/* Title / Breadcrumbs */}
      {breadcrumbs.length > 0 ? (
        <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
          <span className="header-breadcrumb">
            {breadcrumbs[0].page ? (
              <span
                style={{ cursor: 'pointer' }}
                onClick={() => breadcrumbs[0].page && onNavigate(breadcrumbs[0].page)}
              >{breadcrumbs[0].label}</span>
            ) : breadcrumbs[0].label}
          </span>
          {breadcrumbs.length > 1 && (
            <>
              <ChevronRight size={11} style={{ color: 'var(--text-tertiary)' }} />
              <span className="header-title">{breadcrumbs[1].label}</span>
            </>
          )}
        </div>
      ) : (
        <span className="header-title">{page}</span>
      )}

      <div className="header-spacer" />

      <div className="header-actions">
        {/* Demo mode indicator */}
        {demoMode && onToggleDemo && (
          <button
            className="btn btn-ghost"
            onClick={onToggleDemo}
            style={{ color: 'var(--status-running)', fontSize: 'var(--text-xs)', padding: '2px 8px', border: '1px solid rgba(245, 158, 11, 0.3)', background: 'rgba(245, 158, 11, 0.08)' }}
          >
            <FlaskConical size={12} /> DEMO
          </button>
        )}

        {/* Active run timer */}
        {activeRunStartedAt && <ElapsedTimer startedAt={activeRunStartedAt} />}

        {/* Daemon status */}
        <div className="daemon-status">
          <span
            className="daemon-dot"
            style={{
              background: daemonStatus === 'running' ? 'var(--accent)' : daemonStatus === 'error' ? 'var(--status-failed)' : 'var(--text-tertiary)',
              boxShadow: daemonStatus === 'running' ? '0 0 6px rgba(59, 130, 246, 0.5)' : 'none',
            }}
          />
          <span>{daemonAddr}</span>
        </div>

        {/* Workspace selector */}
        <div ref={wsRef} style={{ position: 'relative' }}>
          <button className="workspace-btn" onClick={() => setWsOpen(o => !o)}>
            <Folder size={14} />
            {selectedWorkspaceId}
            <ChevronDown size={12} />
          </button>

          {wsOpen && (
            <div style={{
              position: 'absolute', top: 'calc(100% + 4px)', right: 0,
              minWidth: 180, zIndex: 100, padding: '4px 0',
              background: 'var(--bg-overlay)', border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)', boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
            }}>
              {workspaces.map(ws => (
                <button
                  key={ws.id}
                  className="btn btn-ghost"
                  onClick={() => { onSelectWorkspace(ws.id); setWsOpen(false); }}
                  style={{
                    width: '100%', textAlign: 'left', borderRadius: 0,
                    fontSize: 'var(--text-sm)', padding: '6px 12px', border: 'none',
                  }}
                >
                  <span className="mono" style={{ flex: 1 }}>{ws.id}</span>
                  {ws.id === selectedWorkspaceId && <span style={{ color: 'var(--accent)', fontSize: 'var(--text-xs)' }}>✓</span>}
                </button>
              ))}
              <div style={{ borderTop: '1px solid var(--border-default)', margin: '4px 0' }} />
              <button
                className="btn btn-ghost"
                onClick={() => { setWsOpen(false); onCreateWorkspace(); }}
                style={{
                  width: '100%', textAlign: 'left', borderRadius: 0,
                  fontSize: 'var(--text-sm)', padding: '6px 12px', border: 'none',
                  color: 'var(--accent)',
                }}
              >
                <Plus size={12} /> New Workspace
              </button>
            </div>
          )}
        </div>

        {/* App menu (demo toggle, version) */}
        <div ref={menuRef} style={{ position: 'relative' }}>
          <button className="btn btn-ghost" onClick={() => setMenuOpen(o => !o)} style={{ padding: '4px 6px' }}>
            <MoreVertical size={15} />
          </button>
          {menuOpen && (
            <div style={{
              position: 'absolute', top: 'calc(100% + 4px)', right: 0,
              minWidth: 170, zIndex: 100, padding: '4px 0',
              background: 'var(--bg-overlay)', border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)', boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
            }}>
              {onToggleDemo && (
                <button
                  className="btn btn-ghost"
                  onClick={() => { setMenuOpen(false); onToggleDemo(); }}
                  style={{
                    width: '100%', textAlign: 'left', borderRadius: 0,
                    fontSize: 'var(--text-sm)', padding: '6px 12px', border: 'none',
                    color: demoMode ? 'var(--status-running)' : undefined,
                  }}
                >
                  <FlaskConical size={13} /> {demoMode ? 'Disable Demo' : 'Enable Demo'}
                </button>
              )}
              <div style={{ borderTop: '1px solid var(--border-default)', margin: '4px 0' }} />
              <div style={{ padding: '6px 12px', fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', lineHeight: 1.4 }}>
                <div style={{ color: 'var(--accent)', fontWeight: 600, marginBottom: 2 }}>hadron v0.4.0</div>
                <div>Hollis Labs</div>
              </div>
            </div>
          )}
        </div>
        </div>
    </header>
  );
}
