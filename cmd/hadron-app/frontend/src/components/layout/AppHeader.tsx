import { useState, useRef, useEffect } from 'react';
import { ChevronDown, Plus, ChevronRight, MoreVertical, Settings, HelpCircle, FlaskConical } from 'lucide-react';
import type { Workspace } from '../../api/types';
import type { NavPage } from './AppNav';

interface Breadcrumb { label: string; page?: NavPage; }

function getBreadcrumbs(phase: NavPage): Breadcrumb[] {
  switch (phase) {
    case 'blueprintDetail': return [{ label: 'Blueprints', page: 'blueprints' }, { label: 'Detail' }];
    case 'blueprintWizard': return [{ label: 'Blueprints', page: 'blueprints' }, { label: 'Wizard' }];
    case 'runDetail': return [{ label: 'Run Log', page: 'runs' }, { label: 'Detail' }];
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
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: 'rgb(var(--warn))' }}>
      <span className="pulse-running" style={{ display: 'inline-block', width: '6px', height: '6px', borderRadius: '50%', background: 'rgb(var(--warn))' }} />
      <span style={{ fontFamily: 'monospace' }}>{elapsed}</span>
    </div>
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
  const [open, setOpen] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  // Close dropdowns on outside click
  useEffect(() => {
    if (!open && !menuOpen) return;
    const handler = (e: MouseEvent) => {
      if (open && dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
      if (menuOpen && menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open, menuOpen]);

  return (
    <header className="app-header">
      <span className="app-header-logo">HADRON</span>
      <span className="app-header-title" style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
        {breadcrumbs.length > 0 ? (
          <>
            {breadcrumbs.map((bc, i) => (
              <span key={i} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
                {i > 0 && <ChevronRight size={11} style={{ color: 'rgb(var(--border))' }} />}
                {bc.page ? (
                  <span
                    style={{ cursor: 'pointer', color: 'rgb(var(--accent))' }}
                    onClick={() => onNavigate(bc.page!)}
                  >{bc.label}</span>
                ) : (
                  <span>{bc.label}</span>
                )}
              </span>
            ))}
          </>
        ) : page}
      </span>

      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginLeft: 'auto' }}>
        {/* Workspace selector */}
        <div ref={dropdownRef} style={{ position: 'relative' }}>
          <button
            className="hud-button-ghost"
            onClick={() => setOpen(o => !o)}
            style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: '0.75rem', padding: '0.2rem 0.5rem' }}
          >
            <span style={{ fontFamily: 'monospace' }}>{selectedWorkspaceId}</span>
            <ChevronDown size={12} />
          </button>

          {open && (
            <div
              className="hud-panel"
              style={{
                position: 'absolute',
                top: 'calc(100% + 4px)',
                right: 0,
                minWidth: '160px',
                zIndex: 100,
                padding: '0.25rem 0',
              }}
            >
              {workspaces.map(ws => (
                <button
                  key={ws.id}
                  className="hud-button-ghost"
                  onClick={() => { onSelectWorkspace(ws.id); setOpen(false); }}
                  style={{
                    width: '100%',
                    textAlign: 'left',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.4rem',
                    fontSize: '0.78rem',
                    padding: '0.3rem 0.75rem',
                    borderRadius: 0,
                  }}
                >
                  <span style={{ fontFamily: 'monospace', flex: 1 }}>{ws.id}</span>
                  {ws.id === selectedWorkspaceId && (
                    <span style={{ color: 'rgb(var(--ok))', fontSize: '0.7rem' }}>✓</span>
                  )}
                </button>
              ))}

              {workspaces.length > 0 && (
                <div style={{ borderTop: '1px solid rgba(var(--text) / 0.1)', margin: '0.2rem 0' }} />
              )}

              <button
                className="hud-button-ghost"
                onClick={() => { setOpen(false); onCreateWorkspace(); }}
                style={{
                  width: '100%',
                  textAlign: 'left',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  fontSize: '0.78rem',
                  padding: '0.3rem 0.75rem',
                  borderRadius: 0,
                  color: 'rgb(var(--accent))',
                }}
              >
                <Plus size={12} /> New Workspace
              </button>
            </div>
          )}
        </div>

        {/* Demo mode indicator */}
        {demoMode && (
          <button
            className="hud-button-ghost"
            onClick={onToggleDemo}
            style={{
              display: 'flex', alignItems: 'center', gap: '0.3rem',
              fontSize: '0.68rem', padding: '0.15rem 0.5rem',
              color: 'rgb(var(--warn))',
              borderColor: 'rgba(var(--warn) / 0.4)',
              background: 'rgba(var(--warn) / 0.08)',
            }}
          >
            <FlaskConical size={12} /> DEMO
          </button>
        )}

        {/* Active run timer */}
        {activeRunStartedAt && <ElapsedTimer startedAt={activeRunStartedAt} />}

        {/* Daemon indicator */}
        <div className="app-header-daemon">
          <span className={`daemon-dot ${daemonStatus}`} />
          <span>
            {daemonStatus === 'running'
              ? `daemon ${daemonAddr}`
              : daemonStatus === 'error'
              ? 'daemon error'
              : 'daemon starting...'}
          </span>
        </div>

        {/* App menu */}
        <div ref={menuRef} style={{ position: 'relative' }}>
          <button
            className="hud-button-ghost"
            onClick={() => setMenuOpen(o => !o)}
            style={{ padding: '0.2rem 0.35rem', border: 'none' }}
          >
            <MoreVertical size={15} />
          </button>
          {menuOpen && (
            <div
              className="hud-panel"
              style={{
                position: 'absolute', top: 'calc(100% + 4px)', right: 0,
                minWidth: '150px', zIndex: 100, padding: '0.25rem 0',
                boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
              }}
            >
              <button
                className="hud-button-ghost"
                onClick={() => { setMenuOpen(false); onNavigate('settings'); }}
                style={{
                  width: '100%', textAlign: 'left', display: 'flex',
                  alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem',
                  padding: '0.4rem 0.75rem', borderRadius: 0, border: 'none',
                }}
              >
                <Settings size={13} /> Settings
              </button>
              <button
                className="hud-button-ghost"
                onClick={() => { setMenuOpen(false); onNavigate('help'); }}
                style={{
                  width: '100%', textAlign: 'left', display: 'flex',
                  alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem',
                  padding: '0.4rem 0.75rem', borderRadius: 0, border: 'none',
                }}
              >
                <HelpCircle size={13} /> Help
              </button>
              <div style={{ borderTop: '1px solid rgba(var(--text) / 0.1)', margin: '0.2rem 0' }} />
              {onToggleDemo && (
                <button
                  className="hud-button-ghost"
                  onClick={() => { setMenuOpen(false); onToggleDemo(); }}
                  style={{
                    width: '100%', textAlign: 'left', display: 'flex',
                    alignItems: 'center', gap: '0.5rem', fontSize: '0.78rem',
                    padding: '0.4rem 0.75rem', borderRadius: 0, border: 'none',
                    color: demoMode ? 'rgb(var(--warn))' : undefined,
                  }}
                >
                  <FlaskConical size={13} /> {demoMode ? 'Disable Demo Mode' : 'Enable Demo Mode'}
                </button>
              )}
              <div style={{ borderTop: '1px solid rgba(var(--text) / 0.1)', margin: '0.2rem 0' }} />
              <div style={{
                padding: '0.4rem 0.75rem', fontSize: '0.68rem',
                color: 'rgb(var(--muted))', lineHeight: '1.4',
              }}>
                <div style={{ color: 'rgb(var(--ok))', fontWeight: 700, marginBottom: '0.1rem' }}>HADRON v0.4.0</div>
                <div>Hollis Labs</div>
              </div>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
