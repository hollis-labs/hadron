import { useState, useEffect } from 'react';
import { ChevronDown, ChevronLeft, ChevronRight, Plus, FlaskConical, Folder, MoreVertical, Check } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useDaemon } from '../../contexts/DaemonContext';
import { useNavigation } from '../../contexts/NavigationContext';
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
      <span className="font-mono">{elapsed}</span>
    </span>
  );
}

export function AppHeader({ page, phase }: AppHeaderProps) {
  const daemon = useDaemon();
  const nav = useNavigation();
  const breadcrumbs = getBreadcrumbs(phase);
  const hasBack = breadcrumbs.length > 0;
  const [showWorkspaceModal, setShowWorkspaceModal] = useState(false);
  const [wsFormId, setWsFormId] = useState('');
  const [wsFormName, setWsFormName] = useState('');
  const [wsCreating, setWsCreating] = useState(false);

  const handleCreateWorkspace = async () => {
    const id = wsFormId.trim();
    if (!id) return;
    const name = wsFormName.trim() || id;
    setWsCreating(true);
    try {
      await daemon.createWorkspace(id, name);
      setShowWorkspaceModal(false);
    } catch {
      // toast is handled in DaemonContext
    } finally {
      setWsCreating(false);
    }
  };

  return (
    <>
      <header className="header">
        {hasBack && (
          <Button
            variant="ghost"
            size="xs"
            onClick={() => breadcrumbs[0].page && nav.navigate(breadcrumbs[0].page)}
          >
            <ChevronLeft size={16} />
          </Button>
        )}

        {breadcrumbs.length > 0 ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            <span className="header-breadcrumb">
              {breadcrumbs[0].page ? (
                <span
                  style={{ cursor: 'pointer' }}
                  onClick={() => breadcrumbs[0].page && nav.navigate(breadcrumbs[0].page)}
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
          {daemon.demoMode && (
            <Button
              variant="ghost"
              size="xs"
              onClick={daemon.toggleDemo}
              style={{ color: 'var(--status-running)', padding: '2px 8px', border: '1px solid rgba(245, 158, 11, 0.3)', background: 'rgba(245, 158, 11, 0.08)' }}
            >
              <FlaskConical size={12} /> DEMO
            </Button>
          )}

          {daemon.activeRunStartedAt && <ElapsedTimer startedAt={daemon.activeRunStartedAt} />}

          <div className="daemon-status">
            <span
              className="daemon-dot"
              style={{
                background: daemon.status === 'running' ? 'var(--accent)' : daemon.status === 'error' ? 'var(--status-failed)' : 'var(--text-tertiary)',
                boxShadow: daemon.status === 'running' ? '0 0 6px rgba(59, 130, 246, 0.5)' : 'none',
              }}
            />
            <span>{daemon.address}</span>
          </div>

          <DropdownMenu>
            <DropdownMenuTrigger render={<button className="workspace-btn" />}>
              <Folder size={14} />
              {daemon.workspaceId}
              <ChevronDown size={12} />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="min-w-[180px]">
              {daemon.workspaces.map(ws => (
                <DropdownMenuItem
                  key={ws.id}
                  onClick={() => daemon.selectWorkspace(ws.id)}
                  className="font-mono text-sm"
                >
                  <span className="flex-1">{ws.id}</span>
                  {ws.id === daemon.workspaceId && <Check size={14} className="text-primary" />}
                </DropdownMenuItem>
              ))}
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={() => { setWsFormId(''); setWsFormName(''); setShowWorkspaceModal(true); }} className="text-primary">
                <Plus size={12} /> New Workspace
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>

          <DropdownMenu>
            <DropdownMenuTrigger render={<Button variant="ghost" size="xs" />}>
              <MoreVertical size={15} />
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="min-w-[170px]">
              <DropdownMenuItem
                onClick={daemon.toggleDemo}
                className={daemon.demoMode ? 'text-amber-400' : ''}
              >
                <FlaskConical size={13} /> {daemon.demoMode ? 'Disable Demo' : 'Enable Demo'}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <div className="px-3 py-1.5 text-xs text-muted-foreground leading-snug">
                <div className="text-primary font-semibold mb-0.5">hadron v0.4.0</div>
                <div>Hollis Labs</div>
              </div>
            </DropdownMenuContent>
          </DropdownMenu>
          </div>
      </header>

      <Dialog open={showWorkspaceModal} onOpenChange={(open) => { if (!open) setShowWorkspaceModal(false); }}>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>New Workspace</DialogTitle>
          </DialogHeader>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            <div>
              <Label>Workspace ID</Label>
              <Input
                placeholder="my-workspace"
                value={wsFormId}
                onChange={e => setWsFormId(e.target.value.replace(/[^a-zA-Z0-9-]/g, ''))}
                style={{ width: '100%', boxSizing: 'border-box' }}
                autoFocus
              />
              <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: '0.2rem' }}>
                Letters, numbers, and hyphens only
              </div>
            </div>
            <div>
              <Label>Display Name <span style={{ color: 'var(--text-tertiary)' }}>(optional)</span></Label>
              <Input
                placeholder="My Workspace"
                value={wsFormName}
                onChange={e => setWsFormName(e.target.value)}
                style={{ width: '100%', boxSizing: 'border-box' }}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setShowWorkspaceModal(false)}>Cancel</Button>
            <Button
              onClick={handleCreateWorkspace}
              disabled={!wsFormId.trim() || wsCreating}
              style={{ borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}
            >
              {wsCreating ? 'Creating...' : 'Create'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
