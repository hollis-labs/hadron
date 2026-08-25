import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';
import { toast } from 'sonner';
import { createWorkspace as apiCreateWorkspace, getHealth, getPreference, listWorkspaces, setPreference } from '../api/client';
import { setDemoMode, isDemoMode } from '../demo/demoMode';
import type { Workspace } from '../api/types';

interface DaemonContextValue {
  status: string;
  address: string;
  workspaceId: string;
  workspaces: Workspace[];
  selectWorkspace: (id: string) => void;
  createWorkspace: (id: string, name: string) => Promise<void>;
  activeRunStartedAt: string | null;
  demoMode: boolean;
  toggleDemo: () => void;
}

const DaemonContext = createContext<DaemonContextValue | null>(null);

export function useDaemon() {
  const ctx = useContext(DaemonContext);
  if (!ctx) throw new Error('useDaemon must be used within DaemonProvider');
  return ctx;
}

export function DaemonProvider({ children }: { children: ReactNode }) {
  const [address] = useState(() => globalThis.location?.host || 'same-origin');
  const [status, setStatus] = useState('connecting');
  const [workspaceId, setWorkspaceId] = useState('default');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [activeRunStartedAt, setActiveRunStartedAt] = useState<string | null>(null);
  const [demoMode, setDemoEnabled] = useState(isDemoMode());

  const toggleDemo = () => {
    const next = !demoMode;
    setDemoEnabled(next);
    setDemoMode(next);
    if (next) setStatus('running');
  };

  // The production SPA is served by hadrond, so daemon health is a normal
  // same-origin HTTP capability. Polling is also used by the Vite proxy in dev.
  useEffect(() => {
    let cancelled = false;
    const poll = () => getHealth()
      .then(() => { if (!cancelled) setStatus('running'); })
      .catch(() => { if (!cancelled) setStatus('unavailable'); });
    poll();
    const timer = setInterval(poll, 3_000);
    return () => { cancelled = true; clearInterval(timer); };
  }, []);

  // When daemon comes up: fetch workspaces and restore last workspace
  useEffect(() => {
    if (status !== 'running') return;
    listWorkspaces().then(res => setWorkspaces(res.items ?? [])).catch(() => {});
    getPreference('lastWorkspaceId').then(id => { if (id) setWorkspaceId(id); }).catch(() => {});
  }, [status]);

  // W06-T02 does not expose a workflow-run collection. Do not derive graph
  // activity from the legacy run-list model.
  useEffect(() => setActiveRunStartedAt(null), []);

  const selectWorkspace = (id: string) => {
    setWorkspaceId(id);
    setPreference('lastWorkspaceId', id);
  };

  const handleCreateWorkspace = async (id: string, name: string) => {
    await apiCreateWorkspace(id, name);
    const res = await listWorkspaces();
    setWorkspaces(res.items ?? []);
    selectWorkspace(id);
    toast.success('Workspace created');
  };

  return (
    <DaemonContext.Provider value={{
      status,
      address,
      workspaceId,
      workspaces,
      selectWorkspace,
      createWorkspace: handleCreateWorkspace,
      activeRunStartedAt,
      demoMode,
      toggleDemo,
    }}>
      {children}
    </DaemonContext.Provider>
  );
}
