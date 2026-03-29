import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';
import { toast } from 'sonner';
import { setBaseURL, listWorkspaces, createWorkspace as apiCreateWorkspace, getPreference, setPreference, listRuns } from '../api/client';
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
  const [address, setAddress] = useState('127.0.0.1:8095');
  const [status, setStatus] = useState('stopped');
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

  // On mount: grab daemon addr from Go binding and listen for status events
  useEffect(() => {
    const app = window.go?.main?.App;
    if (app?.GetDaemonAddr) {
      app.GetDaemonAddr().then(addr => {
        if (addr) {
          setAddress(addr);
          setBaseURL(`http://${addr}`);
        }
      });
      app.GetDaemonStatus?.().then(s => {
        if (s) setStatus(s);
      });
    } else {
      // Dev mode: assume daemon is up at default addr
      setStatus('running');
    }

    const unsubscribe = window.runtime?.EventsOn('daemon:status', (data: unknown) => {
      const payload = data as { status: string; addr?: string };
      setStatus(payload.status);
      if (payload.addr) {
        setAddress(payload.addr);
        setBaseURL(`http://${payload.addr}`);
      }
    });

    return () => { if (typeof unsubscribe === 'function') unsubscribe(); };
  }, []);

  // When daemon comes up: fetch workspaces and restore last workspace
  useEffect(() => {
    if (status !== 'running') return;
    listWorkspaces().then(res => setWorkspaces(res.items ?? [])).catch(() => {});
    getPreference('lastWorkspaceId').then(id => { if (id) setWorkspaceId(id); }).catch(() => {});
  }, [status]);

  // Poll for active runs to show timer in header
  useEffect(() => {
    if (status !== 'running') { setActiveRunStartedAt(null); return; }
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
  }, [status]);

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
