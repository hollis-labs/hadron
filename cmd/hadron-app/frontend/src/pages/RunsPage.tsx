import { useState } from 'react';
import { Activity, AlertTriangle, ArrowRight, Database, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useDaemon } from '@/contexts/DaemonContext';
import { useNavigation } from '@/contexts/NavigationContext';

const DEMO_SCENARIOS = [
  { id: 'workflow-active-003', label: 'Active' },
  { id: 'workflow-failed-002', label: 'Failed' },
  { id: 'workflow-waiting', label: 'Waiting' },
  { id: 'workflow-completed-001', label: 'Completed' },
];

export function RunsPage() {
  const daemon = useDaemon();
  const navigation = useNavigation();
  const [runID, setRunID] = useState('');

  const inspect = () => {
    const value = runID.trim();
    if (value) navigation.openRun(value);
  };

  return (
    <div className="flex h-full flex-col gap-4">
      <header className="flex items-center gap-3 border border-border bg-card px-4 py-3">
        <Activity size={17} className="text-blue-400" />
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.12em] text-muted-foreground">Durable graph runs</p>
          <h1 className="text-base font-semibold text-foreground">Inspect a workflow run by its opaque ID</h1>
        </div>
        <span className="ml-auto font-mono text-xs text-muted-foreground">{daemon.status}</span>
      </header>

      <section className="grid flex-1 place-items-center border border-border bg-card/40 p-6">
        <div className="grid w-full max-w-2xl gap-5">
          <div className="flex items-start gap-3">
            <Database size={22} className="mt-0.5 text-blue-400" />
            <div>
              <h2 className="text-lg font-semibold text-foreground">Run collection unavailable</h2>
              <p className="mt-1 max-w-xl text-sm leading-relaxed text-muted-foreground">
                W06-T02 exposes bounded inspection for a known graph run, but not a graph-native run collection. Legacy blueprint run rows are intentionally not substituted.
              </p>
            </div>
          </div>

          <div className="flex gap-2">
            <div className="relative flex-1">
              <Search size={14} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={runID}
                onChange={event => setRunID(event.target.value)}
                onKeyDown={event => { if (event.key === 'Enter') inspect(); }}
                placeholder="opaque workflow run ID (slashes are supported)"
                className="pl-9 font-mono"
                aria-label="Workflow run ID"
              />
            </div>
            <Button disabled={!runID.trim()} onClick={inspect}>Inspect <ArrowRight size={13} /></Button>
          </div>

          <div className="flex items-start gap-2 border border-amber-500/25 bg-amber-500/[0.05] p-3 text-xs leading-relaxed text-amber-100/75">
            <AlertTriangle size={14} className="mt-0.5 shrink-0 text-amber-400" />
            <span>Registry browsing and exposure-profile discovery remain unavailable until their shared daemon APIs land. No private file binding or alternate workflow model is used here.</span>
          </div>

          {daemon.demoMode && (
            <div className="grid gap-2 border-t border-border pt-4">
              <span className="font-mono text-[10px] uppercase tracking-[0.12em] text-muted-foreground">Acceptance fixtures</span>
              <div className="flex flex-wrap gap-2">
                {DEMO_SCENARIOS.map(scenario => (
                  <Button key={scenario.id} variant="outline" size="sm" onClick={() => navigation.openRun(scenario.id)}>{scenario.label}</Button>
                ))}
              </div>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
