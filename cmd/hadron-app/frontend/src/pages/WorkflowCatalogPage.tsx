import { useCallback, useEffect, useMemo, useState } from 'react';
import { BookOpenCheck, Check, CircleOff, Fingerprint, Search, ShieldCheck } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { inspectWorkflowExposure, inspectWorkflowVersion, searchWorkflowCatalog } from '@/api/workflows';
import type { AppworkflowWorkflowCatalogMatch, WorkflowExposureProfile, WorkflowVersionDetail } from '@/api/types';

type LoadState = 'idle' | 'loading' | 'ready' | 'error';

export function WorkflowCatalogPage() {
  const [query, setQuery] = useState('');
  const [namespace, setNamespace] = useState('');
  const [matches, setMatches] = useState<AppworkflowWorkflowCatalogMatch[]>([]);
  const [nextStep, setNextStep] = useState('draft_validate');
  const [searchState, setSearchState] = useState<LoadState>('idle');
  const [selected, setSelected] = useState<WorkflowVersionDetail | null>(null);
  const [profileID, setProfileID] = useState('');
  const [profile, setProfile] = useState<WorkflowExposureProfile | null>(null);
  const [notice, setNotice] = useState('');

  const loadCatalog = useCallback(async (queryValue: string, namespaceValue: string) => {
    setSearchState('loading');
    setNotice('');
    try {
      const result = await searchWorkflowCatalog(queryValue, namespaceValue, 30);
      setMatches(result.matches);
      setNextStep(result.next_step);
      setSearchState('ready');
    } catch (error) {
      setMatches([]);
      setSearchState('error');
      setNotice(error instanceof Error ? error.message : 'Workflow catalog is unavailable');
    }
  }, []);

  const searchCatalog = () => loadCatalog(query.trim(), namespace.trim());

  useEffect(() => {
    void loadCatalog('', '');
    const refresh = () => { void loadCatalog('', ''); };
    window.addEventListener('hadron:refresh', refresh);
    return () => window.removeEventListener('hadron:refresh', refresh);
  }, [loadCatalog]);

  const inspect = async (match: AppworkflowWorkflowCatalogMatch) => {
    setNotice('');
    try {
      setSelected(await inspectWorkflowVersion(match.definition));
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'Exact workflow version is unavailable');
    }
  };

  const inspectProfile = async () => {
    if (!profileID.trim()) return;
    setNotice('');
    try {
      setProfile(await inspectWorkflowExposure(profileID.trim()));
    } catch (error) {
      setProfile(null);
      setNotice(error instanceof Error ? error.message : 'Exposure profile is unavailable');
    }
  };

  const profilePinned = useMemo(() => {
    if (!selected || !profile) return false;
    return (profile.record.pins ?? []).some(pin =>
      pin.id === selected.descriptor.definition.id
      && pin.version === selected.descriptor.definition.version
      && pin.digest === selected.descriptor.definition.digest);
  }, [profile, selected]);

  return (
    <div className="grid h-full min-h-0 grid-cols-[minmax(22rem,0.82fr)_minmax(28rem,1.18fr)] gap-4 max-xl:grid-cols-1 max-xl:overflow-auto">
      <section className="flex min-h-0 flex-col border border-border bg-card">
        <header className="border-b border-border px-4 py-4">
          <div className="flex items-start gap-3">
            <BookOpenCheck size={18} className="mt-0.5 text-blue-400" />
            <div>
              <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-muted-foreground">Qualified workflow registry</p>
              <h1 className="mt-1 text-lg font-semibold text-foreground">Find one exact version, then act on evidence</h1>
            </div>
          </div>
          <div className="mt-4 grid grid-cols-[1fr_10rem_auto] gap-2 max-sm:grid-cols-1">
            <div className="relative">
              <Search size={14} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input value={query} onChange={event => setQuery(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') void searchCatalog(); }} placeholder="Describe the capability you need" className="pl-9" aria-label="Workflow search" />
            </div>
            <Input value={namespace} onChange={event => setNamespace(event.target.value)} placeholder="namespace" className="font-mono" aria-label="Workflow namespace" />
            <Button onClick={() => void searchCatalog()} disabled={searchState === 'loading'}>{searchState === 'loading' ? 'Searching…' : 'Search'}</Button>
          </div>
        </header>

        <div className="min-h-0 flex-1 overflow-auto">
          {matches.map(match => (
            <button key={[match.name, match.definition.version, match.definition.digest].join('@')} type="button" onClick={() => void inspect(match)} className="group block w-full border-b border-border px-4 py-3 text-left transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-400">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="truncate font-mono text-sm font-semibold text-foreground">{match.name}</p>
                  <p className="mt-1 line-clamp-2 text-xs leading-relaxed text-muted-foreground">{match.description || 'No public description.'}</p>
                </div>
                <span className="shrink-0 border border-border bg-background px-2 py-1 font-mono text-[10px] text-blue-300">score {match.score}</span>
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-1.5 font-mono text-[10px] text-muted-foreground">
                <span>{match.definition.version}</span><span>·</span>
                <span>{match.definition.digest?.slice(0, 18) ?? 'digest unavailable'}…</span>
                {match.registry.published && <span className="border border-emerald-500/30 px-1.5 py-0.5 text-emerald-300">published</span>}
                {match.registry.registry_pinned && <span className="border border-blue-500/30 px-1.5 py-0.5 text-blue-300">registry pin</span>}
              </div>
            </button>
          ))}
          {searchState === 'ready' && matches.length === 0 && (
            <div className="m-4 border border-dashed border-border p-5">
              <CircleOff size={18} className="text-muted-foreground" />
              <p className="mt-3 text-sm font-medium text-foreground">No qualified workflow fits this search.</p>
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">Next step: validate a graph-native draft, generate its contract scaffold, and register only after deterministic tests pass.</p>
              <p className="mt-3 font-mono text-[10px] uppercase tracking-[0.12em] text-blue-300">{nextStep.replace('_', ' ')}</p>
            </div>
          )}
        </div>
      </section>

      <section className="min-h-0 overflow-auto border border-border bg-card">
        {!selected ? (
          <div className="grid min-h-full place-items-center p-8 text-center">
            <div className="max-w-sm">
              <Fingerprint size={28} className="mx-auto text-blue-400" />
              <h2 className="mt-4 text-base font-semibold text-foreground">Select an immutable version</h2>
              <p className="mt-2 text-sm leading-relaxed text-muted-foreground">Schemas, effects, qualification evidence, and each independent registry or exposure state appear here.</p>
            </div>
          </div>
        ) : (
          <div>
            <header className="border-b border-border px-5 py-4">
              <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-muted-foreground">{selected.descriptor.namespace} / exact version</p>
              <h2 className="mt-1 break-all font-mono text-base font-semibold text-foreground">{selected.descriptor.name}@{selected.descriptor.version}</h2>
              <p className="mt-2 break-all font-mono text-[10px] text-muted-foreground">{selected.descriptor.digest}</p>
            </header>

            <div className="grid grid-cols-4 border-b border-border max-sm:grid-cols-2">
              <StateCell label="Current alias" active={selected.registry.current} />
              <StateCell label="Registry pin" active={selected.registry.registry_pinned} />
              <StateCell label="Published" active={selected.registry.published} />
              <StateCell label="Profile pin" active={profilePinned} pending={!profile} />
            </div>

            <div className="grid gap-5 p-5">
              <section>
                <div className="flex items-center gap-2 text-xs font-medium text-foreground"><ShieldCheck size={14} className="text-emerald-400" /> Qualification evidence</div>
                <dl className="mt-3 grid grid-cols-[9rem_1fr] gap-x-3 gap-y-2 border border-border bg-background/50 p-3 text-xs">
                  <dt className="text-muted-foreground">Tests</dt><dd className="font-mono text-emerald-300">{selected.descriptor.evidence.tests_passed ? 'passed' : 'not qualified'}</dd>
                  <dt className="text-muted-foreground">Plan digest</dt><dd className="break-all font-mono text-foreground">{selected.descriptor.evidence.plan_digest}</dd>
                  <dt className="text-muted-foreground">Suite digest</dt><dd className="break-all font-mono text-foreground">{selected.descriptor.evidence.contract_suite_digest}</dd>
                  <dt className="text-muted-foreground">Test digest</dt><dd className="break-all font-mono text-foreground">{selected.descriptor.evidence.contract_test_digest}</dd>
                </dl>
              </section>

              <section>
                <p className="text-xs font-medium text-foreground">Policy-visible effects</p>
                <div className="mt-2 flex flex-wrap gap-2">
                  {selected.descriptor.effects.map(effect => <span key={effect} className="border border-amber-500/25 bg-amber-500/[0.05] px-2 py-1 font-mono text-[10px] text-amber-200">{effect}</span>)}
                </div>
              </section>

              <section className="border-t border-border pt-4">
                <p className="text-xs font-medium text-foreground">Check one exposure profile</p>
                <div className="mt-2 flex gap-2">
                  <Input value={profileID} onChange={event => setProfileID(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') void inspectProfile(); }} placeholder="profile ID" className="font-mono" aria-label="Exposure profile ID" />
                  <Button variant="outline" onClick={() => void inspectProfile()} disabled={!profileID.trim()}>Inspect</Button>
                </div>
                {profile && <p className="mt-2 font-mono text-[10px] text-muted-foreground">generation {profile.generation} · {(profile.record.pins ?? []).length} exact pins</p>}
              </section>
            </div>
          </div>
        )}
        {notice && <div role="alert" className="m-5 border border-red-500/30 bg-red-500/[0.06] p-3 text-xs text-red-200">{notice}</div>}
      </section>
    </div>
  );
}

function StateCell({ label, active, pending = false }: { label: string; active: boolean; pending?: boolean }) {
  return (
    <div className="border-r border-border px-3 py-3 last:border-r-0">
      <p className="font-mono text-[9px] uppercase tracking-[0.12em] text-muted-foreground">{label}</p>
      <p className={['mt-2 flex items-center gap-1.5 text-xs font-medium', active ? 'text-emerald-300' : 'text-muted-foreground'].join(' ')}>
        {active ? <Check size={13} /> : <CircleOff size={13} />}
        {pending ? 'not checked' : active ? 'active' : 'inactive'}
      </p>
    </div>
  );
}
