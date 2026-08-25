import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';
import type { NavPage } from '../components/layout/AppNav';

interface NavigationContextValue {
  page: NavPage;
  navigate: (page: NavPage) => void;
  selectedRunId: string | null;
  openRun: (runId: string) => void;
  goBack: () => void;
  refresh: () => void;
}

const NavigationContext = createContext<NavigationContextValue | null>(null);

export function useNavigation() {
  const ctx = useContext(NavigationContext);
  if (!ctx) throw new Error('useNavigation must be used within NavigationProvider');
  return ctx;
}

// Map detail pages to their parent pages for back navigation
const PARENT_PAGE: Partial<Record<NavPage, NavPage>> = {
  runDetail: 'runs',
};

export function NavigationProvider({ children }: { children: ReactNode }) {
  const [page, setPage] = useState<NavPage>('workflowCatalog');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const navigate = useCallback((target: NavPage) => {
    if (target !== 'runDetail') setPage(target);
  }, []);

  const openRun = useCallback((runId: string) => {
    setSelectedRunId(runId);
    setPage('runDetail');
  }, []);

  const goBack = useCallback(() => {
    const parent = PARENT_PAGE[page];
    if (parent) setPage(parent);
  }, [page]);

  // Refresh — dispatches the existing custom event for now.
  // Pages already listen for 'hadron:refresh' via usePoll or addEventListener.
  // This will be replaced with a proper callback pattern in Phase 3.
  const refresh = useCallback(() => {
    window.dispatchEvent(new CustomEvent('hadron:refresh'));
  }, []);

  return (
    <NavigationContext.Provider value={{
      page,
      navigate,
      selectedRunId,
      openRun,
      goBack,
      refresh,
    }}>
      {children}
    </NavigationContext.Provider>
  );
}

type ArchivedNavPage = NavPage | 'blueprints' | 'blueprintDetail' | 'blueprintWizard' | 'pipelines' | 'pipelineDetail';

// useArchivedNavigation keeps dormant legacy page modules type-checkable while
// making their routes and actions unreachable from the active application
// contract. W06-T07 may delete these archive-only modules outright.
export function useArchivedNavigation() {
  const active = useNavigation();
  const unavailable = () => { throw new Error('legacy navigation is unavailable'); };
  return {
    ...active,
    navigate: (page: ArchivedNavPage) => {
      if (page === 'workflowCatalog' || page === 'flowBuilder' || page === 'runs' || page === 'runDetail') {
        active.navigate(page);
        return;
      }
      unavailable();
    },
    selectedBlueprintPath: null as string | null,
    selectedPipelinePath: null as string | null,
    wizardEditPath: null as string | null,
    openBlueprint: async (_path: string) => unavailable(),
    openPipeline: (_path: string) => unavailable(),
    openFlowBuilder: (_path: string) => unavailable(),
    openWizard: (_editPath?: string | null) => unavailable(),
  };
}
