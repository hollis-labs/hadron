import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';
import type { NavPage } from '../components/layout/AppNav';

interface NavigationContextValue {
  page: NavPage;
  navigate: (page: NavPage) => void;
  selectedRunId: string | null;
  selectedBlueprintPath: string | null;
  selectedPipelinePath: string | null;
  wizardEditPath: string | null;
  openRun: (runId: string) => void;
  openBlueprint: (path: string) => Promise<void>;
  openPipeline: (path: string) => void;
  openFlowBuilder: (path: string) => void;
  openWizard: (editPath?: string | null) => void;
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
  blueprintDetail: 'blueprints',
  blueprintWizard: 'blueprints',
  pipelineDetail: 'pipelines',
  flowBuilder: 'pipelines',
};

export function NavigationProvider({ children }: { children: ReactNode }) {
  const [page, setPage] = useState<NavPage>('dashboard');
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedBlueprintPath, setSelectedBlueprintPath] = useState<string | null>(null);
  const [selectedPipelinePath, setSelectedPipelinePath] = useState<string | null>(null);
  const [wizardEditPath, setWizardEditPath] = useState<string | null>(null);
  const navigate = useCallback((target: NavPage) => {
    if (target !== 'runDetail') setPage(target);
  }, []);

  const openRun = useCallback((runId: string) => {
    setSelectedRunId(runId);
    setPage('runDetail');
  }, []);

  const openBlueprint = useCallback(async (path: string) => {
    setSelectedBlueprintPath(path);
    setPage('blueprintDetail');
  }, []);

  const openPipeline = useCallback((path: string) => {
    setSelectedPipelinePath(path);
    setPage('pipelineDetail');
  }, []);

  const openFlowBuilder = useCallback((path: string) => {
    setSelectedPipelinePath(path);
    setPage('flowBuilder');
  }, []);

  const openWizard = useCallback((editPath: string | null = null) => {
    setWizardEditPath(editPath);
    setPage('blueprintWizard');
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
      selectedBlueprintPath,
      selectedPipelinePath,
      wizardEditPath,
      openRun,
      openBlueprint,
      openPipeline,
      openFlowBuilder,
      openWizard,
      goBack,
      refresh,
    }}>
      {children}
    </NavigationContext.Provider>
  );
}
