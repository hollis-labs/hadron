import { lazy, Suspense, useEffect, useCallback } from 'react';
import { Toaster } from 'sonner';
import { DaemonProvider } from './contexts/DaemonContext';
import { NavigationProvider, useNavigation } from './contexts/NavigationContext';
import { AppHeader } from './components/layout/AppHeader';
import { AppNav, type NavPage } from './components/layout/AppNav';
import { AppFooter } from './components/layout/AppFooter';
import { Spinner } from './components/ui/Spinner';

const PAGE_TITLES: Record<NavPage, string> = {
  workflowCatalog: 'Workflow Registry',
  flowBuilder: 'Workflow Graph',
  runs: 'Run Log',
  runDetail: 'Run Detail',
};

const WorkflowCatalogPage = lazy(() => import('./pages/WorkflowCatalogPage').then(m => ({ default: m.WorkflowCatalogPage })));
const FlowBuilderPage = lazy(() => import('./pages/FlowBuilderPage').then(m => ({ default: m.FlowBuilderPage })));
const RunsPage = lazy(() => import('./pages/RunsPage').then(m => ({ default: m.RunsPage })));
const RunDetailPage = lazy(() => import('./pages/RunDetailPage').then(m => ({ default: m.RunDetailPage })));

function PageFallback() {
  return (
    <div className="flex h-full items-center justify-center text-muted-foreground">
      <Spinner size={16} />
    </div>
  );
}

function AppShell() {
  const nav = useNavigation();

  // Keyboard navigation
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (document.querySelector('[data-slot="dialog-overlay"], [data-slot="alert-dialog-overlay"]')) return;
    const tag = (e.target as HTMLElement)?.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;

    if (e.key === 'Escape') {
      nav.goBack();
      e.preventDefault();
    }

    if (e.key === 'r' && !e.metaKey && !e.ctrlKey) {
      nav.refresh();
    }

  }, [nav]);

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  return (
    <div className="app-shell">
      <AppNav current={nav.page} onNavigate={nav.navigate} />

      <div className="main">
        <AppHeader page={PAGE_TITLES[nav.page]} phase={nav.page} />

        <main className="content">
          <Suspense fallback={<PageFallback />}>
            {nav.page === 'workflowCatalog' && <WorkflowCatalogPage />}
            {nav.page === 'flowBuilder' && <FlowBuilderPage />}
            {nav.page === 'runs' && <RunsPage />}
            {nav.page === 'runDetail' && nav.selectedRunId && <RunDetailPage />}
          </Suspense>
        </main>

        <AppFooter phase={nav.page} />
      </div>

      <Toaster
        position="bottom-right"
        toastOptions={{
          style: {
            background: 'var(--bg-raised)',
            border: '1px solid var(--border-default)',
            color: 'var(--text-primary)',
            fontFamily: 'var(--font-ui)',
            fontSize: '13px',
          },
        }}
        theme="dark"
      />
    </div>
  );
}

export default function App() {
  return (
    <DaemonProvider>
      <NavigationProvider>
        <AppShell />
      </NavigationProvider>
    </DaemonProvider>
  );
}
