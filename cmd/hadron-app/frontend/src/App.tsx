import { lazy, Suspense, useEffect, useCallback } from 'react';
import { Toaster } from 'sonner';
import { DaemonProvider } from './contexts/DaemonContext';
import { NavigationProvider, useNavigation } from './contexts/NavigationContext';
import { AppHeader } from './components/layout/AppHeader';
import { AppNav, type NavPage } from './components/layout/AppNav';
import { AppFooter } from './components/layout/AppFooter';
import { Spinner } from './components/ui/Spinner';

// Wails runtime events are exposed on window.runtime in v2
declare global {
  interface Window {
    runtime?: {
      EventsOn: (event: string, callback: (data: unknown) => void) => () => void;
    };
    go?: {
      main?: {
        App?: {
          GetDaemonAddr: () => Promise<string>;
          GetDaemonStatus: () => Promise<string>;
        };
      };
    };
  }
}

const PAGE_TITLES: Record<NavPage, string> = {
  dashboard: 'Operations',
  blueprints: 'Blueprint Browser',
  blueprintDetail: 'Blueprint Detail',
  blueprintWizard: 'Blueprint Wizard',
  pipelines: 'Pipelines',
  pipelineDetail: 'Pipeline Detail',
  flowBuilder: 'Flow Builder',
  runs: 'Run Log',
  runDetail: 'Run Detail',
  schedules: 'Schedules',
  telemetry: 'Telemetry',
  settings: 'Settings',
  help: 'Help',
};

const DashboardPage = lazy(() => import('./pages/DashboardPage').then(m => ({ default: m.DashboardPage })));
const BlueprintsPage = lazy(() => import('./pages/BlueprintsPage').then(m => ({ default: m.BlueprintsPage })));
const BlueprintDetailPage = lazy(() => import('./pages/BlueprintDetailPage').then(m => ({ default: m.BlueprintDetailPage })));
const BlueprintWizardPage = lazy(() => import('./pages/BlueprintWizardPage').then(m => ({ default: m.BlueprintWizardPage })));
const PipelinesPage = lazy(() => import('./pages/PipelinesPage').then(m => ({ default: m.PipelinesPage })));
const PipelineDetailPage = lazy(() => import('./pages/PipelineDetailPage').then(m => ({ default: m.PipelineDetailPage })));
const FlowBuilderPage = lazy(() => import('./pages/FlowBuilderPage').then(m => ({ default: m.FlowBuilderPage })));
const RunsPage = lazy(() => import('./pages/RunsPage').then(m => ({ default: m.RunsPage })));
const RunDetailPage = lazy(() => import('./pages/RunDetailPage').then(m => ({ default: m.RunDetailPage })));
const SchedulerPage = lazy(() => import('./pages/SchedulerPage').then(m => ({ default: m.SchedulerPage })));
const TelemetryPage = lazy(() => import('./pages/TelemetryPage').then(m => ({ default: m.TelemetryPage })));
const SettingsPage = lazy(() => import('./pages/SettingsPage').then(m => ({ default: m.SettingsPage })));
const HelpPage = lazy(() => import('./pages/HelpPage').then(m => ({ default: m.HelpPage })));

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

    if (e.key === 'n' && !e.metaKey && !e.ctrlKey && nav.page === 'blueprints') {
      nav.openWizard();
      e.preventDefault();
    }

    if (e.key === '?' && !e.metaKey && !e.ctrlKey) {
      nav.navigate('help');
      e.preventDefault();
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
            {nav.page === 'dashboard' && <DashboardPage />}
            {nav.page === 'blueprints' && <BlueprintsPage />}
            {nav.page === 'blueprintDetail' && nav.selectedBlueprintPath && <BlueprintDetailPage />}
            {nav.page === 'blueprintWizard' && <BlueprintWizardPage />}
            {nav.page === 'pipelines' && <PipelinesPage />}
            {nav.page === 'pipelineDetail' && nav.selectedPipelinePath && <PipelineDetailPage />}
            {nav.page === 'flowBuilder' && <FlowBuilderPage />}
            {nav.page === 'runs' && <RunsPage />}
            {nav.page === 'schedules' && <SchedulerPage />}
            {nav.page === 'telemetry' && <TelemetryPage />}
            {nav.page === 'settings' && <SettingsPage />}
            {nav.page === 'help' && <HelpPage />}
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
