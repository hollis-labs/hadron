import { useEffect, useCallback } from 'react';
import { Toaster } from 'sonner';
import { DaemonProvider, useDaemon } from './contexts/DaemonContext';
import { NavigationProvider, useNavigation } from './contexts/NavigationContext';
import { AppHeader } from './components/layout/AppHeader';
import { AppNav, type NavPage } from './components/layout/AppNav';
import { AppFooter } from './components/layout/AppFooter';
import { DashboardPage } from './pages/DashboardPage';
import { BlueprintsPage } from './pages/BlueprintsPage';
import { BlueprintDetailPage } from './pages/BlueprintDetailPage';
import { BlueprintWizardPage } from './pages/BlueprintWizardPage';
import { RunsPage } from './pages/RunsPage';
import { RunDetailPage } from './pages/RunDetailPage';
import { SchedulerPage } from './pages/SchedulerPage';
import { PipelinesPage } from './pages/PipelinesPage';
import { PipelineDetailPage } from './pages/PipelineDetailPage';
import { FlowBuilderPage } from './pages/FlowBuilderPage';
import { SettingsPage } from './pages/SettingsPage';
import { TelemetryPage } from './pages/TelemetryPage';
import { HelpPage } from './pages/HelpPage';

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
  dashboard: 'Dashboard',
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
