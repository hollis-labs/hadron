import { LayoutDashboard, FolderOpen, List, CalendarClock, ChevronRight, GitBranch, Settings, HelpCircle, Activity, Workflow } from 'lucide-react';

export type NavPage = 'dashboard' | 'blueprints' | 'blueprintDetail' | 'blueprintWizard' | 'runs' | 'runDetail' | 'schedules' | 'pipelines' | 'pipelineDetail' | 'flowBuilder' | 'telemetry' | 'settings' | 'help';

interface AppNavProps {
  current: NavPage;
  onNavigate: (page: NavPage) => void;
}

const NAV_ITEMS: { page: NavPage; label: string; icon: React.ReactNode }[] = [
  { page: 'dashboard', label: 'Dashboard', icon: <LayoutDashboard size={15} /> },
  { page: 'blueprints', label: 'Blueprints', icon: <FolderOpen size={15} /> },
  { page: 'pipelines', label: 'Pipelines', icon: <GitBranch size={15} /> },
  { page: 'flowBuilder', label: 'Flow Builder', icon: <Workflow size={15} /> },
  { page: 'runs', label: 'Run Log', icon: <List size={15} /> },
  { page: 'schedules', label: 'Schedules', icon: <CalendarClock size={15} /> },
  { page: 'telemetry', label: 'Telemetry', icon: <Activity size={15} /> },
  { page: 'settings', label: 'Settings', icon: <Settings size={15} /> },
  { page: 'help', label: 'Help', icon: <HelpCircle size={15} /> },
];

export function AppNav({ current, onNavigate }: AppNavProps) {
  return (
    <nav className="app-sidebar">
      {NAV_ITEMS.map(({ page, label, icon }) => (
        <button
          key={page}
          className={`nav-item ${current === page || (current === 'runDetail' && page === 'runs') || (current === 'blueprintDetail' && page === 'blueprints') || (current === 'blueprintWizard' && page === 'blueprints') ? 'active' : ''}`}
          onClick={() => onNavigate(page)}
        >
          {icon}
          <span style={{ flex: 1 }}>{label}</span>
          {(current === page || (current === 'runDetail' && page === 'runs') || (current === 'blueprintDetail' && page === 'blueprints') || (current === 'blueprintWizard' && page === 'blueprints')) && (
            <ChevronRight size={12} />
          )}
        </button>
      ))}
    </nav>
  );
}
