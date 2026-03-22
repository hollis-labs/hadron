import { LayoutDashboard, FileText, GitBranch, Workflow, Activity as ActivityIcon, Clock, BarChart3, Settings, HelpCircle } from 'lucide-react';

export type NavPage = 'dashboard' | 'blueprints' | 'blueprintDetail' | 'blueprintWizard' | 'runs' | 'runDetail' | 'schedules' | 'pipelines' | 'pipelineDetail' | 'flowBuilder' | 'telemetry' | 'settings' | 'help';

interface AppNavProps {
  current: NavPage;
  onNavigate: (page: NavPage) => void;
}

const MAIN_NAV: { page: NavPage; label: string; icon: React.ReactNode; parents?: NavPage[] }[] = [
  { page: 'dashboard', label: 'Dashboard', icon: <LayoutDashboard size={18} /> },
  { page: 'blueprints', label: 'Blueprints', icon: <FileText size={18} />, parents: ['blueprintDetail', 'blueprintWizard'] },
  { page: 'pipelines', label: 'Pipelines', icon: <GitBranch size={18} />, parents: ['pipelineDetail'] },
  { page: 'flowBuilder', label: 'Flow Builder', icon: <Workflow size={18} /> },
  { page: 'runs', label: 'Runs', icon: <ActivityIcon size={18} />, parents: ['runDetail'] },
  { page: 'schedules', label: 'Schedules', icon: <Clock size={18} /> },
  { page: 'telemetry', label: 'Telemetry', icon: <BarChart3 size={18} /> },
];

const FOOTER_NAV: { page: NavPage; label: string; icon: React.ReactNode }[] = [
  { page: 'settings', label: 'Settings', icon: <Settings size={18} /> },
  { page: 'help', label: 'Help', icon: <HelpCircle size={18} /> },
];

function isActive(current: NavPage, item: { page: NavPage; parents?: NavPage[] }): boolean {
  return current === item.page || (item.parents?.includes(current) ?? false);
}

export function AppNav({ current, onNavigate }: AppNavProps) {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <polygon points="12 2 22 8.5 22 15.5 12 22 2 15.5 2 8.5 12 2"/>
          <line x1="12" y1="22" x2="12" y2="15.5"/>
          <polyline points="22 8.5 12 15.5 2 8.5"/>
          <polyline points="2 15.5 12 8.5 22 15.5"/>
          <line x1="12" y1="2" x2="12" y2="8.5"/>
        </svg>
      </div>
      <nav className="sidebar-nav">
        {MAIN_NAV.map(item => (
          <button
            key={item.page}
            className={`nav-item${isActive(current, item) ? ' active' : ''}`}
            onClick={() => onNavigate(item.page)}
          >
            {item.icon}
            <span className="tooltip">{item.label}</span>
          </button>
        ))}
      </nav>
      <div className="sidebar-footer">
        {FOOTER_NAV.map(item => (
          <button
            key={item.page}
            className={`nav-item${isActive(current, item) ? ' active' : ''}`}
            onClick={() => onNavigate(item.page)}
          >
            {item.icon}
            <span className="tooltip">{item.label}</span>
          </button>
        ))}
      </div>
    </aside>
  );
}
