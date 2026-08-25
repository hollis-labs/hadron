import { Activity as ActivityIcon, Workflow, BookOpenCheck } from 'lucide-react';

export type NavPage = 'workflowCatalog' | 'flowBuilder' | 'runs' | 'runDetail';

interface AppNavProps {
  current: NavPage;
  onNavigate: (page: NavPage) => void;
}

const MAIN_NAV: { page: NavPage; label: string; icon: React.ReactNode; parents?: NavPage[] }[] = [
  { page: 'workflowCatalog', label: 'Workflow Registry', icon: <BookOpenCheck size={18} /> },
  { page: 'flowBuilder', label: 'Workflow Graph', icon: <Workflow size={18} /> },
  { page: 'runs', label: 'Runs', icon: <ActivityIcon size={18} />, parents: ['runDetail'] },
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
            type="button"
            aria-label={item.label}
            aria-current={isActive(current, item) ? 'page' : undefined}
            className={`nav-item${isActive(current, item) ? ' active' : ''}`}
            onClick={() => onNavigate(item.page)}
          >
            {item.icon}
            <span className="tooltip">{item.label}</span>
          </button>
        ))}
      </nav>
    </aside>
  );
}
