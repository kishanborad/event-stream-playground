export type SidebarTab = 'visual' | 'code' | 'deploy';

interface Props {
  active: SidebarTab;
  onChange: (tab: SidebarTab) => void;
}

const TABS: { id: SidebarTab; label: string }[] = [
  { id: 'visual', label: 'Visual' },
  { id: 'code', label: 'Code' },
  { id: 'deploy', label: 'Deploy' },
];

export default function TabBar({ active, onChange }: Props) {
  return (
    <div className="flex border-b border-canvas-border">
      {TABS.map(tab => (
        <button
          key={tab.id}
          onClick={() => onChange(tab.id)}
          className={`flex-1 py-2.5 text-[11px] font-medium tracking-wide uppercase transition-all duration-200
            ${active === tab.id
              ? 'text-canvas-accent border-b-2 border-canvas-accent bg-canvas-accent/5'
              : 'text-canvas-muted hover:text-canvas-secondary hover:bg-white/[0.03]'
            }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
