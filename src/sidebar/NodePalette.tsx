import type { NodeKind } from '../types';

const ITEMS: { kind: NodeKind; label: string; color: string }[] = [
  { kind: 'producer', label: 'Producer', color: '#22c55e' },
  { kind: 'topic', label: 'Topic', color: '#3b82f6' },
  { kind: 'consumer', label: 'Consumer', color: '#f59e0b' },
];

export default function NodePalette() {
  return (
    <div className="space-y-1.5">
      <div className="text-[10px] text-canvas-muted uppercase tracking-widest mb-3 font-medium">Drag to canvas</div>
      {ITEMS.map(item => (
        <div
          key={item.kind}
          draggable
          onDragStart={e => {
            e.dataTransfer.setData('node-kind', item.kind);
            e.dataTransfer.effectAllowed = 'copy';
          }}
          className="flex items-center gap-3 px-3 py-2 rounded-lg cursor-grab active:cursor-grabbing
                     hover:bg-white/5 border border-transparent hover:border-canvas-borderHover
                     transition-all duration-200 group"
        >
          <div
            className="w-4 h-4 rounded-full flex-shrink-0 shadow-lg group-hover:scale-110 transition-transform"
            style={{ backgroundColor: item.color, boxShadow: `0 0 8px ${item.color}40` }}
          />
          <span className="text-sm text-canvas-secondary group-hover:text-canvas-text transition-colors">{item.label}</span>
        </div>
      ))}
    </div>
  );
}
