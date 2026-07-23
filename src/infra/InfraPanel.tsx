import { useState, useMemo } from 'react';
import type { SimulationState } from '../types';
import { generateDockerCompose } from './dockerCompose';
import { generateCliCommands } from './kafkaCli';

type InfraTab = 'docker' | 'cli';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function InfraPanel({ stateRef }: Props) {
  const [tab, setTab] = useState<InfraTab>('docker');

  const content = useMemo(() => {
    return tab === 'docker'
      ? generateDockerCompose(stateRef.current)
      : generateCliCommands(stateRef.current);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, stateRef.current.nodes.length, stateRef.current.connections.length]);

  const handleCopy = () => {
    navigator.clipboard.writeText(content);
  };

  return (
    <div className="flex flex-col h-full">
      {/* Sub-tabs */}
      <div className="flex border-b border-canvas-border">
        {([
          { id: 'docker' as const, label: 'Docker Compose' },
          { id: 'cli' as const, label: 'Kafka CLI' },
        ]).map(t => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`flex-1 py-2 text-[10px] font-medium uppercase tracking-wider transition-colors
              ${tab === t.id
                ? 'text-canvas-accent border-b border-canvas-accent'
                : 'text-canvas-muted hover:text-canvas-secondary'}`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Generated content */}
      <div className="flex-1 overflow-auto p-3">
        <pre className="text-[11px] text-canvas-secondary font-mono whitespace-pre leading-relaxed">
          {content}
        </pre>
      </div>

      {/* Copy button */}
      <div className="border-t border-canvas-border p-3">
        <button
          onClick={handleCopy}
          className="w-full py-2 rounded-lg text-sm font-medium transition-all duration-200
                     bg-white/5 border border-canvas-border text-canvas-secondary
                     hover:bg-white/10 hover:text-canvas-text"
        >
          Copy to Clipboard
        </button>
      </div>
    </div>
  );
}
