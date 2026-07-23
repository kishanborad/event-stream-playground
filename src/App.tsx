import { useRef, useState, useCallback } from 'react';
import type { SimulationState } from './types';
import { createState } from './simulation/state';
import { PRESETS } from './simulation/presets';
import NodeGraph from './canvas/NodeGraph';

export default function App() {
  const stateRef = useRef<SimulationState>(createState(PRESETS[0]));
  const [, setRenderTick] = useState(0);

  const handleRenderTick = useCallback(() => {
    setRenderTick(t => t + 1);
  }, []);

  return (
    <div className="flex h-screen bg-canvas-bg text-canvas-text overflow-hidden">
      {/* Sidebar */}
      <div className="w-64 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-r border-canvas-border flex flex-col shadow-glass">
        <div className="p-4 text-sm font-semibold border-b border-canvas-border tracking-wide text-canvas-accent">
          Event Stream
        </div>
        <div className="flex-1 p-4 text-canvas-muted text-sm">
          <button
            className="px-3 py-1.5 bg-canvas-accentDim text-white rounded text-xs"
            onClick={() => { stateRef.current.running = !stateRef.current.running; }}
          >
            {stateRef.current.running ? 'Pause' : 'Play'}
          </button>
        </div>
      </div>

      {/* Canvas area */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Toolbar */}
        <div className="h-11 bg-canvas-surface backdrop-blur-[12px] border-b border-canvas-border flex items-center px-5 text-sm">
          <span className="font-medium text-canvas-text">{PRESETS[0].name}</span>
        </div>
        {/* Canvas */}
        <div className="flex-1 relative bg-canvas-deep">
          <NodeGraph stateRef={stateRef} onRenderTick={handleRenderTick} />
        </div>
      </div>

      {/* Charts panel */}
      <div className="w-72 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-l border-canvas-border flex flex-col shadow-glass">
        <div className="p-4 text-sm font-semibold border-b border-canvas-border tracking-wide text-canvas-accent">
          Metrics
        </div>
        <div className="flex-1 p-4 text-canvas-muted text-sm space-y-1">
          <div>Produced: {stateRef.current.counters.produced}</div>
          <div>Consumed: {stateRef.current.counters.consumed}</div>
          <div>Failed: {stateRef.current.counters.failed}</div>
        </div>
      </div>
    </div>
  );
}
