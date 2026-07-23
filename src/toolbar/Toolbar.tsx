import type { SimulationState } from '../types';
import { PRESETS } from '../simulation/presets';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
  currentPresetId: string;
}

export default function Toolbar({ stateRef, currentPresetId }: Props) {
  const state = stateRef.current;
  const preset = PRESETS.find(p => p.id === currentPresetId);

  return (
    <div className="h-11 bg-canvas-surface backdrop-blur-[12px] border-b border-canvas-border flex items-center px-5 gap-6 text-sm">
      <span className="font-medium text-canvas-text">{preset?.name ?? 'Custom'}</span>

      <div className="flex items-center gap-5 text-xs text-canvas-muted">
        <span>
          Produced <span className="text-canvas-producer font-mono ml-1">{state.counters.produced}</span>
        </span>
        <span>
          Consumed <span className="text-canvas-success font-mono ml-1">{state.counters.consumed}</span>
        </span>
        <span>
          Failed <span className="text-canvas-dlq font-mono ml-1">{state.counters.failed}</span>
        </span>
      </div>

      <div className="ml-auto">
        <button
          onClick={() => {
            stateRef.current.metricsHistory = [];
            stateRef.current._producedThisInterval = {};
            stateRef.current._metricsAccumulator = 0;
          }}
          className="px-3 py-1.5 text-xs bg-white/5 hover:bg-white/10 border border-canvas-border
                     hover:border-canvas-borderHover rounded-lg text-canvas-secondary
                     hover:text-canvas-text transition-all duration-200"
        >
          Clear Metrics
        </button>
      </div>
    </div>
  );
}
