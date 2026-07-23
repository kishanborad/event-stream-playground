import type { SimulationState, PresetConfig } from '../types';
import { PRESETS } from '../simulation/presets';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
  onPresetChange: (preset: PresetConfig) => void;
  currentPresetId: string;
}

export default function ControlPanel({ stateRef, onPresetChange, currentPresetId }: Props) {
  const state = stateRef.current;

  return (
    <div className="space-y-4">
      {/* Play / Pause */}
      <button
        onClick={() => { stateRef.current.running = !stateRef.current.running; }}
        className={`w-full px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 border ${
          state.running
            ? 'bg-red-500/10 border-red-500/30 text-red-400 hover:bg-red-500/20'
            : 'bg-canvas-accent/10 border-canvas-accent/30 text-canvas-accent hover:bg-canvas-accent/20'
        }`}
      >
        {state.running ? 'Pause' : 'Play'}
      </button>

      {/* Time Scale */}
      <div>
        <label className="text-[10px] text-canvas-muted uppercase tracking-widest block mb-2 font-medium">
          Speed: <span className="text-canvas-accent">{state.timeScale.toFixed(1)}x</span>
        </label>
        <input
          type="range"
          min={0.5}
          max={3}
          step={0.5}
          value={state.timeScale}
          onChange={e => { stateRef.current.timeScale = parseFloat(e.target.value); }}
          className="w-full accent-canvas-accent"
        />
      </div>

      {/* Preset Selector */}
      <div>
        <label className="text-[10px] text-canvas-muted uppercase tracking-widest block mb-2 font-medium">Scenario</label>
        <select
          value={currentPresetId}
          onChange={e => {
            const preset = PRESETS.find(p => p.id === e.target.value);
            if (preset) onPresetChange(preset);
          }}
          className="w-full bg-canvas-deep border border-canvas-border rounded-lg px-3 py-2 text-sm
                     text-canvas-secondary focus:border-canvas-accent/50 focus:outline-none transition-colors"
        >
          {PRESETS.map(p => (
            <option key={p.id} value={p.id}>{p.name}</option>
          ))}
        </select>
      </div>

      {/* Reset */}
      <button
        onClick={() => {
          const preset = PRESETS.find(p => p.id === currentPresetId) ?? PRESETS[0];
          onPresetChange(preset);
        }}
        className="w-full px-3 py-2 bg-white/5 hover:bg-white/10 border border-canvas-border
                   hover:border-canvas-borderHover rounded-lg text-sm text-canvas-secondary
                   hover:text-canvas-text transition-all duration-200"
      >
        Reset
      </button>
    </div>
  );
}
