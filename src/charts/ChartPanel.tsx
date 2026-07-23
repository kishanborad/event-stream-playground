import type { SimulationState } from '../types';
import MessagesPerSecChart from './MessagesPerSecChart';
import ConsumerLagChart from './ConsumerLagChart';
import ConsumerOffsetChart from './ConsumerOffsetChart';
import RetryDlqChart from './RetryDlqChart';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function ChartPanel({ stateRef }: Props) {
  return (
    <div className="w-72 flex-shrink-0 bg-canvas-surface backdrop-blur-[12px] border-l border-canvas-border flex flex-col shadow-glass">
      <div className="p-4 text-xs font-semibold tracking-widest text-canvas-accent uppercase border-b border-canvas-border">
        Metrics
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-3">
        <MessagesPerSecChart stateRef={stateRef} />
        <ConsumerLagChart stateRef={stateRef} />
        <ConsumerOffsetChart stateRef={stateRef} />
        <RetryDlqChart stateRef={stateRef} />
      </div>
    </div>
  );
}
