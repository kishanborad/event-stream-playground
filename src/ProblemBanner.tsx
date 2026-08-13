import { useState } from 'react';

export function ProblemBanner() {
  const [dismissed, setDismissed] = useState(false);
  if (dismissed) return null;

  return (
    <div className="flex items-center justify-between gap-4 px-4 py-2 bg-white/[0.04] border-b border-white/[0.08] text-[11px] text-canvas-text/60">
      <div className="flex items-center gap-3 min-w-0">
        <span className="shrink-0 px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 font-medium uppercase tracking-wider text-[10px]">Free</span>
        <span className="truncate">
          Learning Kafka usually means spinning up a cluster. Confluent courses cost $47/mo. This simulates producers, consumers, partitions, backpressure, and failure recovery — all in your browser. Drag-and-drop topology, no Docker required.
        </span>
      </div>
      <button onClick={() => setDismissed(true)} className="shrink-0 text-canvas-text/40 hover:text-canvas-text/80 cursor-pointer" aria-label="Dismiss">✕</button>
    </div>
  );
}
