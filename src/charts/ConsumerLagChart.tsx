import { useRef, useEffect } from 'react';
import type { SimulationState } from '../types';
import { getConsumers } from '../simulation/state';
import { drawBackground, drawBars, drawTitle } from './chartUtils';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function ConsumerLagChart({ stateRef }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    let animId = 0;
    function draw() {
      const ctx = canvas!.getContext('2d');
      if (!ctx) return;

      const w = canvas!.width;
      const h = canvas!.height;
      drawBackground(ctx, w, h);

      const history = stateRef.current.metricsHistory;
      const consumers = getConsumers(stateRef.current);

      if (history.length === 0 || consumers.length === 0) {
        drawTitle(ctx, 'Consumer Lag', '—', w);
        animId = requestAnimationFrame(draw);
        return;
      }

      const latest = history[history.length - 1];
      const values = consumers.map(c => ({
        label: c.name,
        value: latest.consumerLag[c.id] ?? 0,
        color: c.crashed ? '#ef4444' : '#f59e0b',
      }));

      const maxVal = Math.max(1, ...values.map(v => v.value));
      drawBars(ctx, values, maxVal * 1.1, w, h - 16, 20);

      const totalLag = values.reduce((sum, v) => sum + v.value, 0);
      drawTitle(ctx, 'Consumer Lag', `${totalLag}`, w);

      animId = requestAnimationFrame(draw);
    }

    draw();
    return () => cancelAnimationFrame(animId);
  }, [stateRef]);

  return <canvas ref={canvasRef} width={260} height={110} className="w-full rounded" />;
}
