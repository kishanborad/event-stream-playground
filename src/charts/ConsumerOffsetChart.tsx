import { useRef, useEffect } from 'react';
import type { SimulationState } from '../types';
import { getConsumers } from '../simulation/state';
import { drawBackground, drawLine, drawTitle } from './chartUtils';

const COLORS = ['#f59e0b', '#f97316', '#ef4444', '#ec4899', '#a855f7'];

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function ConsumerOffsetChart({ stateRef }: Props) {
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

      if (history.length < 2 || consumers.length === 0) {
        drawTitle(ctx, 'Consumer Offset', '—', w);
        animId = requestAnimationFrame(draw);
        return;
      }

      let maxVal = 1;
      let colorIdx = 0;

      for (const consumer of consumers) {
        const series = history.map(snap => snap.consumerOffsets[consumer.id] ?? 0);
        const localMax = Math.max(...series);
        if (localMax > maxVal) maxVal = localMax;
        drawLine(ctx, series, maxVal * 1.1, w, h - 16, COLORS[colorIdx % COLORS.length], 20);
        colorIdx++;
      }

      drawTitle(ctx, 'Consumer Offset', `${consumers.length} consumers`, w);

      animId = requestAnimationFrame(draw);
    }

    draw();
    return () => cancelAnimationFrame(animId);
  }, [stateRef]);

  return <canvas ref={canvasRef} width={260} height={110} className="w-full rounded" />;
}
