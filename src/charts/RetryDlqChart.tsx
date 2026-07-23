import { useRef, useEffect } from 'react';
import type { SimulationState } from '../types';
import { drawBackground, drawStackedArea, drawTitle } from './chartUtils';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function RetryDlqChart({ stateRef }: Props) {
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

      if (history.length < 2) {
        drawTitle(ctx, 'Retry / DLQ', '—', w);
        animId = requestAnimationFrame(draw);
        return;
      }

      const retrySeries = history.map(s => s.retryDepth);
      const dlqSeries = history.map(s => s.dlqDepth);
      const maxVal = Math.max(1, ...retrySeries.map((r, i) => r + (dlqSeries[i] ?? 0)));

      drawStackedArea(ctx, retrySeries, dlqSeries, maxVal * 1.1, w, h - 16, '#f97316', '#ef4444', 20);

      const latest = history[history.length - 1];
      drawTitle(ctx, 'Retry / DLQ', `${latest.retryDepth} / ${latest.dlqDepth}`, w);

      animId = requestAnimationFrame(draw);
    }

    draw();
    return () => cancelAnimationFrame(animId);
  }, [stateRef]);

  return <canvas ref={canvasRef} width={260} height={110} className="w-full rounded" />;
}
