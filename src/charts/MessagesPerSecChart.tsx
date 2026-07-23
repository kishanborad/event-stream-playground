import { useRef, useEffect } from 'react';
import type { SimulationState } from '../types';
import { getProducers } from '../simulation/state';
import { drawBackground, drawLine, drawTitle } from './chartUtils';

const COLORS = ['#22c55e', '#10b981', '#14b8a6', '#06b6d4', '#0ea5e9'];

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
}

export default function MessagesPerSecChart({ stateRef }: Props) {
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
      const producers = getProducers(stateRef.current);

      if (history.length < 2) {
        drawTitle(ctx, 'Messages/sec', '—', w);
        animId = requestAnimationFrame(draw);
        return;
      }

      let maxVal = 1;
      const seriesMap = new Map<string, number[]>();

      for (const producer of producers) {
        const series: number[] = [];
        for (const snap of history) {
          const val = snap.producedPerSec[producer.id] ?? 0;
          series.push(val);
          if (val > maxVal) maxVal = val;
        }
        seriesMap.set(producer.id, series);
      }

      let colorIdx = 0;
      let totalLatest = 0;
      for (const [, series] of seriesMap) {
        drawLine(ctx, series, maxVal * 1.1, w, h - 16, COLORS[colorIdx % COLORS.length], 20);
        totalLatest += series[series.length - 1] ?? 0;
        colorIdx++;
      }

      drawTitle(ctx, 'Messages/sec', `${totalLatest}`, w);

      animId = requestAnimationFrame(draw);
    }

    draw();
    return () => cancelAnimationFrame(animId);
  }, [stateRef]);

  return <canvas ref={canvasRef} width={260} height={110} className="w-full rounded" />;
}
