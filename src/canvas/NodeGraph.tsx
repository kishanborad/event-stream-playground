import { useRef, useEffect, useCallback } from 'react';
import type { SimulationState, Particle } from '../types';
import { PARTICLE_SPEED } from '../types';
import { tick } from '../simulation/engine';
import { renderFrame } from './renderer';
import { computeLayout } from './layout';

interface Props {
  stateRef: React.MutableRefObject<SimulationState>;
  onRenderTick: () => void;
}

export default function NodeGraph({ stateRef, onRenderTick }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const particlesRef = useRef<Particle[]>([]);
  const lastTimeRef = useRef(0);
  const frameCountRef = useRef(0);
  const nextParticleIdRef = useRef(0);
  const containerRef = useRef<HTMLDivElement>(null);

  const resize = useCallback(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;
    const { width, height } = container.getBoundingClientRect();
    canvas.width = width * devicePixelRatio;
    canvas.height = height * devicePixelRatio;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const ctx = canvas.getContext('2d');
    if (ctx) ctx.scale(devicePixelRatio, devicePixelRatio);
    computeLayout(stateRef.current.nodes, width, height);
  }, [stateRef]);

  useEffect(() => {
    resize();
    window.addEventListener('resize', resize);
    return () => window.removeEventListener('resize', resize);
  }, [resize]);

  useEffect(() => {
    let animId = 0;

    function loop(timestamp: number) {
      const canvas = canvasRef.current;
      const container = containerRef.current;
      if (!canvas || !container) { animId = requestAnimationFrame(loop); return; }

      const ctx = canvas.getContext('2d');
      if (!ctx) { animId = requestAnimationFrame(loop); return; }

      const delta = lastTimeRef.current ? timestamp - lastTimeRef.current : 16;
      lastTimeRef.current = timestamp;

      const state = stateRef.current;

      if (state.running) {
        tick(state, delta);
      }

      // Spawn particles from events
      for (const event of state.events) {
        if (event.type === 'produced') {
          const conn = state.connections.find(
            c => c.sourceId === event.producerId && c.targetId === event.topicId,
          );
          if (conn) {
            particlesRef.current.push({
              id: nextParticleIdRef.current++,
              connectionId: conn.id,
              progress: 0,
              color: '#3b82f6',
            });
          }
        } else if (event.type === 'consumed') {
          const conn = state.connections.find(
            c => c.sourceId === event.topicId && c.targetId === event.consumerId,
          );
          if (conn) {
            particlesRef.current.push({
              id: nextParticleIdRef.current++,
              connectionId: conn.id,
              progress: 0,
              color: '#22c55e',
            });
          }
        } else if (event.type === 'retry') {
          const conn = state.connections.find(
            c => c.sourceId === event.topicId && c.targetId === event.consumerId,
          );
          if (conn) {
            particlesRef.current.push({
              id: nextParticleIdRef.current++,
              connectionId: conn.id,
              progress: 0,
              color: '#f97316',
            });
          }
        } else if (event.type === 'dlq') {
          // DLQ particles flash red at consumer position — handled as a brief particle
          const consumerConns = state.connections.filter(c => c.targetId === event.consumerId);
          if (consumerConns.length > 0) {
            particlesRef.current.push({
              id: nextParticleIdRef.current++,
              connectionId: consumerConns[0].id,
              progress: 0.95,
              color: '#ef4444',
            });
          }
        }
      }
      state.events = [];

      // Update particles
      if (state.running) {
        const progressDelta = PARTICLE_SPEED * (delta * state.timeScale) / 1000;
        for (const p of particlesRef.current) {
          p.progress += progressDelta;
        }
        particlesRef.current = particlesRef.current.filter(p => p.progress < 1);
      }

      const { width, height } = container.getBoundingClientRect();
      renderFrame(ctx, state, particlesRef.current, width, height);

      frameCountRef.current++;
      if (frameCountRef.current % 6 === 0) {
        onRenderTick();
      }

      animId = requestAnimationFrame(loop);
    }

    animId = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(animId);
  }, [stateRef, onRenderTick]);

  return (
    <div ref={containerRef} className="w-full h-full">
      <canvas ref={canvasRef} className="block" />
    </div>
  );
}
