import type { SimulationState, Particle, Position } from '../types';

const NODE_RADIUS = 24;
const TOPIC_W = 80;
const TOPIC_H = 40;

const COLORS = {
  producer: '#22c55e',
  topic: '#6366f1',
  consumer: '#f59e0b',
  crashed: '#ef4444',
  connection: 'rgba(99, 102, 241, 0.2)',
  connectionGlow: 'rgba(99, 102, 241, 0.05)',
  text: '#f4f4f6',
  textMuted: '#aaa6c3',
  bg: '#050816',
  partition: 'rgba(255, 255, 255, 0.1)',
  nodeGlow: 'rgba(99, 102, 241, 0.15)',
};

export function renderFrame(
  ctx: CanvasRenderingContext2D,
  state: SimulationState,
  particles: Particle[],
  w: number,
  h: number,
): void {
  ctx.clearRect(0, 0, w, h);

  for (const conn of state.connections) {
    const src = state.nodes.find(n => n.id === conn.sourceId);
    const tgt = state.nodes.find(n => n.id === conn.targetId);
    if (src && tgt) drawConnection(ctx, src.position, tgt.position);
  }

  for (const particle of particles) {
    drawParticle(ctx, state, particle);
  }

  for (const node of state.nodes) {
    drawNode(ctx, node);
  }
}

function drawConnection(ctx: CanvasRenderingContext2D, from: Position, to: Position): void {
  const cpOffset = (to.x - from.x) * 0.4;
  ctx.beginPath();
  ctx.moveTo(from.x, from.y);
  ctx.bezierCurveTo(from.x + cpOffset, from.y, to.x - cpOffset, to.y, to.x, to.y);
  ctx.strokeStyle = COLORS.connection;
  ctx.lineWidth = 2;
  ctx.stroke();
}

function drawNode(ctx: CanvasRenderingContext2D, node: SimulationState['nodes'][number]): void {
  const { x, y } = node.position;

  if (node.kind === 'topic') {
    ctx.fillStyle = COLORS.topic;
    ctx.beginPath();
    ctx.roundRect(x - TOPIC_W / 2, y - TOPIC_H / 2, TOPIC_W, TOPIC_H, 6);
    ctx.fill();

    // Partition lines
    const pCount = node.partitionCount;
    if (pCount > 1) {
      ctx.strokeStyle = COLORS.partition;
      ctx.lineWidth = 1;
      const slotW = TOPIC_W / pCount;
      for (let i = 1; i < pCount; i++) {
        const lx = x - TOPIC_W / 2 + slotW * i;
        ctx.beginPath();
        ctx.moveTo(lx, y - TOPIC_H / 2 + 4);
        ctx.lineTo(lx, y + TOPIC_H / 2 - 4);
        ctx.stroke();
      }
    }

    ctx.fillStyle = COLORS.text;
    ctx.font = '500 11px Poppins, system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(node.name, x, y);
  } else {
    const isCrashed = node.kind === 'consumer' && node.crashed;
    const color = isCrashed ? COLORS.crashed : COLORS[node.kind];

    ctx.beginPath();
    ctx.arc(x, y, NODE_RADIUS, 0, Math.PI * 2);
    ctx.fillStyle = color;
    ctx.fill();

    if (isCrashed) {
      ctx.strokeStyle = '#fca5a5';
      ctx.lineWidth = 2;
      ctx.setLineDash([4, 4]);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.fillStyle = COLORS.text;
    ctx.font = '500 12px Poppins, system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(node.name, x, y);
  }
}

export function bezierPoint(from: Position, to: Position, t: number): Position {
  const cpOffset = (to.x - from.x) * 0.4;
  const cp1x = from.x + cpOffset;
  const cp1y = from.y;
  const cp2x = to.x - cpOffset;
  const cp2y = to.y;

  const u = 1 - t;
  return {
    x: u * u * u * from.x + 3 * u * u * t * cp1x + 3 * u * t * t * cp2x + t * t * t * to.x,
    y: u * u * u * from.y + 3 * u * u * t * cp1y + 3 * u * t * t * cp2y + t * t * t * to.y,
  };
}

function drawParticle(
  ctx: CanvasRenderingContext2D,
  state: SimulationState,
  particle: Particle,
): void {
  const conn = state.connections.find(c => c.id === particle.connectionId);
  if (!conn) return;

  const src = state.nodes.find(n => n.id === conn.sourceId);
  const tgt = state.nodes.find(n => n.id === conn.targetId);
  if (!src || !tgt) return;

  const pos = bezierPoint(src.position, tgt.position, particle.progress);

  ctx.beginPath();
  ctx.arc(pos.x, pos.y, 5, 0, Math.PI * 2);
  ctx.fillStyle = particle.color;
  ctx.fill();
}
