import type { SimNode } from '../types';

const MARGIN_X = 0.15;
const CENTER_X = 0.5;
const RIGHT_X = 0.85;
const MARGIN_TOP = 80;
const SPACING = 110;

export function computeLayout(nodes: SimNode[], width: number, height: number): void {
  const producers = nodes.filter(n => n.kind === 'producer');
  const topics = nodes.filter(n => n.kind === 'topic');
  const consumers = nodes.filter(n => n.kind === 'consumer');

  layoutColumn(producers, width * MARGIN_X, height, MARGIN_TOP, SPACING);
  layoutColumn(topics, width * CENTER_X, height, MARGIN_TOP, SPACING);
  layoutColumn(consumers, width * RIGHT_X, height, MARGIN_TOP, SPACING);
}

function layoutColumn(
  nodes: SimNode[],
  x: number,
  canvasHeight: number,
  marginTop: number,
  spacing: number,
): void {
  if (nodes.length === 0) return;
  const totalHeight = (nodes.length - 1) * spacing;
  const startY = Math.max(marginTop, (canvasHeight - totalHeight) / 2);

  for (let i = 0; i < nodes.length; i++) {
    nodes[i].position = { x, y: startY + i * spacing };
  }
}

export function findNodeAt(
  nodes: SimNode[],
  x: number,
  y: number,
  radius: number = 30,
): SimNode | undefined {
  for (const node of nodes) {
    const dx = x - node.position.x;
    const dy = y - node.position.y;
    if (node.kind === 'topic') {
      if (Math.abs(dx) <= 45 && Math.abs(dy) <= 25) return node;
    } else {
      if (dx * dx + dy * dy <= radius * radius) return node;
    }
  }
  return undefined;
}
