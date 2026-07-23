import type { SimulationState, NodeKind, ProducerNode, TopicNode, ConsumerNode, SimNode } from '../types';
import { generateId, getTopics, getConsumers } from '../simulation/state';
import { rebalanceConsumers } from '../simulation/rebalance';

export function addNode(state: SimulationState, kind: NodeKind, x: number, y: number): SimNode {
  const topics = getTopics(state);

  if ((kind === 'producer' || kind === 'consumer') && topics.length === 0) {
    addNode(state, 'topic', x, y + 50);
  }

  const id = generateId(state, kind[0]);
  const position = { x, y };

  if (kind === 'producer') {
    const node: ProducerNode = {
      id, kind: 'producer', name: id.toUpperCase(),
      position, messageRate: 5, color: '#22c55e',
      _accumulator: 0, _roundRobinIndex: 0,
    };
    state.nodes.push(node);

    const nearestTopic = findNearest(getTopics(state), position);
    if (nearestTopic) {
      state.connections.push({
        id: generateId(state, 'conn'),
        sourceId: id,
        targetId: nearestTopic.id,
      });
    }
    return node;
  }

  if (kind === 'topic') {
    const node: TopicNode = {
      id, kind: 'topic', name: id.toUpperCase(),
      position, partitionCount: 2,
      partitions: [{ produced: 0, queued: 0 }, { produced: 0, queued: 0 }],
    };
    state.nodes.push(node);
    return node;
  }

  // consumer
  const node: ConsumerNode = {
    id, kind: 'consumer', name: id.toUpperCase(),
    position, processingDelay: 50, failureProbability: 0,
    crashed: false, assignments: [], consumed: 0, _timer: 0, _pullIndex: 0,
  };
  state.nodes.push(node);

  const nearestTopic = findNearest(getTopics(state), position);
  if (nearestTopic) {
    state.connections.push({
      id: generateId(state, 'conn'),
      sourceId: nearestTopic.id,
      targetId: id,
    });
    rebalanceConsumers(state, nearestTopic.id);
  }
  return node;
}

export function removeNode(state: SimulationState, nodeId: string): void {
  const node = state.nodes.find(n => n.id === nodeId);
  if (!node) return;

  state.connections = state.connections.filter(
    c => c.sourceId !== nodeId && c.targetId !== nodeId,
  );
  state.nodes = state.nodes.filter(n => n.id !== nodeId);

  if (node.kind === 'topic') {
    for (const consumer of getConsumers(state)) {
      consumer.assignments = consumer.assignments.filter(a => a.topicId !== nodeId);
    }
  }

  if (node.kind === 'consumer') {
    const topicIds = new Set(
      state.connections.filter(c => c.targetId === nodeId).map(c => c.sourceId),
    );
    for (const topicId of topicIds) {
      rebalanceConsumers(state, topicId);
    }
  }
}

function findNearest<T extends SimNode>(nodes: T[], pos: { x: number; y: number }): T | undefined {
  let best: T | undefined;
  let bestDist = Infinity;
  for (const node of nodes) {
    const dx = node.position.x - pos.x;
    const dy = node.position.y - pos.y;
    const dist = dx * dx + dy * dy;
    if (dist < bestDist) { bestDist = dist; best = node; }
  }
  return best;
}
