import type { SimulationState, ConsumerNode, TopicNode } from '../types';
import { findNode } from './state';

export function rebalanceConsumers(state: SimulationState, topicId: string): void {
  const topic = findNode<TopicNode>(state, topicId);
  if (!topic) return;

  const connectedConsumerIds = state.connections
    .filter(c => c.sourceId === topicId)
    .map(c => c.targetId);

  const activeConsumers = state.nodes.filter(
    (n): n is ConsumerNode =>
      n.kind === 'consumer' && connectedConsumerIds.includes(n.id) && !n.crashed,
  );

  // Clear existing assignments for this topic
  for (const consumer of state.nodes) {
    if (consumer.kind !== 'consumer') continue;
    (consumer as ConsumerNode).assignments = (consumer as ConsumerNode).assignments.filter(
      a => a.topicId !== topicId,
    );
  }

  if (activeConsumers.length === 0) return;

  // Distribute partitions round-robin
  for (let p = 0; p < topic.partitionCount; p++) {
    const consumer = activeConsumers[p % activeConsumers.length];
    consumer.assignments.push({ topicId, partitionIndex: p });
  }
}
