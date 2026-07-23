import type { SimulationState, ConsumerNode } from '../types';
import { findNode } from './state';

const MAX_RETRIES = 3;

export function processRetryQueue(state: SimulationState): void {
  const pending = [...state.retryQueue];
  state.retryQueue = [];

  for (const entry of pending) {
    const consumer = findNode<ConsumerNode>(state, entry.consumerId);
    if (!consumer || consumer.crashed) {
      state.retryQueue.push(entry);
      continue;
    }

    if (entry.retryCount >= MAX_RETRIES) {
      state.dlqCount++;
      state.events.push({ type: 'dlq', consumerId: entry.consumerId });
      continue;
    }

    const failed = consumer.failureProbability > 0 && Math.random() < consumer.failureProbability;
    if (failed) {
      state.retryQueue.push({ ...entry, retryCount: entry.retryCount + 1 });
      state.events.push({ type: 'retry', consumerId: entry.consumerId, topicId: entry.topicId });
    } else {
      consumer.consumed++;
      state.counters.consumed++;
      state.events.push({ type: 'consumed', consumerId: entry.consumerId, topicId: entry.topicId });
    }
  }
}
