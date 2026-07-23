import { describe, it, expect, vi } from 'vitest';
import { processRetryQueue } from '../simulation/retry';
import { createState } from '../simulation/state';
import type { PresetConfig, TopicNode, ConsumerNode } from '../types';

function makeRetryPreset(): PresetConfig {
  return {
    id: 'test', name: 'Test', description: '',
    build: () => ({
      nodes: [
        {
          id: 'c1', kind: 'consumer', name: 'C1', position: { x: 0, y: 0 },
          processingDelay: 50, failureProbability: 0, crashed: false,
          assignments: [{ topicId: 't1', partitionIndex: 0 }],
          consumed: 0, _timer: 0, _pullIndex: 0,
        } as ConsumerNode,
        {
          id: 't1', kind: 'topic', name: 'T1', position: { x: 0, y: 0 },
          partitionCount: 1, partitions: [{ produced: 5, queued: 0 }],
        } as TopicNode,
      ],
      connections: [],
      scriptedEvents: [],
    }),
  };
}

describe('processRetryQueue', () => {
  it('retries messages and increments consumed on success', () => {
    const state = createState(makeRetryPreset());
    state.running = true;
    vi.spyOn(Math, 'random').mockReturnValue(0.99);
    state.retryQueue.push({
      consumerId: 'c1', topicId: 't1', partitionIndex: 0, retryCount: 1,
    });

    processRetryQueue(state);

    expect(state.retryQueue).toHaveLength(0);
    expect(state.counters.consumed).toBe(1);
    vi.restoreAllMocks();
  });

  it('increments retryCount on failure and keeps in queue', () => {
    const state = createState(makeRetryPreset());
    state.running = true;
    const consumer = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    consumer.failureProbability = 1;
    state.retryQueue.push({
      consumerId: 'c1', topicId: 't1', partitionIndex: 0, retryCount: 1,
    });

    processRetryQueue(state);

    expect(state.retryQueue).toHaveLength(1);
    expect(state.retryQueue[0].retryCount).toBe(2);
  });

  it('moves to DLQ after 3 retries', () => {
    const state = createState(makeRetryPreset());
    state.running = true;
    const consumer = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    consumer.failureProbability = 1;
    state.retryQueue.push({
      consumerId: 'c1', topicId: 't1', partitionIndex: 0, retryCount: 3,
    });

    processRetryQueue(state);

    expect(state.retryQueue).toHaveLength(0);
    expect(state.dlqCount).toBe(1);
    expect(state.events.some(e => e.type === 'dlq')).toBe(true);
  });

  it('skips crashed consumers', () => {
    const state = createState(makeRetryPreset());
    state.running = true;
    const consumer = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    consumer.crashed = true;
    state.retryQueue.push({
      consumerId: 'c1', topicId: 't1', partitionIndex: 0, retryCount: 1,
    });

    processRetryQueue(state);

    expect(state.retryQueue).toHaveLength(1);
  });
});
