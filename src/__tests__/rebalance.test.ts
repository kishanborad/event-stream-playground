import { describe, it, expect } from 'vitest';
import { rebalanceConsumers } from '../simulation/rebalance';
import { createState } from '../simulation/state';
import type { PresetConfig, TopicNode, ConsumerNode } from '../types';

function makeRebalancePreset(consumerCount: number): PresetConfig {
  return {
    id: 'test', name: 'Test', description: '',
    build: () => {
      const consumers: ConsumerNode[] = Array.from({ length: consumerCount }, (_, i) => ({
        id: `c${i + 1}`, kind: 'consumer' as const, name: `C${i + 1}`,
        position: { x: 0, y: 0 }, processingDelay: 50, failureProbability: 0,
        crashed: false, assignments: [], consumed: 0, _timer: 0, _pullIndex: 0,
      }));

      return {
        nodes: [
          {
            id: 't1', kind: 'topic' as const, name: 'T1', position: { x: 0, y: 0 },
            partitionCount: 4, partitions: Array.from({ length: 4 }, () => ({ produced: 0, queued: 0 })),
          } as TopicNode,
          ...consumers,
        ],
        connections: consumers.map((c, i) => ({ id: `conn-${i}`, sourceId: 't1', targetId: c.id })),
        scriptedEvents: [],
      };
    },
  };
}

describe('rebalanceConsumers', () => {
  it('distributes partitions evenly across consumers', () => {
    const state = createState(makeRebalancePreset(2));
    rebalanceConsumers(state, 't1');

    const c1 = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    const c2 = state.nodes.find(n => n.id === 'c2') as ConsumerNode;
    expect(c1.assignments).toHaveLength(2);
    expect(c2.assignments).toHaveLength(2);
  });

  it('assigns all partitions even with uneven split', () => {
    const state = createState(makeRebalancePreset(3));
    rebalanceConsumers(state, 't1');

    const consumers = state.nodes.filter(n => n.kind === 'consumer') as ConsumerNode[];
    const totalAssigned = consumers.reduce((sum, c) => sum + c.assignments.length, 0);
    expect(totalAssigned).toBe(4);
  });

  it('skips crashed consumers', () => {
    const state = createState(makeRebalancePreset(3));
    const c3 = state.nodes.find(n => n.id === 'c3') as ConsumerNode;
    c3.crashed = true;

    rebalanceConsumers(state, 't1');

    expect(c3.assignments).toHaveLength(0);
    const c1 = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    const c2 = state.nodes.find(n => n.id === 'c2') as ConsumerNode;
    expect(c1.assignments.length + c2.assignments.length).toBe(4);
  });

  it('handles single consumer', () => {
    const state = createState(makeRebalancePreset(1));
    rebalanceConsumers(state, 't1');

    const c1 = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    expect(c1.assignments).toHaveLength(4);
  });
});
