import { describe, it, expect } from 'vitest';
import { createState, generateId, findNode, getProducers, getTopics, getConsumers } from '../simulation/state';
import type { PresetConfig, ProducerNode, TopicNode, ConsumerNode } from '../types';

const testPreset: PresetConfig = {
  id: 'test',
  name: 'Test',
  description: 'Test preset',
  build: () => ({
    nodes: [
      {
        id: 'p1', kind: 'producer', name: 'P1', position: { x: 0, y: 0 },
        messageRate: 5, color: '#22c55e', _accumulator: 0, _roundRobinIndex: 0,
      } as ProducerNode,
      {
        id: 't1', kind: 'topic', name: 'T1', position: { x: 0, y: 0 },
        partitionCount: 2, partitions: [{ produced: 0, queued: 0 }, { produced: 0, queued: 0 }],
      } as TopicNode,
      {
        id: 'c1', kind: 'consumer', name: 'C1', position: { x: 0, y: 0 },
        processingDelay: 50, failureProbability: 0, crashed: false,
        assignments: [{ topicId: 't1', partitionIndex: 0 }],
        consumed: 0, _timer: 0, _pullIndex: 0,
      } as ConsumerNode,
    ],
    connections: [
      { id: 'conn-1', sourceId: 'p1', targetId: 't1' },
      { id: 'conn-2', sourceId: 't1', targetId: 'c1' },
    ],
    scriptedEvents: [],
  }),
};

describe('createState', () => {
  it('creates state from preset with correct defaults', () => {
    const state = createState(testPreset);
    expect(state.nodes).toHaveLength(3);
    expect(state.connections).toHaveLength(2);
    expect(state.running).toBe(false);
    expect(state.timeScale).toBe(1);
    expect(state.elapsedTime).toBe(0);
    expect(state.counters).toEqual({ produced: 0, consumed: 0, failed: 0 });
    expect(state.retryQueue).toEqual([]);
    expect(state.dlqCount).toBe(0);
    expect(state.metricsHistory).toEqual([]);
  });
});

describe('generateId', () => {
  it('generates unique ids with prefix', () => {
    const state = createState(testPreset);
    const id1 = generateId(state, 'node');
    const id2 = generateId(state, 'node');
    expect(id1).toBe('node-100');
    expect(id2).toBe('node-101');
  });
});

describe('findNode', () => {
  it('finds a node by id', () => {
    const state = createState(testPreset);
    const node = findNode(state, 'p1');
    expect(node).toBeDefined();
    expect(node!.kind).toBe('producer');
  });

  it('returns undefined for missing id', () => {
    const state = createState(testPreset);
    expect(findNode(state, 'missing')).toBeUndefined();
  });
});

describe('node filters', () => {
  it('getProducers returns only producers', () => {
    const state = createState(testPreset);
    expect(getProducers(state)).toHaveLength(1);
    expect(getProducers(state)[0].kind).toBe('producer');
  });

  it('getTopics returns only topics', () => {
    const state = createState(testPreset);
    expect(getTopics(state)).toHaveLength(1);
  });

  it('getConsumers returns only consumers', () => {
    const state = createState(testPreset);
    expect(getConsumers(state)).toHaveLength(1);
  });
});
