import { describe, it, expect } from 'vitest';
import { tick } from '../simulation/engine';
import { createState } from '../simulation/state';
import type { PresetConfig, ProducerNode, TopicNode, ConsumerNode } from '../types';

function makePreset(overrides?: {
  messageRate?: number;
  partitionCount?: number;
  processingDelay?: number;
  failureProbability?: number;
  consumerCount?: number;
}): PresetConfig {
  const rate = overrides?.messageRate ?? 10;
  const partitions = overrides?.partitionCount ?? 2;
  const delay = overrides?.processingDelay ?? 0;
  const failure = overrides?.failureProbability ?? 0;
  const consumers = overrides?.consumerCount ?? 1;

  return {
    id: 'test', name: 'Test', description: '',
    build: () => {
      const partitionStates = Array.from({ length: partitions }, () => ({ produced: 0, queued: 0 }));
      const consumerNodes: ConsumerNode[] = Array.from({ length: consumers }, (_, i) => ({
        id: `c${i + 1}`, kind: 'consumer', name: `C${i + 1}`,
        position: { x: 0, y: 0 }, processingDelay: delay,
        failureProbability: failure, crashed: false,
        assignments: [], consumed: 0, _timer: 0, _pullIndex: 0,
      }));

      // Assign partitions round-robin to consumers
      for (let p = 0; p < partitions; p++) {
        consumerNodes[p % consumers].assignments.push({ topicId: 't1', partitionIndex: p });
      }

      return {
        nodes: [
          {
            id: 'p1', kind: 'producer', name: 'P1', position: { x: 0, y: 0 },
            messageRate: rate, color: '#22c55e', _accumulator: 0, _roundRobinIndex: 0,
          } as ProducerNode,
          {
            id: 't1', kind: 'topic', name: 'T1', position: { x: 0, y: 0 },
            partitionCount: partitions, partitions: partitionStates,
          } as TopicNode,
          ...consumerNodes,
        ],
        connections: [
          { id: 'conn-1', sourceId: 'p1', targetId: 't1' },
          ...consumerNodes.map((c, i) => ({ id: `conn-c${i}`, sourceId: 't1', targetId: c.id })),
        ],
        scriptedEvents: [],
      };
    },
  };
}

describe('tick', () => {
  it('does nothing when paused', () => {
    const state = createState(makePreset());
    state.running = false;
    tick(state, 100);
    expect(state.counters.produced).toBe(0);
    expect(state.elapsedTime).toBe(0);
  });

  it('advances elapsed time when running', () => {
    const state = createState(makePreset());
    state.running = true;
    tick(state, 100);
    expect(state.elapsedTime).toBe(100);
  });

  it('scales elapsed time by timeScale', () => {
    const state = createState(makePreset());
    state.running = true;
    state.timeScale = 2;
    tick(state, 100);
    expect(state.elapsedTime).toBe(200);
  });

  it('produces messages at configured rate', () => {
    const state = createState(makePreset({ messageRate: 10 }));
    state.running = true;
    // 10 msg/sec * 1 sec = 10 messages
    tick(state, 1000);
    expect(state.counters.produced).toBe(10);
  });

  it('distributes messages round-robin across partitions', () => {
    const state = createState(makePreset({ messageRate: 4, partitionCount: 2 }));
    state.running = true;
    tick(state, 1000);
    const topic = state.nodes.find(n => n.id === 't1') as TopicNode;
    expect(topic.partitions[0].produced).toBe(2);
    expect(topic.partitions[1].produced).toBe(2);
  });

  it('consumers process messages from assigned partitions', () => {
    const state = createState(makePreset({ messageRate: 10, processingDelay: 0 }));
    state.running = true;
    tick(state, 1000);
    // With 0 delay, consumer processes all queued messages instantly
    expect(state.counters.consumed).toBe(10);
    const topic = state.nodes.find(n => n.id === 't1') as TopicNode;
    expect(topic.partitions[0].queued).toBe(0);
    expect(topic.partitions[1].queued).toBe(0);
  });

  it('emits produced and consumed events', () => {
    const state = createState(makePreset({ messageRate: 2, processingDelay: 0 }));
    state.running = true;
    tick(state, 1000);
    const producedEvents = state.events.filter(e => e.type === 'produced');
    const consumedEvents = state.events.filter(e => e.type === 'consumed');
    expect(producedEvents).toHaveLength(2);
    expect(consumedEvents).toHaveLength(2);
  });

  it('does not consume when consumer is crashed', () => {
    const state = createState(makePreset({ messageRate: 5, processingDelay: 0 }));
    const consumer = state.nodes.find(n => n.id === 'c1') as ConsumerNode;
    consumer.crashed = true;
    state.running = true;
    tick(state, 1000);
    expect(state.counters.produced).toBe(5);
    expect(state.counters.consumed).toBe(0);
  });

  it('records metrics snapshots at intervals', () => {
    const state = createState(makePreset({ messageRate: 5 }));
    state.running = true;
    tick(state, 1000);
    expect(state.metricsHistory.length).toBeGreaterThanOrEqual(1);
  });
});
