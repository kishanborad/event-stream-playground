import type {
  PresetConfig, ProducerNode, TopicNode, ConsumerNode,
  Connection, PartitionState,
} from '../types';

function pos(x: number, y: number) { return { x, y }; }

function makePartitions(count: number): PartitionState[] {
  return Array.from({ length: count }, () => ({ produced: 0, queued: 0 }));
}

function producer(id: string, name: string, rate: number, y: number): ProducerNode {
  return {
    id, kind: 'producer', name, position: pos(100, y),
    messageRate: rate, color: '#22c55e', _accumulator: 0, _roundRobinIndex: 0,
  };
}

function topic(id: string, name: string, partitions: number, y: number): TopicNode {
  return {
    id, kind: 'topic', name, position: pos(400, y),
    partitionCount: partitions, partitions: makePartitions(partitions),
  };
}

function consumer(
  id: string, name: string, delay: number, failure: number, y: number,
  assignments: { topicId: string; partitionIndex: number }[] = [],
): ConsumerNode {
  return {
    id, kind: 'consumer', name, position: pos(700, y),
    processingDelay: delay, failureProbability: failure, crashed: false,
    assignments, consumed: 0, _timer: 0, _pullIndex: 0,
  };
}

function conn(id: string, src: string, tgt: string): Connection {
  return { id, sourceId: src, targetId: tgt };
}

export const PRESETS: PresetConfig[] = [
  {
    id: 'happy-path',
    name: 'Happy Path',
    description: 'Balanced throughput — 2 producers, 3 partitions, 3 consumers, zero failures',
    build: () => ({
      nodes: [
        producer('p1', 'P1', 5, 150),
        producer('p2', 'P2', 5, 300),
        topic('t1', 'orders', 3, 225),
        consumer('c1', 'C1', 50, 0, 120, [{ topicId: 't1', partitionIndex: 0 }]),
        consumer('c2', 'C2', 50, 0, 225, [{ topicId: 't1', partitionIndex: 1 }]),
        consumer('c3', 'C3', 50, 0, 330, [{ topicId: 't1', partitionIndex: 2 }]),
      ],
      connections: [
        conn('c-p1t1', 'p1', 't1'), conn('c-p2t1', 'p2', 't1'),
        conn('c-t1c1', 't1', 'c1'), conn('c-t1c2', 't1', 'c2'), conn('c-t1c3', 't1', 'c3'),
      ],
      scriptedEvents: [],
    }),
  },
  {
    id: 'consumer-lag',
    name: 'Consumer Lag',
    description: "One slow consumer can't keep up — lag grows visibly",
    build: () => ({
      nodes: [
        producer('p1', 'P1', 10, 150),
        producer('p2', 'P2', 10, 300),
        topic('t1', 'events', 2, 225),
        consumer('c1', 'C1', 200, 0, 225, [
          { topicId: 't1', partitionIndex: 0 },
          { topicId: 't1', partitionIndex: 1 },
        ]),
      ],
      connections: [
        conn('c-p1t1', 'p1', 't1'), conn('c-p2t1', 'p2', 't1'),
        conn('c-t1c1', 't1', 'c1'),
      ],
      scriptedEvents: [],
    }),
  },
  {
    id: 'crash-rebalance',
    name: 'Crash & Rebalance',
    description: 'Consumer crashes after 10s, partitions rebalance, recovers at 20s',
    build: () => ({
      nodes: [
        producer('p1', 'P1', 5, 225),
        topic('t1', 'logs', 4, 225),
        consumer('c1', 'C1', 50, 0, 120, [
          { topicId: 't1', partitionIndex: 0 },
          { topicId: 't1', partitionIndex: 1 },
        ]),
        consumer('c2', 'C2', 50, 0, 225, [{ topicId: 't1', partitionIndex: 2 }]),
        consumer('c3', 'C3', 50, 0, 330, [{ topicId: 't1', partitionIndex: 3 }]),
      ],
      connections: [
        conn('c-p1t1', 'p1', 't1'),
        conn('c-t1c1', 't1', 'c1'), conn('c-t1c2', 't1', 'c2'), conn('c-t1c3', 't1', 'c3'),
      ],
      scriptedEvents: [
        { triggerTime: 10, action: 'crash', targetId: 'c3', fired: false },
        { triggerTime: 20, action: 'recover', targetId: 'c3', fired: false },
      ],
    }),
  },
  {
    id: 'retry-dlq',
    name: 'Retry & DLQ',
    description: 'One consumer fails 30% — watch retries and dead letters accumulate',
    build: () => ({
      nodes: [
        producer('p1', 'P1', 5, 225),
        topic('t1', 'payments', 2, 225),
        consumer('c1', 'C1', 50, 0, 170, [{ topicId: 't1', partitionIndex: 0 }]),
        consumer('c2', 'C2', 50, 0.3, 280, [{ topicId: 't1', partitionIndex: 1 }]),
      ],
      connections: [
        conn('c-p1t1', 'p1', 't1'),
        conn('c-t1c1', 't1', 'c1'), conn('c-t1c2', 't1', 'c2'),
      ],
      scriptedEvents: [],
    }),
  },
];

export function getPreset(id: string): PresetConfig {
  return PRESETS.find(p => p.id === id) ?? PRESETS[0];
}
