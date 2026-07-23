export interface Position {
  x: number;
  y: number;
}

export type NodeKind = 'producer' | 'topic' | 'consumer';

export interface ProducerNode {
  id: string;
  kind: 'producer';
  name: string;
  position: Position;
  messageRate: number;
  color: string;
  _accumulator: number;
  _roundRobinIndex: number;
}

export interface PartitionState {
  produced: number;
  queued: number;
}

export interface TopicNode {
  id: string;
  kind: 'topic';
  name: string;
  position: Position;
  partitionCount: number;
  partitions: PartitionState[];
}

export interface PartitionRef {
  topicId: string;
  partitionIndex: number;
}

export interface ConsumerNode {
  id: string;
  kind: 'consumer';
  name: string;
  position: Position;
  processingDelay: number;
  failureProbability: number;
  crashed: boolean;
  assignments: PartitionRef[];
  consumed: number;
  _timer: number;
  _pullIndex: number;
}

export type SimNode = ProducerNode | TopicNode | ConsumerNode;

export interface Connection {
  id: string;
  sourceId: string;
  targetId: string;
}

export type SimEvent =
  | { type: 'produced'; producerId: string; topicId: string }
  | { type: 'consumed'; consumerId: string; topicId: string }
  | { type: 'retry'; consumerId: string; topicId: string }
  | { type: 'dlq'; consumerId: string };

export interface RetryEntry {
  consumerId: string;
  topicId: string;
  partitionIndex: number;
  retryCount: number;
}

export interface MetricsSnapshot {
  time: number;
  producedPerSec: Record<string, number>;
  consumerOffsets: Record<string, number>;
  consumerLag: Record<string, number>;
  retryDepth: number;
  dlqDepth: number;
}

export interface SimulationState {
  nodes: SimNode[];
  connections: Connection[];
  retryQueue: RetryEntry[];
  dlqCount: number;
  events: SimEvent[];
  metricsHistory: MetricsSnapshot[];
  counters: { produced: number; consumed: number; failed: number };
  running: boolean;
  timeScale: number;
  elapsedTime: number;
  nextId: number;
  scriptedEvents: ScriptedEvent[];
  _metricsAccumulator: number;
  _producedThisInterval: Record<string, number>;
}

export interface ScriptedEvent {
  triggerTime: number;
  action: 'crash' | 'recover';
  targetId: string;
  fired: boolean;
}

export interface PresetConfig {
  id: string;
  name: string;
  description: string;
  build: () => Pick<SimulationState, 'nodes' | 'connections' | 'scriptedEvents'>;
}

export interface Particle {
  id: number;
  connectionId: string;
  progress: number;
  color: string;
}

export const PARTICLE_SPEED = 1.2;
export const METRICS_INTERVAL_MS = 1000;
export const METRICS_HISTORY_LIMIT = 30;
export const MAX_RETRIES = 3;
