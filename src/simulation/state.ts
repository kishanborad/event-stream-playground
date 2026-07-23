import type { SimulationState, PresetConfig } from '../types';

export function createState(preset: PresetConfig): SimulationState {
  const { nodes, connections, scriptedEvents } = preset.build();
  return {
    nodes,
    connections,
    retryQueue: [],
    dlqCount: 0,
    events: [],
    metricsHistory: [],
    counters: { produced: 0, consumed: 0, failed: 0 },
    running: false,
    timeScale: 1,
    elapsedTime: 0,
    nextId: 100,
    scriptedEvents: scriptedEvents.map(e => ({ ...e, fired: false })),
    _metricsAccumulator: 0,
    _producedThisInterval: {},
  };
}

export function generateId(state: SimulationState, prefix: string): string {
  return `${prefix}-${state.nextId++}`;
}

export function findNode<T extends SimulationState['nodes'][number]>(
  state: SimulationState,
  id: string,
): T | undefined {
  return state.nodes.find(n => n.id === id) as T | undefined;
}

export function getProducers(state: SimulationState) {
  return state.nodes.filter(n => n.kind === 'producer') as import('../types').ProducerNode[];
}

export function getTopics(state: SimulationState) {
  return state.nodes.filter(n => n.kind === 'topic') as import('../types').TopicNode[];
}

export function getConsumers(state: SimulationState) {
  return state.nodes.filter(n => n.kind === 'consumer') as import('../types').ConsumerNode[];
}
