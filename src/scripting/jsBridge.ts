import type { SimulationState, ProducerNode, TopicNode, ConsumerNode } from '../types';
import { generateId } from '../simulation/state';
import { rebalanceConsumers } from '../simulation/rebalance';
import { computeLayout } from '../canvas/layout';

export interface ExecutionResult {
  success: boolean;
  error?: string;
}

export async function executeJs(
  code: string,
  state: SimulationState,
  canvasWidth: number,
  canvasHeight: number,
): Promise<ExecutionResult> {
  try {
    // Clear previous state
    state.nodes = [];
    state.connections = [];
    state.retryQueue = [];
    state.dlqCount = 0;
    state.events = [];
    state.metricsHistory = [];
    state.counters = { produced: 0, consumed: 0, failed: 0 };
    state.elapsedTime = 0;
    state.scriptedEvents = [];
    state._metricsAccumulator = 0;
    state._producedThisInterval = {};

    let shouldStart = false;

    // Build mock kafkajs
    const mockKafka = {
      Kafka: (_config: Record<string, unknown>) => ({
        admin: () => ({
          createTopics: (opts: { topics: { topic: string; numPartitions?: number }[] }) => {
            for (const t of opts.topics) {
              const partitions = t.numPartitions ?? 2;
              const id = generateId(state, 't');
              const node: TopicNode = {
                id, kind: 'topic', name: t.topic,
                position: { x: 0, y: 0 },
                partitionCount: partitions,
                partitions: Array.from({ length: partitions }, () => ({ produced: 0, queued: 0 })),
              };
              state.nodes.push(node);
            }
          },
          scheduleCrash: (clientId: string, opts: { atSeconds: number }) => {
            const node = state.nodes.find(n => n.name === clientId);
            if (node) {
              state.scriptedEvents.push({
                triggerTime: opts.atSeconds, action: 'crash', targetId: node.id, fired: false,
              });
            }
          },
          scheduleRecover: (clientId: string, opts: { atSeconds: number }) => {
            const node = state.nodes.find(n => n.name === clientId);
            if (node) {
              state.scriptedEvents.push({
                triggerTime: opts.atSeconds, action: 'recover', targetId: node.id, fired: false,
              });
            }
          },
          start: () => { shouldStart = true; },
        }),
        producer: (opts: Record<string, unknown>) => {
          const id = generateId(state, 'p');
          const node: ProducerNode = {
            id, kind: 'producer',
            name: (opts?.clientId as string) ?? id.toUpperCase(),
            position: { x: 0, y: 0 },
            messageRate: (opts?.rate as number) ?? 5,
            color: '#22c55e', _accumulator: 0, _roundRobinIndex: 0,
          };
          state.nodes.push(node);
          return {
            connect: async () => {},
            target: (topicName: string) => {
              const topic = state.nodes.find(n => n.kind === 'topic' && n.name === topicName);
              if (topic) {
                state.connections.push({ id: generateId(state, 'conn'), sourceId: id, targetId: topic.id });
              }
            },
          };
        },
        consumer: (opts: Record<string, unknown>) => {
          const id = generateId(state, 'c');
          const node: ConsumerNode = {
            id, kind: 'consumer',
            name: (opts?.clientId as string) ?? id.toUpperCase(),
            position: { x: 0, y: 0 },
            processingDelay: (opts?.delayMs as number) ?? 50,
            failureProbability: (opts?.failureRate as number) ?? 0,
            crashed: false, assignments: [], consumed: 0, _timer: 0, _pullIndex: 0,
          };
          state.nodes.push(node);
          return {
            subscribe: async (subOpts: { topic: string }) => {
              const topic = state.nodes.find(n => n.kind === 'topic' && n.name === subOpts.topic);
              if (topic) {
                state.connections.push({ id: generateId(state, 'conn'), sourceId: topic.id, targetId: id });
                rebalanceConsumers(state, topic.id);
              }
            },
          };
        },
      }),
    };

    // Execute with mock require
    const wrappedCode = `
      const require = (mod) => {
        if (mod === "kafkajs") return mockKafka;
        throw new Error("Module not found: " + mod);
      };
      (async () => { ${code} })();
    `;

    const fn = new Function('mockKafka', `return ${wrappedCode}`);
    await fn(mockKafka);

    computeLayout(state.nodes, canvasWidth, canvasHeight);
    if (shouldStart) state.running = true;

    return { success: true };
  } catch (err) {
    return { success: false, error: String(err) };
  }
}
