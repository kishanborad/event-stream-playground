import type { SimulationState, ProducerNode, TopicNode, ConsumerNode } from '../types';
import { generateId } from '../simulation/state';
import { rebalanceConsumers } from '../simulation/rebalance';
import { computeLayout } from '../canvas/layout';
import { getPyodide } from './pyodideLoader';

export interface ExecutionResult {
  success: boolean;
  error?: string;
}

export async function executePython(
  code: string,
  state: SimulationState,
  canvasWidth: number,
  canvasHeight: number,
): Promise<ExecutionResult> {
  try {
    const pyodide = await getPyodide();

    // Clear previous state (keep counters at 0)
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

    // Register the mock kafka module
    pyodide.registerJsModule('kafka', {
      KafkaAdminClient: () => ({
        create_topic: (name: string, kwargs: Record<string, number>) => {
          const partitions = kwargs?.partitions ?? 2;
          const id = generateId(state, 't');
          const node: TopicNode = {
            id, kind: 'topic', name,
            position: { x: 0, y: 0 },
            partitionCount: partitions,
            partitions: Array.from({ length: partitions }, () => ({ produced: 0, queued: 0 })),
          };
          state.nodes.push(node);
        },
        schedule_crash: (clientId: string, kwargs: Record<string, number>) => {
          const consumer = state.nodes.find(n => n.name === clientId);
          if (consumer) {
            state.scriptedEvents.push({
              triggerTime: kwargs?.at_seconds ?? 10,
              action: 'crash',
              targetId: consumer.id,
              fired: false,
            });
          }
        },
        schedule_recover: (clientId: string, kwargs: Record<string, number>) => {
          const consumer = state.nodes.find(n => n.name === clientId);
          if (consumer) {
            state.scriptedEvents.push({
              triggerTime: kwargs?.at_seconds ?? 20,
              action: 'recover',
              targetId: consumer.id,
              fired: false,
            });
          }
        },
        start: () => { shouldStart = true; },
      }),
      KafkaProducer: (kwargs: Record<string, unknown>) => {
        const id = generateId(state, 'p');
        const node: ProducerNode = {
          id, kind: 'producer',
          name: (kwargs?.client_id as string) ?? id.toUpperCase(),
          position: { x: 0, y: 0 },
          messageRate: (kwargs?.rate as number) ?? 5,
          color: '#22c55e',
          _accumulator: 0,
          _roundRobinIndex: 0,
        };
        state.nodes.push(node);
        return {
          target: (topicName: string) => {
            const topic = state.nodes.find(n => n.kind === 'topic' && n.name === topicName);
            if (topic) {
              state.connections.push({
                id: generateId(state, 'conn'),
                sourceId: id,
                targetId: topic.id,
              });
            }
          },
        };
      },
      KafkaConsumer: (topicName: string, kwargs: Record<string, unknown>) => {
        const id = generateId(state, 'c');
        const node: ConsumerNode = {
          id, kind: 'consumer',
          name: (kwargs?.client_id as string) ?? id.toUpperCase(),
          position: { x: 0, y: 0 },
          processingDelay: (kwargs?.delay_ms as number) ?? 50,
          failureProbability: (kwargs?.failure_rate as number) ?? 0,
          crashed: false,
          assignments: [],
          consumed: 0,
          _timer: 0,
          _pullIndex: 0,
        };
        state.nodes.push(node);

        const topic = state.nodes.find(n => n.kind === 'topic' && n.name === topicName);
        if (topic) {
          state.connections.push({
            id: generateId(state, 'conn'),
            sourceId: topic.id,
            targetId: id,
          });
          rebalanceConsumers(state, topic.id);
        }
        return {};
      },
    });

    await pyodide.runPythonAsync(code);

    computeLayout(state.nodes, canvasWidth, canvasHeight);
    if (shouldStart) state.running = true;

    return { success: true };
  } catch (err) {
    return { success: false, error: String(err) };
  }
}
