import type {
  SimulationState, TopicNode, ConsumerNode,
  MetricsSnapshot,
} from '../types';
import { getProducers, getConsumers, findNode } from './state';

const METRICS_MS = 1000;
const HISTORY_CAP = 30;

export function tick(state: SimulationState, deltaMs: number): void {
  if (!state.running) return;

  const scaled = deltaMs * state.timeScale;
  state.elapsedTime += scaled;

  produceMessages(state, scaled);
  consumeMessages(state, scaled);
  processScriptedEvents(state);
  accumulateMetrics(state, scaled);
}

function produceMessages(state: SimulationState, scaledMs: number): void {
  for (const producer of getProducers(state)) {
    producer._accumulator += producer.messageRate * (scaledMs / 1000);

    while (producer._accumulator >= 1) {
      producer._accumulator -= 1;

      const connectedTopics = state.connections
        .filter(c => c.sourceId === producer.id)
        .map(c => findNode<TopicNode>(state, c.targetId))
        .filter((t): t is TopicNode => t !== undefined);

      for (const topic of connectedTopics) {
        if (topic.partitions.length === 0) continue;
        const pIndex = producer._roundRobinIndex % topic.partitions.length;
        topic.partitions[pIndex].produced++;
        topic.partitions[pIndex].queued++;
        producer._roundRobinIndex++;
        state.counters.produced++;
        state.events.push({ type: 'produced', producerId: producer.id, topicId: topic.id });

        if (!state._producedThisInterval[producer.id]) {
          state._producedThisInterval[producer.id] = 0;
        }
        state._producedThisInterval[producer.id]++;
      }
    }
  }
}

function consumeMessages(state: SimulationState, scaledMs: number): void {
  for (const consumer of getConsumers(state)) {
    if (consumer.crashed) continue;

    if (consumer.processingDelay <= 0) {
      drainAllAssigned(state, consumer);
      continue;
    }

    consumer._timer -= scaledMs;
    while (consumer._timer <= 0) {
      const pulled = pullOneMessage(state, consumer);
      if (!pulled) break;
      consumer._timer += consumer.processingDelay;
    }
    if (consumer._timer < 0) consumer._timer = 0;
  }
}

function drainAllAssigned(state: SimulationState, consumer: ConsumerNode): void {
  for (const assignment of consumer.assignments) {
    const topic = findNode<TopicNode>(state, assignment.topicId);
    if (!topic) continue;
    const partition = topic.partitions[assignment.partitionIndex];
    if (!partition) continue;

    while (partition.queued > 0) {
      partition.queued--;
      const failed = consumer.failureProbability > 0 && Math.random() < consumer.failureProbability;
      if (failed) {
        handleFailure(state, consumer, assignment.topicId, assignment.partitionIndex);
      } else {
        consumer.consumed++;
        state.counters.consumed++;
        state.events.push({ type: 'consumed', consumerId: consumer.id, topicId: assignment.topicId });
      }
    }
  }
}

function pullOneMessage(state: SimulationState, consumer: ConsumerNode): boolean {
  if (consumer.assignments.length === 0) return false;

  for (let i = 0; i < consumer.assignments.length; i++) {
    const idx = (consumer._pullIndex + i) % consumer.assignments.length;
    const assignment = consumer.assignments[idx];
    const topic = findNode<TopicNode>(state, assignment.topicId);
    if (!topic) continue;
    const partition = topic.partitions[assignment.partitionIndex];
    if (!partition || partition.queued <= 0) continue;

    partition.queued--;
    consumer._pullIndex = (idx + 1) % consumer.assignments.length;

    const failed = consumer.failureProbability > 0 && Math.random() < consumer.failureProbability;
    if (failed) {
      handleFailure(state, consumer, assignment.topicId, assignment.partitionIndex);
    } else {
      consumer.consumed++;
      state.counters.consumed++;
      state.events.push({ type: 'consumed', consumerId: consumer.id, topicId: assignment.topicId });
    }
    return true;
  }
  return false;
}

function handleFailure(
  state: SimulationState,
  consumer: ConsumerNode,
  topicId: string,
  partitionIndex: number,
): void {
  state.counters.failed++;
  state.retryQueue.push({
    consumerId: consumer.id,
    topicId,
    partitionIndex,
    retryCount: 1,
  });
  state.events.push({ type: 'retry', consumerId: consumer.id, topicId });
}

function processScriptedEvents(state: SimulationState): void {
  const elapsedSec = state.elapsedTime / 1000;
  for (const event of state.scriptedEvents) {
    if (event.fired || elapsedSec < event.triggerTime) continue;
    event.fired = true;

    const node = findNode<ConsumerNode>(state, event.targetId);
    if (!node || node.kind !== 'consumer') continue;

    if (event.action === 'crash') {
      node.crashed = true;
    } else if (event.action === 'recover') {
      node.crashed = false;
    }
  }
}

function accumulateMetrics(state: SimulationState, scaledMs: number): void {
  state._metricsAccumulator += scaledMs;
  if (state._metricsAccumulator < METRICS_MS) return;
  state._metricsAccumulator -= METRICS_MS;

  const snapshot: MetricsSnapshot = {
    time: state.elapsedTime / 1000,
    producedPerSec: { ...state._producedThisInterval },
    consumerOffsets: {},
    consumerLag: {},
    retryDepth: state.retryQueue.length,
    dlqDepth: state.dlqCount,
  };

  for (const consumer of getConsumers(state)) {
    snapshot.consumerOffsets[consumer.id] = consumer.consumed;

    let lag = 0;
    for (const assignment of consumer.assignments) {
      const topic = findNode<TopicNode>(state, assignment.topicId);
      if (!topic) continue;
      const partition = topic.partitions[assignment.partitionIndex];
      if (partition) lag += partition.queued;
    }
    snapshot.consumerLag[consumer.id] = lag;
  }

  state.metricsHistory.push(snapshot);
  if (state.metricsHistory.length > HISTORY_CAP) {
    state.metricsHistory.shift();
  }
  state._producedThisInterval = {};
}
