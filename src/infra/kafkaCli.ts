import type { SimulationState } from '../types';
import { getProducers, getTopics, getConsumers } from '../simulation/state';

export function generateCliCommands(state: SimulationState): string {
  const topics = getTopics(state);
  const producers = getProducers(state);
  const consumers = getConsumers(state);
  const lines: string[] = [];

  lines.push('#!/bin/bash');
  lines.push('# Kafka CLI commands for this topology');
  lines.push('# Assumes Kafka is running at localhost:9092');
  lines.push('');

  lines.push('# --- Create topics ---');
  for (const topic of topics) {
    lines.push(`kafka-topics.sh --bootstrap-server localhost:9092 \\`);
    lines.push(`  --create --topic ${topic.name} \\`);
    lines.push(`  --partitions ${topic.partitionCount} \\`);
    lines.push(`  --replication-factor 1`);
    lines.push('');
  }

  lines.push('# --- List topics ---');
  lines.push('kafka-topics.sh --bootstrap-server localhost:9092 --list');
  lines.push('');

  if (producers.length > 0) {
    lines.push('# --- Produce messages ---');
    for (const producer of producers) {
      const targetTopics = state.connections
        .filter(c => c.sourceId === producer.id)
        .map(c => state.nodes.find(n => n.id === c.targetId))
        .filter(n => n?.kind === 'topic')
        .map(n => n!.name);

      for (const topicName of targetTopics) {
        lines.push(`# ${producer.name} -> ${topicName} (${producer.messageRate} msg/s)`);
        lines.push(`kafka-console-producer.sh --bootstrap-server localhost:9092 \\`);
        lines.push(`  --topic ${topicName}`);
        lines.push('');
      }
    }
  }

  if (consumers.length > 0) {
    lines.push('# --- Consume messages ---');
    for (const consumer of consumers) {
      const subTopics = state.connections
        .filter(c => c.targetId === consumer.id)
        .map(c => state.nodes.find(n => n.id === c.sourceId))
        .filter(n => n?.kind === 'topic')
        .map(n => n!.name);

      for (const topicName of subTopics) {
        lines.push(`# ${consumer.name} <- ${topicName}`);
        lines.push(`kafka-console-consumer.sh --bootstrap-server localhost:9092 \\`);
        lines.push(`  --topic ${topicName} \\`);
        lines.push(`  --group processors \\`);
        lines.push(`  --from-beginning`);
        lines.push('');
      }
    }
  }

  lines.push('# --- Describe consumer group ---');
  lines.push('kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\');
  lines.push('  --group processors --describe');

  return lines.join('\n');
}
