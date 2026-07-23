import type { SimulationState } from '../types';
import { getProducers, getConsumers } from '../simulation/state';

export function generateDockerCompose(state: SimulationState): string {
  const producers = getProducers(state);
  const consumers = getConsumers(state);

  const services: string[] = [];

  services.push(`  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"`);

  services.push(`  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`);

  for (const producer of producers) {
    const targetTopics = state.connections
      .filter(c => c.sourceId === producer.id)
      .map(c => state.nodes.find(n => n.id === c.targetId))
      .filter(n => n?.kind === 'topic')
      .map(n => n!.name);

    services.push(`  ${producer.name.toLowerCase()}:
    build: ./producer
    depends_on:
      - kafka
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      TOPICS: "${targetTopics.join(',')}"
      RATE: "${producer.messageRate}"
      CLIENT_ID: "${producer.name}"`);
  }

  for (const consumer of consumers) {
    const subTopics = state.connections
      .filter(c => c.targetId === consumer.id)
      .map(c => state.nodes.find(n => n.id === c.sourceId))
      .filter(n => n?.kind === 'topic')
      .map(n => n!.name);

    services.push(`  ${consumer.name.toLowerCase()}:
    build: ./consumer
    depends_on:
      - kafka
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      TOPICS: "${subTopics.join(',')}"
      GROUP_ID: "processors"
      PROCESSING_DELAY_MS: "${consumer.processingDelay}"
      FAILURE_RATE: "${consumer.failureProbability}"
      CLIENT_ID: "${consumer.name}"`);
  }

  return `version: "3.8"

services:
${services.join('\n\n')}
`;
}
