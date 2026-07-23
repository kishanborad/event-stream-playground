export type ScriptLanguage = 'python' | 'javascript';

const PYTHON_TEMPLATES: Record<string, string> = {
  'happy-path': `from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer

admin = KafkaAdminClient()
admin.create_topic("orders", partitions=3)

# Two producers at 5 msg/sec each
p1 = KafkaProducer(client_id="P1", rate=5)
p2 = KafkaProducer(client_id="P2", rate=5)
p1.target("orders")
p2.target("orders")

# Three consumers in the same group
c1 = KafkaConsumer("orders", group_id="processors", client_id="C1", delay_ms=50)
c2 = KafkaConsumer("orders", group_id="processors", client_id="C2", delay_ms=50)
c3 = KafkaConsumer("orders", group_id="processors", client_id="C3", delay_ms=50)

admin.start()
`,

  'consumer-lag': `from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer

admin = KafkaAdminClient()
admin.create_topic("events", partitions=2)

# High-throughput producers
p1 = KafkaProducer(client_id="P1", rate=10)
p2 = KafkaProducer(client_id="P2", rate=10)
p1.target("events")
p2.target("events")

# One slow consumer — can't keep up
c1 = KafkaConsumer("events", group_id="slow", client_id="C1", delay_ms=200)

admin.start()
`,

  'crash-rebalance': `from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer

admin = KafkaAdminClient()
admin.create_topic("logs", partitions=4)

p1 = KafkaProducer(client_id="P1", rate=5)
p1.target("logs")

c1 = KafkaConsumer("logs", group_id="workers", client_id="C1", delay_ms=50)
c2 = KafkaConsumer("logs", group_id="workers", client_id="C2", delay_ms=50)
c3 = KafkaConsumer("logs", group_id="workers", client_id="C3", delay_ms=50)

# C3 crashes at 10s, recovers at 20s — partitions rebalance
admin.schedule_crash("C3", at_seconds=10)
admin.schedule_recover("C3", at_seconds=20)

admin.start()
`,

  'retry-dlq': `from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer

admin = KafkaAdminClient()
admin.create_topic("payments", partitions=2)

p1 = KafkaProducer(client_id="P1", rate=5)
p1.target("payments")

# C1 processes reliably
c1 = KafkaConsumer("payments", group_id="billing", client_id="C1", delay_ms=50)

# C2 fails 30% of the time — retries then DLQ
c2 = KafkaConsumer("payments", group_id="billing", client_id="C2",
                   delay_ms=50, failure_rate=0.3)

admin.start()
`,
};

const JS_TEMPLATES: Record<string, string> = {
  'happy-path': `const { Kafka } = require("kafkajs");
const kafka = new Kafka({ clientId: "event-stream" });
const admin = kafka.admin();

await admin.createTopics({
  topics: [{ topic: "orders", numPartitions: 3 }],
});

const p1 = kafka.producer({ clientId: "P1", rate: 5 });
const p2 = kafka.producer({ clientId: "P2", rate: 5 });
await p1.connect(); p1.target("orders");
await p2.connect(); p2.target("orders");

const c1 = kafka.consumer({ groupId: "processors", clientId: "C1", delayMs: 50 });
const c2 = kafka.consumer({ groupId: "processors", clientId: "C2", delayMs: 50 });
const c3 = kafka.consumer({ groupId: "processors", clientId: "C3", delayMs: 50 });
await c1.subscribe({ topic: "orders" });
await c2.subscribe({ topic: "orders" });
await c3.subscribe({ topic: "orders" });

await admin.start();
`,

  'consumer-lag': `const { Kafka } = require("kafkajs");
const kafka = new Kafka({ clientId: "event-stream" });
const admin = kafka.admin();

await admin.createTopics({
  topics: [{ topic: "events", numPartitions: 2 }],
});

const p1 = kafka.producer({ clientId: "P1", rate: 10 });
const p2 = kafka.producer({ clientId: "P2", rate: 10 });
await p1.connect(); p1.target("events");
await p2.connect(); p2.target("events");

// One slow consumer — can't keep up
const c1 = kafka.consumer({ groupId: "slow", clientId: "C1", delayMs: 200 });
await c1.subscribe({ topic: "events" });

await admin.start();
`,

  'crash-rebalance': `const { Kafka } = require("kafkajs");
const kafka = new Kafka({ clientId: "event-stream" });
const admin = kafka.admin();

await admin.createTopics({
  topics: [{ topic: "logs", numPartitions: 4 }],
});

const p1 = kafka.producer({ clientId: "P1", rate: 5 });
await p1.connect(); p1.target("logs");

const c1 = kafka.consumer({ groupId: "workers", clientId: "C1", delayMs: 50 });
const c2 = kafka.consumer({ groupId: "workers", clientId: "C2", delayMs: 50 });
const c3 = kafka.consumer({ groupId: "workers", clientId: "C3", delayMs: 50 });
await c1.subscribe({ topic: "logs" });
await c2.subscribe({ topic: "logs" });
await c3.subscribe({ topic: "logs" });

// C3 crashes at 10s, recovers at 20s
admin.scheduleCrash("C3", { atSeconds: 10 });
admin.scheduleRecover("C3", { atSeconds: 20 });

await admin.start();
`,

  'retry-dlq': `const { Kafka } = require("kafkajs");
const kafka = new Kafka({ clientId: "event-stream" });
const admin = kafka.admin();

await admin.createTopics({
  topics: [{ topic: "payments", numPartitions: 2 }],
});

const p1 = kafka.producer({ clientId: "P1", rate: 5 });
await p1.connect(); p1.target("payments");

const c1 = kafka.consumer({ groupId: "billing", clientId: "C1", delayMs: 50 });
// C2 fails 30% — retries then DLQ
const c2 = kafka.consumer({ groupId: "billing", clientId: "C2", delayMs: 50, failureRate: 0.3 });
await c1.subscribe({ topic: "payments" });
await c2.subscribe({ topic: "payments" });

await admin.start();
`,
};

export function getTemplate(presetId: string, language: ScriptLanguage): string {
  const map = language === 'python' ? PYTHON_TEMPLATES : JS_TEMPLATES;
  return map[presetId] ?? map['happy-path'] ?? '';
}
