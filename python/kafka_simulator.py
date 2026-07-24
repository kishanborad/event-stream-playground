#!/usr/bin/env python3
"""
kafka_simulator.py — Educational Kafka protocol simulator.

Implements a minimal in-memory Kafka broker demonstrating:
  - Topic and partition management
  - Round-robin and key-based partitioning
  - Producer record batching and acknowledgement modes
  - Consumer group registration and rebalancing
  - Offset management and consumer lag tracking
  - Log compaction simulation

This is a teaching tool — it mirrors real Kafka semantics without
network I/O so the browser playground can show how Kafka works internally.

Usage:
    python kafka_simulator.py demo
    python kafka_simulator.py demo --topics 3 --partitions 4 --consumers 3
    python kafka_simulator.py benchmark --events 10000 --rate 1000
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Core data types
# ---------------------------------------------------------------------------
@dataclass
class ProducerRecord:
    """A single record submitted by a producer."""

    topic: str
    key: Optional[str]
    value: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])


@dataclass
class LogEntry:
    """An immutable entry in a partition log — the fundamental storage unit."""

    offset: int
    record: ProducerRecord
    partition: int
    checksum: int = 0

    def __post_init__(self) -> None:
        self.checksum = hash(json.dumps(self.record.value, sort_keys=True))

    def size_bytes(self) -> int:
        return len(json.dumps(self.record.value)) + len(self.record.key or "") + 64

    def to_dict(self) -> dict[str, Any]:
        return {
            "offset": self.offset,
            "partition": self.partition,
            "timestamp": self.record.timestamp,
            "key": self.record.key,
            "value": self.record.value,
            "headers": self.record.headers,
            "record_id": self.record.record_id,
            "checksum": self.checksum,
        }


class Acks(Enum):
    NONE = 0    # fire-and-forget
    LEADER = 1  # leader acknowledges
    ALL = -1    # all in-sync replicas acknowledge


class IsolationLevel(Enum):
    READ_UNCOMMITTED = auto()
    READ_COMMITTED = auto()


# ---------------------------------------------------------------------------
# Partition log
# ---------------------------------------------------------------------------
class PartitionLog:
    """
    Simulates a single Kafka partition — an ordered, append-only log.
    Maintains high watermark, log start offset, and segment management.
    """

    def __init__(self, topic: str, partition_id: int, retention_bytes: int = 10 * 1024 * 1024) -> None:
        self.topic = topic
        self.partition_id = partition_id
        self.retention_bytes = retention_bytes
        self._log: list[LogEntry] = []
        self._next_offset: int = 0
        self._high_watermark: int = 0
        self._log_start_offset: int = 0
        self._total_bytes: int = 0

    @property
    def high_watermark(self) -> int:
        return self._high_watermark

    @property
    def log_start_offset(self) -> int:
        return self._log_start_offset

    @property
    def end_offset(self) -> int:
        return self._next_offset

    def append(self, record: ProducerRecord, acks: Acks = Acks.LEADER) -> LogEntry:
        entry = LogEntry(offset=self._next_offset, record=record, partition=self.partition_id)
        self._log.append(entry)
        self._next_offset += 1
        self._total_bytes += entry.size_bytes()

        if acks != Acks.NONE:
            self._high_watermark = self._next_offset

        self._maybe_truncate()
        return entry

    def read(self, start_offset: int, max_records: int = 500,
             isolation: IsolationLevel = IsolationLevel.READ_UNCOMMITTED) -> list[LogEntry]:
        if start_offset < self._log_start_offset:
            start_offset = self._log_start_offset

        readable_end = self._high_watermark if isolation == IsolationLevel.READ_COMMITTED else self._next_offset
        results: list[LogEntry] = []

        for entry in self._log:
            if entry.offset < start_offset:
                continue
            if entry.offset >= readable_end:
                break
            results.append(entry)
            if len(results) >= max_records:
                break
        return results

    def _maybe_truncate(self) -> None:
        """Enforce retention policy by dropping oldest segments."""
        while self._total_bytes > self.retention_bytes and len(self._log) > 1:
            evicted = self._log.pop(0)
            self._total_bytes -= evicted.size_bytes()
            self._log_start_offset = self._log[0].offset if self._log else self._next_offset

    def stats(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "partition": self.partition_id,
            "log_start_offset": self._log_start_offset,
            "high_watermark": self._high_watermark,
            "end_offset": self._next_offset,
            "entries": len(self._log),
            "total_bytes": self._total_bytes,
        }


# ---------------------------------------------------------------------------
# Topic
# ---------------------------------------------------------------------------
@dataclass
class TopicConfig:
    num_partitions: int = 4
    replication_factor: int = 1
    retention_bytes: int = 10 * 1024 * 1024
    compaction_enabled: bool = False


class Topic:
    """A named, partitioned Kafka topic."""

    def __init__(self, name: str, config: TopicConfig) -> None:
        self.name = name
        self.config = config
        self.partitions: list[PartitionLog] = [
            PartitionLog(name, i, config.retention_bytes)
            for i in range(config.num_partitions)
        ]
        self.created_at: float = time.time()

    @property
    def num_partitions(self) -> int:
        return len(self.partitions)

    def partition_for(self, key: Optional[str], strategy: str = "key") -> int:
        if strategy == "roundrobin" or key is None:
            # Caller is responsible for incrementing RR counter
            return 0
        return hash(key) % self.num_partitions

    def stats(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "num_partitions": self.num_partitions,
            "replication_factor": self.config.replication_factor,
            "partitions": [p.stats() for p in self.partitions],
        }


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------
class KafkaBroker:
    """
    Minimal in-memory Kafka broker.

    Manages topics, routes produce requests, coordinates consumer groups,
    and tracks consumer offsets — mirroring the responsibilities of a real
    Kafka broker controller.
    """

    def __init__(self, broker_id: int = 1) -> None:
        self.broker_id = broker_id
        self.topics: dict[str, Topic] = {}
        self.consumer_groups: dict[str, "ConsumerGroup"] = {}
        self._rr_counters: dict[str, int] = defaultdict(int)  # per-topic RR counter

    # ---- Topic management --------------------------------------------------

    def create_topic(self, name: str, config: Optional[TopicConfig] = None) -> Topic:
        if name in self.topics:
            raise ValueError(f"Topic '{name}' already exists")
        config = config or TopicConfig()
        topic = Topic(name, config)
        self.topics[name] = topic
        print(f"  [broker-{self.broker_id}] Created topic '{name}' "
              f"({config.num_partitions} partitions, RF={config.replication_factor})")
        return topic

    def delete_topic(self, name: str) -> None:
        if name not in self.topics:
            raise KeyError(f"Topic '{name}' not found")
        del self.topics[name]

    def list_topics(self) -> list[str]:
        return sorted(self.topics.keys())

    # ---- Produce -----------------------------------------------------------

    def produce(
        self,
        record: ProducerRecord,
        acks: Acks = Acks.LEADER,
        partition_strategy: str = "key",
    ) -> LogEntry:
        topic = self.topics.get(record.topic)
        if topic is None:
            raise KeyError(f"Unknown topic: '{record.topic}'")

        if partition_strategy == "roundrobin":
            partition_id = self._rr_counters[record.topic] % topic.num_partitions
            self._rr_counters[record.topic] += 1
        else:
            partition_id = topic.partition_for(record.key, strategy="key")

        entry = topic.partitions[partition_id].append(record, acks)
        return entry

    def produce_batch(
        self,
        records: list[ProducerRecord],
        acks: Acks = Acks.LEADER,
        partition_strategy: str = "key",
    ) -> list[LogEntry]:
        return [self.produce(r, acks, partition_strategy) for r in records]

    # ---- Consumer groups ---------------------------------------------------

    def register_consumer_group(self, group_id: str, topics: list[str]) -> "ConsumerGroup":
        if group_id in self.consumer_groups:
            return self.consumer_groups[group_id]
        group = ConsumerGroup(group_id=group_id, broker=self, topics=topics)
        self.consumer_groups[group_id] = group
        print(f"  [broker-{self.broker_id}] Registered consumer group '{group_id}' "
              f"on topics {topics}")
        return group

    def committed_offset(self, group_id: str, topic: str, partition: int) -> int:
        group = self.consumer_groups.get(group_id)
        if group is None:
            return -1
        return group.offsets.get((topic, partition), -1)

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        group = self.consumer_groups.get(group_id)
        if group is None:
            raise KeyError(f"Consumer group '{group_id}' not registered")
        group.offsets[(topic, partition)] = offset

    # ---- Fetch -------------------------------------------------------------

    def fetch(
        self,
        topic: str,
        partition: int,
        offset: int,
        max_records: int = 100,
        isolation: IsolationLevel = IsolationLevel.READ_UNCOMMITTED,
    ) -> list[LogEntry]:
        t = self.topics.get(topic)
        if t is None:
            raise KeyError(f"Unknown topic: '{topic}'")
        if partition >= len(t.partitions):
            raise IndexError(f"Partition {partition} out of range for topic '{topic}'")
        return t.partitions[partition].read(offset, max_records, isolation)

    def topic_stats(self) -> list[dict[str, Any]]:
        return [t.stats() for t in self.topics.values()]


# ---------------------------------------------------------------------------
# Consumer group & rebalancing
# ---------------------------------------------------------------------------
@dataclass
class ConsumerMember:
    member_id: str
    subscribed_topics: list[str]
    assigned_partitions: dict[str, list[int]] = field(default_factory=dict)
    joined_at: float = field(default_factory=time.time)


class ConsumerGroup:
    """
    Simulates Kafka consumer group protocol:
    - Member join / leave
    - Partition assignment (range and round-robin strategies)
    - Rebalance triggering
    - Offset commit / fetch
    """

    def __init__(self, group_id: str, broker: KafkaBroker, topics: list[str]) -> None:
        self.group_id = group_id
        self.broker = broker
        self.topics = topics
        self.members: dict[str, ConsumerMember] = {}
        self.offsets: dict[tuple[str, int], int] = {}
        self.generation: int = 0
        self._rebalance_count: int = 0

    def join(self, member_id: Optional[str] = None) -> ConsumerMember:
        member_id = member_id or f"consumer-{uuid.uuid4().hex[:6]}"
        member = ConsumerMember(member_id=member_id, subscribed_topics=self.topics)
        self.members[member_id] = member
        print(f"  [group:{self.group_id}] Member '{member_id}' joined — triggering rebalance")
        self._rebalance()
        return member

    def leave(self, member_id: str) -> None:
        if member_id not in self.members:
            raise KeyError(f"Member '{member_id}' not in group")
        del self.members[member_id]
        print(f"  [group:{self.group_id}] Member '{member_id}' left — triggering rebalance")
        self._rebalance()

    def _rebalance(self, strategy: str = "range") -> None:
        """Reassign partitions across all active members."""
        self.generation += 1
        self._rebalance_count += 1
        member_list = sorted(self.members.keys())

        if not member_list:
            return

        # Clear existing assignments
        for m in self.members.values():
            m.assigned_partitions = {t: [] for t in self.topics}

        for topic_name in self.topics:
            topic = self.broker.topics.get(topic_name)
            if topic is None:
                continue
            all_partitions = list(range(topic.num_partitions))

            if strategy == "range":
                # Range: divide partitions across members in order
                chunk = max(1, len(all_partitions) // len(member_list))
                for i, mid in enumerate(member_list):
                    start = i * chunk
                    end = start + chunk if i < len(member_list) - 1 else len(all_partitions)
                    self.members[mid].assigned_partitions[topic_name] = all_partitions[start:end]
            else:
                # Round-robin: interleave partitions across members
                for idx, partition in enumerate(all_partitions):
                    mid = member_list[idx % len(member_list)]
                    self.members[mid].assigned_partitions[topic_name].append(partition)

        print(f"  [group:{self.group_id}] Rebalance #{self._rebalance_count} "
              f"(gen={self.generation}, strategy={strategy})")
        for mid, m in self.members.items():
            for topic, parts in m.assigned_partitions.items():
                print(f"    {mid} -> {topic}: partitions {parts}")

    def consumer_lag(self) -> dict[str, dict[int, int]]:
        """Return per-topic per-partition consumer lag."""
        lag: dict[str, dict[int, int]] = {}
        for topic_name in self.topics:
            topic = self.broker.topics.get(topic_name)
            if topic is None:
                continue
            lag[topic_name] = {}
            for p in topic.partitions:
                committed = self.offsets.get((topic_name, p.partition_id), -1)
                end = p.end_offset
                lag[topic_name][p.partition_id] = max(end - (committed + 1), 0)
        return lag

    def poll(self, member_id: str, max_records_per_partition: int = 50) -> list[LogEntry]:
        """Fetch records for a member's assigned partitions and advance offsets."""
        member = self.members.get(member_id)
        if member is None:
            raise KeyError(f"Unknown member: '{member_id}'")

        fetched: list[LogEntry] = []
        for topic_name, partitions in member.assigned_partitions.items():
            for partition_id in partitions:
                committed = self.offsets.get((topic_name, partition_id), -1)
                start = committed + 1
                records = self.broker.fetch(topic_name, partition_id, start, max_records_per_partition)
                for entry in records:
                    self.offsets[(topic_name, partition_id)] = entry.offset
                fetched.extend(records)
        return fetched


# ---------------------------------------------------------------------------
# Demo runner
# ---------------------------------------------------------------------------
def run_demo(num_topics: int = 2, num_partitions: int = 4, num_consumers: int = 3,
             num_events: int = 200) -> None:
    print("\n" + "=" * 60)
    print("  Kafka Simulator — Educational Demo")
    print("=" * 60)

    broker = KafkaBroker(broker_id=1)

    # Create topics
    topics = []
    for i in range(num_topics):
        topic_name = f"events-topic-{i}"
        broker.create_topic(topic_name, TopicConfig(num_partitions=num_partitions))
        topics.append(topic_name)

    print()

    # Register consumer group
    group = broker.register_consumer_group("analytics-group", topics)
    print()

    # Consumers join
    members = []
    for i in range(num_consumers):
        member = group.join(f"consumer-{i:02d}")
        members.append(member)
    print()

    # Produce events
    print(f"Producing {num_events} events across {num_topics} topics...")
    event_types = ["PageView", "Click", "Purchase", "Heartbeat", "MetricSample"]
    user_ids = [f"user_{i:04d}" for i in range(50)]

    for i in range(num_events):
        topic_name = random.choice(topics)
        user_id = random.choice(user_ids)
        event_type = random.choice(event_types)
        record = ProducerRecord(
            topic=topic_name,
            key=user_id,
            value={
                "event_type": event_type,
                "user_id": user_id,
                "sequence": i,
                "ts": time.time(),
            },
        )
        broker.produce(record, acks=Acks.LEADER, partition_strategy="key")

    print(f"  Produced {num_events} events")
    print()

    # Consumers poll
    print("Consumers polling...")
    total_consumed = 0
    for member in members:
        records = group.poll(member.member_id, max_records_per_partition=50)
        total_consumed += len(records)
        print(f"  {member.member_id} polled {len(records)} records")

    print(f"\n  Total consumed: {total_consumed}")
    print()

    # Consumer lag
    print("Consumer lag per topic/partition:")
    lag = group.consumer_lag()
    for topic_name, partitions in lag.items():
        for partition_id, lag_count in sorted(partitions.items()):
            status = "OK" if lag_count == 0 else f"LAG={lag_count}"
            print(f"  {topic_name}[{partition_id}]  {status}")

    # Simulate a consumer leaving and triggering rebalance
    print("\nSimulating consumer failure (consumer-00 leaves)...")
    group.leave("consumer-00")
    print()

    # Re-poll with remaining consumers
    print("Remaining consumers catching up after rebalance...")
    for member in members[1:]:
        records = group.poll(member.member_id, max_records_per_partition=200)
        print(f"  {member.member_id} polled {len(records)} records (catch-up)")

    # Final stats
    print("\n" + "=" * 60)
    print("  Topic Stats")
    print("=" * 60)
    for stats in broker.topic_stats():
        print(f"\n  Topic: {stats['name']}")
        for ps in stats["partitions"]:
            print(f"    partition-{ps['partition']}  "
                  f"entries={ps['entries']}  hwm={ps['high_watermark']}  "
                  f"bytes={ps['total_bytes']:,}")
    print()


def run_benchmark(num_events: int = 10_000, target_rate: float = 5000.0) -> None:
    print(f"\nBenchmark: {num_events:,} events at ~{target_rate:.0f}/s")
    broker = KafkaBroker()
    broker.create_topic("bench-topic", TopicConfig(num_partitions=8))

    event_types = ["PageView", "Click", "MetricSample"]

    start = time.monotonic()
    last_report = start

    for i in range(num_events):
        record = ProducerRecord(
            topic="bench-topic",
            key=f"key-{i % 100}",
            value={"event_type": random.choice(event_types), "seq": i},
        )
        broker.produce(record, acks=Acks.NONE, partition_strategy="key")

        # Minimal sleep to approach target rate
        elapsed = time.monotonic() - start
        expected = (i + 1) / target_rate
        if expected > elapsed:
            time.sleep(min(expected - elapsed, 0.001))

        if time.monotonic() - last_report >= 1.0:
            real_rate = (i + 1) / (time.monotonic() - start)
            print(f"  {i + 1:>8,} / {num_events:,}  ({real_rate:,.0f} events/s)")
            last_report = time.monotonic()

    duration = time.monotonic() - start
    actual_rate = num_events / duration
    print(f"\nDone in {duration:.2f}s — actual throughput: {actual_rate:,.0f} events/s")

    stats = broker.topic_stats()[0]
    total_bytes = sum(p["total_bytes"] for p in stats["partitions"])
    print(f"Stored {total_bytes:,} bytes across {stats['num_partitions']} partitions")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka protocol simulator for educational demos")
    subparsers = parser.add_subparsers(dest="command")

    demo_p = subparsers.add_parser("demo", help="Run interactive demo")
    demo_p.add_argument("--topics", type=int, default=2)
    demo_p.add_argument("--partitions", type=int, default=4)
    demo_p.add_argument("--consumers", type=int, default=3)
    demo_p.add_argument("--events", type=int, default=200)

    bench_p = subparsers.add_parser("benchmark", help="Throughput benchmark")
    bench_p.add_argument("--events", type=int, default=10_000)
    bench_p.add_argument("--rate", type=float, default=5000.0)

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if args.command == "demo" or args.command is None:
        run_demo(
            num_topics=getattr(args, "topics", 2),
            num_partitions=getattr(args, "partitions", 4),
            num_consumers=getattr(args, "consumers", 3),
            num_events=getattr(args, "events", 200),
        )
    elif args.command == "benchmark":
        run_benchmark(args.events, args.rate)


if __name__ == "__main__":
    main()
