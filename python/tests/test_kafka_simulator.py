"""
test_kafka_simulator.py — Tests for the Kafka protocol simulator.

Covers:
  - Topic creation and deletion
  - Producer record batching and partition assignment
  - Round-robin vs key-based partitioning
  - Consumer group registration
  - Member join/leave and rebalance triggering
  - Offset management (commit, fetch, lag)
  - PartitionLog retention and high watermark
  - Acks modes (NONE, LEADER, ALL)
  - Consumer poll mechanics
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from kafka_simulator import (
    Acks,
    ConsumerGroup,
    ConsumerMember,
    IsolationLevel,
    KafkaBroker,
    LogEntry,
    PartitionLog,
    ProducerRecord,
    Topic,
    TopicConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_record(topic: str = "test-topic", key: str | None = "user-001",
                value: dict | None = None) -> ProducerRecord:
    return ProducerRecord(
        topic=topic,
        key=key,
        value=value or {"event_type": "PageView", "seq": 1},
    )


def make_broker(num_partitions: int = 4) -> KafkaBroker:
    b = KafkaBroker(broker_id=99)
    b.create_topic("test-topic", TopicConfig(num_partitions=num_partitions))
    return b


# ---------------------------------------------------------------------------
# PartitionLog
# ---------------------------------------------------------------------------
class TestPartitionLog:
    def test_append_returns_log_entry(self):
        log = PartitionLog("topic-a", 0)
        entry = log.append(make_record("topic-a"), Acks.LEADER)
        assert isinstance(entry, LogEntry)
        assert entry.offset == 0

    def test_offsets_increment_sequentially(self):
        log = PartitionLog("topic-a", 0)
        entries = [log.append(make_record("topic-a")) for _ in range(5)]
        assert [e.offset for e in entries] == list(range(5))

    def test_end_offset_equals_entry_count(self):
        log = PartitionLog("topic-a", 0)
        for _ in range(10):
            log.append(make_record("topic-a"))
        assert log.end_offset == 10

    def test_high_watermark_advances_on_leader_ack(self):
        log = PartitionLog("topic-a", 0)
        log.append(make_record("topic-a"), Acks.LEADER)
        assert log.high_watermark == 1

    def test_fire_and_forget_does_not_advance_hwm(self):
        log = PartitionLog("topic-a", 0)
        log.append(make_record("topic-a"), Acks.NONE)
        assert log.high_watermark == 0

    def test_read_returns_entries_from_offset(self):
        log = PartitionLog("topic-a", 0)
        for i in range(10):
            log.append(ProducerRecord("topic-a", f"k{i}", {"seq": i}), Acks.LEADER)
        entries = log.read(start_offset=5, max_records=3)
        assert len(entries) == 3
        assert entries[0].offset == 5

    def test_read_respects_max_records(self):
        log = PartitionLog("topic-a", 0)
        for i in range(20):
            log.append(make_record("topic-a"), Acks.LEADER)
        entries = log.read(0, max_records=5)
        assert len(entries) == 5

    def test_read_committed_isolation_stops_at_hwm(self):
        log = PartitionLog("topic-a", 0)
        log.append(make_record("topic-a"), Acks.NONE)  # hwm stays 0
        log.append(make_record("topic-a"), Acks.LEADER)  # hwm = 2
        # READ_COMMITTED should not return records past hwm
        entries = log.read(0, max_records=10, isolation=IsolationLevel.READ_COMMITTED)
        assert len(entries) == 2  # hwm is 2 after leader ack on second entry

    def test_retention_truncates_old_entries(self):
        # 100-byte retention with ~100-byte entries → should truncate quickly
        log = PartitionLog("topic-a", 0, retention_bytes=500)
        for i in range(20):
            log.append(ProducerRecord("topic-a", f"k{i}", {"data": "x" * 50}), Acks.LEADER)
        # After truncation, log_start_offset > 0
        assert log.log_start_offset > 0 or log._total_bytes <= 500

    def test_checksum_stored_in_entry(self):
        log = PartitionLog("topic-a", 0)
        record = make_record("topic-a")
        entry = log.append(record)
        assert isinstance(entry.checksum, int)

    def test_size_bytes_is_positive(self):
        log = PartitionLog("topic-a", 0)
        record = make_record("topic-a")
        entry = log.append(record)
        assert entry.size_bytes() > 0


# ---------------------------------------------------------------------------
# Topic creation
# ---------------------------------------------------------------------------
class TestTopicCreation:
    def test_create_topic_stores_in_broker(self):
        b = KafkaBroker()
        b.create_topic("my-topic", TopicConfig(num_partitions=3))
        assert "my-topic" in b.topics

    def test_duplicate_topic_raises_value_error(self):
        b = KafkaBroker()
        b.create_topic("events", TopicConfig())
        with pytest.raises(ValueError, match="already exists"):
            b.create_topic("events", TopicConfig())

    def test_topic_has_correct_partition_count(self):
        b = KafkaBroker()
        b.create_topic("t5", TopicConfig(num_partitions=5))
        assert len(b.topics["t5"].partitions) == 5

    def test_delete_topic_removes_from_broker(self):
        b = KafkaBroker()
        b.create_topic("to-delete", TopicConfig())
        b.delete_topic("to-delete")
        assert "to-delete" not in b.topics

    def test_delete_nonexistent_topic_raises(self):
        b = KafkaBroker()
        with pytest.raises(KeyError):
            b.delete_topic("ghost-topic")

    def test_list_topics_returns_sorted(self):
        b = KafkaBroker()
        b.create_topic("zebra", TopicConfig())
        b.create_topic("apple", TopicConfig())
        b.create_topic("mango", TopicConfig())
        assert b.list_topics() == ["apple", "mango", "zebra"]


# ---------------------------------------------------------------------------
# Partition assignment
# ---------------------------------------------------------------------------
class TestPartitionAssignment:
    def test_key_based_partition_is_deterministic(self):
        b = make_broker(num_partitions=8)
        r = make_record(key="user-42")
        e1 = b.produce(r, partition_strategy="key")
        r2 = make_record(key="user-42")
        e2 = b.produce(r2, partition_strategy="key")
        assert e1.partition == e2.partition

    def test_key_based_partition_is_in_range(self):
        b = make_broker(num_partitions=4)
        for i in range(50):
            r = make_record(key=f"user-{i}")
            entry = b.produce(r, partition_strategy="key")
            assert 0 <= entry.partition < 4

    def test_roundrobin_distributes_across_partitions(self):
        b = make_broker(num_partitions=4)
        partitions_used = set()
        for i in range(12):
            r = make_record(key=None)
            entry = b.produce(r, partition_strategy="roundrobin")
            partitions_used.add(entry.partition)
        assert len(partitions_used) == 4

    def test_produce_to_unknown_topic_raises(self):
        b = KafkaBroker()
        with pytest.raises(KeyError, match="ghost"):
            b.produce(make_record("ghost"))

    def test_produce_batch_returns_all_entries(self):
        b = make_broker()
        records = [make_record() for _ in range(10)]
        entries = b.produce_batch(records)
        assert len(entries) == 10

    def test_produce_batch_assigns_sequential_offsets_per_partition(self):
        b = make_broker(num_partitions=1)
        records = [make_record(key=f"k{i}") for i in range(5)]
        entries = b.produce_batch(records)
        offsets = [e.offset for e in entries]
        # All land on partition 0 → offsets 0..4
        assert offsets == list(range(5))


# ---------------------------------------------------------------------------
# Consumer group rebalancing
# ---------------------------------------------------------------------------
class TestConsumerGroupRebalance:
    def test_register_consumer_group(self):
        b = make_broker()
        group = b.register_consumer_group("cg-1", ["test-topic"])
        assert "cg-1" in b.consumer_groups

    def test_member_join_triggers_rebalance(self, capsys):
        b = make_broker(num_partitions=4)
        group = b.register_consumer_group("cg-2", ["test-topic"])
        group.join("consumer-01")
        out = capsys.readouterr().out
        assert "consumer-01" in out

    def test_single_member_gets_all_partitions(self):
        b = make_broker(num_partitions=4)
        group = b.register_consumer_group("cg-solo", ["test-topic"])
        member = group.join("solo-consumer")
        assigned = member.assigned_partitions.get("test-topic", [])
        assert len(assigned) == 4

    def test_two_members_share_partitions(self):
        b = make_broker(num_partitions=4)
        group = b.register_consumer_group("cg-2m", ["test-topic"])
        m1 = group.join("c-01")
        m2 = group.join("c-02")
        p1 = set(m1.assigned_partitions["test-topic"])
        p2 = set(m2.assigned_partitions["test-topic"])
        # No overlap; union covers all 4
        assert p1.isdisjoint(p2)
        assert p1 | p2 == {0, 1, 2, 3}

    def test_member_leave_triggers_rebalance(self):
        b = make_broker(num_partitions=4)
        group = b.register_consumer_group("cg-leave", ["test-topic"])
        m1 = group.join("c-01")
        m2 = group.join("c-02")
        group.leave("c-01")
        # c-02 should now own all partitions
        assert len(m2.assigned_partitions["test-topic"]) == 4

    def test_generation_increments_on_rebalance(self):
        b = make_broker()
        group = b.register_consumer_group("cg-gen", ["test-topic"])
        gen_before = group.generation
        group.join("c-01")
        group.join("c-02")
        assert group.generation > gen_before + 1


# ---------------------------------------------------------------------------
# Offset management
# ---------------------------------------------------------------------------
class TestOffsetManagement:
    def test_committed_offset_starts_at_minus_one(self):
        b = make_broker()
        assert b.committed_offset("unknown-group", "test-topic", 0) == -1

    def test_commit_and_read_offset(self):
        b = make_broker()
        b.register_consumer_group("cg", ["test-topic"])
        b.commit_offset("cg", "test-topic", 0, 42)
        assert b.committed_offset("cg", "test-topic", 0) == 42

    def test_commit_to_unknown_group_raises(self):
        b = make_broker()
        with pytest.raises(KeyError):
            b.commit_offset("ghost-group", "test-topic", 0, 10)

    def test_consumer_lag_is_zero_after_full_poll(self):
        b = make_broker(num_partitions=2)
        group = b.register_consumer_group("cg-lag", ["test-topic"])
        member = group.join("c-01")

        # Produce 10 events
        for i in range(10):
            b.produce(make_record(key=f"user-{i % 2}"), partition_strategy="key")

        # Poll all
        records = group.poll("c-01", max_records_per_partition=100)

        lag = group.consumer_lag()
        total_lag = sum(sum(p.values()) for p in lag.values())
        assert total_lag == 0


# ---------------------------------------------------------------------------
# Fetch API
# ---------------------------------------------------------------------------
class TestFetch:
    def test_fetch_returns_produced_records(self):
        b = make_broker(num_partitions=1)
        for i in range(5):
            b.produce(make_record(key="k"), partition_strategy="key")
        entries = b.fetch("test-topic", 0, offset=0, max_records=10)
        assert len(entries) == 5

    def test_fetch_from_mid_offset(self):
        b = make_broker(num_partitions=1)
        for i in range(10):
            b.produce(make_record(key="k"), partition_strategy="key")
        entries = b.fetch("test-topic", 0, offset=5, max_records=10)
        assert entries[0].offset == 5

    def test_fetch_respects_max_records(self):
        b = make_broker(num_partitions=1)
        for _ in range(20):
            b.produce(make_record(key="k"), partition_strategy="key")
        entries = b.fetch("test-topic", 0, offset=0, max_records=5)
        assert len(entries) == 5

    def test_fetch_from_unknown_topic_raises(self):
        b = KafkaBroker()
        with pytest.raises(KeyError):
            b.fetch("ghost", 0, 0)

    def test_fetch_from_out_of_range_partition_raises(self):
        b = make_broker(num_partitions=2)
        with pytest.raises(IndexError):
            b.fetch("test-topic", 99, 0)

    def test_topic_stats_reports_correct_entry_count(self):
        b = make_broker(num_partitions=1)
        for _ in range(7):
            b.produce(make_record(key="k"))
        stats = b.topic_stats()
        total = sum(p["entries"] for p in stats[0]["partitions"])
        assert total == 7
