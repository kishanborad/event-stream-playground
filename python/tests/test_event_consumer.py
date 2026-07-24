"""
test_event_consumer.py — Tests for the event consumer module.

Covers:
  - Event filtering by type and partition
  - Metric accumulation (counts, throughput, latency)
  - Offset tracking and consumer group semantics
  - Duplicate detection
  - File-based consumption from JSON-lines
  - Stats formatting (JSON and table)
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from event_consumer import (
    ConsumedEvent,
    ConsumerGroup,
    ConsumerMetrics,
    EventConsumer,
    EventFilter,
    PartitionOffset,
    parse_args,
)


# ---------------------------------------------------------------------------
# ConsumedEvent helpers
# ---------------------------------------------------------------------------
def make_consumed(
    event_type: str = "PageView",
    partition: int = 0,
    sequence: int = 1,
    producer_ts: float | None = None,
) -> ConsumedEvent:
    ts = producer_ts if producer_ts is not None else time.time() - 0.05
    return ConsumedEvent(
        raw={
            "event_id": f"test-{sequence:04d}",
            "event_type": event_type,
            "partition": partition,
            "sequence_number": sequence,
            "timestamp": ts,
            "producer_id": "test-producer",
            "partition_key": f"key-{sequence}",
        },
        received_at=time.time(),
    )


# ---------------------------------------------------------------------------
# ConsumedEvent properties
# ---------------------------------------------------------------------------
class TestConsumedEvent:
    def test_event_type_from_raw(self):
        e = make_consumed("Purchase")
        assert e.event_type == "Purchase"

    def test_partition_from_raw(self):
        e = make_consumed(partition=3)
        assert e.partition == 3

    def test_sequence_number_from_raw(self):
        e = make_consumed(sequence=42)
        assert e.sequence_number == 42

    def test_end_to_end_latency_is_positive(self):
        past_ts = time.time() - 0.1
        e = make_consumed(producer_ts=past_ts)
        assert e.end_to_end_latency_ms > 0

    def test_end_to_end_latency_is_approximately_100ms(self):
        past_ts = time.time() - 0.1
        e = ConsumedEvent(raw={"timestamp": past_ts, "event_type": "Click", "partition": 0,
                               "sequence_number": 1, "event_id": "x"},
                          received_at=time.time())
        assert 80 <= e.end_to_end_latency_ms <= 300

    def test_missing_fields_use_defaults(self):
        e = ConsumedEvent(raw={})
        assert e.event_id == ""
        assert e.event_type == ""
        assert e.partition == 0
        assert e.sequence_number == 0


# ---------------------------------------------------------------------------
# EventFilter
# ---------------------------------------------------------------------------
class TestEventFilter:
    def test_no_filter_accepts_all(self):
        f = EventFilter()
        assert f.accepts(make_consumed("PageView", 0))
        assert f.accepts(make_consumed("Purchase", 3))

    def test_type_filter_accepts_matching(self):
        f = EventFilter(allowed_types={"PageView", "Click"})
        assert f.accepts(make_consumed("PageView"))
        assert f.accepts(make_consumed("Click"))

    def test_type_filter_rejects_non_matching(self):
        f = EventFilter(allowed_types={"PageView"})
        assert not f.accepts(make_consumed("Purchase"))
        assert not f.accepts(make_consumed("Heartbeat"))

    def test_partition_filter_accepts_matching(self):
        f = EventFilter(allowed_partitions={0, 1})
        assert f.accepts(make_consumed(partition=0))
        assert f.accepts(make_consumed(partition=1))

    def test_partition_filter_rejects_non_matching(self):
        f = EventFilter(allowed_partitions={0, 1})
        assert not f.accepts(make_consumed(partition=2))
        assert not f.accepts(make_consumed(partition=3))

    def test_combined_filter_requires_both(self):
        f = EventFilter(allowed_types={"PageView"}, allowed_partitions={0})
        assert f.accepts(make_consumed("PageView", 0))
        assert not f.accepts(make_consumed("Click", 0))
        assert not f.accepts(make_consumed("PageView", 1))

    def test_custom_predicate_is_applied(self):
        f = EventFilter(custom_predicate=lambda e: e.sequence_number > 5)
        assert f.accepts(make_consumed(sequence=10))
        assert not f.accepts(make_consumed(sequence=3))


# ---------------------------------------------------------------------------
# ConsumerMetrics
# ---------------------------------------------------------------------------
class TestConsumerMetrics:
    def test_initial_counts_are_zero(self):
        m = ConsumerMetrics()
        assert m.total_received == 0
        assert m.total_processed == 0
        assert m.total_filtered_out == 0

    def test_record_increments_processed(self):
        m = ConsumerMetrics()
        m.record(make_consumed("PageView"))
        assert m.total_processed == 1

    def test_record_increments_by_type(self):
        m = ConsumerMetrics()
        m.record(make_consumed("PageView"))
        m.record(make_consumed("PageView"))
        m.record(make_consumed("Click"))
        assert m.by_type["PageView"] == 2
        assert m.by_type["Click"] == 1

    def test_record_increments_by_partition(self):
        m = ConsumerMetrics()
        m.record(make_consumed(partition=0))
        m.record(make_consumed(partition=0))
        m.record(make_consumed(partition=2))
        assert m.by_partition[0] == 2
        assert m.by_partition[2] == 1

    def test_throughput_is_positive_after_record(self):
        m = ConsumerMetrics()
        m.record(make_consumed())
        assert m.throughput_eps > 0

    def test_percentile_latency_empty(self):
        m = ConsumerMetrics()
        assert m.percentile_latency(50) is None

    def test_percentile_latency_p50(self):
        m = ConsumerMetrics()
        for i in range(1, 101):
            e = make_consumed(producer_ts=time.time() - i * 0.001)
            m.record(e)
        p50 = m.percentile_latency(50)
        assert p50 is not None
        assert p50 > 0

    def test_to_dict_has_required_keys(self):
        m = ConsumerMetrics()
        m.record(make_consumed())
        d = m.to_dict()
        assert "elapsed_s" in d
        assert "throughput_eps" in d
        assert "latency_ms" in d
        assert "by_type" in d
        assert "by_partition" in d

    def test_to_dict_latency_has_percentiles(self):
        m = ConsumerMetrics()
        m.record(make_consumed(producer_ts=time.time() - 0.05))
        lm = m.to_dict()["latency_ms"]
        assert "p50" in lm
        assert "p95" in lm
        assert "p99" in lm


# ---------------------------------------------------------------------------
# PartitionOffset
# ---------------------------------------------------------------------------
class TestPartitionOffset:
    def test_initial_offsets_are_minus_one(self):
        po = PartitionOffset(partition=0)
        assert po.committed_offset == -1
        assert po.pending_offset == -1

    def test_update_tracks_max_offset(self):
        po = PartitionOffset(partition=0)
        po.update(5)
        po.update(3)
        po.update(8)
        assert po.pending_offset == 8

    def test_commit_advances_committed(self):
        po = PartitionOffset(partition=0)
        po.update(10)
        po.commit()
        assert po.committed_offset == 10

    def test_commit_does_not_regress(self):
        po = PartitionOffset(partition=0)
        po.update(10)
        po.commit()
        po.update(5)  # lower — shouldn't matter since update takes max
        po.commit()
        assert po.committed_offset == 10


# ---------------------------------------------------------------------------
# ConsumerGroup
# ---------------------------------------------------------------------------
class TestConsumerGroup:
    def test_group_tracks_offsets(self):
        group = ConsumerGroup("test-group", num_partitions=4)
        event = make_consumed(partition=2, sequence=50)
        group.mark_processed(event)
        assert group.offsets[2].pending_offset == 50

    def test_duplicate_detected_after_processing(self):
        group = ConsumerGroup("test-group", num_partitions=4)
        event = make_consumed(sequence=1)
        assert not group.is_duplicate(event)
        group.mark_processed(event)
        assert group.is_duplicate(event)

    def test_commit_all_returns_offsets(self):
        group = ConsumerGroup("test-group", num_partitions=4)
        for i in range(4):
            event = make_consumed(partition=i, sequence=10 + i)
            group.mark_processed(event)
        committed = group.commit_all()
        assert committed[0] == 10
        assert committed[1] == 11
        assert committed[2] == 12
        assert committed[3] == 13

    def test_lag_returns_zero_when_all_committed(self):
        group = ConsumerGroup("test-group", num_partitions=2)
        for i in range(2):
            event = make_consumed(partition=i, sequence=5)
            group.mark_processed(event)
        group.commit_all()
        lag = group.lag()
        assert all(v == 0 for v in lag.values())

    def test_lag_reflects_uncommitted_events(self):
        group = ConsumerGroup("test-group", num_partitions=1)
        event = make_consumed(partition=0, sequence=10)
        group.mark_processed(event)
        # committed stays at -1, pending is 10 → lag = 11
        lag = group.lag()
        assert lag[0] == 11


# ---------------------------------------------------------------------------
# EventConsumer — file mode
# ---------------------------------------------------------------------------
class TestEventConsumerFile:
    def test_consume_file_processes_all_events(self, tmp_path):
        events_file = tmp_path / "test.jsonl"
        events = [make_consumed(sequence=i + 1) for i in range(10)]
        with events_file.open("w") as fh:
            for e in events:
                fh.write(json.dumps(e.raw) + "\n")

        consumer = EventConsumer()
        asyncio.run(consumer.consume_file(str(events_file)))
        assert consumer.metrics.total_processed == 10

    def test_consume_file_filters_by_type(self, tmp_path):
        events_file = tmp_path / "mixed.jsonl"
        raw_events = [
            make_consumed("PageView", sequence=1).raw,
            make_consumed("Click", sequence=2).raw,
            make_consumed("Purchase", sequence=3).raw,
            make_consumed("PageView", sequence=4).raw,
        ]
        with events_file.open("w") as fh:
            for e in raw_events:
                fh.write(json.dumps(e) + "\n")

        from event_consumer import EventFilter
        consumer = EventConsumer(event_filter=EventFilter(allowed_types={"PageView"}))
        asyncio.run(consumer.consume_file(str(events_file)))
        assert consumer.metrics.total_processed == 2
        assert consumer.metrics.total_filtered_out == 2

    def test_consume_file_skips_malformed_json(self, tmp_path):
        events_file = tmp_path / "bad.jsonl"
        with events_file.open("w") as fh:
            fh.write(json.dumps(make_consumed(sequence=1).raw) + "\n")
            fh.write("NOT JSON\n")
            fh.write(json.dumps(make_consumed(sequence=2).raw) + "\n")

        consumer = EventConsumer()
        asyncio.run(consumer.consume_file(str(events_file)))
        assert consumer.metrics.total_processed == 2

    def test_consume_file_skips_blank_lines(self, tmp_path):
        events_file = tmp_path / "blanks.jsonl"
        with events_file.open("w") as fh:
            fh.write(json.dumps(make_consumed(sequence=1).raw) + "\n")
            fh.write("\n")
            fh.write("   \n")
            fh.write(json.dumps(make_consumed(sequence=2).raw) + "\n")

        consumer = EventConsumer()
        asyncio.run(consumer.consume_file(str(events_file)))
        assert consumer.metrics.total_processed == 2

    def test_consume_file_deduplicates_with_group(self, tmp_path):
        events_file = tmp_path / "dups.jsonl"
        event = make_consumed(sequence=1)
        with events_file.open("w") as fh:
            # Write same event twice
            fh.write(json.dumps(event.raw) + "\n")
            fh.write(json.dumps(event.raw) + "\n")

        group = ConsumerGroup("g1", num_partitions=4)
        consumer = EventConsumer(consumer_group=group)
        asyncio.run(consumer.consume_file(str(events_file)))
        assert consumer.metrics.total_processed == 1
        assert consumer.metrics.total_filtered_out == 1


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
class TestConsumerCLI:
    def test_default_mode_is_file(self):
        args = parse_args([])
        assert args.mode == "file"

    def test_default_consumer_group(self):
        args = parse_args([])
        assert args.consumer_group == "cg-default"

    def test_parse_filter_types(self):
        args = parse_args(["--filter-types", "PageView", "Click"])
        assert set(args.filter_types) == {"PageView", "Click"}

    def test_parse_filter_partitions(self):
        args = parse_args(["--filter-partitions", "0", "2"])
        assert set(args.filter_partitions) == {0, 2}

    def test_parse_stats_format_json(self):
        args = parse_args(["--stats-format", "json"])
        assert args.stats_format == "json"

    def test_parse_commit_interval(self):
        args = parse_args(["--commit-interval", "50"])
        assert args.commit_interval == 50
