"""
test_event_producer.py — Tests for the event producer module.

Covers:
  - Event generation for each of the six event types
  - Payload structure and field validation
  - Partition key assignment (key-based and round-robin)
  - Rate limiter mechanics
  - Producer stream termination with max_events
  - Sequence number monotonicity
  - JSON serialization round-trip
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from event_producer import (
    EventPayload,
    EventProducer,
    EventType,
    Partitioner,
    _PAYLOAD_FACTORIES,
    parse_args,
)


# ---------------------------------------------------------------------------
# Payload factories — one test per event type
# ---------------------------------------------------------------------------
class TestPayloadFactories:
    def test_page_view_has_required_fields(self):
        key, data = _PAYLOAD_FACTORIES[EventType.PAGE_VIEW]()
        assert "user_id" in data
        assert "page" in data
        assert "session_id" in data
        assert isinstance(data["duration_ms"], int)
        assert data["duration_ms"] > 0

    def test_click_has_coordinates(self):
        key, data = _PAYLOAD_FACTORIES[EventType.CLICK]()
        assert "x" in data and "y" in data
        assert 0 <= data["x"] <= 1920
        assert 0 <= data["y"] <= 1080
        assert "element" in data

    def test_purchase_has_financial_fields(self):
        key, data = _PAYLOAD_FACTORIES[EventType.PURCHASE]()
        assert "order_id" in data
        assert "unit_price_usd" in data
        assert "total_usd" in data
        assert "quantity" in data
        assert data["quantity"] >= 1
        assert data["unit_price_usd"] > 0
        total_expected = round(data["unit_price_usd"] * data["quantity"], 2)
        assert abs(data["total_usd"] - total_expected) < 0.01

    def test_heartbeat_has_host(self):
        key, data = _PAYLOAD_FACTORIES[EventType.HEARTBEAT]()
        assert "host" in data
        assert data["status"] == "ok"
        assert data["uptime_s"] > 0

    def test_metric_sample_has_cpu_and_memory(self):
        key, data = _PAYLOAD_FACTORIES[EventType.METRIC_SAMPLE]()
        assert "cpu_pct" in data
        assert "mem_mb" in data
        assert "metric_name" in data
        assert isinstance(data["mem_mb"], int)

    def test_sensor_reading_has_environmental_fields(self):
        key, data = _PAYLOAD_FACTORIES[EventType.SENSOR_READING]()
        assert "temperature_c" in data
        assert "humidity_pct" in data
        assert "pressure_hpa" in data
        assert 0 <= data["humidity_pct"] <= 100
        assert data["battery_pct"] >= 0

    def test_all_factories_return_string_key(self):
        for event_type, factory in _PAYLOAD_FACTORIES.items():
            key, data = factory()
            assert isinstance(key, str), f"{event_type}: partition key must be a string"
            assert len(key) > 0, f"{event_type}: partition key must be non-empty"


# ---------------------------------------------------------------------------
# Partitioner tests
# ---------------------------------------------------------------------------
class TestPartitioner:
    def test_key_strategy_is_deterministic(self):
        p = Partitioner(num_partitions=8, strategy="key")
        partition_a = p.assign("user_001")
        partition_b = p.assign("user_001")
        assert partition_a == partition_b

    def test_key_strategy_stays_in_range(self):
        p = Partitioner(num_partitions=4, strategy="key")
        for i in range(100):
            result = p.assign(f"key-{i}")
            assert 0 <= result < 4

    def test_roundrobin_distributes_evenly(self):
        num_partitions = 4
        p = Partitioner(num_partitions=num_partitions, strategy="roundrobin")
        counts = [0] * num_partitions
        for _ in range(100):
            counts[p.assign("any-key")] += 1
        # Each partition should get exactly 25 events
        assert all(c == 25 for c in counts)

    def test_roundrobin_ignores_key(self):
        p = Partitioner(num_partitions=2, strategy="roundrobin")
        first = p.assign("same-key")
        second = p.assign("same-key")
        # Should differ with round-robin regardless of key
        assert first != second

    def test_invalid_partitions_raises(self):
        with pytest.raises(ValueError):
            Partitioner(num_partitions=0)

    def test_single_partition_always_zero(self):
        p = Partitioner(num_partitions=1, strategy="key")
        for i in range(20):
            assert p.assign(f"key-{i}") == 0

    def test_different_keys_land_on_different_partitions(self):
        p = Partitioner(num_partitions=8, strategy="key")
        partitions = {p.assign(f"key-{i}") for i in range(50)}
        # With 50 keys and 8 partitions, expect multiple distinct partitions
        assert len(partitions) > 2


# ---------------------------------------------------------------------------
# EventProducer — event building
# ---------------------------------------------------------------------------
class TestEventProducerBuild:
    def test_build_event_returns_event_payload(self, producer):
        event = producer.build_event()
        assert isinstance(event, EventPayload)

    def test_event_has_uuid_event_id(self, producer):
        event = producer.build_event()
        # Must be a valid UUID4 format (32 hex chars with dashes)
        assert len(event.event_id) == 36
        parts = event.event_id.split("-")
        assert len(parts) == 5

    def test_event_type_is_one_of_configured(self, producer):
        types_seen = set()
        for _ in range(50):
            event = producer.build_event()
            types_seen.add(event.event_type)
        allowed = {e.value for e in producer.event_types}
        assert types_seen.issubset(allowed)

    def test_partition_in_range(self, producer):
        for _ in range(50):
            event = producer.build_event()
            assert 0 <= event.partition < producer.partitioner.num_partitions

    def test_sequence_numbers_are_monotonic(self, producer):
        events = [producer.build_event() for _ in range(20)]
        seqs = [e.sequence_number for e in events]
        assert seqs == sorted(seqs)
        assert seqs[0] == 1
        assert seqs[-1] == 20

    def test_timestamp_is_recent(self, producer):
        before = time.time()
        event = producer.build_event()
        after = time.time()
        assert before <= event.timestamp <= after

    def test_producer_id_embedded(self, producer):
        event = producer.build_event()
        assert event.producer_id == "test-producer"

    def test_metadata_schema_version(self, producer):
        event = producer.build_event()
        assert event.metadata.get("schema_version") == "1.0"

    def test_metadata_num_partitions(self, producer):
        event = producer.build_event()
        assert event.metadata["num_partitions"] == producer.partitioner.num_partitions

    def test_single_event_type_producer(self):
        single_type_producer = EventProducer(
            event_types=[EventType.HEARTBEAT],
            partitions=2,
            events_per_second=1000.0,
        )
        for _ in range(10):
            event = single_type_producer.build_event()
            assert event.event_type == "Heartbeat"


# ---------------------------------------------------------------------------
# EventProducer — stream / async
# ---------------------------------------------------------------------------
class TestEventProducerStream:
    def test_stream_respects_max_events(self, producer):
        events = asyncio.run(_collect_events(producer, max_events=10))
        assert len(events) == 10

    def test_stream_events_are_unique(self, producer):
        events = asyncio.run(_collect_events(producer, max_events=20))
        ids = [e.event_id for e in events]
        assert len(set(ids)) == 20

    def test_stream_sequence_is_monotonic(self, producer):
        events = asyncio.run(_collect_events(producer, max_events=15))
        seqs = [e.sequence_number for e in events]
        assert seqs == sorted(seqs)

    def test_stream_five_events_completes(self, producer):
        # Confirm the generator terminates cleanly
        events = asyncio.run(_collect_events(producer, max_events=5))
        assert len(events) == 5


async def _collect_events(producer: EventProducer, max_events: int) -> list[EventPayload]:
    collected = []
    async for event in producer.stream(max_events=max_events):
        collected.append(event)
    return collected


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------
class TestEventPayloadSerialization:
    def test_to_json_is_valid_json(self, producer):
        event = producer.build_event()
        raw = event.to_json()
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)

    def test_to_json_round_trip_preserves_event_id(self, producer):
        event = producer.build_event()
        parsed = json.loads(event.to_json())
        assert parsed["event_id"] == event.event_id

    def test_to_json_round_trip_preserves_partition(self, producer):
        event = producer.build_event()
        parsed = json.loads(event.to_json())
        assert parsed["partition"] == event.partition

    def test_json_has_no_extra_whitespace(self, producer):
        event = producer.build_event()
        raw = event.to_json()
        # Compact JSON: no spaces after : or ,
        assert ": " not in raw
        assert ", " not in raw


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------
class TestCLI:
    def test_default_mode_is_stdout(self):
        args = parse_args([])
        assert args.mode == "stdout"

    def test_default_rate(self):
        args = parse_args([])
        assert args.rate == 10.0

    def test_default_partitions(self):
        args = parse_args([])
        assert args.partitions == 4

    def test_parse_event_types(self):
        args = parse_args(["--event-types", "PageView", "Click"])
        assert args.event_types == ["PageView", "Click"]

    def test_parse_count(self):
        args = parse_args(["--count", "500"])
        assert args.count == 500

    def test_parse_ws_mode(self):
        args = parse_args(["--mode", "ws", "--port", "9000"])
        assert args.mode == "ws"
        assert args.port == 9000

    def test_parse_partition_strategy(self):
        args = parse_args(["--partition-strategy", "roundrobin"])
        assert args.partition_strategy == "roundrobin"
