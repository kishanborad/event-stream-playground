"""
conftest.py — Shared pytest fixtures for the event-stream-playground test suite.

Provides:
  - Sample event dicts and EventRecord instances
  - Pre-built EventProducer / EventConsumer instances
  - Temporary file helpers for JSON-lines event logs
  - Mock stream connection utilities
"""

from __future__ import annotations

import json
import random
import uuid
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Raw event dict fixtures
# ---------------------------------------------------------------------------
_BASE_TS = 1_700_000_000.0


def _make_raw_event(
    event_type: str = "PageView",
    partition: int = 0,
    sequence: int = 1,
    producer_id: str = "test-producer",
    ts_offset: float = 0.0,
    event_id: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": _BASE_TS + ts_offset,
        "partition_key": f"user_{sequence:04d}",
        "partition": partition,
        "sequence_number": sequence,
        "producer_id": producer_id,
        "metadata": {"schema_version": "1.0", "topic": "events", "num_partitions": 4},
        "payload": {
            "user_id": f"user_{sequence:04d}",
            "page": "/dashboard",
        },
    }


@pytest.fixture
def raw_page_view() -> dict[str, Any]:
    return _make_raw_event("PageView", partition=0, sequence=1)


@pytest.fixture
def raw_click() -> dict[str, Any]:
    return _make_raw_event("Click", partition=1, sequence=2)


@pytest.fixture
def raw_purchase() -> dict[str, Any]:
    return _make_raw_event("Purchase", partition=2, sequence=3)


@pytest.fixture
def raw_heartbeat() -> dict[str, Any]:
    return _make_raw_event("Heartbeat", partition=3, sequence=4)


@pytest.fixture
def raw_metric() -> dict[str, Any]:
    return _make_raw_event("MetricSample", partition=0, sequence=5)


@pytest.fixture
def raw_sensor() -> dict[str, Any]:
    return _make_raw_event("SensorReading", partition=1, sequence=6)


@pytest.fixture
def sample_events_all_types() -> list[dict[str, Any]]:
    """All six event types, one each."""
    return [
        _make_raw_event(t, partition=i % 4, sequence=i + 1)
        for i, t in enumerate(
            ["PageView", "Click", "Purchase", "Heartbeat", "MetricSample", "SensorReading"]
        )
    ]


@pytest.fixture
def sample_events_large(tmp_path: Path) -> Path:
    """
    Writes 500 synthetic events to a temp JSON-lines file.
    Returns the path.
    """
    event_types = ["PageView", "Click", "Purchase", "Heartbeat", "MetricSample", "SensorReading"]
    events_file = tmp_path / "test_events.jsonl"
    with events_file.open("w") as fh:
        for i in range(500):
            event = _make_raw_event(
                event_type=random.choice(event_types),
                partition=i % 4,
                sequence=i + 1,
                ts_offset=i * 0.1,  # 100ms apart
            )
            fh.write(json.dumps(event) + "\n")
    return events_file


@pytest.fixture
def events_with_anomalies(tmp_path: Path) -> Path:
    """
    Event log with deliberate anomalies:
    - A large time gap at event 50
    - A duplicate event_id at event 100
    - A sequence gap in producer-001 after event 150
    """
    events_file = tmp_path / "anomaly_events.jsonl"
    event_types = ["PageView", "Click", "Purchase", "Heartbeat"]

    with events_file.open("w") as fh:
        fixed_id = str(uuid.uuid4())
        for i in range(300):
            ts_offset = i * 0.1
            # Insert 15s gap at event 50
            if i >= 50:
                ts_offset += 15.0
            eid = fixed_id if i == 100 else None  # duplicate at 100
            event = _make_raw_event(
                event_type=event_types[i % 4],
                partition=i % 4,
                sequence=i + 1 if i < 150 else i + 51,  # sequence gap after 150
                producer_id="producer-001",
                ts_offset=ts_offset,
                event_id=eid,
            )
            fh.write(json.dumps(event) + "\n")
    return events_file


# ---------------------------------------------------------------------------
# Producer / consumer fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def producer():
    """A default EventProducer with deterministic settings for testing."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from event_producer import EventProducer, EventType

    return EventProducer(
        event_types=list(EventType),
        partitions=4,
        partition_strategy="key",
        events_per_second=1000.0,  # high rate — tests don't wait
        producer_id="test-producer",
    )


@pytest.fixture
def consumer():
    """A default EventConsumer with no filtering."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from event_consumer import EventConsumer

    return EventConsumer()


@pytest.fixture
def filtered_consumer():
    """An EventConsumer that only accepts PageView and Click on partitions 0 and 1."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from event_consumer import EventConsumer, EventFilter

    return EventConsumer(
        event_filter=EventFilter(
            allowed_types={"PageView", "Click"},
            allowed_partitions={0, 1},
        )
    )


# ---------------------------------------------------------------------------
# Kafka simulator fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def broker():
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from kafka_simulator import KafkaBroker, TopicConfig

    b = KafkaBroker(broker_id=99)
    b.create_topic("test-topic", TopicConfig(num_partitions=4))
    return b


@pytest.fixture
def broker_with_events(broker):
    """Broker pre-loaded with 100 events across the test-topic."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from kafka_simulator import Acks, ProducerRecord

    for i in range(100):
        broker.produce(
            ProducerRecord(
                topic="test-topic",
                key=f"user-{i % 20}",
                value={"event_type": "PageView", "seq": i},
            ),
            acks=Acks.LEADER,
        )
    return broker


# ---------------------------------------------------------------------------
# Stream analyzer fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def analyzer():
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from stream_analyzer import StreamAnalyzer

    return StreamAnalyzer(window_type="tumbling", window_size_s=5.0, top_n=5)


# ---------------------------------------------------------------------------
# Load generator fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def load_generator():
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from load_generator import LoadGenerator

    return LoadGenerator(payload_size=64, output_sink="null")
