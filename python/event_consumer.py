#!/usr/bin/env python3
"""
event_consumer.py — Event consumer with filtering, metrics, and offset tracking.

Subscribes to an SSE or WebSocket event stream, applies configurable filters,
and accumulates throughput / latency / count metrics. Simulates consumer-group
semantics with per-partition offset tracking.

Usage:
    python event_consumer.py --mode sse  --url http://localhost:8080/events
    python event_consumer.py --mode ws   --url ws://localhost:8080/events \\
        --filter-types PageView Purchase --filter-partitions 0 1
    python event_consumer.py --mode file --source events.jsonl --stats-format table
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("event_consumer")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class ConsumedEvent:
    """Wrapper around a raw event dict, enriched with consumer-side metadata."""

    raw: dict[str, Any]
    received_at: float = field(default_factory=time.time)

    @property
    def event_id(self) -> str:
        return self.raw.get("event_id", "")

    @property
    def event_type(self) -> str:
        return self.raw.get("event_type", "")

    @property
    def partition(self) -> int:
        return int(self.raw.get("partition", 0))

    @property
    def sequence_number(self) -> int:
        return int(self.raw.get("sequence_number", 0))

    @property
    def producer_timestamp(self) -> float:
        return float(self.raw.get("timestamp", 0.0))

    @property
    def end_to_end_latency_ms(self) -> float:
        """Milliseconds between event creation and consumer receipt."""
        return (self.received_at - self.producer_timestamp) * 1000.0


@dataclass
class PartitionOffset:
    """Tracks the last committed offset for a single partition."""

    partition: int
    committed_offset: int = -1
    pending_offset: int = -1

    def update(self, sequence: int) -> None:
        self.pending_offset = max(self.pending_offset, sequence)

    def commit(self) -> None:
        self.committed_offset = self.pending_offset


@dataclass
class ConsumerMetrics:
    """Accumulates statistics over the lifetime of a consumer session."""

    start_time: float = field(default_factory=time.time)
    total_received: int = 0
    total_filtered_out: int = 0
    total_processed: int = 0
    by_type: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    by_partition: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    latencies_ms: list[float] = field(default_factory=list)
    _throughput_samples: list[tuple[float, int]] = field(default_factory=list)

    def record(self, event: ConsumedEvent) -> None:
        self.total_processed += 1
        self.by_type[event.event_type] += 1
        self.by_partition[event.partition] += 1
        latency = event.end_to_end_latency_ms
        if latency >= 0:
            self.latencies_ms.append(latency)
        self._throughput_samples.append((time.monotonic(), self.total_processed))

    @property
    def elapsed_seconds(self) -> float:
        return max(time.monotonic() - self.start_time, 0.001)

    @property
    def throughput_eps(self) -> float:
        return self.total_processed / self.elapsed_seconds

    def percentile_latency(self, p: float) -> Optional[float]:
        if not self.latencies_ms:
            return None
        sorted_lats = sorted(self.latencies_ms)
        idx = int(len(sorted_lats) * p / 100)
        return sorted_lats[min(idx, len(sorted_lats) - 1)]

    def to_dict(self) -> dict[str, Any]:
        return {
            "elapsed_s": round(self.elapsed_seconds, 2),
            "total_received": self.total_received,
            "total_filtered_out": self.total_filtered_out,
            "total_processed": self.total_processed,
            "throughput_eps": round(self.throughput_eps, 2),
            "latency_ms": {
                "p50": round(self.percentile_latency(50) or 0, 2),
                "p95": round(self.percentile_latency(95) or 0, 2),
                "p99": round(self.percentile_latency(99) or 0, 2),
                "count": len(self.latencies_ms),
            },
            "by_type": dict(self.by_type),
            "by_partition": {str(k): v for k, v in self.by_partition.items()},
        }

    def print_table(self) -> None:
        d = self.to_dict()
        print("\n┌─────────────────────────── Consumer Metrics ───────────────────────────┐")
        print(f"│  Elapsed:     {d['elapsed_s']:>8.2f} s                                          │")
        print(f"│  Received:    {d['total_received']:>8,}                                          │")
        print(f"│  Filtered:    {d['total_filtered_out']:>8,}                                          │")
        print(f"│  Processed:   {d['total_processed']:>8,}                                          │")
        print(f"│  Throughput:  {d['throughput_eps']:>8.2f} events/s                               │")
        lm = d["latency_ms"]
        print(f"│  Latency p50: {lm['p50']:>8.2f} ms                                          │")
        print(f"│  Latency p95: {lm['p95']:>8.2f} ms                                          │")
        print(f"│  Latency p99: {lm['p99']:>8.2f} ms                                          │")
        print("├─────────────────────── By Event Type ──────────────────────────────────┤")
        for etype, cnt in sorted(d["by_type"].items(), key=lambda x: -x[1]):
            print(f"│  {etype:<20}  {cnt:>8,}                                          │")
        print("├─────────────────────── By Partition ───────────────────────────────────┤")
        for part, cnt in sorted(d["by_partition"].items(), key=lambda x: int(x[0])):
            print(f"│  partition-{part:<9}  {cnt:>8,}                                          │")
        print("└─────────────────────────────────────────────────────────────────────────┘\n")


# ---------------------------------------------------------------------------
# Event filter
# ---------------------------------------------------------------------------
class EventFilter:
    """Evaluates whether a ConsumedEvent should be processed or dropped."""

    def __init__(
        self,
        allowed_types: Optional[set[str]] = None,
        allowed_partitions: Optional[set[int]] = None,
        custom_predicate: Optional[Callable[[ConsumedEvent], bool]] = None,
    ) -> None:
        self.allowed_types = allowed_types
        self.allowed_partitions = allowed_partitions
        self.custom_predicate = custom_predicate

    def accepts(self, event: ConsumedEvent) -> bool:
        if self.allowed_types and event.event_type not in self.allowed_types:
            return False
        if self.allowed_partitions and event.partition not in self.allowed_partitions:
            return False
        if self.custom_predicate and not self.custom_predicate(event):
            return False
        return True


# ---------------------------------------------------------------------------
# Consumer group / offset manager
# ---------------------------------------------------------------------------
class ConsumerGroup:
    """
    Simulates Kafka consumer group semantics with per-partition offset tracking.
    Offsets are tracked in memory; a real implementation would persist to broker.
    """

    def __init__(self, group_id: str, num_partitions: int) -> None:
        self.group_id = group_id
        self.offsets: dict[int, PartitionOffset] = {
            p: PartitionOffset(partition=p) for p in range(num_partitions)
        }
        self._processed: set[str] = set()  # duplicate detection via event_id

    def is_duplicate(self, event: ConsumedEvent) -> bool:
        return event.event_id in self._processed

    def mark_processed(self, event: ConsumedEvent) -> None:
        self._processed.add(event.event_id)
        partition_offset = self.offsets.get(event.partition)
        if partition_offset is None:
            # Partition appeared after group init — register it
            self.offsets[event.partition] = PartitionOffset(partition=event.partition)
            partition_offset = self.offsets[event.partition]
        partition_offset.update(event.sequence_number)

    def commit_all(self) -> dict[int, int]:
        """Commit all pending offsets and return the new committed state."""
        for po in self.offsets.values():
            po.commit()
        return {p: po.committed_offset for p, po in self.offsets.items()}

    def lag(self) -> dict[int, int]:
        """Return per-partition consumer lag (pending - committed)."""
        return {
            p: max(po.pending_offset - po.committed_offset, 0)
            for p, po in self.offsets.items()
        }


# ---------------------------------------------------------------------------
# Event consumer core
# ---------------------------------------------------------------------------
class EventConsumer:
    """
    Reads events from a stream, applies filtering, tracks offsets, and
    accumulates metrics.
    """

    def __init__(
        self,
        event_filter: Optional[EventFilter] = None,
        consumer_group: Optional[ConsumerGroup] = None,
        commit_interval: int = 100,
    ) -> None:
        self.event_filter = event_filter or EventFilter()
        self.consumer_group = consumer_group
        self.commit_interval = commit_interval
        self.metrics = ConsumerMetrics()

    def _process_raw(self, raw_json: str) -> Optional[ConsumedEvent]:
        """Parse, filter, deduplicate, and record an event."""
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse event JSON: %s — %s", raw_json[:80], exc)
            return None

        event = ConsumedEvent(raw=data)
        self.metrics.total_received += 1

        # Deduplication
        if self.consumer_group and self.consumer_group.is_duplicate(event):
            self.metrics.total_filtered_out += 1
            return None

        # Filtering
        if not self.event_filter.accepts(event):
            self.metrics.total_filtered_out += 1
            return None

        self.metrics.record(event)
        if self.consumer_group:
            self.consumer_group.mark_processed(event)
            if self.metrics.total_processed % self.commit_interval == 0:
                committed = self.consumer_group.commit_all()
                logger.debug("Committed offsets: %s", committed)

        return event

    async def consume_sse(self, url: str, max_events: Optional[int] = None) -> None:
        """Consume from an SSE endpoint."""
        try:
            import aiohttp
        except ImportError:
            logger.error("aiohttp required: pip install aiohttp")
            sys.exit(1)

        logger.info("Connecting to SSE stream: %s", url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                async for line in response.content:
                    decoded = line.decode("utf-8").strip()
                    if decoded.startswith("data:"):
                        raw = decoded[5:].strip()
                        event = self._process_raw(raw)
                        if event:
                            self._on_event(event)
                    if max_events and self.metrics.total_processed >= max_events:
                        break

    async def consume_websocket(self, url: str, max_events: Optional[int] = None) -> None:
        """Consume from a WebSocket endpoint."""
        try:
            import websockets
        except ImportError:
            logger.error("websockets required: pip install websockets")
            sys.exit(1)

        logger.info("Connecting to WebSocket stream: %s", url)
        async with websockets.connect(url) as ws:  # type: ignore[attr-defined]
            async for message in ws:
                event = self._process_raw(message)
                if event:
                    self._on_event(event)
                if max_events and self.metrics.total_processed >= max_events:
                    break

    async def consume_file(self, path: str) -> None:
        """Replay events from a JSON-lines file."""
        logger.info("Replaying events from file: %s", path)
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                event = self._process_raw(line)
                if event:
                    self._on_event(event)
                await asyncio.sleep(0)  # yield to event loop

    def _on_event(self, event: ConsumedEvent) -> None:
        """Hook called for every successfully processed event."""
        if self.metrics.total_processed % 500 == 0:
            logger.info(
                "processed=%d throughput=%.1f/s",
                self.metrics.total_processed,
                self.metrics.throughput_eps,
            )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Event consumer — subscribes to and processes event streams",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--mode", choices=["sse", "ws", "file"], default="file",
                        help="Input mode (default: file)")
    parser.add_argument("--url", default="http://localhost:8080/events",
                        help="Stream URL for sse/ws modes")
    parser.add_argument("--source", default="events.jsonl",
                        help="Path to JSON-lines file (file mode)")
    parser.add_argument("--filter-types", nargs="+", default=None,
                        help="Only process these event types")
    parser.add_argument("--filter-partitions", nargs="+", type=int, default=None,
                        help="Only process these partition numbers")
    parser.add_argument("--consumer-group", default="cg-default",
                        help="Consumer group ID (default: cg-default)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Total partitions to track (default: 4)")
    parser.add_argument("--commit-interval", type=int, default=100,
                        help="Commit offsets every N processed events (default: 100)")
    parser.add_argument("--count", type=int, default=None,
                        help="Stop after processing N events")
    parser.add_argument("--stats-format", choices=["json", "table"], default="table",
                        help="Stats output format (default: table)")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    event_filter = EventFilter(
        allowed_types=set(args.filter_types) if args.filter_types else None,
        allowed_partitions=set(args.filter_partitions) if args.filter_partitions else None,
    )
    consumer_group = ConsumerGroup(args.consumer_group, args.partitions)
    consumer = EventConsumer(
        event_filter=event_filter,
        consumer_group=consumer_group,
        commit_interval=args.commit_interval,
    )

    logger.info(
        "Starting consumer | mode=%s group=%s",
        args.mode, args.consumer_group,
    )

    try:
        if args.mode == "file":
            asyncio.run(consumer.consume_file(args.source))
        elif args.mode == "sse":
            asyncio.run(consumer.consume_sse(args.url, args.count))
        elif args.mode == "ws":
            asyncio.run(consumer.consume_websocket(args.url, args.count))
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        if args.stats_format == "json":
            print(json.dumps(consumer.metrics.to_dict(), indent=2))
        else:
            consumer.metrics.print_table()

        lag = consumer_group.lag()
        if any(v > 0 for v in lag.values()):
            logger.warning("Uncommitted lag: %s", lag)


if __name__ == "__main__":
    main()
