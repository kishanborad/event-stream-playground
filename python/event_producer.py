#!/usr/bin/env python3
"""
event_producer.py — Simulated event producer for SSE and WebSocket streams.

Generates realistic event payloads for browser-side visualization in the
event-stream playground. Supports multiple event types, configurable
throughput, and key-based or round-robin partitioning.

Usage:
    python event_producer.py --mode sse --rate 50 --partitions 4
    python event_producer.py --mode ws  --port 8765 --event-types PageView Click
    python event_producer.py --mode stdout --count 1000 --output-file events.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, AsyncGenerator, Callable, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("event_producer")


# ---------------------------------------------------------------------------
# Event taxonomy
# ---------------------------------------------------------------------------
class EventType(str, Enum):
    PAGE_VIEW = "PageView"
    CLICK = "Click"
    PURCHASE = "Purchase"
    HEARTBEAT = "Heartbeat"
    METRIC_SAMPLE = "MetricSample"
    SENSOR_READING = "SensorReading"


@dataclass
class EventPayload:
    """Canonical event envelope used across the playground."""

    event_id: str
    event_type: str
    timestamp: float          # Unix epoch seconds (float for sub-ms precision)
    partition_key: str
    partition: int
    sequence_number: int
    producer_id: str
    metadata: dict[str, Any]
    payload: dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


# ---------------------------------------------------------------------------
# Payload factories — one per event type
# ---------------------------------------------------------------------------
_USER_IDS = [f"user_{i:04d}" for i in range(1, 201)]
_PAGES = ["/", "/pricing", "/docs", "/blog", "/signup", "/login", "/dashboard"]
_PRODUCTS = ["sku-001", "sku-042", "sku-105", "sku-203", "sku-317"]
_HOSTS = [f"node-{i:02d}.prod" for i in range(1, 9)]
_SENSORS = [f"sensor-{chr(65 + i)}{j:02d}" for i in range(4) for j in range(1, 6)]


def _page_view_payload() -> tuple[str, dict[str, Any]]:
    user_id = random.choice(_USER_IDS)
    return user_id, {
        "user_id": user_id,
        "page": random.choice(_PAGES),
        "referrer": random.choice(["google", "direct", "twitter", "newsletter", ""]),
        "session_id": str(uuid.uuid4())[:8],
        "duration_ms": random.randint(200, 45_000),
        "viewport": {"w": random.choice([1280, 1440, 1920]), "h": random.choice([720, 900, 1080])},
    }


def _click_payload() -> tuple[str, dict[str, Any]]:
    user_id = random.choice(_USER_IDS)
    return user_id, {
        "user_id": user_id,
        "element": random.choice(["btn-cta", "nav-link", "card", "icon", "hero-banner"]),
        "x": random.randint(0, 1920),
        "y": random.randint(0, 1080),
        "page": random.choice(_PAGES),
    }


def _purchase_payload() -> tuple[str, dict[str, Any]]:
    user_id = random.choice(_USER_IDS)
    qty = random.randint(1, 5)
    price = round(random.uniform(9.99, 299.99), 2)
    return user_id, {
        "user_id": user_id,
        "order_id": f"ord-{uuid.uuid4().hex[:10]}",
        "product_id": random.choice(_PRODUCTS),
        "quantity": qty,
        "unit_price_usd": price,
        "total_usd": round(price * qty, 2),
        "currency": "USD",
        "payment_method": random.choice(["card", "paypal", "crypto"]),
    }


def _heartbeat_payload() -> tuple[str, dict[str, Any]]:
    host = random.choice(_HOSTS)
    return host, {
        "host": host,
        "status": "ok",
        "uptime_s": random.randint(3600, 86400 * 30),
    }


def _metric_sample_payload() -> tuple[str, dict[str, Any]]:
    host = random.choice(_HOSTS)
    return host, {
        "host": host,
        "cpu_pct": round(random.gauss(35, 20), 1),
        "mem_mb": random.randint(512, 8192),
        "disk_io_kbps": random.randint(0, 5000),
        "net_rx_kbps": random.randint(0, 10000),
        "net_tx_kbps": random.randint(0, 10000),
        "metric_name": random.choice(["cpu", "memory", "disk_io", "network"]),
    }


def _sensor_reading_payload() -> tuple[str, dict[str, Any]]:
    sensor_id = random.choice(_SENSORS)
    return sensor_id, {
        "sensor_id": sensor_id,
        "temperature_c": round(random.gauss(22, 5), 2),
        "humidity_pct": round(random.uniform(30, 80), 1),
        "pressure_hpa": round(random.gauss(1013, 5), 1),
        "battery_pct": random.randint(10, 100),
    }


_PAYLOAD_FACTORIES: dict[EventType, Callable[[], tuple[str, dict[str, Any]]]] = {
    EventType.PAGE_VIEW: _page_view_payload,
    EventType.CLICK: _click_payload,
    EventType.PURCHASE: _purchase_payload,
    EventType.HEARTBEAT: _heartbeat_payload,
    EventType.METRIC_SAMPLE: _metric_sample_payload,
    EventType.SENSOR_READING: _sensor_reading_payload,
}


# ---------------------------------------------------------------------------
# Partitioner
# ---------------------------------------------------------------------------
class Partitioner:
    """Assigns events to partitions via round-robin or key-based hashing."""

    def __init__(self, num_partitions: int, strategy: str = "key") -> None:
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")
        self.num_partitions = num_partitions
        self.strategy = strategy
        self._rr_counter = 0

    def assign(self, partition_key: str) -> int:
        if self.strategy == "roundrobin":
            partition = self._rr_counter % self.num_partitions
            self._rr_counter += 1
            return partition
        # key-based: stable hash so same key always lands on same partition
        return hash(partition_key) % self.num_partitions


# ---------------------------------------------------------------------------
# Rate limiter (token-bucket style)
# ---------------------------------------------------------------------------
class RateLimiter:
    """Simple token-bucket rate limiter for async generators."""

    def __init__(self, events_per_second: float) -> None:
        self.interval = 1.0 / max(events_per_second, 0.001)
        self._last = time.monotonic()

    async def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last
        if elapsed < self.interval:
            await asyncio.sleep(self.interval - elapsed)
        self._last = time.monotonic()


# ---------------------------------------------------------------------------
# Core producer
# ---------------------------------------------------------------------------
class EventProducer:
    """
    Generates a stream of EventPayload objects at a configured rate.

    Parameters
    ----------
    event_types : list of EventType
        Which event types to emit. Chosen randomly each tick.
    partitions : int
        Number of topic partitions.
    partition_strategy : str
        "key" (default) or "roundrobin".
    events_per_second : float
        Target throughput. Actual throughput may be lower on slow machines.
    producer_id : str
        Identifier embedded in every event envelope.
    """

    def __init__(
        self,
        event_types: list[EventType],
        partitions: int = 4,
        partition_strategy: str = "key",
        events_per_second: float = 10.0,
        producer_id: Optional[str] = None,
    ) -> None:
        self.event_types = event_types or list(EventType)
        self.partitioner = Partitioner(partitions, partition_strategy)
        self.rate_limiter = RateLimiter(events_per_second)
        self.producer_id = producer_id or f"producer-{uuid.uuid4().hex[:6]}"
        self._sequence = 0

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def build_event(self) -> EventPayload:
        """Build a single event without rate limiting."""
        event_type = random.choice(self.event_types)
        factory = _PAYLOAD_FACTORIES[event_type]
        partition_key, data = factory()
        partition = self.partitioner.assign(partition_key)

        return EventPayload(
            event_id=str(uuid.uuid4()),
            event_type=event_type.value,
            timestamp=time.time(),
            partition_key=partition_key,
            partition=partition,
            sequence_number=self._next_sequence(),
            producer_id=self.producer_id,
            metadata={
                "schema_version": "1.0",
                "topic": "events",
                "num_partitions": self.partitioner.num_partitions,
            },
            payload=data,
        )

    async def stream(self, max_events: Optional[int] = None) -> AsyncGenerator[EventPayload, None]:
        """Yield events respecting the configured rate limit."""
        count = 0
        while max_events is None or count < max_events:
            await self.rate_limiter.wait()
            yield self.build_event()
            count += 1


# ---------------------------------------------------------------------------
# Output modes
# ---------------------------------------------------------------------------
async def run_stdout(producer: EventProducer, count: Optional[int], output_file: Optional[str]) -> None:
    """Write JSON-lines to stdout or a file."""
    out = open(output_file, "w") if output_file else sys.stdout
    emitted = 0
    try:
        async for event in producer.stream(max_events=count):
            line = event.to_json() + "\n"
            out.write(line)
            if out is not sys.stdout:
                sys.stdout.write(f"\r  emitted {emitted + 1:>8,} events")
                sys.stdout.flush()
            emitted += 1
    finally:
        if out is not sys.stdout:
            out.close()
            print(f"\nWrote {emitted:,} events to {output_file}")


async def run_sse(producer: EventProducer, host: str, port: int, count: Optional[int]) -> None:
    """Serve events as Server-Sent Events over HTTP."""
    try:
        from aiohttp import web
    except ImportError:
        logger.error("aiohttp is required for SSE mode: pip install aiohttp")
        sys.exit(1)

    async def sse_handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        response.headers["Content-Type"] = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Access-Control-Allow-Origin"] = "*"
        await response.prepare(request)

        emitted = 0
        async for event in producer.stream(max_events=count):
            data = event.to_json()
            await response.write(f"id: {event.event_id}\ndata: {data}\n\n".encode())
            emitted += 1
            if emitted % 100 == 0:
                logger.info("SSE: emitted %d events", emitted)

        return response

    app = web.Application()
    app.router.add_get("/events", sse_handler)
    app.router.add_get("/health", lambda r: web.json_response({"status": "ok"}))

    logger.info("SSE producer listening on http://%s:%d/events", host, port)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    await asyncio.Event().wait()  # run forever


async def run_websocket(producer: EventProducer, host: str, port: int, count: Optional[int]) -> None:
    """Broadcast events to all connected WebSocket clients."""
    try:
        import websockets
    except ImportError:
        logger.error("websockets is required for ws mode: pip install websockets")
        sys.exit(1)

    clients: set = set()

    async def handler(websocket: Any) -> None:
        clients.add(websocket)
        logger.info("WS client connected (%d total)", len(clients))
        try:
            await websocket.wait_closed()
        finally:
            clients.discard(websocket)
            logger.info("WS client disconnected (%d total)", len(clients))

    async def broadcast() -> None:
        emitted = 0
        async for event in producer.stream(max_events=count):
            if clients:
                data = event.to_json()
                await asyncio.gather(
                    *[c.send(data) for c in list(clients)],
                    return_exceptions=True,
                )
            emitted += 1
            if emitted % 100 == 0:
                logger.info("WS: emitted %d events to %d clients", emitted, len(clients))

    server = await websockets.serve(handler, host, port)  # type: ignore[attr-defined]
    logger.info("WebSocket producer listening on ws://%s:%d", host, port)
    await asyncio.gather(server.wait_closed(), broadcast())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Event producer — generates simulated event streams",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--mode", choices=["sse", "ws", "stdout"], default="stdout",
                        help="Output mode (default: stdout)")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host for sse/ws modes")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument("--rate", type=float, default=10.0,
                        help="Events per second (default: 10)")
    parser.add_argument("--partitions", type=int, default=4,
                        help="Number of partitions (default: 4)")
    parser.add_argument("--partition-strategy", choices=["key", "roundrobin"], default="key",
                        help="Partition assignment strategy (default: key)")
    parser.add_argument("--event-types", nargs="+",
                        choices=[e.value for e in EventType],
                        default=[e.value for e in EventType],
                        help="Event types to emit (default: all)")
    parser.add_argument("--count", type=int, default=None,
                        help="Stop after N events (default: unlimited)")
    parser.add_argument("--output-file", default=None,
                        help="Write JSON-lines to file instead of stdout (stdout mode only)")
    parser.add_argument("--producer-id", default=None,
                        help="Override auto-generated producer ID")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    event_types = [EventType(t) for t in args.event_types]
    producer = EventProducer(
        event_types=event_types,
        partitions=args.partitions,
        partition_strategy=args.partition_strategy,
        events_per_second=args.rate,
        producer_id=args.producer_id,
    )

    logger.info(
        "Starting producer | mode=%s rate=%.1f/s partitions=%d strategy=%s types=%s",
        args.mode, args.rate, args.partitions, args.partition_strategy,
        [e.value for e in event_types],
    )

    if args.mode == "stdout":
        asyncio.run(run_stdout(producer, args.count, args.output_file))
    elif args.mode == "sse":
        asyncio.run(run_sse(producer, args.host, args.port, args.count))
    elif args.mode == "ws":
        asyncio.run(run_websocket(producer, args.host, args.port, args.count))


if __name__ == "__main__":
    main()
