#!/usr/bin/env python3
"""
load_generator.py — Configurable event load testing tool.

Generates synthetic event load across multiple patterns and measures
producer throughput and end-to-end latency. Outputs a JSON report with
percentile latencies, pattern conformance, and throughput over time.

Load patterns:
  steady      — constant rate
  burst       — short high-rate spikes
  ramp_up     — linearly increasing rate
  ramp_down   — linearly decreasing rate
  spike       — brief extreme spike then baseline
  sinusoidal  — sinusoidal rate oscillation

Usage:
    python load_generator.py --pattern steady   --rate 100 --duration 30
    python load_generator.py --pattern burst    --rate 50  --burst-rate 500
    python load_generator.py --pattern sinusoidal --min-rate 10 --max-rate 200 --period 15
    python load_generator.py --pattern all --duration 10 --report-file load-report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, AsyncGenerator, Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------
@dataclass
class LoadEvent:
    """A minimal event record used during load tests."""
    event_id: str
    timestamp: float
    pattern: str
    sequence: int
    payload_size: int
    payload: dict[str, Any]

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


@dataclass
class LatencyBucket:
    """Accumulates latency samples and computes percentiles."""
    samples: list[float] = field(default_factory=list)

    def record(self, latency_ms: float) -> None:
        self.samples.append(latency_ms)

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        sorted_s = sorted(self.samples)
        idx = max(0, int(len(sorted_s) * p / 100) - 1)
        return sorted_s[idx]

    def mean(self) -> float:
        return sum(self.samples) / len(self.samples) if self.samples else 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "count": len(self.samples),
            "mean_ms": round(self.mean(), 3),
            "p50_ms": round(self.percentile(50), 3),
            "p95_ms": round(self.percentile(95), 3),
            "p99_ms": round(self.percentile(99), 3),
            "p999_ms": round(self.percentile(99.9), 3),
            "min_ms": round(min(self.samples), 3) if self.samples else 0.0,
            "max_ms": round(max(self.samples), 3) if self.samples else 0.0,
        }


@dataclass
class PatternResult:
    """Collects results from a single load pattern run."""
    pattern_name: str
    target_rate: float
    actual_rate: float
    duration_s: float
    total_events: int
    latency: LatencyBucket = field(default_factory=LatencyBucket)
    throughput_series: list[dict[str, Any]] = field(default_factory=list)
    dropped_events: int = 0
    errors: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern_name,
            "target_rate_eps": round(self.target_rate, 2),
            "actual_rate_eps": round(self.actual_rate, 2),
            "rate_conformance_pct": round(
                100 * self.actual_rate / max(self.target_rate, 0.001), 1
            ),
            "duration_s": round(self.duration_s, 2),
            "total_events": self.total_events,
            "dropped_events": self.dropped_events,
            "errors": self.errors,
            "latency": self.latency.to_dict(),
            "throughput_series": self.throughput_series,
        }


# ---------------------------------------------------------------------------
# Load patterns — each yields (target_rate, interval) tuples
# ---------------------------------------------------------------------------
class LoadPattern:
    """Base class for load patterns."""

    name: str = "base"

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        """Yield the sleep interval (seconds) between consecutive events."""
        raise NotImplementedError

    @staticmethod
    def _safe_interval(rate: float) -> float:
        return 1.0 / max(rate, 0.1)


class SteadyPattern(LoadPattern):
    """Constant event rate."""
    name = "steady"

    def __init__(self, rate: float) -> None:
        self.rate = rate

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        end = time.monotonic() + duration_s
        interval = self._safe_interval(self.rate)
        while time.monotonic() < end:
            yield interval
            await asyncio.sleep(interval)


class BurstPattern(LoadPattern):
    """
    Alternates between a low baseline rate and a high burst rate.
    Bursts last `burst_duration_s` seconds every `burst_interval_s`.
    """
    name = "burst"

    def __init__(self, baseline_rate: float, burst_rate: float,
                 burst_duration_s: float = 2.0, burst_interval_s: float = 10.0) -> None:
        self.baseline_rate = baseline_rate
        self.burst_rate = burst_rate
        self.burst_duration_s = burst_duration_s
        self.burst_interval_s = burst_interval_s

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        end = time.monotonic() + duration_s
        last_burst = 0.0
        while time.monotonic() < end:
            now = time.monotonic()
            in_burst = (now - last_burst) >= self.burst_interval_s
            if in_burst:
                rate = self.burst_rate
                burst_end = now + self.burst_duration_s
                while time.monotonic() < burst_end and time.monotonic() < end:
                    interval = self._safe_interval(rate)
                    yield interval
                    await asyncio.sleep(interval)
                last_burst = time.monotonic()
            else:
                interval = self._safe_interval(self.baseline_rate)
                yield interval
                await asyncio.sleep(interval)


class RampUpPattern(LoadPattern):
    """Linearly ramps rate from `start_rate` to `end_rate`."""
    name = "ramp_up"

    def __init__(self, start_rate: float, end_rate: float) -> None:
        self.start_rate = start_rate
        self.end_rate = end_rate

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        start = time.monotonic()
        end = start + duration_s
        while time.monotonic() < end:
            elapsed = time.monotonic() - start
            progress = min(elapsed / duration_s, 1.0)
            rate = self.start_rate + (self.end_rate - self.start_rate) * progress
            interval = self._safe_interval(rate)
            yield interval
            await asyncio.sleep(interval)


class RampDownPattern(LoadPattern):
    """Linearly ramps rate from `start_rate` down to `end_rate`."""
    name = "ramp_down"

    def __init__(self, start_rate: float, end_rate: float) -> None:
        self.start_rate = start_rate
        self.end_rate = end_rate

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        start = time.monotonic()
        end = start + duration_s
        while time.monotonic() < end:
            elapsed = time.monotonic() - start
            progress = min(elapsed / duration_s, 1.0)
            rate = self.start_rate - (self.start_rate - self.end_rate) * progress
            interval = self._safe_interval(rate)
            yield interval
            await asyncio.sleep(interval)


class SpikePattern(LoadPattern):
    """
    Baseline rate with a single massive spike in the middle.
    Demonstrates consumer backpressure handling.
    """
    name = "spike"

    def __init__(self, baseline_rate: float, spike_rate: float, spike_duration_s: float = 3.0) -> None:
        self.baseline_rate = baseline_rate
        self.spike_rate = spike_rate
        self.spike_duration_s = spike_duration_s

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        spike_start_pct = 0.4
        start = time.monotonic()
        end = start + duration_s
        spike_triggered = False
        spike_end_time = 0.0

        while time.monotonic() < end:
            now = time.monotonic()
            elapsed = now - start
            progress = elapsed / duration_s

            if not spike_triggered and progress >= spike_start_pct:
                spike_triggered = True
                spike_end_time = now + self.spike_duration_s
                print(f"    [spike] Triggering spike at {elapsed:.1f}s "
                      f"({self.spike_rate} eps for {self.spike_duration_s}s)")

            in_spike = spike_triggered and now < spike_end_time
            rate = self.spike_rate if in_spike else self.baseline_rate
            interval = self._safe_interval(rate)
            yield interval
            await asyncio.sleep(interval)


class SinusoidalPattern(LoadPattern):
    """Oscillates rate between min and max using a sine wave."""
    name = "sinusoidal"

    def __init__(self, min_rate: float, max_rate: float, period_s: float = 20.0) -> None:
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.period_s = period_s

    async def intervals(self, duration_s: float) -> AsyncGenerator[float, None]:
        start = time.monotonic()
        end = start + duration_s
        while time.monotonic() < end:
            elapsed = time.monotonic() - start
            sine = (math.sin(2 * math.pi * elapsed / self.period_s) + 1) / 2
            rate = self.min_rate + (self.max_rate - self.min_rate) * sine
            interval = self._safe_interval(rate)
            yield interval
            await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Load generator engine
# ---------------------------------------------------------------------------
class LoadGenerator:
    """Orchestrates load pattern execution and collects metrics."""

    def __init__(self, payload_size: int = 128, output_sink: str = "null") -> None:
        self.payload_size = payload_size
        self.output_sink = output_sink  # "null", "stdout", "file"
        self._sequence = 0

    def _make_event(self, pattern_name: str) -> LoadEvent:
        self._sequence += 1
        payload = {
            "data": "x" * max(0, self.payload_size - 80),
            "rand": random.random(),
        }
        return LoadEvent(
            event_id=uuid.uuid4().hex[:12],
            timestamp=time.time(),
            pattern=pattern_name,
            sequence=self._sequence,
            payload_size=self.payload_size,
            payload=payload,
        )

    def _emit(self, event: LoadEvent) -> float:
        """Emit an event to the configured sink; return processing latency in ms."""
        t0 = time.monotonic()
        if self.output_sink == "stdout":
            print(event.to_json())
        elif self.output_sink == "null":
            pass  # discard — measure pure generation overhead
        latency = (time.monotonic() - t0) * 1000
        return latency

    async def run_pattern(self, pattern: LoadPattern, duration_s: float) -> PatternResult:
        """Run a single pattern and return results."""
        print(f"\n  Running pattern '{pattern.name}' for {duration_s}s...")
        result = PatternResult(
            pattern_name=pattern.name,
            target_rate=getattr(pattern, "rate", getattr(pattern, "baseline_rate", 0.0)),
            actual_rate=0.0,
            duration_s=duration_s,
            total_events=0,
        )

        start = time.monotonic()
        report_interval = 2.0
        last_report = start
        events_since_report = 0

        async for _ in pattern.intervals(duration_s):
            event = self._make_event(pattern.name)
            latency = self._emit(event)
            result.latency.record(latency)
            result.total_events += 1
            events_since_report += 1

            now = time.monotonic()
            if now - last_report >= report_interval:
                period_rate = events_since_report / (now - last_report)
                result.throughput_series.append({
                    "t": round(now - start, 1),
                    "eps": round(period_rate, 1),
                    "total": result.total_events,
                })
                print(f"    t={now - start:.0f}s  {result.total_events:>6,} events  "
                      f"{period_rate:.0f} eps  lat={latency:.3f}ms")
                events_since_report = 0
                last_report = now

        elapsed = time.monotonic() - start
        result.duration_s = round(elapsed, 3)
        result.actual_rate = result.total_events / max(elapsed, 0.001)
        print(f"  Done: {result.total_events:,} events in {elapsed:.2f}s "
              f"({result.actual_rate:.1f} eps)")
        return result


# ---------------------------------------------------------------------------
# Report renderer
# ---------------------------------------------------------------------------
def render_report(results: list[PatternResult], report_file: Optional[str]) -> dict[str, Any]:
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_patterns": len(results),
        "total_events": sum(r.total_events for r in results),
        "patterns": [r.to_dict() for r in results],
    }

    if report_file:
        with open(report_file, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nReport written to {report_file}")
    else:
        print("\n" + "=" * 60)
        print("  Load Test Report")
        print("=" * 60)
        for r in results:
            d = r.to_dict()
            print(f"\n  Pattern: {d['pattern']}")
            print(f"    Events:        {d['total_events']:,}")
            print(f"    Duration:      {d['duration_s']:.2f}s")
            print(f"    Actual rate:   {d['actual_rate_eps']:.1f} eps")
            print(f"    Conformance:   {d['rate_conformance_pct']}%")
            lat = d["latency"]
            print(f"    Latency p50:   {lat['p50_ms']:.3f}ms")
            print(f"    Latency p95:   {lat['p95_ms']:.3f}ms")
            print(f"    Latency p99:   {lat['p99_ms']:.3f}ms")
        print(f"\n  Total events across all patterns: {report['total_events']:,}")
        print("=" * 60)

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load generator — configurable event load patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--pattern",
                        choices=["steady", "burst", "ramp_up", "ramp_down", "spike", "sinusoidal", "all"],
                        default="steady")
    parser.add_argument("--duration", type=float, default=15.0, help="Pattern duration in seconds")
    parser.add_argument("--rate", type=float, default=50.0, help="Base event rate (eps)")
    parser.add_argument("--burst-rate", type=float, default=500.0, help="Burst rate (eps)")
    parser.add_argument("--burst-duration", type=float, default=2.0, help="Burst duration (s)")
    parser.add_argument("--burst-interval", type=float, default=10.0, help="Time between bursts (s)")
    parser.add_argument("--min-rate", type=float, default=10.0, help="Min rate for sinusoidal")
    parser.add_argument("--max-rate", type=float, default=200.0, help="Max rate for sinusoidal")
    parser.add_argument("--period", type=float, default=20.0, help="Sinusoidal period (s)")
    parser.add_argument("--spike-rate", type=float, default=1000.0, help="Spike peak rate (eps)")
    parser.add_argument("--spike-duration", type=float, default=3.0, help="Spike duration (s)")
    parser.add_argument("--payload-size", type=int, default=128, help="Event payload size in bytes")
    parser.add_argument("--sink", choices=["null", "stdout"], default="null",
                        help="Output sink (null=discard for throughput measurement)")
    parser.add_argument("--report-file", default=None, help="Write JSON report to file")
    return parser.parse_args(argv)


def build_patterns(args: argparse.Namespace) -> list[LoadPattern]:
    all_patterns: list[LoadPattern] = [
        SteadyPattern(args.rate),
        BurstPattern(args.rate, args.burst_rate, args.burst_duration, args.burst_interval),
        RampUpPattern(args.min_rate, args.max_rate),
        RampDownPattern(args.max_rate, args.min_rate),
        SpikePattern(args.rate, args.spike_rate, args.spike_duration),
        SinusoidalPattern(args.min_rate, args.max_rate, args.period),
    ]
    if args.pattern == "all":
        return all_patterns
    name_map = {p.name: p for p in all_patterns}
    chosen = name_map.get(args.pattern)
    if chosen is None:
        print(f"Unknown pattern: {args.pattern}", file=sys.stderr)
        sys.exit(1)
    return [chosen]


async def _async_main(args: argparse.Namespace) -> None:
    patterns = build_patterns(args)
    gen = LoadGenerator(payload_size=args.payload_size, output_sink=args.sink)

    print(f"\nLoad generator starting — {len(patterns)} pattern(s), "
          f"duration={args.duration}s each")

    results: list[PatternResult] = []
    for pattern in patterns:
        result = await gen.run_pattern(pattern, args.duration)
        results.append(result)

    render_report(results, args.report_file)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
