#!/usr/bin/env python3
"""
stream_analyzer.py — Offline analytics engine for event log files.

Reads JSON-lines event logs produced by event_producer.py, computes
windowed aggregations, detects anomalies, and generates a standalone
HTML analytics dashboard that matches the portfolio dark theme.

Usage:
    python stream_analyzer.py --input events.jsonl
    python stream_analyzer.py --input events.jsonl --window-type sliding --window-size 30
    python stream_analyzer.py --input events.jsonl --output report.html --top-n 10
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class EventRecord:
    """Lightweight representation of a parsed event log entry."""

    event_id: str
    event_type: str
    timestamp: float
    partition: int
    sequence_number: int
    producer_id: str
    partition_key: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "EventRecord":
        return cls(
            event_id=d.get("event_id", ""),
            event_type=d.get("event_type", "unknown"),
            timestamp=float(d.get("timestamp", 0.0)),
            partition=int(d.get("partition", 0)),
            sequence_number=int(d.get("sequence_number", 0)),
            producer_id=d.get("producer_id", ""),
            partition_key=d.get("partition_key", ""),
        )


@dataclass
class Window:
    """A time bucket containing events."""

    start: float
    end: float
    events: list[EventRecord] = field(default_factory=list)

    @property
    def duration_s(self) -> float:
        return self.end - self.start

    @property
    def count(self) -> int:
        return len(self.events)

    @property
    def throughput_eps(self) -> float:
        return self.count / max(self.duration_s, 0.001)

    def type_distribution(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for e in self.events:
            counts[e.event_type] += 1
        return dict(counts)

    def partition_distribution(self) -> dict[int, int]:
        counts: dict[int, int] = defaultdict(int)
        for e in self.events:
            counts[e.partition] += 1
        return dict(counts)


@dataclass
class AnomalyReport:
    """Describes a detected anomaly in the event stream."""

    kind: str          # "rate_spike", "rate_gap", "duplicate", "sequence_gap"
    window_start: float
    window_end: float
    description: str
    severity: str      # "low", "medium", "high"
    details: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Windowing strategies
# ---------------------------------------------------------------------------
class TumblingWindowAggregator:
    """Non-overlapping fixed-size time windows."""

    def __init__(self, window_size_s: float) -> None:
        self.window_size_s = window_size_s

    def aggregate(self, events: list[EventRecord]) -> list[Window]:
        if not events:
            return []
        start = events[0].timestamp
        end_ts = events[-1].timestamp
        windows: list[Window] = []
        bucket_start = math.floor(start / self.window_size_s) * self.window_size_s
        while bucket_start <= end_ts:
            bucket_end = bucket_start + self.window_size_s
            window = Window(start=bucket_start, end=bucket_end)
            for e in events:
                if bucket_start <= e.timestamp < bucket_end:
                    window.events.append(e)
            windows.append(window)
            bucket_start = bucket_end
        return windows


class SlidingWindowAggregator:
    """Overlapping sliding windows."""

    def __init__(self, window_size_s: float, slide_s: float) -> None:
        self.window_size_s = window_size_s
        self.slide_s = slide_s

    def aggregate(self, events: list[EventRecord]) -> list[Window]:
        if not events:
            return []
        start = events[0].timestamp
        end_ts = events[-1].timestamp
        windows: list[Window] = []
        cursor = start
        while cursor <= end_ts:
            window_end = cursor + self.window_size_s
            window = Window(start=cursor, end=window_end)
            for e in events:
                if cursor <= e.timestamp < window_end:
                    window.events.append(e)
            windows.append(window)
            cursor += self.slide_s
        return windows


class SessionWindowAggregator:
    """Session-based windows — split when gap between events exceeds threshold."""

    def __init__(self, gap_threshold_s: float = 5.0) -> None:
        self.gap_threshold_s = gap_threshold_s

    def aggregate(self, events: list[EventRecord]) -> list[Window]:
        if not events:
            return []
        sessions: list[Window] = []
        session = Window(start=events[0].timestamp, end=events[0].timestamp)
        session.events.append(events[0])

        for event in events[1:]:
            gap = event.timestamp - session.events[-1].timestamp
            if gap > self.gap_threshold_s:
                sessions.append(session)
                session = Window(start=event.timestamp, end=event.timestamp)
            session.events.append(event)
            session.end = event.timestamp

        sessions.append(session)
        return sessions


# ---------------------------------------------------------------------------
# Anomaly detector
# ---------------------------------------------------------------------------
class AnomalyDetector:
    """Identifies statistical anomalies in event stream windows."""

    def __init__(self, spike_threshold: float = 3.0, gap_threshold_s: float = 10.0) -> None:
        self.spike_threshold = spike_threshold   # standard deviations
        self.gap_threshold_s = gap_threshold_s

    def detect(self, windows: list[Window], events: list[EventRecord]) -> list[AnomalyReport]:
        anomalies: list[AnomalyReport] = []
        anomalies.extend(self._detect_rate_anomalies(windows))
        anomalies.extend(self._detect_gaps(events))
        anomalies.extend(self._detect_duplicates(events))
        anomalies.extend(self._detect_sequence_gaps(events))
        return anomalies

    def _detect_rate_anomalies(self, windows: list[Window]) -> list[AnomalyReport]:
        anomalies: list[AnomalyReport] = []
        rates = [w.throughput_eps for w in windows if w.count > 0]
        if len(rates) < 3:
            return anomalies
        mean = sum(rates) / len(rates)
        variance = sum((r - mean) ** 2 for r in rates) / len(rates)
        std = math.sqrt(variance)
        if std < 0.001:
            return anomalies

        for window, rate in zip(windows, rates):
            z_score = abs(rate - mean) / std
            if z_score > self.spike_threshold:
                severity = "high" if z_score > self.spike_threshold * 2 else "medium"
                kind = "rate_spike" if rate > mean else "rate_gap"
                anomalies.append(AnomalyReport(
                    kind=kind,
                    window_start=window.start,
                    window_end=window.end,
                    description=f"Rate {rate:.1f} eps deviates {z_score:.1f}σ from mean {mean:.1f} eps",
                    severity=severity,
                    details={"rate_eps": round(rate, 2), "mean_eps": round(mean, 2), "z_score": round(z_score, 2)},
                ))
        return anomalies

    def _detect_gaps(self, events: list[EventRecord]) -> list[AnomalyReport]:
        anomalies: list[AnomalyReport] = []
        for i in range(1, len(events)):
            gap = events[i].timestamp - events[i - 1].timestamp
            if gap > self.gap_threshold_s:
                anomalies.append(AnomalyReport(
                    kind="rate_gap",
                    window_start=events[i - 1].timestamp,
                    window_end=events[i].timestamp,
                    description=f"Event gap of {gap:.1f}s between events",
                    severity="medium" if gap < self.gap_threshold_s * 3 else "high",
                    details={"gap_s": round(gap, 2), "prev_event": events[i - 1].event_id},
                ))
        return anomalies

    def _detect_duplicates(self, events: list[EventRecord]) -> list[AnomalyReport]:
        seen: set[str] = set()
        duplicates: list[AnomalyReport] = []
        for event in events:
            if event.event_id in seen:
                duplicates.append(AnomalyReport(
                    kind="duplicate",
                    window_start=event.timestamp,
                    window_end=event.timestamp,
                    description=f"Duplicate event_id detected: {event.event_id}",
                    severity="high",
                    details={"event_id": event.event_id, "event_type": event.event_type},
                ))
            seen.add(event.event_id)
        return duplicates

    def _detect_sequence_gaps(self, events: list[EventRecord]) -> list[AnomalyReport]:
        anomalies: list[AnomalyReport] = []
        by_producer: dict[str, list[EventRecord]] = defaultdict(list)
        for e in events:
            by_producer[e.producer_id].append(e)

        for producer_id, producer_events in by_producer.items():
            sorted_events = sorted(producer_events, key=lambda e: e.sequence_number)
            for i in range(1, len(sorted_events)):
                gap = sorted_events[i].sequence_number - sorted_events[i - 1].sequence_number
                if gap > 1:
                    anomalies.append(AnomalyReport(
                        kind="sequence_gap",
                        window_start=sorted_events[i - 1].timestamp,
                        window_end=sorted_events[i].timestamp,
                        description=f"Sequence gap of {gap - 1} in producer {producer_id}",
                        severity="medium",
                        details={
                            "producer_id": producer_id,
                            "from_seq": sorted_events[i - 1].sequence_number,
                            "to_seq": sorted_events[i].sequence_number,
                            "gap": gap - 1,
                        },
                    ))
        return anomalies


# ---------------------------------------------------------------------------
# Analytics engine
# ---------------------------------------------------------------------------
class StreamAnalyzer:
    """
    Top-level analytics engine: loads events, runs windowing and anomaly
    detection, and renders results.
    """

    def __init__(
        self,
        window_type: str = "tumbling",
        window_size_s: float = 10.0,
        slide_s: float = 5.0,
        top_n: int = 5,
        spike_threshold: float = 3.0,
        gap_threshold_s: float = 10.0,
    ) -> None:
        self.window_type = window_type
        self.window_size_s = window_size_s
        self.slide_s = slide_s
        self.top_n = top_n
        self.anomaly_detector = AnomalyDetector(spike_threshold, gap_threshold_s)

        if window_type == "tumbling":
            self.aggregator: Any = TumblingWindowAggregator(window_size_s)
        elif window_type == "sliding":
            self.aggregator = SlidingWindowAggregator(window_size_s, slide_s)
        elif window_type == "session":
            self.aggregator = SessionWindowAggregator(gap_threshold_s)
        else:
            raise ValueError(f"Unknown window type: {window_type}")

    def load_events(self, path: str) -> list[EventRecord]:
        records: list[EventRecord] = []
        errors = 0
        with open(path) as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    records.append(EventRecord.from_dict(data))
                except (json.JSONDecodeError, KeyError) as exc:
                    errors += 1
                    if errors <= 5:
                        print(f"  [warn] line {lineno}: {exc}", file=sys.stderr)
        records.sort(key=lambda e: e.timestamp)
        print(f"Loaded {len(records):,} events ({errors} parse errors)")
        return records

    def analyze(self, events: list[EventRecord]) -> dict[str, Any]:
        if not events:
            return {}

        windows = self.aggregator.aggregate(events)
        anomalies = self.anomaly_detector.detect(windows, events)

        # Top-N event types
        type_counts: dict[str, int] = defaultdict(int)
        for e in events:
            type_counts[e.event_type] += 1
        top_types = sorted(type_counts.items(), key=lambda x: -x[1])[: self.top_n]

        # Partition distribution
        partition_counts: dict[int, int] = defaultdict(int)
        for e in events:
            partition_counts[e.partition] += 1

        # Producer stats
        producer_counts: dict[str, int] = defaultdict(int)
        for e in events:
            producer_counts[e.producer_id] += 1

        # Throughput over time (per window)
        throughput_series = [
            {"t": round(w.start, 2), "eps": round(w.throughput_eps, 2), "count": w.count}
            for w in windows
        ]

        # Inter-event gap statistics
        gaps = [
            events[i].timestamp - events[i - 1].timestamp
            for i in range(1, len(events))
        ]
        gap_stats = {}
        if gaps:
            gap_stats = {
                "min_ms": round(min(gaps) * 1000, 2),
                "max_ms": round(max(gaps) * 1000, 2),
                "mean_ms": round((sum(gaps) / len(gaps)) * 1000, 2),
            }

        duration = events[-1].timestamp - events[0].timestamp
        overall_throughput = len(events) / max(duration, 0.001)

        return {
            "summary": {
                "total_events": len(events),
                "duration_s": round(duration, 2),
                "overall_throughput_eps": round(overall_throughput, 2),
                "num_windows": len(windows),
                "num_anomalies": len(anomalies),
                "window_type": self.window_type,
                "window_size_s": self.window_size_s,
            },
            "top_event_types": [{"type": t, "count": c} for t, c in top_types],
            "partition_distribution": {str(k): v for k, v in sorted(partition_counts.items())},
            "producer_distribution": dict(producer_counts),
            "throughput_series": throughput_series,
            "gap_stats": gap_stats,
            "anomalies": [
                {
                    "kind": a.kind,
                    "severity": a.severity,
                    "description": a.description,
                    "window_start": round(a.window_start, 2),
                    "window_end": round(a.window_end, 2),
                    "details": a.details,
                }
                for a in anomalies
            ],
        }

    # -----------------------------------------------------------------------
    # HTML report generator
    # -----------------------------------------------------------------------
    def generate_html(self, analysis: dict[str, Any], output_path: str) -> None:
        summary = analysis.get("summary", {})
        top_types = analysis.get("top_event_types", [])
        partitions = analysis.get("partition_distribution", {})
        throughput_series = analysis.get("throughput_series", [])
        anomalies = analysis.get("anomalies", [])
        gap_stats = analysis.get("gap_stats", {})

        throughput_labels = json.dumps([s["t"] for s in throughput_series])
        throughput_data = json.dumps([s["eps"] for s in throughput_series])
        type_labels = json.dumps([t["type"] for t in top_types])
        type_data = json.dumps([t["count"] for t in top_types])
        partition_labels = json.dumps(list(partitions.keys()))
        partition_data = json.dumps(list(partitions.values()))

        anomaly_rows = ""
        for a in anomalies[:50]:
            sev_color = {"high": "#ef4444", "medium": "#f59e0b", "low": "#6b7280"}.get(a["severity"], "#6b7280")
            anomaly_rows += f"""
            <tr>
              <td><span style="color:{sev_color};font-weight:600">{a['severity'].upper()}</span></td>
              <td>{a['kind']}</td>
              <td>{a['description']}</td>
            </tr>"""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Stream Analytics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    :root{{
      --bg:#0a0a0f;--surface:#111118;--border:#1e1e2e;--accent:#7c3aed;
      --accent2:#06b6d4;--text:#e2e8f0;--muted:#64748b;
      --green:#10b981;--red:#ef4444;--yellow:#f59e0b;
    }}
    body{{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;
          font-size:14px;line-height:1.6;min-height:100vh;padding:2rem}}
    h1{{font-size:1.75rem;font-weight:700;background:linear-gradient(135deg,var(--accent),var(--accent2));
        -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:.25rem}}
    .subtitle{{color:var(--muted);margin-bottom:2rem;font-size:.875rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1rem;margin-bottom:2rem}}
    .card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.25rem}}
    .card h3{{color:var(--muted);font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;margin-bottom:.5rem}}
    .card .value{{font-size:1.75rem;font-weight:700;color:var(--text)}}
    .card .sub{{font-size:.75rem;color:var(--muted);margin-top:.25rem}}
    .charts{{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:2rem}}
    .chart-card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.25rem}}
    .chart-card h2{{font-size:.9rem;font-weight:600;margin-bottom:1rem;color:var(--text)}}
    .chart-card.wide{{grid-column:1/-1}}
    canvas{{max-height:260px}}
    table{{width:100%;border-collapse:collapse}}
    th{{text-align:left;padding:.5rem .75rem;border-bottom:1px solid var(--border);
        color:var(--muted);font-size:.75rem;text-transform:uppercase;letter-spacing:.06em}}
    td{{padding:.5rem .75rem;border-bottom:1px solid var(--border);font-size:.85rem}}
    tr:last-child td{{border-bottom:none}}
    .badge{{display:inline-block;padding:.15rem .5rem;border-radius:4px;font-size:.7rem;font-weight:600}}
    .badge.high{{background:rgba(239,68,68,.15);color:#ef4444}}
    .badge.medium{{background:rgba(245,158,11,.15);color:#f59e0b}}
    .badge.low{{background:rgba(107,114,128,.15);color:#9ca3af}}
    .footer{{margin-top:2rem;color:var(--muted);font-size:.75rem;text-align:center}}
  </style>
</head>
<body>
  <h1>Stream Analytics Dashboard</h1>
  <p class="subtitle">
    Window: {summary.get('window_type','—')} · Size: {summary.get('window_size_s','—')}s ·
    Generated: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}
  </p>

  <div class="grid">
    <div class="card">
      <h3>Total Events</h3>
      <div class="value">{summary.get('total_events',0):,}</div>
      <div class="sub">across {summary.get('num_windows',0)} windows</div>
    </div>
    <div class="card">
      <h3>Duration</h3>
      <div class="value">{summary.get('duration_s',0):.1f}s</div>
      <div class="sub">stream time span</div>
    </div>
    <div class="card">
      <h3>Throughput</h3>
      <div class="value">{summary.get('overall_throughput_eps',0):.1f}</div>
      <div class="sub">events / second</div>
    </div>
    <div class="card">
      <h3>Anomalies</h3>
      <div class="value" style="color:{'var(--red)' if summary.get('num_anomalies',0) > 0 else 'var(--green)'}">{summary.get('num_anomalies',0)}</div>
      <div class="sub">detected</div>
    </div>
    <div class="card">
      <h3>Avg Gap</h3>
      <div class="value">{gap_stats.get('mean_ms',0):.1f}</div>
      <div class="sub">ms between events</div>
    </div>
  </div>

  <div class="charts">
    <div class="chart-card wide">
      <h2>Throughput Over Time (events/s)</h2>
      <canvas id="throughputChart"></canvas>
    </div>
    <div class="chart-card">
      <h2>Top Event Types</h2>
      <canvas id="typeChart"></canvas>
    </div>
    <div class="chart-card">
      <h2>Partition Distribution</h2>
      <canvas id="partChart"></canvas>
    </div>
  </div>

  <div class="chart-card" style="margin-bottom:1rem">
    <h2>Anomalies ({len(anomalies)} detected)</h2>
    {'<p style="color:var(--muted);padding:.5rem 0">No anomalies detected.</p>' if not anomalies else f'''
    <table>
      <thead><tr><th>Severity</th><th>Kind</th><th>Description</th></tr></thead>
      <tbody>{anomaly_rows}</tbody>
    </table>'''}
  </div>

  <p class="footer">event-stream-playground · stream_analyzer.py</p>

  <script>
    const CHART_DEFAULTS = {{
      responsive:true, maintainAspectRatio:true,
      plugins:{{legend:{{labels:{{color:'#94a3b8',font:{{size:11}}}}}}}},
      scales:{{x:{{ticks:{{color:'#475569',maxTicksLimit:8}},grid:{{color:'#1e1e2e'}}}},
               y:{{ticks:{{color:'#475569'}},grid:{{color:'#1e1e2e'}}}}}}
    }};

    new Chart(document.getElementById('throughputChart'), {{
      type:'line',
      data:{{
        labels:{throughput_labels},
        datasets:[{{
          label:'Events/s',data:{throughput_data},
          borderColor:'#7c3aed',backgroundColor:'rgba(124,58,237,.15)',
          fill:true,tension:.4,pointRadius:2,borderWidth:2
        }}]
      }},
      options:CHART_DEFAULTS
    }});

    new Chart(document.getElementById('typeChart'), {{
      type:'doughnut',
      data:{{
        labels:{type_labels},
        datasets:[{{
          data:{type_data},
          backgroundColor:['#7c3aed','#06b6d4','#10b981','#f59e0b','#ef4444','#8b5cf6'],
          borderWidth:0
        }}]
      }},
      options:{{responsive:true,maintainAspectRatio:true,
                plugins:{{legend:{{position:'right',labels:{{color:'#94a3b8',font:{{size:11}}}}}}}}}}
    }});

    new Chart(document.getElementById('partChart'), {{
      type:'bar',
      data:{{
        labels:{partition_labels},
        datasets:[{{
          label:'Events',data:{partition_data},
          backgroundColor:'rgba(6,182,212,.7)',borderRadius:6,borderWidth:0
        }}]
      }},
      options:CHART_DEFAULTS
    }});
  </script>
</body>
</html>"""

        Path(output_path).write_text(html, encoding="utf-8")
        print(f"HTML report written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream analytics engine for event logs")
    parser.add_argument("--input", required=True, help="JSON-lines event log file")
    parser.add_argument("--output", default="stream-report.html", help="Output HTML report path")
    parser.add_argument("--window-type", choices=["tumbling", "sliding", "session"],
                        default="tumbling")
    parser.add_argument("--window-size", type=float, default=10.0, help="Window size in seconds")
    parser.add_argument("--slide", type=float, default=5.0, help="Slide interval for sliding windows")
    parser.add_argument("--top-n", type=int, default=5, help="Top-N event types to highlight")
    parser.add_argument("--spike-threshold", type=float, default=3.0,
                        help="Anomaly Z-score threshold")
    parser.add_argument("--gap-threshold", type=float, default=10.0,
                        help="Gap anomaly threshold in seconds")
    parser.add_argument("--json", action="store_true", help="Also print JSON analysis to stdout")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    analyzer = StreamAnalyzer(
        window_type=args.window_type,
        window_size_s=args.window_size,
        slide_s=args.slide,
        top_n=args.top_n,
        spike_threshold=args.spike_threshold,
        gap_threshold_s=args.gap_threshold,
    )

    print(f"Loading events from: {args.input}")
    events = analyzer.load_events(args.input)
    if not events:
        print("No events found — exiting.")
        return

    print(f"Analyzing with {args.window_type} windows ({args.window_size}s)...")
    analysis = analyzer.analyze(events)

    s = analysis["summary"]
    print(f"  Total events:    {s['total_events']:,}")
    print(f"  Duration:        {s['duration_s']:.2f}s")
    print(f"  Throughput:      {s['overall_throughput_eps']:.2f} eps")
    print(f"  Windows:         {s['num_windows']}")
    print(f"  Anomalies:       {s['num_anomalies']}")

    if args.json:
        print(json.dumps(analysis, indent=2))

    analyzer.generate_html(analysis, args.output)


if __name__ == "__main__":
    main()
