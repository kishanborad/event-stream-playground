"""
test_stream_analyzer.py — Tests for the stream analytics engine.

Covers:
  - Event loading from JSON-lines files
  - Tumbling, sliding, and session window aggregations
  - Anomaly detection (rate spikes, gaps, duplicates, sequence gaps)
  - Top-N event type ranking
  - Partition distribution calculation
  - HTML report generation (structure + required elements)
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from stream_analyzer import (
    AnomalyDetector,
    EventRecord,
    SessionWindowAggregator,
    SlidingWindowAggregator,
    StreamAnalyzer,
    TumblingWindowAggregator,
    Window,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
BASE_TS = 1_700_000_000.0


def make_record(
    event_type: str = "PageView",
    partition: int = 0,
    sequence: int = 1,
    ts_offset: float = 0.0,
    producer_id: str = "p-001",
    event_id: str | None = None,
) -> EventRecord:
    return EventRecord(
        event_id=event_id or uuid.uuid4().hex,
        event_type=event_type,
        timestamp=BASE_TS + ts_offset,
        partition=partition,
        sequence_number=sequence,
        producer_id=producer_id,
        partition_key=f"key-{sequence}",
    )


def records_steady(
    count: int = 100, rate_eps: float = 10.0, event_types: list[str] | None = None
) -> list[EventRecord]:
    """Generate a steady stream of events spaced 1/rate_eps apart."""
    types = event_types or ["PageView", "Click", "MetricSample"]
    return [
        make_record(
            event_type=types[i % len(types)],
            partition=i % 4,
            sequence=i + 1,
            ts_offset=i / rate_eps,
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# EventRecord.from_dict
# ---------------------------------------------------------------------------
class TestEventRecordFromDict:
    def test_parses_all_fields(self):
        d = {
            "event_id": "abc123",
            "event_type": "Click",
            "timestamp": BASE_TS,
            "partition": 2,
            "sequence_number": 42,
            "producer_id": "p-x",
            "partition_key": "user_001",
        }
        r = EventRecord.from_dict(d)
        assert r.event_id == "abc123"
        assert r.event_type == "Click"
        assert r.partition == 2
        assert r.sequence_number == 42

    def test_missing_fields_use_defaults(self):
        r = EventRecord.from_dict({})
        assert r.event_type == "unknown"
        assert r.partition == 0
        assert r.sequence_number == 0


# ---------------------------------------------------------------------------
# StreamAnalyzer.load_events
# ---------------------------------------------------------------------------
class TestLoadEvents:
    def test_loads_valid_jsonl(self, tmp_path):
        events_file = tmp_path / "events.jsonl"
        records = records_steady(50)
        with events_file.open("w") as fh:
            for r in records:
                fh.write(json.dumps({
                    "event_id": r.event_id, "event_type": r.event_type,
                    "timestamp": r.timestamp, "partition": r.partition,
                    "sequence_number": r.sequence_number, "producer_id": r.producer_id,
                    "partition_key": r.partition_key,
                }) + "\n")

        analyzer = StreamAnalyzer()
        loaded = analyzer.load_events(str(events_file))
        assert len(loaded) == 50

    def test_skips_invalid_json_lines(self, tmp_path):
        events_file = tmp_path / "bad.jsonl"
        with events_file.open("w") as fh:
            fh.write(json.dumps({"event_id": "1", "event_type": "Click",
                                  "timestamp": BASE_TS, "partition": 0,
                                  "sequence_number": 1, "producer_id": "p",
                                  "partition_key": "k"}) + "\n")
            fh.write("NOT JSON\n")
        analyzer = StreamAnalyzer()
        loaded = analyzer.load_events(str(events_file))
        assert len(loaded) == 1

    def test_events_sorted_by_timestamp(self, tmp_path):
        events_file = tmp_path / "unsorted.jsonl"
        with events_file.open("w") as fh:
            for ts_offset in [5.0, 1.0, 3.0, 2.0, 4.0]:
                fh.write(json.dumps({
                    "event_id": uuid.uuid4().hex, "event_type": "Click",
                    "timestamp": BASE_TS + ts_offset, "partition": 0,
                    "sequence_number": 1, "producer_id": "p", "partition_key": "k",
                }) + "\n")
        analyzer = StreamAnalyzer()
        loaded = analyzer.load_events(str(events_file))
        timestamps = [r.timestamp for r in loaded]
        assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# Tumbling windows
# ---------------------------------------------------------------------------
class TestTumblingWindows:
    def test_correct_number_of_windows(self):
        records = records_steady(100, rate_eps=10.0)  # 10s of data
        agg = TumblingWindowAggregator(window_size_s=1.0)
        windows = agg.aggregate(records)
        assert 10 <= len(windows) <= 12  # allow fence-post

    def test_windows_are_non_overlapping(self):
        records = records_steady(50, rate_eps=10.0)
        agg = TumblingWindowAggregator(window_size_s=2.0)
        windows = agg.aggregate(records)
        for i in range(1, len(windows)):
            assert windows[i].start >= windows[i - 1].end

    def test_empty_input_returns_empty(self):
        agg = TumblingWindowAggregator(window_size_s=5.0)
        assert agg.aggregate([]) == []

    def test_window_contains_correct_events(self):
        records = records_steady(20, rate_eps=1.0)  # 1 event/s over 20s
        agg = TumblingWindowAggregator(window_size_s=5.0)
        windows = agg.aggregate(records)
        # Each 5s window should have ~5 events
        non_empty = [w for w in windows if w.count > 0]
        for w in non_empty:
            assert w.count <= 6  # small buffer for timing


# ---------------------------------------------------------------------------
# Sliding windows
# ---------------------------------------------------------------------------
class TestSlidingWindows:
    def test_more_windows_than_tumbling(self):
        records = records_steady(50, rate_eps=10.0)
        tumbling = TumblingWindowAggregator(5.0).aggregate(records)
        sliding = SlidingWindowAggregator(5.0, 1.0).aggregate(records)
        assert len(sliding) > len(tumbling)

    def test_windows_overlap(self):
        records = records_steady(30, rate_eps=10.0)
        agg = SlidingWindowAggregator(window_size_s=5.0, slide_s=2.0)
        windows = agg.aggregate(records)
        # Consecutive windows must overlap: next.start < prev.end
        overlapping = sum(
            1 for i in range(1, len(windows))
            if windows[i].start < windows[i - 1].end
        )
        assert overlapping > 0


# ---------------------------------------------------------------------------
# Session windows
# ---------------------------------------------------------------------------
class TestSessionWindows:
    def test_single_session_with_no_gaps(self):
        records = records_steady(20, rate_eps=10.0)
        agg = SessionWindowAggregator(gap_threshold_s=5.0)
        sessions = agg.aggregate(records)
        assert len(sessions) == 1

    def test_splits_on_gap(self):
        # First 10 events, 10s gap, then 10 more
        r1 = records_steady(10, rate_eps=10.0)
        r2 = [make_record(sequence=i + 11, ts_offset=10.0 + (i + 1) / 10.0)
               for i in range(10)]
        all_records = sorted(r1 + r2, key=lambda r: r.timestamp)
        agg = SessionWindowAggregator(gap_threshold_s=5.0)
        sessions = agg.aggregate(all_records)
        assert len(sessions) == 2

    def test_each_session_has_events(self):
        records = records_steady(5, rate_eps=5.0)
        agg = SessionWindowAggregator(gap_threshold_s=1.0)
        sessions = agg.aggregate(records)
        for s in sessions:
            assert s.count > 0


# ---------------------------------------------------------------------------
# Window statistics
# ---------------------------------------------------------------------------
class TestWindowStats:
    def test_throughput_eps_calculation(self):
        # 100 events in 10s window → 10 eps
        w = Window(start=BASE_TS, end=BASE_TS + 10.0)
        w.events = records_steady(100, rate_eps=10.0)
        assert abs(w.throughput_eps - 10.0) < 1.0

    def test_type_distribution_counts(self):
        w = Window(start=BASE_TS, end=BASE_TS + 5.0)
        w.events = [make_record("PageView"), make_record("Click"), make_record("PageView")]
        dist = w.type_distribution()
        assert dist["PageView"] == 2
        assert dist["Click"] == 1

    def test_partition_distribution_counts(self):
        w = Window(start=BASE_TS, end=BASE_TS + 5.0)
        w.events = [make_record(partition=0), make_record(partition=1), make_record(partition=0)]
        dist = w.partition_distribution()
        assert dist[0] == 2
        assert dist[1] == 1


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------
class TestAnomalyDetector:
    def test_no_anomalies_in_steady_stream(self):
        records = records_steady(200, rate_eps=10.0)
        windows = TumblingWindowAggregator(5.0).aggregate(records)
        detector = AnomalyDetector(spike_threshold=5.0, gap_threshold_s=30.0)
        anomalies = detector.detect(windows, records)
        # No gaps, no spikes, no duplicates
        assert len(anomalies) == 0

    def test_detects_rate_spike(self):
        # Normal rate then a 10x spike
        steady = records_steady(50, rate_eps=10.0)
        spike_start = steady[-1].timestamp + 0.1
        spike_records = [
            make_record(sequence=51 + i, ts_offset=spike_start - BASE_TS + i * 0.01)
            for i in range(100)  # 100 events in 1s = 100 eps
        ]
        all_records = sorted(steady + spike_records, key=lambda r: r.timestamp)
        windows = TumblingWindowAggregator(1.0).aggregate(all_records)
        detector = AnomalyDetector(spike_threshold=2.0, gap_threshold_s=30.0)
        anomalies = detector.detect(windows, all_records)
        rate_anomalies = [a for a in anomalies if a.kind in ("rate_spike", "rate_gap")]
        assert len(rate_anomalies) > 0

    def test_detects_time_gap(self):
        # 30s gap between events
        r1 = records_steady(10, rate_eps=10.0)
        r2 = [make_record(sequence=i + 11, ts_offset=31.0 + i * 0.1)
               for i in range(10)]
        all_records = sorted(r1 + r2, key=lambda r: r.timestamp)
        windows = TumblingWindowAggregator(5.0).aggregate(all_records)
        detector = AnomalyDetector(spike_threshold=10.0, gap_threshold_s=5.0)
        anomalies = detector.detect(windows, all_records)
        gap_anomalies = [a for a in anomalies if a.kind == "rate_gap"]
        assert len(gap_anomalies) > 0

    def test_detects_duplicate_event_id(self):
        fixed_id = "dup-event-001"
        records = [
            make_record(sequence=1, event_id=fixed_id),
            make_record(sequence=2, ts_offset=1.0),
            make_record(sequence=3, ts_offset=2.0, event_id=fixed_id),
        ]
        windows = TumblingWindowAggregator(5.0).aggregate(records)
        detector = AnomalyDetector()
        anomalies = detector.detect(windows, records)
        dup_anomalies = [a for a in anomalies if a.kind == "duplicate"]
        assert len(dup_anomalies) == 1

    def test_detects_sequence_gap(self):
        records = [
            make_record(sequence=1, ts_offset=0.0, producer_id="p1"),
            make_record(sequence=2, ts_offset=1.0, producer_id="p1"),
            # Gap: sequence 3-9 missing
            make_record(sequence=10, ts_offset=2.0, producer_id="p1"),
        ]
        windows = TumblingWindowAggregator(5.0).aggregate(records)
        detector = AnomalyDetector()
        anomalies = detector.detect(windows, records)
        seq_anomalies = [a for a in anomalies if a.kind == "sequence_gap"]
        assert len(seq_anomalies) == 1
        # Sequences 3-9 missing between 2 and 10
        assert seq_anomalies[0].details["gap"] in (7, 8)


# ---------------------------------------------------------------------------
# StreamAnalyzer.analyze
# ---------------------------------------------------------------------------
class TestStreamAnalyzerAnalyze:
    def test_analyze_returns_summary(self):
        analyzer = StreamAnalyzer(window_type="tumbling", window_size_s=1.0)
        records = records_steady(30, rate_eps=10.0)
        result = analyzer.analyze(records)
        assert "summary" in result
        assert result["summary"]["total_events"] == 30

    def test_top_event_types_limited_to_top_n(self):
        analyzer = StreamAnalyzer(top_n=3)
        records = records_steady(60, rate_eps=10.0,
                                  event_types=["PageView", "Click", "Purchase", "Heartbeat"])
        result = analyzer.analyze(records)
        assert len(result["top_event_types"]) <= 3

    def test_partition_distribution_sums_to_total(self):
        analyzer = StreamAnalyzer()
        records = records_steady(40, rate_eps=10.0)
        result = analyzer.analyze(records)
        total = sum(result["partition_distribution"].values())
        assert total == 40

    def test_throughput_series_is_non_empty(self):
        analyzer = StreamAnalyzer(window_type="tumbling", window_size_s=1.0)
        records = records_steady(20, rate_eps=10.0)
        result = analyzer.analyze(records)
        assert len(result["throughput_series"]) > 0

    def test_empty_events_returns_empty_dict(self):
        analyzer = StreamAnalyzer()
        result = analyzer.analyze([])
        assert result == {}

    def test_overall_throughput_is_reasonable(self):
        analyzer = StreamAnalyzer(window_type="tumbling", window_size_s=1.0)
        # 100 events at 10 eps → ~10 second stream → ~10 eps
        records = records_steady(100, rate_eps=10.0)
        result = analyzer.analyze(records)
        throughput = result["summary"]["overall_throughput_eps"]
        assert 8 <= throughput <= 12


# ---------------------------------------------------------------------------
# HTML report generation
# ---------------------------------------------------------------------------
class TestHTMLReportGeneration:
    def test_html_file_is_created(self, tmp_path):
        output = str(tmp_path / "report.html")
        analyzer = StreamAnalyzer(window_type="tumbling", window_size_s=1.0)
        records = records_steady(30, rate_eps=10.0)
        analysis = analyzer.analyze(records)
        analyzer.generate_html(analysis, output)
        assert Path(output).exists()

    def test_html_has_doctype(self, tmp_path):
        output = str(tmp_path / "report.html")
        analyzer = StreamAnalyzer()
        analysis = analyzer.analyze(records_steady(20))
        analyzer.generate_html(analysis, output)
        content = Path(output).read_text()
        assert "<!DOCTYPE html>" in content

    def test_html_contains_chart_js(self, tmp_path):
        output = str(tmp_path / "report.html")
        analyzer = StreamAnalyzer()
        analysis = analyzer.analyze(records_steady(20))
        analyzer.generate_html(analysis, output)
        content = Path(output).read_text()
        assert "chart.js" in content.lower() or "Chart" in content

    def test_html_contains_total_events(self, tmp_path):
        output = str(tmp_path / "report.html")
        analyzer = StreamAnalyzer()
        records = records_steady(42)
        analysis = analyzer.analyze(records)
        analyzer.generate_html(analysis, output)
        content = Path(output).read_text()
        assert "42" in content

    def test_html_is_non_empty(self, tmp_path):
        output = str(tmp_path / "report.html")
        analyzer = StreamAnalyzer()
        analysis = analyzer.analyze(records_steady(10))
        analyzer.generate_html(analysis, output)
        assert Path(output).stat().st_size > 1000  # at least 1KB
