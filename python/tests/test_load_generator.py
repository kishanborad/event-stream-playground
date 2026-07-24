"""
test_load_generator.py — Tests for the load generator.

Covers:
  - All six load patterns (steady, burst, ramp_up, ramp_down, spike, sinusoidal)
  - LoadEvent structure and JSON serialization
  - LatencyBucket percentile calculations
  - PatternResult throughput and conformance reporting
  - LoadGenerator event emission and sink modes
  - Report rendering (dict structure validation)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from load_generator import (
    BurstPattern,
    LatencyBucket,
    LoadEvent,
    LoadGenerator,
    PatternResult,
    RampDownPattern,
    RampUpPattern,
    SinusoidalPattern,
    SpikePattern,
    SteadyPattern,
    build_patterns,
    parse_args,
    render_report,
)


# ---------------------------------------------------------------------------
# LoadEvent
# ---------------------------------------------------------------------------
class TestLoadEvent:
    def _make(self) -> LoadEvent:
        return LoadEvent(
            event_id="abc123",
            timestamp=1_700_000_000.0,
            pattern="steady",
            sequence=1,
            payload_size=128,
            payload={"data": "x" * 64},
        )

    def test_to_json_is_valid(self):
        e = self._make()
        parsed = json.loads(e.to_json())
        assert parsed["event_id"] == "abc123"

    def test_to_json_has_all_fields(self):
        e = self._make()
        parsed = json.loads(e.to_json())
        for field in ("event_id", "timestamp", "pattern", "sequence", "payload_size", "payload"):
            assert field in parsed

    def test_to_json_is_compact(self):
        e = self._make()
        raw = e.to_json()
        assert ": " not in raw  # no space after colon


# ---------------------------------------------------------------------------
# LatencyBucket
# ---------------------------------------------------------------------------
class TestLatencyBucket:
    def test_empty_bucket_returns_zero(self):
        b = LatencyBucket()
        assert b.percentile(50) == 0.0
        assert b.mean() == 0.0

    def test_single_sample(self):
        b = LatencyBucket()
        b.record(5.0)
        assert b.percentile(50) == 5.0
        assert b.percentile(99) == 5.0
        assert b.mean() == 5.0

    def test_p50_is_median(self):
        b = LatencyBucket()
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            b.record(v)
        # Median of [1,2,3,4,5] is 3
        assert abs(b.percentile(50) - 3.0) < 0.01

    def test_p99_is_near_max(self):
        b = LatencyBucket()
        for i in range(1, 101):
            b.record(float(i))
        # p99 of [1..100] should be 99
        assert b.percentile(99) >= 98.0

    def test_mean_calculation(self):
        b = LatencyBucket()
        for v in [2.0, 4.0, 6.0]:
            b.record(v)
        assert b.mean() == 4.0

    def test_to_dict_has_all_percentiles(self):
        b = LatencyBucket()
        for i in range(100):
            b.record(float(i))
        d = b.to_dict()
        assert "p50_ms" in d
        assert "p95_ms" in d
        assert "p99_ms" in d
        assert "p999_ms" in d
        assert "min_ms" in d
        assert "max_ms" in d
        assert "mean_ms" in d

    def test_to_dict_count_matches_records(self):
        b = LatencyBucket()
        for _ in range(42):
            b.record(1.0)
        assert b.to_dict()["count"] == 42


# ---------------------------------------------------------------------------
# PatternResult
# ---------------------------------------------------------------------------
class TestPatternResult:
    def test_to_dict_has_required_keys(self):
        r = PatternResult(
            pattern_name="steady",
            target_rate=100.0,
            actual_rate=98.5,
            duration_s=10.0,
            total_events=985,
        )
        r.latency.record(1.5)
        d = r.to_dict()
        for key in ("pattern", "target_rate_eps", "actual_rate_eps",
                     "rate_conformance_pct", "duration_s", "total_events", "latency"):
            assert key in d

    def test_rate_conformance_pct_calculation(self):
        r = PatternResult(
            pattern_name="test",
            target_rate=100.0,
            actual_rate=95.0,
            duration_s=5.0,
            total_events=475,
        )
        d = r.to_dict()
        assert d["rate_conformance_pct"] == 95.0

    def test_zero_target_rate_does_not_divide_by_zero(self):
        r = PatternResult(
            pattern_name="edge",
            target_rate=0.0,
            actual_rate=0.0,
            duration_s=1.0,
            total_events=0,
        )
        d = r.to_dict()
        assert d["rate_conformance_pct"] >= 0  # no crash


# ---------------------------------------------------------------------------
# Load patterns — short-duration smoke tests
# ---------------------------------------------------------------------------
SHORT_DURATION = 0.5  # 500ms per pattern test


class TestSteadyPattern:
    def test_emits_events_at_configured_rate(self):
        gen = LoadGenerator(output_sink="null")
        pattern = SteadyPattern(rate=50.0)
        result = asyncio.run(gen.run_pattern(pattern, SHORT_DURATION))
        # At 50 eps for 0.5s → expect ~25 events (±50% tolerance for CI)
        assert result.total_events >= 10

    def test_result_has_pattern_name(self):
        gen = LoadGenerator(output_sink="null")
        result = asyncio.run(gen.run_pattern(SteadyPattern(20.0), SHORT_DURATION))
        assert result.pattern_name == "steady"

    def test_actual_rate_is_positive(self):
        gen = LoadGenerator(output_sink="null")
        result = asyncio.run(gen.run_pattern(SteadyPattern(20.0), SHORT_DURATION))
        assert result.actual_rate > 0


class TestBurstPattern:
    def test_emits_events(self):
        gen = LoadGenerator(output_sink="null")
        pattern = BurstPattern(baseline_rate=10.0, burst_rate=100.0,
                               burst_duration_s=0.1, burst_interval_s=0.2)
        result = asyncio.run(gen.run_pattern(pattern, SHORT_DURATION))
        assert result.total_events > 0

    def test_pattern_name(self):
        assert BurstPattern(10.0, 100.0).name == "burst"


class TestRampUpPattern:
    def test_emits_events(self):
        gen = LoadGenerator(output_sink="null")
        result = asyncio.run(gen.run_pattern(RampUpPattern(5.0, 50.0), SHORT_DURATION))
        assert result.total_events > 0

    def test_pattern_name(self):
        assert RampUpPattern(5.0, 50.0).name == "ramp_up"


class TestRampDownPattern:
    def test_emits_events(self):
        gen = LoadGenerator(output_sink="null")
        result = asyncio.run(gen.run_pattern(RampDownPattern(50.0, 5.0), SHORT_DURATION))
        assert result.total_events > 0

    def test_pattern_name(self):
        assert RampDownPattern(50.0, 5.0).name == "ramp_down"


class TestSpikePattern:
    def test_emits_events(self):
        gen = LoadGenerator(output_sink="null")
        pattern = SpikePattern(baseline_rate=10.0, spike_rate=200.0, spike_duration_s=0.1)
        result = asyncio.run(gen.run_pattern(pattern, SHORT_DURATION))
        assert result.total_events > 0

    def test_pattern_name(self):
        assert SpikePattern(10.0, 200.0).name == "spike"


class TestSinusoidalPattern:
    def test_emits_events(self):
        gen = LoadGenerator(output_sink="null")
        pattern = SinusoidalPattern(min_rate=5.0, max_rate=50.0, period_s=0.5)
        result = asyncio.run(gen.run_pattern(pattern, SHORT_DURATION))
        assert result.total_events > 0

    def test_pattern_name(self):
        assert SinusoidalPattern(5.0, 50.0).name == "sinusoidal"


# ---------------------------------------------------------------------------
# LoadGenerator
# ---------------------------------------------------------------------------
class TestLoadGenerator:
    def test_events_have_unique_ids(self):
        gen = LoadGenerator(output_sink="null")
        ids = {gen._make_event("test").event_id for _ in range(50)}
        assert len(ids) == 50

    def test_sequence_increments(self):
        gen = LoadGenerator(output_sink="null")
        e1 = gen._make_event("p1")
        e2 = gen._make_event("p1")
        assert e2.sequence == e1.sequence + 1

    def test_payload_respects_size(self):
        gen = LoadGenerator(payload_size=200, output_sink="null")
        e = gen._make_event("p1")
        assert e.payload_size == 200

    def test_null_sink_returns_zero_latency(self):
        gen = LoadGenerator(output_sink="null")
        event = gen._make_event("test")
        latency = gen._emit(event)
        assert latency >= 0.0
        assert latency < 100.0  # should be near-zero


# ---------------------------------------------------------------------------
# render_report
# ---------------------------------------------------------------------------
class TestRenderReport:
    def _make_result(self, name: str = "steady", events: int = 100) -> PatternResult:
        r = PatternResult(
            pattern_name=name,
            target_rate=50.0,
            actual_rate=48.5,
            duration_s=2.0,
            total_events=events,
        )
        for _ in range(50):
            r.latency.record(1.0)
        return r

    def test_render_returns_dict(self):
        results = [self._make_result()]
        report = render_report(results, report_file=None)
        assert isinstance(report, dict)

    def test_report_has_total_events(self):
        results = [self._make_result(events=200), self._make_result(events=100)]
        report = render_report(results, report_file=None)
        assert report["total_events"] == 300

    def test_report_written_to_file(self, tmp_path):
        output = tmp_path / "load-report.json"
        results = [self._make_result()]
        render_report(results, report_file=str(output))
        assert output.exists()
        data = json.loads(output.read_text())
        assert "patterns" in data

    def test_report_file_has_correct_structure(self, tmp_path):
        output = tmp_path / "report.json"
        results = [self._make_result("steady", 500)]
        render_report(results, report_file=str(output))
        data = json.loads(output.read_text())
        assert data["total_patterns"] == 1
        assert len(data["patterns"]) == 1
        assert data["patterns"][0]["pattern"] == "steady"

    def test_report_includes_latency_percentiles(self, tmp_path):
        output = tmp_path / "report.json"
        render_report([self._make_result()], report_file=str(output))
        data = json.loads(output.read_text())
        lat = data["patterns"][0]["latency"]
        assert "p50_ms" in lat
        assert "p99_ms" in lat


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------
class TestLoadGeneratorCLI:
    def test_default_pattern_is_steady(self):
        args = parse_args([])
        assert args.pattern == "steady"

    def test_default_duration(self):
        args = parse_args([])
        assert args.duration == 15.0

    def test_parse_pattern(self):
        args = parse_args(["--pattern", "sinusoidal"])
        assert args.pattern == "sinusoidal"

    def test_parse_rate(self):
        args = parse_args(["--rate", "200"])
        assert args.rate == 200.0

    def test_parse_all_pattern(self):
        args = parse_args(["--pattern", "all"])
        patterns = build_patterns(args)
        assert len(patterns) == 6

    def test_parse_report_file(self):
        args = parse_args(["--report-file", "/tmp/report.json"])
        assert args.report_file == "/tmp/report.json"
