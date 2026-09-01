import json
from pathlib import Path

from benchmarks.executor_benchmark import BenchmarkConfig, run_benchmark


def test_run_benchmark_emits_summary(tmp_path):
    config = BenchmarkConfig(
        total_chunks=40,
        chunk_sleep_s=0.0,
        total_cpu=2,
        max_workers=2,
        batch_size="auto",
        max_inflight_tasks=16,
        max_inflight_items=64,
        prefetch_factor=2.0,
        refill_threshold=2,
        poll_interval=0.01,
        timeout_s=20.0,
        warmup_runs=0,
        measured_runs=1,
    )
    payload = run_benchmark(config=config, workdir=tmp_path / "bench")

    assert payload["config"]["total_chunks"] == 40
    assert payload["config"]["batch_size"] == "auto"
    assert payload["config"]["max_inflight_items"] == 64
    assert payload["summary"]["runs"] == 1
    assert payload["summary"]["throughput_avg_chunks_s"] > 0.0
    assert payload["summary"]["peak_loop_latency_max_ms"] >= 0.0
    assert payload["summary"]["peak_rss_max_mb"] >= 0.0
    assert payload["summary"]["peak_tracemalloc_max_mb"] >= 0.0
    assert len(payload["results"]) == 1
    assert payload["results"][0]["status"] == "completed"
    assert payload["results"][0]["dispatch_policy"]["batch_size"] == "auto"
    assert payload["results"][0]["dispatch_policy"]["max_inflight_tasks"] == 16
    assert payload["results"][0]["dispatch_policy"]["max_inflight_items"] == 64
    assert payload["results"][0]["stability"]["sample_count"] >= 1
    assert payload["results"][0]["stability"]["peak_loop_latency_ms"] >= 0.0
    assert payload["results"][0]["stability"]["peak_rss_mb"] >= 0.0
    assert payload["results"][0]["stability"]["peak_tracemalloc_mb"] >= 0.0

    # Validate result is JSON serializable for CI/report pipelines.
    encoded = json.dumps(payload, sort_keys=True)
    assert "\"summary\"" in encoded
    assert "\"dispatch_policy\"" in encoded


def test_run_benchmark_handles_large_chunk_volume(tmp_path):
    config = BenchmarkConfig(
        total_chunks=256,
        chunk_sleep_s=0.0,
        total_cpu=4,
        max_workers=4,
        batch_size="auto",
        max_inflight_tasks=128,
        max_inflight_items=512,
        prefetch_factor=1.0,
        refill_threshold=1,
        poll_interval=0.01,
        sample_interval_s=0.05,
        timeout_s=30.0,
        warmup_runs=0,
        measured_runs=1,
    )
    payload = run_benchmark(config=config, workdir=tmp_path / "bench_thousands")

    result = payload["results"][0]
    assert result["status"] == "completed"
    assert result["chunks_done"] == 256
    assert result["throughput_chunks_s"] > 0.0
    assert result["stability"]["sample_count"] >= 1
    assert result["stability"]["peak_cpu_used"] >= 0
    assert result["stability"]["min_cpu_available"] >= 0
    assert result["stability"]["peak_rss_mb"] >= 0.0
    assert payload["summary"]["peak_backlog_max"] >= 0.0
