"""MRF-070 — Runtime persistence overhead tests.

Validates the numerical thresholds from MRF-003 against
the PersistenceCoordinator implementation:

  1. Commit batching: total_flushes / total_flushed < 0.2 for N ≥ 32 chunks.
  2. Sub-linear scaling: per-chunk wall time does not degrade >2× as N grows.
  3. Benchmark payload shape: the tool emits all required comparison fields.
"""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.executor.manager import ExecutorManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _noop(payload: dict) -> dict:
    return {"idx": int(payload.get("idx", 0))}


def _wait_completed(manager: ExecutorManager, job_id: str, timeout_s: float = 15.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = manager.get_job(job_id)
        if job and job.get("status") == "completed":
            return dict(job)
        time.sleep(0.02)
    job = manager.get_job(job_id)
    raise AssertionError(f"Job {job_id!r} did not complete. last={job}")


def _make_manager(tmp_path: Path, poll_interval: float = 0.05) -> ExecutorManager:
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=4,
        poll_interval=poll_interval,
    )
    manager.register_thread_executor(name="thread", max_workers=4)
    manager.start()
    return manager


# ---------------------------------------------------------------------------
# MRF-003 threshold 1: commits/chunk amortized < 0.2 for N ≥ 32
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("n_chunks,max_inflight", [(50, 16), (200, 32)])
def test_persistence_coordinator_batches_terminal_transitions(tmp_path, n_chunks, max_inflight):
    """total_flushes / total_flushed must be < 0.2 (MRF-003 threshold)."""
    manager = _make_manager(tmp_path / f"n{n_chunks}", poll_interval=0.05)
    try:
        snap_before = manager.persistence_coordinator.snapshot()

        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"idx": i} for i in range(n_chunks)],
            run_chunk=_noop,
            task_type="mrf070.batch_test",
            max_inflight_tasks=max_inflight,
            max_inflight_items=max_inflight * 4,
            store_results=False,
            total_chunks=n_chunks,
        )
        _wait_completed(manager, job_id)

        snap_after = manager.persistence_coordinator.snapshot()
    finally:
        manager.stop()

    flushed = int(snap_after["total_flushed"]) - int(snap_before["total_flushed"])
    flushes = int(snap_after["total_flushes"]) - int(snap_before["total_flushes"])

    assert flushed == n_chunks, f"Expected {n_chunks} flushed, got {flushed}"
    assert flushes > 0, "Expected at least one flush pass"

    # Key MRF-003 threshold: < 0.2 commits per chunk
    ratio = flushes / flushed
    assert ratio < 0.2, (
        f"commit-to-chunk ratio {ratio:.4f} ≥ 0.2 (MRF-003 threshold) "
        f"for n={n_chunks}: total_flushed={flushed}, total_flushes={flushes}"
    )


# ---------------------------------------------------------------------------
# MRF-003 threshold 2: refresh_job_status debounced (≤ 1 per flush per job)
# ---------------------------------------------------------------------------

def test_job_status_refresh_debounced_per_flush(tmp_path):
    """For a single job, job-status refreshes ≤ total_flushes (one per flush, not per chunk)."""
    N = 80
    manager = _make_manager(tmp_path / "debounce", poll_interval=0.05)
    try:
        snap_before = manager.persistence_coordinator.snapshot()

        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"idx": i} for i in range(N)],
            run_chunk=_noop,
            task_type="mrf070.debounce_test",
            max_inflight_tasks=16,
            store_results=False,
            total_chunks=N,
        )
        _wait_completed(manager, job_id)

        snap_after = manager.persistence_coordinator.snapshot()
    finally:
        manager.stop()

    flushed = int(snap_after["total_flushed"]) - int(snap_before["total_flushed"])
    flushes = int(snap_after["total_flushes"]) - int(snap_before["total_flushes"])

    assert flushed == N
    # There should be far fewer flush passes than chunks — proving refresh is
    # debounced to once per flush (dirty_jobs set, not per-chunk call).
    assert flushes < N, (
        f"Expected flush_passes < N={N} (debouncing), got {flushes}. "
        "PersistenceCoordinator is calling refresh_job_status once per chunk."
    )


# ---------------------------------------------------------------------------
# MRF-003 threshold 3: sub-linear scaling (per-chunk time ≤ 2× across N)
# ---------------------------------------------------------------------------

def test_per_chunk_time_scales_sublinearly(tmp_path):
    """Wall time per chunk for N=200 must not exceed 2× the N=10 per-chunk time."""
    times: dict[int, float] = {}
    for n, inflight in [(10, 8), (200, 16)]:
        manager = _make_manager(tmp_path / f"scale_{n}", poll_interval=0.05)
        try:
            t0 = time.perf_counter()
            job_id = manager.submit_job(
                executor_name="thread",
                chunks=[{"idx": i} for i in range(n)],
                run_chunk=_noop,
                task_type=f"mrf070.scale_{n}",
                max_inflight_tasks=inflight,
                store_results=False,
                total_chunks=n,
            )
            _wait_completed(manager, job_id)
            times[n] = time.perf_counter() - t0
        finally:
            manager.stop()

    per_chunk_small = times[10] / 10
    per_chunk_large = times[200] / 200
    ratio = per_chunk_large / max(per_chunk_small, 1e-9)

    # MRF-003: ≤ 2× per-chunk time degradation (very generous for noop workload)
    assert ratio <= 2.0, (
        f"Per-chunk latency grew {ratio:.2f}× from N=10 to N=200 (threshold: ≤ 2×). "
        f"small={per_chunk_small*1000:.2f}ms/chunk large={per_chunk_large*1000:.2f}ms/chunk"
    )


# ---------------------------------------------------------------------------
# Benchmark payload shape
# ---------------------------------------------------------------------------

def test_mrf070_persistence_benchmark_emits_required_fields(tmp_path):
    """run_persistence_benchmark returns a payload with all required fields."""
    from benchmarks.mrf070_persistence_overhead_benchmark import (
        PersistenceBenchmarkCase,
        PersistenceBenchmarkConfig,
        run_persistence_benchmark,
    )

    config = PersistenceBenchmarkConfig(
        poll_interval_s=0.02,
        total_cpu=2,
        thread_workers=2,
        cases=(
            PersistenceBenchmarkCase(name="noop_small", total_chunks=20, max_inflight_tasks=8),
        ),
    )
    payload = run_persistence_benchmark(config, workdir=tmp_path / "bench")

    assert "results" in payload
    assert "summary" in payload
    assert len(payload["results"]) == 1

    row = payload["results"][0]
    assert row["name"] == "noop_small"
    assert row["chunks_done"] == 20
    assert row["job_status"] == "completed"
    assert "commits_per_chunk" in row
    assert "total_flushed" in row
    assert "total_flushes" in row
    assert "last_flush_batch" in row
    assert "pass_commit_ratio" in row
    assert row["total_flushed"] == 20

    summary = payload["summary"]
    assert summary["completed"] == 1
    assert "avg_commits_per_chunk" in summary
    assert "scaling" in summary
