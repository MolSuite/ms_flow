"""GPU as a second admission token (the 'mini-ray for loky' second semaphore).

Mirrors the existing CPU admission tests: a local process executor must not run
more GPU-tagged chunks at once than the manager's total_gpu budget, independently
of how many CPUs / pool slots are free.
"""
import time
from pathlib import Path

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.executor.manager import ExecutorManager


def _gpu_task(payload: dict):
    start = time.time()
    time.sleep(0.4)
    end = time.time()
    with open(payload["log"], "a") as fh:
        fh.write(f"{start} {end}\n")
    return {"ok": True}


def _max_concurrency(intervals):
    events = []
    for s, e in intervals:
        events.append((s, 1))
        events.append((e, -1))
    events.sort()
    cur = peak = 0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)
    return peak


def _wait(manager, job_id, timeout_s=40.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row = manager.get_job(job_id)
        if row and row["status"] in ("completed", "failed"):
            return row
        time.sleep(0.05)
    return manager.get_job(job_id)


def _read_intervals(log: Path):
    return [
        (float(a), float(b))
        for a, b in (line.split() for line in log.read_text().splitlines() if line.strip())
    ]


def test_gpu_tokens_limit_process_concurrency(tmp_path):
    log = tmp_path / "gpu_timings.txt"
    log.write_text("")
    # 16 CPUs so CPU is never the limiter; only 2 GPUs -> at most 2 GPU chunks at once.
    manager = ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "e.db"),
        master_db=MasterDB(tmp_path / "p.db"),
        total_cpu=16,
        total_gpu=2,
        poll_interval=0.02,
    )
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"log": str(log)} for _ in range(6)],
            run_chunk=_gpu_task,
            default_cpu_required=1,
            default_gpu_required=1,
            store_results=True,
        )
        row = _wait(manager, job_id)
        assert row is not None and row["status"] == "completed", row
        intervals = _read_intervals(log)
        assert len(intervals) == 6, f"expected 6 tasks to run, got {len(intervals)}"
        conc = _max_concurrency(intervals)
        assert conc <= 2, f"GPU semaphore breached: {conc} concurrent (budget=2)"
    finally:
        manager.stop()


def test_gpu_required_beyond_budget_is_not_admitted(tmp_path):
    log = tmp_path / "starved.txt"
    log.write_text("")
    # No GPUs at all, but the chunk asks for one -> never admissible, must not run.
    manager = ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "e.db"),
        master_db=MasterDB(tmp_path / "p.db"),
        total_cpu=8,
        total_gpu=0,
        poll_interval=0.02,
    )
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"log": str(log)}],
            run_chunk=_gpu_task,
            default_cpu_required=1,
            default_gpu_required=1,
            store_results=True,
        )
        time.sleep(2.0)
        row = manager.get_job(job_id)
        assert row["status"] != "completed", "job ran despite no GPU budget"
        assert _read_intervals(log) == [], "task executed despite gpu starvation"
    finally:
        manager.stop()
