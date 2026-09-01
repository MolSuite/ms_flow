"""HPC/monitor tasks run on a thread executor that coexists with the compute
backend for free: threads don't consume CPU/GPU tokens, so an I/O 'monitor' task
is never blocked by a saturated compute pool. This is why HPC orchestration
(copy -> submit -> poll -> copy back) belongs on threads regardless of whether
compute is loky or ray.
"""
import time
from pathlib import Path

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.executor.manager import ExecutorManager


def _busy(payload: dict):
    time.sleep(payload.get("sleep", 2.0))
    return {"ok": True}


def _monitor(payload: dict):
    # stand-in for an HPC poll/copy step: I/O-ish, short
    time.sleep(payload.get("sleep", 0.2))
    return {"polled": True}


def _wait(manager, job_id, timeout_s=20.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row = manager.get_job(job_id)
        if row and row["status"] in ("completed", "failed", "canceled"):
            return row
        time.sleep(0.02)
    return manager.get_job(job_id)


def test_io_thread_task_not_blocked_by_saturated_compute(tmp_path):
    # Only 1 CPU token: the loky compute job saturates it for ~2s.
    manager = ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "e.db"),
        master_db=MasterDB(tmp_path / "p.db"),
        total_cpu=1,
        poll_interval=0.02,
    )
    manager.register_thread_executor(name="io", max_workers=4)
    manager.start()
    try:
        manager.activate_compute_backend("loky", max_workers=2)

        compute_job = manager.submit_job(
            executor_name="compute",
            chunks=[{"sleep": 2.0}],
            run_chunk=_busy,
            store_results=True,
        )
        time.sleep(0.2)  # let compute grab the single CPU token

        t0 = time.time()
        io_job = manager.submit_job(
            executor_name="io",
            chunks=[{"sleep": 0.2}],
            run_chunk=_monitor,
            store_results=True,
        )
        io_row = _wait(manager, io_job, timeout_s=5.0)
        io_elapsed = time.time() - t0

        assert io_row["status"] == "completed"
        # The monitor finished quickly instead of waiting ~2s for the CPU token.
        assert io_elapsed < 1.5, f"io task appears blocked by compute (took {io_elapsed:.2f}s)"
        assert _wait(manager, compute_job)["status"] == "completed"
    finally:
        manager.stop()
