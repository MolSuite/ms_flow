import time
from pathlib import Path

import pytest

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.executor.manager import ExecutorManager


def _double_value(payload: dict):
    return int(payload["value"]) * 2


def _wait_for_status(manager: ExecutorManager, job_id: str, expected: str, timeout_s: float = 8.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row = manager.get_job(job_id)
        if row and row["status"] == expected:
            return row
        time.sleep(0.05)
    row = manager.get_job(job_id)
    raise AssertionError(f"Job {job_id} did not reach status='{expected}'. last={row}")


def test_ray_native_e2e_local_cluster(tmp_path):
    pytest.importorskip("ray", reason="ray not installed")
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_ray_executor(
        name="ray-native",
        mode="local",
        cpus=1,
        shared_fs=True,
        native=True,
        address=None,
    )
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="ray-native",
            chunks=[{"value": 2}, {"value": 5}],
            run_chunk=_double_value,
            store_results=True,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 2
    finally:
        manager.stop()