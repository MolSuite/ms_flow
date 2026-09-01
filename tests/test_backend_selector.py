"""Dynamic loky<->ray compute-backend switching behind one stable executor name."""
import time
from pathlib import Path

import pytest

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.executor.manager import ExecutorManager


def _double(payload: dict):
    return {"value": int(payload["value"]) * 2}


def _sleep_task(payload: dict):
    time.sleep(payload.get("sleep", 0.6))
    return {"ok": True}


def _wait(manager, job_id, timeout_s=30.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row = manager.get_job(job_id)
        if row and row["status"] in ("completed", "failed", "canceled"):
            return row
        time.sleep(0.05)
    return manager.get_job(job_id)


def _make_manager(tmp_path):
    return ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "e.db"),
        master_db=MasterDB(tmp_path / "p.db"),
        total_cpu=4,
        poll_interval=0.02,
    )


def test_activate_loky_and_execute(tmp_path):
    manager = _make_manager(tmp_path)
    manager.start()
    try:
        status = manager.activate_compute_backend("loky")
        assert status["backend"] == "loky"
        assert status["state"] == "healthy"
        assert status["changed"] is True
        assert status["healthy"] is True
        assert manager.compute_backend_status()["backend"] == "loky"

        # a real job targets the stable name, unaware of the backend underneath
        job_id = manager.submit_job(
            executor_name="compute",
            chunks=[{"value": 21}],
            run_chunk=_double,
            store_results=True,
        )
        assert _wait(manager, job_id)["status"] == "completed"
    finally:
        manager.stop()


def test_switch_refuses_running_then_kills(tmp_path):
    pytest.importorskip("ray", reason="ray not installed")
    manager = _make_manager(tmp_path)
    manager.start()
    try:
        manager.activate_compute_backend("loky")

        # long job still running on loky
        long_job = manager.submit_job(
            executor_name="compute",
            chunks=[{"sleep": 3.0} for _ in range(4)],
            run_chunk=_sleep_task,
            store_results=True,
        )
        # give it a moment to actually start running
        deadline = time.time() + 5
        while time.time() < deadline and not manager.get_job(long_job)["status"] == "running":
            time.sleep(0.05)

        # refuse: switching with work in flight and kill_running=False must raise
        with pytest.raises(RuntimeError):
            manager.activate_compute_backend("ray", policy="refuse_if_busy", cpus=2, wait_healthy_s=30.0)
        assert manager.compute_backend_status()["backend"] == "loky"

        # kill: switch anyway, old job is canceled, ray comes up healthy
        status = manager.activate_compute_backend("ray", policy="cancel_and_wait", cpus=2, wait_healthy_s=40.0)
        assert status["backend"] == "ray" and status["changed"] and status["healthy"]
        assert manager.compute_backend_status()["backend"] == "ray"
        assert manager.get_job(long_job)["status"] in ("canceled", "failed")

        # ray now executes jobs on the same stable name
        job_id = manager.submit_job(
            executor_name="compute",
            chunks=[{"value": 5}],
            run_chunk=_double,
            store_results=True,
        )
        assert _wait(manager, job_id, timeout_s=40.0)["status"] == "completed"
    finally:
        manager.stop()
