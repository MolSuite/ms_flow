import sys
import time
import json
import sqlite3
import threading
import types
from datetime import datetime
from pathlib import Path

import pytest

from sqlmodel import select

from ms_flow.core.data import DataContext, DbOutputSpec, FileInputSpec, FileOutputSpec
from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.database.executor_models import (
    ExecutorJob,
    ExecutorJobChunk,
    ExecutorJobEvent,
    ExecutorJobFeedState,
)
from ms_flow.core.executor.job_monitoring import (
    JobRuntimeMetrics,
    RuntimeHealthDbSnapshot,
    SchedulerNoteSnapshot,
    build_job_snapshot,
    derive_job_status,
)
from ms_flow.core.executor.manager import ExecutorManager
from ms_flow.core.executor.result_handlers import OutputSpecResultHandler
from ms_flow.core.executor.runtime_state import RunningChunk


_FLAKY_STATE = {}
_FINALIZE_STATE = {}
_FINALIZE_CONTEXT = {}


class _InjectedSessionOutage:
    def __init__(self, db, error_message: str):
        self._db = db
        self._error = sqlite3.OperationalError(error_message)
        self._lock = threading.Lock()
        self._fail_next_calls = 0
        self._always_fail = False
        self._original_get_session = db.get_session

    def fail_next(self, count: int) -> None:
        with self._lock:
            self._fail_next_calls = max(0, int(count))

    def set_always_fail(self, enabled: bool) -> None:
        with self._lock:
            self._always_fail = bool(enabled)

    def install(self, monkeypatch) -> None:
        monkeypatch.setattr(self._db, "get_session", self.get_session)

    def get_session(self):
        with self._lock:
            if self._always_fail:
                raise self._error
            if self._fail_next_calls > 0:
                self._fail_next_calls -= 1
                raise self._error
        return self._original_get_session()


def _thread_double(payload: dict):
    time.sleep(payload.get("sleep", 0.01))
    return payload["value"] * 2


def _thread_fail(payload: dict):
    time.sleep(payload.get("sleep", 0.01))
    raise RuntimeError(payload.get("error", "boom"))


def _thread_maybe_fail(payload: dict):
    time.sleep(payload.get("sleep", 0.01))
    if payload.get("fail"):
        raise RuntimeError(payload.get("error", "boom"))
    return payload.get("value", 1) * 2


def _process_sleep(payload: dict):
    time.sleep(payload.get("sleep", 0.2))
    return {"ok": payload.get("name", "chunk")}


def _flaky_once(payload: dict):
    key = payload["id"]
    if key not in _FLAKY_STATE:
        _FLAKY_STATE[key] = 1
        raise RuntimeError("flaky")
    return {"done": key}


def _thread_progress(payload: dict, progress_cb):
    steps = payload.get("steps", 4)
    delay = payload.get("sleep", 0.02)
    for idx in range(steps):
        time.sleep(delay)
        progress_cb(((idx + 1) / steps) * 100.0)
    return {"steps": steps}


def _setup_context(_payload: dict, _context: dict):
    return {"prepared": True, "tag": "staged"}


def _stage_attach_metadata(payload: dict, context: dict):
    enriched = dict(payload)
    enriched["prepared"] = bool(context.get("setup_data", {}).get("prepared"))
    enriched["tag"] = context.get("setup_data", {}).get("tag", "")
    return enriched


def _stage_fail_for_negative(payload: dict, _context: dict):
    value = int(payload.get("value", 0))
    if value < 0:
        raise RuntimeError(f"invalid value {value}")
    return payload


def _echo_payload(payload: dict):
    return dict(payload)


def _finalize_mark_done(_payload: dict, context: dict):
    _FINALIZE_STATE[context["job_id"]] = True
    _FINALIZE_CONTEXT[context["job_id"]] = dict(context)
    return {"ok": True}


def _blob_length(payload: dict):
    blob = payload.get("blob", "")
    return {"size": len(blob)}


def _emit_large_result(payload: dict):
    return {"blob": "x" * int(payload.get("size", 0))}


def _read_text_from_path_payload(payload: dict):
    data_path = Path(payload["data_path"])
    return {"text": data_path.read_text(encoding="utf-8"), "staged_path": str(data_path)}


def _read_text_from_staged_file(payload: dict):
    ligand_path = Path(payload["ligands"])
    return {
        "text": ligand_path.read_text(encoding="utf-8"),
        "staged_path": str(ligand_path),
    }


def _stage_delay(payload: dict, _context: dict):
    enriched = dict(payload)
    time.sleep(float(enriched.get("stage_sleep", 0.01)))
    enriched["staged"] = True
    return enriched


_stage_gate = threading.Event()


def _stage_wait_for_cancel(payload: dict, _context: dict):
    _stage_gate.wait(timeout=5.0)
    enriched = dict(payload)
    enriched["staged"] = True
    return enriched


def _run_delay(payload: dict):
    time.sleep(float(payload.get("run_sleep", 0.01)))
    return dict(payload)


# Gate for the cancel-during-setup test. The setup callable blocks on this
# event until the test has issued the cancel, making the "cancel arrives mid
# setup" condition deterministic instead of racing a fixed sleep against load.
_setup_gate = threading.Event()


def _setup_delay(_payload: dict, _context: dict):
    # Bounded wait so a regression that never releases the gate fails the test
    # via timeout instead of hanging the whole suite.
    _setup_gate.wait(timeout=5.0)
    return {"prepared": True}


def _finalize_delay(_payload: dict, _context: dict):
    time.sleep(0.2)
    return {"done": True}


class _BucketHandler:
    def __init__(self, bucket: list):
        self.bucket = bucket

    def handle(self, chunk_id: str, result):
        del chunk_id
        self.bucket.append(result)

    def flush(self):
        return None


class _FailingHandler:
    def handle(self, chunk_id: str, result):
        raise RuntimeError("sink unavailable")

    def flush(self):
        return None


def _heavy_chunk_stream(total: int, *, stage_sleep: float = 0.01):
    for idx in range(total):
        yield {"value": idx, "stage_sleep": stage_sleep}


def _lazy_value_stream(total: int, *, sleep: float = 0.01):
    for idx in range(total):
        yield {"value": idx + 1, "sleep": sleep}


def _slow_feed_stream(total: int, *, feed_sleep: float = 0.05, run_sleep: float = 0.2):
    for idx in range(total):
        time.sleep(feed_sleep)
        yield {"value": idx + 1, "sleep": run_sleep}


def _wait_for_status(manager: ExecutorManager, job_id: str, expected: str, timeout_s: float = 8.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row = manager.get_job(job_id)
        if row and row["status"] == expected:
            return row
        time.sleep(0.05)
    row = manager.get_job(job_id)
    raise AssertionError(f"Job {job_id} did not reach status='{expected}'. last={row}")


def _wait_for_event(executor_db: ExecutorDB, job_id: str, event_type: str, timeout_s: float = 5.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        with executor_db.get_session() as session:
            event = session.exec(
                select(ExecutorJobEvent).where(
                    ExecutorJobEvent.job_id == job_id,
                    ExecutorJobEvent.event_type == event_type,
                )
            ).first()
        if event is not None:
            return event
        time.sleep(0.02)
    raise AssertionError(f"Job {job_id} did not emit event_type='{event_type}' in time.")


def _write_fake_hpc_scheduler(script_path: Path):
    script_path.write_text(
        """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def main(argv: list[str]) -> int:
    command = argv[1]
    if command == "submit":
        script_path = Path(argv[2]).resolve()
        control_dir = Path(argv[3]).resolve()
        state_dir = Path(argv[4]).resolve()
        scheduler_job_id = uuid.uuid4().hex
        proc = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        _save(
            state_dir / f"{scheduler_job_id}.json",
            {
                "pid": proc.pid,
                "control_dir": str(control_dir),
            },
        )
        print(scheduler_job_id)
        return 0

    scheduler_job_id = argv[2]
    state_dir = Path(argv[3]).resolve()
    state_path = state_dir / f"{scheduler_job_id}.json"
    state = _load(state_path)
    if not state:
        print(json.dumps({"state": "FAILED", "error": "Unknown scheduler job id"}))
        return 1

    control_dir = Path(state["control_dir"]).resolve()
    status_path = control_dir / "status.json"
    result_path = control_dir / "result.json"
    pid = int(state.get("pid", 0))

    if command == "poll":
        if result_path.exists():
            result = _load(result_path)
            if result.get("ok"):
                print(json.dumps({"state": "DONE"}))
                return 0
            print(json.dumps({"state": "FAILED", "error": result.get("error", "Execution failed")}))
            return 0
        if status_path.exists():
            status = _load(status_path)
            current = str(status.get("state", "")).upper()
            if current in {"FAILED", "ERROR", "CANCELED", "CANCELLED"}:
                print(json.dumps({"state": current, "error": status.get("error", "")}))
                return 0
        if pid and _is_alive(pid):
            print(json.dumps({"state": "RUNNING"}))
            return 0
        print(json.dumps({"state": "FAILED", "error": "Scheduler process exited without result"}))
        return 0

    if command == "cancel":
        if pid and _is_alive(pid):
            os.kill(pid, signal.SIGTERM)
        status = _load(status_path) if status_path.exists() else {}
        status.update({"state": "CANCELED"})
        _save(status_path, status)
        print("OK")
        return 0

    raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
""",
        encoding="utf-8",
    )
    script_path.chmod(0o755)


def _install_fake_ray(monkeypatch):
    import concurrent.futures

    class _FakeRemoteFn:
        def __init__(self, ray_module, fn):
            self._ray = ray_module
            self._fn = fn
            self._options = {}

        def options(self, **kwargs):
            clone = _FakeRemoteFn(self._ray, self._fn)
            clone._options = dict(kwargs)
            return clone

        def remote(self, *args, **kwargs):
            future = self._ray._pool.submit(self._fn, *args, **kwargs)
            future._num_cpus = self._options.get("num_cpus")
            future._num_gpus = self._options.get("num_gpus")
            self._ray._futures.append(future)
            return future

    class _FakeRay(types.SimpleNamespace):
        def __init__(self):
            super().__init__()
            self._initialized = False
            self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
            self._futures = []
            self._put_calls = 0

        def is_initialized(self):
            return self._initialized

        def init(self, **kwargs):
            self._initialized = True
            self._init_kwargs = kwargs

        def remote(self, fn):
            return _FakeRemoteFn(self, fn)

        def put(self, value):
            self._put_calls += 1
            future = concurrent.futures.Future()
            future.set_result(value)
            return future

        def wait(self, refs, timeout=0):
            ready = [ref for ref in refs if ref.done()]
            pending = [ref for ref in refs if not ref.done()]
            return ready, pending

        def get(self, ref):
            return ref.result()

        def cancel(self, ref, force=True):
            del force
            return ref.cancel()

        def shutdown(self):
            self._initialized = False

    fake_ray = _FakeRay()
    monkeypatch.setitem(sys.modules, "ray", fake_ray)
    return fake_ray
def test_executor_manager_thread_job_completes(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=4)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}, {"value": 2}, {"value": 3}],
            run_chunk=_thread_double,
            default_cpu_required=1,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 3
        assert job["chunks_failed"] == 0
    finally:
        manager.stop()


def test_executor_manager_restart_terminalizes_previous_active_work_without_reexecution(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    now = datetime.now()
    with executor_db.get_session() as session:
        session.add(
            ExecutorJob(
                job_id="interrupted-job",
                executor_name="thread",
                status="running",
                progress=25.0,
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            ExecutorJobChunk(
                job_id="interrupted-job",
                chunk_id="interrupted-chunk",
                executor_name="thread",
                status="pending",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            ExecutorJob(
                job_id="completed-job",
                executor_name="thread",
                status="completed",
                progress=100.0,
                created_at=now,
                updated_at=now,
                finished_at=now,
            )
        )
        session.commit()

    for _ in range(20):
        manager = ExecutorManager(
            executor_db=executor_db,
            master_db=master_db,
            total_cpu=2,
            poll_interval=0.01,
        )
        manager.register_thread_executor(name="thread", max_workers=1)
        manager.start()
        try:
            interrupted = _wait_for_status(manager, "interrupted-job", "failed")
            assert interrupted["error"] == "runtime_interrupted"
            assert manager.running_chunks_snapshot() == []
            with executor_db.get_session() as session:
                completed = session.exec(
                    select(ExecutorJob).where(
                        ExecutorJob.job_id == "completed-job"
                    )
                ).one()
            assert completed.status == "completed"
        finally:
            manager.stop()

    with executor_db.get_session() as session:
        chunk = session.exec(
            select(ExecutorJobChunk).where(
                ExecutorJobChunk.chunk_id == "interrupted-chunk"
            )
        ).one()
        events = session.exec(
            select(ExecutorJobEvent).where(
                ExecutorJobEvent.job_id == "interrupted-job",
                ExecutorJobEvent.event_type == "job_interrupted",
            )
        ).all()
    assert chunk.status == "failed"
    assert chunk.error == "runtime_interrupted"
    assert len(events) == 1


def test_executor_manager_shutdown_terminalizes_active_job(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=2,
        poll_interval=0.01,
    )
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    job_id = manager.submit_job(
        executor_name="thread",
        chunks=[{"value": 1, "sleep": 1.0}],
        run_chunk=_thread_double,
        max_inflight_tasks=1,
    )
    _wait_for_status(manager, job_id, "running")
    manager.stop()

    with executor_db.get_session() as session:
        job = session.exec(
            select(ExecutorJob).where(ExecutorJob.job_id == job_id)
        ).one()
        chunk = session.exec(
            select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
        ).one()
    assert job.status == "failed"
    assert job.error == "runtime_interrupted"
    assert chunk.status == "failed"
    assert chunk.error == "runtime_interrupted"


def test_executor_manager_uses_local_scheduler_for_dispatch(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    original_iter = manager._scheduler.iter_admissible
    calls = {"count": 0}

    def _tracked_iter(*args, **kwargs):
        calls["count"] += 1
        yield from original_iter(*args, **kwargs)

    manager._scheduler.iter_admissible = _tracked_iter
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}, {"value": 2}],
            run_chunk=_thread_double,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 2
        assert calls["count"] >= 1
    finally:
        manager.stop()


def test_executor_manager_control_plane_commands_run_on_engine_thread(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    caller_thread_id = threading.get_ident()
    observed: dict[str, int] = {}

    original_submit = manager.submission_service.submit_job
    original_cancel = manager._cancel_job_now

    def _submit_on_engine(**kwargs):
        observed["submit"] = threading.get_ident()
        return original_submit(**kwargs)

    def _cancel_on_engine(job_id):
        observed["cancel"] = threading.get_ident()
        return original_cancel(job_id)

    monkeypatch.setattr(manager.submission_service, "submit_job", _submit_on_engine)
    monkeypatch.setattr(manager, "_cancel_job_now", _cancel_on_engine)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1, "run_sleep": 0.2}],
            run_chunk=_run_delay,
        )
        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)

        assert observed["submit"] == manager._engine_thread_id
        assert observed["cancel"] == manager._engine_thread_id
        assert observed["submit"] != caller_thread_id
    finally:
        manager.stop()


def test_executor_manager_rejects_control_plane_mutation_before_start(tmp_path):
    manager = ExecutorManager(
        executor_db=ExecutorDB(tmp_path / "executor.db"),
        master_db=MasterDB(tmp_path / "projects.db"),
        total_cpu=1,
        poll_interval=0.02,
    )
    manager.register_thread_executor(name="thread", max_workers=1)

    with pytest.raises(RuntimeError, match="must be started"):
        manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_thread_double,
        )


def test_executor_manager_delegates_staging_cycle_to_lifecycle_controller(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    original_run = manager._lifecycle_controller.run_staging_cycle
    calls = {"count": 0}

    def _tracked_run():
        calls["count"] += 1
        return original_run()

    manager._lifecycle_controller.run_staging_cycle = _tracked_run
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo_payload,
            setup_ref=_setup_context,
            stage_ref=_stage_attach_metadata,
        )
        job = _wait_for_status(manager, job_id, "completed")
        deadline = time.time() + 2.0
        while time.time() < deadline and job["chunks_done"] != 1:
            job = manager.get_job(job_id)
            time.sleep(0.02)
        assert job["chunks_done"] == 1
        assert calls["count"] >= 1
    finally:
        manager.stop()


def test_executor_manager_capability_matrix_exposes_adapter_metadata(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.register_process_pool_executor(name="process")
    manager.register_ray_executor(name="ray_local", mode="local", cpus=1)

    matrix = manager.get_executor_capability_matrix()

    assert matrix["thread"]["backend"] == "thread"
    assert matrix["thread"]["mode"] == "local"
    assert matrix["thread"]["support_level"] == "stable"
    assert matrix["thread"]["supports_file_input"] is True
    assert matrix["process"]["consumes_local_cpu_tokens"] is True
    assert matrix["process"]["support_level"] == "experimental"
    assert matrix["ray_local"]["backend"] == "ray"
    assert matrix["ray_local"]["mode"] == "local"
    assert matrix["ray_local"]["support_level"] == "experimental"
    assert matrix["ray_local"]["shared_filesystem"] is True
    assert matrix["ray_local"]["supports_db_input"] is True


def test_executor_manager_process_respects_global_cpu_tokens(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.start()

    max_running = 0
    try:
        job_a = manager.submit_job(
            executor_name="process",
            chunks=[{"name": "a", "sleep": 0.25, "_cpu_required": 2}],
            run_chunk=_process_sleep,
        )
        job_b = manager.submit_job(
            executor_name="process",
            chunks=[{"name": "b", "sleep": 0.25, "_cpu_required": 2}],
            run_chunk=_process_sleep,
        )

        deadline = time.time() + 8.0
        while time.time() < deadline:
            snap = manager.get_status()
            max_running = max(max_running, snap["executors"]["process"]["running_chunks"])
            status_a = manager.get_job(job_a)["status"]
            status_b = manager.get_job(job_b)["status"]
            if status_a == "completed" and status_b == "completed":
                break
            if status_a == "failed" and status_b == "failed":
                with executor_db.get_session() as session:
                    errors = session.exec(
                        select(ExecutorJobChunk.error).where(
                            ExecutorJobChunk.job_id.in_((job_a, job_b))
                        )
                    ).all()
                if any("Permission denied" in (err or "") for err in errors):
                    pytest.skip("Process spawning is blocked in this sandbox environment.")
                raise AssertionError(f"Process jobs failed unexpectedly: {errors}")
            time.sleep(0.03)
        else:
            raise AssertionError("Process jobs did not complete in time.")

        assert max_running <= 1
    finally:
        manager.stop()


def test_executor_manager_constructs_owned_runtime_components(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    try:
        assert manager.payload_store is not None
        assert manager.event_recorder is not None
        assert manager.job_store is not None
        assert manager.feeding_service is not None
        assert manager.dispatch_service is not None
    finally:
        manager.stop()


def test_executor_manager_process_skips_oversized_chunk_and_runs_smaller_later_chunk(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[
                {"name": "too_big", "sleep": 0.2, "_cpu_required": 4},
                {"name": "fits", "sleep": 0.05, "_cpu_required": 1},
            ],
            run_chunk=_process_sleep,
            max_inflight_tasks=2,
        )

        deadline = time.time() + 8.0
        observed_done = False
        while time.time() < deadline:
            row = manager.get_job(job_id)
            if row is not None and row["chunks_done"] >= 1:
                observed_done = True
                break
            if row is not None and row["chunks_failed"] >= 1:
                with executor_db.get_session() as session:
                    errors = session.exec(
                        select(ExecutorJobChunk.error).where(
                            ExecutorJobChunk.job_id == job_id
                        )
                    ).all()
                if any("Permission denied" in (err or "") for err in errors):
                    pytest.skip("Process spawning is blocked in this sandbox environment.")
            time.sleep(0.03)

        assert observed_done

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        completed_rows = [row for row in rows if row.status == "completed"]
        assert len(completed_rows) == 1
        assert json.loads(completed_rows[0].output_json or "{}").get("ok") == "fits"

        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)
    finally:
        manager.stop()


def test_executor_manager_reports_queued_status_for_emitted_but_undispatched_job(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        blocker_job = manager.submit_job(
            executor_name="process",
            chunks=[{"name": "blocker", "sleep": 0.4, "_cpu_required": 1}],
            run_chunk=_process_sleep,
            max_inflight_tasks=1,
        )
        queued_job = manager.submit_job(
            executor_name="process",
            chunks=[{"name": "queued", "sleep": 0.1, "_cpu_required": 1}],
            run_chunk=_process_sleep,
            max_inflight_tasks=1,
        )

        deadline = time.time() + 8.0
        observed_queued = False
        while time.time() < deadline:
            blocker = manager.get_job(blocker_job)
            queued = manager.get_job(queued_job)
            with executor_db.get_session() as session:
                errors = session.exec(
                    select(ExecutorJobChunk.error).where(
                        ExecutorJobChunk.job_id.in_((blocker_job, queued_job)),
                        ExecutorJobChunk.status == "failed",
                    )
                ).all()
            if any("Permission denied" in (err or "") for err in errors):
                pytest.skip("Process spawning is blocked in this sandbox environment.")
            if blocker is not None and queued is not None:
                if blocker["status"] == "running" and queued["status"] == "queued":
                    observed_queued = True
                    assert queued["chunks_emitted"] >= 1
                    assert queued["chunks_dispatched"] == 0
                    assert queued["chunks_ready_not_dispatched"] == 1
                    assert queued["backlog_chunks"] == 1
                    assert queued["backlog_dispatch_chunks"] == 1
                    assert queued["backlog_stage_chunks"] == 0
                    assert queued["scheduler_block_reason"] == "waiting_for_global_cpu"
                    assert queued["last_scheduler_reason"] == "waiting_for_global_cpu"
                    assert queued["last_scheduler_reason_at"] is not None
                    assert queued["last_dispatch_attempt_at"] is None
                    assert queued["first_chunk_emitted_at"] is not None
                    assert queued["first_chunk_dispatched_at"] is None
                    assert queued["job_age_s"] >= 0.0
                    assert queued["active_work_age_s"] is not None
                    assert queued["running_cpu"] == 0
                    break
            time.sleep(0.03)

        assert observed_queued
        _wait_for_event(executor_db, queued_job, "job_waiting_for_global_cpu")
        _wait_for_status(manager, blocker_job, "completed")
        _wait_for_status(manager, queued_job, "completed")
    finally:
        manager.stop()


def test_executor_manager_seeds_multiple_new_feeds_before_filling_one_window(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        first_job = manager.submit_job(
            executor_name="thread",
            chunks=_slow_feed_stream(4, feed_sleep=0.08, run_sleep=0.2),
            run_chunk=_thread_double,
            max_inflight_tasks=4,
        )
        second_job = manager.submit_job(
            executor_name="thread",
            chunks=_slow_feed_stream(4, feed_sleep=0.08, run_sleep=0.2),
            run_chunk=_thread_double,
            max_inflight_tasks=4,
        )

        deadline = time.time() + 2.0
        observed_seeded_round = False
        while time.time() < deadline:
            first = manager.get_job(first_job)
            second = manager.get_job(second_job)
            if first is not None and second is not None:
                if second["chunks_emitted"] >= 1 and first["chunks_emitted"] < 4:
                    observed_seeded_round = True
                    break
            time.sleep(0.02)

        assert observed_seeded_round, (manager.get_job(first_job), manager.get_job(second_job))
        _wait_for_status(manager, first_job, "completed")
        _wait_for_status(manager, second_job, "completed")
    finally:
        manager.stop()


def test_job_monitoring_clamps_progress_before_feed_is_exhausted():
    now = datetime.now()
    job = ExecutorJob(
        job_id="job",
        executor_name="thread",
        queue_policy="fifo",
        priority=0,
        status="running",
        progress=0.0,
        total_emitted=1,
        total_chunks=None,
        payload_json="{}",
        created_at=now,
        updated_at=now,
    )
    metrics = JobRuntimeMetrics(
        done=1,
        failed=0,
        canceled=0,
        stage_failed=0,
        staging=0,
        running=0,
        pending=0,
        total=1,
        processed=1,
        chunks_dispatched=1,
        chunks_ready_not_dispatched=0,
        chunk_queue_wait_avg_s=0.0,
        chunk_queue_wait_max_s=0.0,
        first_chunk_emitted_at=now,
        first_chunk_dispatched_at=now,
        last_dispatch_attempt_at=now,
        last_progress_at=now,
        chunks_emitted=1,
        feed_cursor_position=1,
        feed_items_acked=1,
        running_cpu=0,
        running_progress_sum=0.0,
        running_progress_avg=0.0,
        progress_structural=100.0,
        progress_operational=100.0,
        backlog_chunks=0,
        backlog_dispatch_chunks=0,
        backlog_stage_chunks=0,
        sink_lag_chunks=0,
        sink_lag_bytes=0,
        sink_oldest_lag_s=None,
        job_age_s=0.0,
        active_work_age_s=None,
    )
    status, progress = derive_job_status(
        job=job,
        lifecycle=None,
        metrics=metrics,
        feed_exhausted=False,
    )
    snapshot = build_job_snapshot(
        job=job,
        status=status,
        progress=progress,
        metrics=metrics,
        feed_exhausted=False,
        max_job_cpu=None,
        scheduler_block_reason="waiting_for_feed",
        scheduler_notes=SchedulerNoteSnapshot(
            current_scheduler_reason="waiting_for_feed",
            last_dispatch_attempt_at=now,
            last_scheduler_reason_at=now,
            last_scheduler_reason="waiting_for_feed",
            last_scheduler_payload={"note": "feed"},
        ),
        output_sink={
            "buffered_items": 2,
            "buffered_bytes": 128,
            "max_pending_chunks": 8,
            "max_pending_bytes": 1024,
            "flush_count": 3,
            "retry_count": 1,
            "flush_failures": 0,
            "last_flush_duration_ms": 12.5,
            "total_bytes_written": 2048,
            "total_items_written": 9,
        },
    )

    assert status == "running"
    assert snapshot["progress"] == 99.0
    assert snapshot["progress_structural"] == 99.0
    assert snapshot["progress_operational"] == 99.0
    assert snapshot["scheduler_block_category"] == "feed"
    assert snapshot["scheduler_block_details"] == {"note": "feed"}
    assert snapshot["sink_buffered_items"] == 2
    assert snapshot["sink_buffered_bytes"] == 128
    assert snapshot["sink_pending_chunks_pressure"] == 0.25
    assert snapshot["sink_pending_bytes_pressure"] == 0.125
    assert snapshot["sink_writer_flush_count"] == 3
    assert snapshot["sink_writer_retry_count"] == 1
    assert snapshot["sink_writer_last_flush_duration_ms"] == 12.5
    assert snapshot["sink_writer_total_bytes_written"] == 2048


def test_job_monitoring_snapshot_exposes_quota_block_details():
    now = datetime.now()
    job = ExecutorJob(
        job_id="job-quota",
        executor_name="thread",
        queue_policy="fifo",
        priority=0,
        status="queued",
        progress=0.0,
        total_emitted=2,
        total_chunks=2,
        payload_json="{}",
        created_at=now,
        updated_at=now,
        origin_id="origin",
        task_type="task",
    )
    metrics = JobRuntimeMetrics(
        done=0,
        failed=0,
        canceled=0,
        stage_failed=0,
        staging=0,
        running=0,
        pending=2,
        total=2,
        processed=0,
        chunks_dispatched=0,
        chunks_ready_not_dispatched=2,
        chunk_queue_wait_avg_s=0.0,
        chunk_queue_wait_max_s=0.0,
        first_chunk_emitted_at=now,
        first_chunk_dispatched_at=None,
        last_dispatch_attempt_at=None,
        last_progress_at=None,
        chunks_emitted=2,
        feed_cursor_position=2,
        feed_items_acked=0,
        running_cpu=0,
        running_progress_sum=0.0,
        running_progress_avg=0.0,
        progress_structural=0.0,
        progress_operational=0.0,
        backlog_chunks=2,
        backlog_dispatch_chunks=2,
        backlog_stage_chunks=0,
        sink_lag_chunks=0,
        sink_lag_bytes=0,
        sink_oldest_lag_s=None,
        job_age_s=0.0,
        active_work_age_s=0.0,
    )
    snapshot = build_job_snapshot(
        job=job,
        status="queued",
        progress=0.0,
        metrics=metrics,
        feed_exhausted=False,
        max_job_cpu=None,
        scheduler_block_reason="waiting_for_output_sink_quota",
        scheduler_notes=SchedulerNoteSnapshot(
            current_scheduler_reason="waiting_for_output_sink_quota",
            last_dispatch_attempt_at=None,
            last_scheduler_reason_at=now,
            last_scheduler_reason="waiting_for_output_sink_quota",
            last_scheduler_payload={"pending_chunks": 4, "max_pending_chunks": 2},
        ),
        output_sink={
            "buffered_items": 4,
            "buffered_bytes": 4096,
            "max_pending_chunks": 2,
            "max_pending_bytes": 2048,
        },
    )

    assert snapshot["scheduler_block_category"] == "quota"
    assert snapshot["scheduler_block_details"]["pending_chunks"] == 4
    assert snapshot["sink_pending_chunks_pressure"] == 2.0
    assert snapshot["sink_pending_bytes_pressure"] == 2.0


def test_job_monitoring_failed_terminal_job_keeps_partial_progress():
    now = datetime.now()
    job = ExecutorJob(
        job_id="job-failed",
        executor_name="thread",
        queue_policy="fifo",
        priority=0,
        status="failed",
        progress=0.0,
        total_emitted=101,
        total_chunks=101,
        payload_json="{}",
        created_at=now,
        updated_at=now,
        finished_at=now,
    )
    metrics = JobRuntimeMetrics(
        done=42,
        failed=59,
        canceled=0,
        stage_failed=0,
        staging=0,
        running=0,
        pending=0,
        total=101,
        processed=101,
        chunks_dispatched=101,
        chunks_ready_not_dispatched=0,
        chunk_queue_wait_avg_s=0.0,
        chunk_queue_wait_max_s=0.0,
        first_chunk_emitted_at=now,
        first_chunk_dispatched_at=now,
        last_dispatch_attempt_at=now,
        last_progress_at=now,
        chunks_emitted=101,
        feed_cursor_position=101,
        feed_items_acked=101,
        running_cpu=0,
        running_progress_sum=0.0,
        running_progress_avg=0.0,
        progress_structural=100.0,
        progress_operational=41.58,
        backlog_chunks=0,
        backlog_dispatch_chunks=0,
        backlog_stage_chunks=0,
        sink_lag_chunks=0,
        sink_lag_bytes=0,
        sink_oldest_lag_s=None,
        job_age_s=0.0,
        active_work_age_s=None,
    )

    status, progress = derive_job_status(
        job=job,
        lifecycle=None,
        metrics=metrics,
        feed_exhausted=True,
    )

    assert status == "failed"
    assert progress == pytest.approx(41.58)


def test_event_recorder_flush_progress_refreshes_parent_job(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)

    created_at = datetime.now()
    with executor_db.get_session() as session:
        session.add(
            ExecutorJob(
                job_id="job-progress-refresh",
                executor_name="process_pool",
                queue_policy="fifo",
                priority=0,
                status="running",
                progress=0.0,
                total_emitted=1,
                total_chunks=1,
                payload_json="{}",
                created_at=created_at,
                updated_at=created_at,
                started_at=created_at,
            )
        )
        session.add(
            ExecutorJobChunk(
                job_id="job-progress-refresh",
                chunk_id="chunk-progress-refresh",
                executor_name="process_pool",
                status="running",
                progress=0.0,
                created_at=created_at,
                updated_at=created_at,
                started_at=created_at,
            )
        )
        session.commit()

    manager.event_recorder.progress_flush_interval = 0.0
    manager.event_recorder.record_chunk_progress("job-progress-refresh", "chunk-progress-refresh", 50.0)
    manager.event_recorder.flush_progress()

    with executor_db.get_session() as session:
        job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == "job-progress-refresh")).first()
        chunk = session.exec(select(ExecutorJobChunk).where(ExecutorJobChunk.chunk_id == "chunk-progress-refresh")).first()

    assert chunk is not None
    assert job is not None
    assert chunk.progress == pytest.approx(50.0)
    assert job.progress > 0.0
    assert job.updated_at is not None
    assert job.updated_at > created_at


def test_executor_manager_process_respects_max_job_cpu_cap(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.start()
    max_running = 0
    observed_cap_reason = False
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"name": f"chunk_{idx}", "sleep": 0.2, "_cpu_required": 1} for idx in range(6)],
            run_chunk=_process_sleep,
            max_job_cpu=2,
            max_inflight_tasks=6,
        )

        deadline = time.time() + 10.0
        while time.time() < deadline:
            row = manager.get_job(job_id)
            with executor_db.get_session() as session:
                running = session.exec(
                    select(ExecutorJobChunk).where(
                        ExecutorJobChunk.job_id == job_id,
                        ExecutorJobChunk.status == "running",
                    )
                ).all()
                errors = session.exec(
                    select(ExecutorJobChunk.error).where(
                        ExecutorJobChunk.job_id == job_id,
                        ExecutorJobChunk.status == "failed",
                    )
                ).all()
            max_running = max(max_running, len(running))
            if any("Permission denied" in (err or "") for err in errors):
                pytest.skip("Process spawning is blocked in this sandbox environment.")
            if row is not None:
                assert row["max_job_cpu"] == 2
                assert row["running_cpu"] <= 2
                if row["chunks_dispatched"] > 0:
                    assert row["last_dispatch_attempt_at"] is not None
                if row["scheduler_block_reason"] == "waiting_for_job_cpu_cap":
                    observed_cap_reason = True
                    assert row["last_scheduler_reason"] == "waiting_for_job_cpu_cap"
                    assert row["last_scheduler_reason_at"] is not None
            if row is not None and row["status"] == "completed":
                break
            time.sleep(0.03)
        else:
            raise AssertionError(f"Process job did not complete in time. last={manager.get_job(job_id)}")

        assert max_running <= 2
        assert observed_cap_reason
        _wait_for_event(executor_db, job_id, "job_waiting_for_job_cpu_cap")
    finally:
        manager.stop()


def test_executor_manager_threads_do_not_consume_cpu_pool(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "sleep": 0.2, "_cpu_required": 1},
                {"value": 2, "sleep": 0.2, "_cpu_required": 1},
            ],
            run_chunk=_thread_double,
        )

        deadline = time.time() + 4.0
        observed_parallel = False
        while time.time() < deadline:
            snap = manager.get_status()
            thread_state = snap["executors"]["thread"]
            if thread_state["running_chunks"] >= 2:
                observed_parallel = True
                assert snap["cpu"]["used"] == 0
                assert snap["cpu"]["available"] == 1
                break
            if manager.get_job(job_id)["status"] == "completed":
                break
            time.sleep(0.02)

        _wait_for_status(manager, job_id, "completed")
        assert observed_parallel
    finally:
        manager.stop()


def test_executor_manager_max_inflight_tasks_limits_materialized_chunks(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=({"value": idx, "sleep": 0.03} for idx in range(5)),
            run_chunk=_thread_double,
            max_inflight_tasks=2,
        )

        max_live = 0
        deadline = time.time() + 4.0
        while time.time() < deadline:
            with executor_db.get_session() as session:
                rows = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).all()
            live = sum(
                1
                for row in rows
                if row.status in {"pending", "staging", "dispatching", "running"}
            )
            max_live = max(max_live, live)
            if manager.get_job(job_id)["status"] == "completed":
                break
            time.sleep(0.02)

        _wait_for_status(manager, job_id, "completed")

        with executor_db.get_session() as session:
            final_chunks = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert len(final_chunks) == 5
        assert max_live <= 2
    finally:
        manager.stop()


def test_executor_manager_inflight_window_counts_submit_future(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    adapter = manager._executors["process"]
    submit_started = threading.Event()
    release_submit = threading.Event()

    def _slow_submit(job_id, chunk_id, payload, fn_ref, progress_cb, submit_context=None):
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        submit_started.set()
        release_submit.wait(timeout=3.0)
        return "inflight-handle"

    monkeypatch.setattr(adapter, "submit", _slow_submit)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=({"value": idx, "_cpu_required": 1} for idx in range(3)),
            run_chunk=_process_sleep,
            max_inflight_tasks=1,
        )
        assert submit_started.wait(timeout=4.0)
        time.sleep(0.15)

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert len(rows) == 1
        assert manager._dispatch_pool.active_chunk_ids(job_id) == {rows[0].chunk_id}

        manager.cancel_job(job_id)
        release_submit.set()
        _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)
    finally:
        release_submit.set()
        manager.stop()


def test_executor_manager_persists_internal_dispatch_policy(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}, {"value": 2}],
            run_chunk=_thread_double,
            max_inflight_tasks=4,
            batch_size="auto",
            max_inflight_items=16,
            prefetch_factor=2.0,
            refill_threshold=2,
        )
        _wait_for_status(manager, job_id, "completed")

        with executor_db.get_session() as session:
            job = session.exec(
                select(ExecutorJob).where(ExecutorJob.job_id == job_id)
            ).first()

        assert job is not None
        payload = json.loads(job.payload_json or "{}")
        assert payload["_dispatch_policy"]["max_inflight_tasks"] == 4
        assert payload["_dispatch_policy"]["batch_size"] == "auto"
        assert payload["_dispatch_policy"]["max_inflight_items"] == 16
        assert payload["_dispatch_policy"]["prefetch_factor"] == 2.0
        assert payload["_dispatch_policy"]["refill_threshold"] == 2
        assert "_window_size" not in payload
    finally:
        manager.stop()


def test_executor_manager_load_respects_inflight_window_with_auto_batch_policy(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=({"value": idx, "sleep": 0.01} for idx in range(40)),
            run_chunk=_thread_double,
            batch_size="auto",
            max_inflight_tasks=3,
            max_inflight_items=12,
            prefetch_factor=2.0,
            refill_threshold=2,
        )

        max_live = 0
        deadline = time.time() + 6.0
        while time.time() < deadline:
            with executor_db.get_session() as session:
                rows = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).all()
            live = sum(1 for row in rows if row.status in {"pending", "running"})
            max_live = max(max_live, live)
            row = manager.get_job(job_id)
            if row is not None and row["status"] == "completed":
                break
            time.sleep(0.02)

        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 40
        assert max_live <= 3
    finally:
        manager.stop()


def test_executor_manager_persists_partial_progress(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=2,
        poll_interval=0.02,
        progress_flush_interval=0.02,
    )
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"steps": 8, "sleep": 0.05}],
            run_chunk=_thread_progress,
        )

        deadline = time.time() + 4.0
        observed_partial = False
        observed_job_partial = False
        while time.time() < deadline:
            with executor_db.get_session() as session:
                chunk = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).first()
            if chunk is not None and 0.0 < chunk.progress < 100.0:
                observed_partial = True
            job = manager.get_job(job_id)
            if job is not None and 0.0 < float(job["progress"]) < 100.0:
                observed_job_partial = True
                assert float(job["progress_structural"]) == 0.0
                assert float(job["progress_operational"]) == float(job["progress"])
                assert float(job["progress_running_chunks_avg"]) > 0.0
                assert job["last_progress_at"] is not None
            if observed_partial and observed_job_partial:
                break
            if manager.get_job(job_id)["status"] == "completed":
                break
            time.sleep(0.02)

        final = _wait_for_status(manager, job_id, "completed")
        assert observed_partial
        assert observed_job_partial
        assert float(final["progress"]) == 100.0
        assert float(final["progress_structural"]) == 100.0
        assert float(final["progress_operational"]) == 100.0
    finally:
        manager.stop()


def test_executor_manager_setup_stage_finalize_lifecycle(tmp_path):
    _FINALIZE_STATE.clear()
    _FINALIZE_CONTEXT.clear()
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 10}, {"value": 20}],
            run_chunk=_echo_payload,
            setup_ref=_setup_context,
            stage_ref=_stage_attach_metadata,
            finalize_ref=_finalize_mark_done,
            job_payload={
                "job_name": "lifecycle_test",
                "_chunker_params": {"profile": "default"},
            },
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 2
        assert _FINALIZE_STATE.get(job_id) is True
        assert _FINALIZE_CONTEXT[job_id]["terminal_status"] == "completed"
        assert _FINALIZE_CONTEXT[job_id]["job_name"] == "lifecycle_test"
        assert _FINALIZE_CONTEXT[job_id]["job_params"] == {"profile": "default"}

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        outputs = [json.loads(row.output_json or "{}") for row in rows]
        assert all(item.get("prepared") is True for item in outputs)
        assert all(item.get("tag") == "staged" for item in outputs)
    finally:
        manager.stop()


def test_executor_manager_finalize_context_reports_failed_terminal_status(tmp_path):
    _FINALIZE_CONTEXT.clear()
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=1,
        poll_interval=0.02,
    )
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"error": "expected"}],
            run_chunk=_thread_fail,
            finalize_ref=_finalize_mark_done,
            job_payload={
                "job_name": "failed_lifecycle_test",
                "_chunker_params": {"profile": "alphafold"},
            },
        )
        _wait_for_status(manager, job_id, "failed")
        assert _FINALIZE_CONTEXT[job_id]["terminal_status"] == "failed"
        assert _FINALIZE_CONTEXT[job_id]["job_params"] == {
            "profile": "alphafold"
        }
    finally:
        manager.stop()


def test_executor_manager_lifecycle_events_cover_all_stages(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "stage_sleep": 0.03, "run_sleep": 0.03},
                {"value": 2, "stage_sleep": 0.03, "run_sleep": 0.03},
            ],
            run_chunk=_run_delay,
            setup_ref=_setup_context,
            stage_ref=_stage_delay,
            finalize_ref=_finalize_mark_done,
        )
        _wait_for_status(manager, job_id, "completed", timeout_s=8.0)
        _wait_for_event(executor_db, job_id, "job_finalize_completed")

        with executor_db.get_session() as session:
            events = session.exec(
                select(ExecutorJobEvent.event_type).where(
                    ExecutorJobEvent.job_id == job_id
                ).order_by(ExecutorJobEvent.id.asc())
            ).all()

        expected = [
            "job_setup_started",
            "job_setup_completed",
            "chunk_staging_started",
            "chunk_staging_completed",
            "chunk_dispatched",
            "chunk_completed",
            "job_finalize_started",
            "job_finalize_completed",
        ]
        for event_type in expected:
            assert event_type in events

        # Check coarse ordering by first occurrence across lifecycle phases.
        first_idx = {event_type: events.index(event_type) for event_type in expected}
        assert first_idx["job_setup_started"] < first_idx["job_setup_completed"]
        assert first_idx["job_setup_completed"] < first_idx["chunk_staging_started"]
        assert first_idx["chunk_staging_started"] < first_idx["chunk_staging_completed"]
        assert first_idx["chunk_staging_completed"] < first_idx["chunk_dispatched"]
        assert first_idx["chunk_dispatched"] < first_idx["chunk_completed"]
        assert first_idx["chunk_completed"] < first_idx["job_finalize_started"]
        assert first_idx["job_finalize_started"] < first_idx["job_finalize_completed"]
    finally:
        manager.stop()


def test_executor_manager_stage_fail_fast_marks_job_failed(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": -1}],
            run_chunk=_echo_payload,
            stage_ref=_stage_fail_for_negative,
            stage_fail_policy="fail_fast",
        )
        job = _wait_for_status(manager, job_id, "failed")
        assert job["chunks_stage_failed"] == 1
        assert job["chunks_done"] == 0

        with executor_db.get_session() as session:
            events = session.exec(
                select(ExecutorJobChunk.status).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert "stage_failed" in events
    finally:
        manager.stop()


def test_executor_manager_stage_continue_with_threshold(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": -2}, {"value": 1}, {"value": -1}, {"value": 3}],
            run_chunk=_echo_payload,
            stage_ref=_stage_fail_for_negative,
            stage_fail_policy="continue_with_threshold",
            max_stage_failures=2,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_stage_failed"] == 2
        assert job["chunks_done"] == 2

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
            events = session.exec(
                select(ExecutorJobEvent.event_type).where(ExecutorJobEvent.job_id == job_id)
            ).all()
        outputs = sorted(
            json.loads(row.output_json or "{}")["value"]
            for row in rows
            if row.status == "completed"
        )
        assert outputs == [1, 3]
        assert len([row for row in rows if row.status == "stage_failed"]) == 2
        assert "chunk_stage_failed" in events
    finally:
        manager.stop()


def test_executor_manager_auto_materializes_file_input_specs(tmp_path):
    source = tmp_path / "ligand.txt"
    source.write_text("LIG-001", encoding="utf-8")

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": FileInputSpec(str(source), fmt="text")}],
            run_chunk=_echo_payload,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 1
        assert job["chunks_stage_failed"] == 0

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        assert chunk is not None
        assert json.loads(chunk.output_json) == {"value": "LIG-001"}
    finally:
        manager.stop()


def test_executor_manager_externalizes_large_chunk_payload_and_cleans_spool(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        large_blob = "x" * (1024 * 1024)  # 1MB
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"blob": large_blob}],
            run_chunk=_blob_length,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )
        job = _wait_for_status(manager, job_id, "completed")
        deadline = time.time() + 2.0
        while time.time() < deadline and job["chunks_done"] != 1:
            job = manager.get_job(job_id)
            time.sleep(0.02)
        assert job["chunks_done"] == 1

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        assert chunk is not None
        assert len(chunk.payload_json) < 300
        assert chunk.checkpoint_ref != ""
        assert not Path(chunk.checkpoint_ref).exists()
    finally:
        manager.stop()


def test_executor_manager_hpc_transport_stages_file_and_passes_remote_path(tmp_path):
    source = tmp_path / "source_ligand.smi"
    source.write_text("CCO ligand_1\n", encoding="utf-8")
    hpc_wdir = tmp_path / "simulated_hpc"

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"data_path": FileInputSpec(str(source), fmt="text")}],
            run_chunk=_read_text_from_path_payload,
            job_payload={
                "_data_context": {
                    "project_path": str(tmp_path),
                    "executor_backend": "hpc",
                    "executor_mode": "external",
                    "executor_shared_fs": False,
                    "hpc_wdir": str(hpc_wdir),
                }
            },
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 1

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        assert chunk is not None
        output = json.loads(chunk.output_json or "{}")
        assert output["text"] == "CCO ligand_1\n"
        staged_path = Path(output["staged_path"])
        assert staged_path.exists()
        assert str(staged_path).startswith(str(hpc_wdir))
    finally:
        manager.stop()


def test_executor_manager_hpc_command_adapter_runs_end_to_end(tmp_path):
    source = tmp_path / "source_ligand.smi"
    source.write_text("CCO ligand_1\n", encoding="utf-8")
    scheduler_script = tmp_path / "fake_hpc_scheduler.py"
    scheduler_state_dir = tmp_path / "fake_hpc_state"
    _write_fake_hpc_scheduler(scheduler_script)

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_hpc_executor(
        name="hpc",
        shared_fs=False,
        submit_command=[
            sys.executable,
            str(scheduler_script),
            "submit",
            "{submit_script_path}",
            "{control_dir}",
            "{scheduler_state_dir}",
        ],
        poll_command=[
            sys.executable,
            str(scheduler_script),
            "poll",
            "{scheduler_job_id}",
            "{scheduler_state_dir}",
        ],
        cancel_command=[
            sys.executable,
            str(scheduler_script),
            "cancel",
            "{scheduler_job_id}",
            "{scheduler_state_dir}",
        ],
        command_context={"scheduler_state_dir": str(scheduler_state_dir)},
    )
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="hpc",
            chunks=[{"ligands": FileInputSpec(str(source), fmt="text")}],
            run_chunk=_read_text_from_staged_file,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 1

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        assert chunk is not None
        output = json.loads(chunk.output_json or "{}")
        assert output["text"] == "CCO ligand_1\n"
        staged_path = Path(output["staged_path"])
        assert staged_path.exists()
        assert str(staged_path).startswith(str(tmp_path / "tmp" / "hpc_wdir"))
    finally:
        manager.stop()


def test_executor_manager_native_ray_adapter_runs_job_with_fake_ray(tmp_path, monkeypatch):
    fake_ray = _install_fake_ray(monkeypatch)
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_ray_executor(
        name="ray-native",
        mode="external",
        shared_fs=True,
        native=True,
    )
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="ray-native",
            chunks=[{"value": 2}, {"value": 5}],
            run_chunk=_thread_double,
            store_results=True,
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 2
        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk.output_json)
                .where(ExecutorJobChunk.job_id == job_id)
                .order_by(ExecutorJobChunk.id.asc())
            ).all()
        outputs = [json.loads(raw) for raw in rows if raw]
        assert sorted(outputs) == [4, 10]
        assert fake_ray._initialized is True
    finally:
        manager.stop()
def test_executor_manager_healthcheck_exposes_remote_executor_details(tmp_path, monkeypatch):
    _install_fake_ray(monkeypatch)
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_hpc_executor(name="hpc")
    manager.register_ray_executor(name="ray-native", mode="external", shared_fs=True, native=True)
    manager.start()
    try:
        snapshot = manager.get_status()
        assert snapshot["executors"]["hpc"]["integration"] == "stub"
        assert snapshot["executors"]["hpc"]["support_level"] == "experimental"
        assert snapshot["executors"]["ray-native"]["integration"] == "native"
        assert snapshot["executors"]["ray-native"]["support_level"] == "experimental"

        health = manager.get_healthcheck()
        assert health["checks"]["executors"]["hpc"]["ok"] is True
        assert health["checks"]["executors"]["hpc"]["support_level"] == "experimental"
        assert health["checks"]["executors"]["ray-native"]["initialized"] is True
        assert health["checks"]["executors"]["ray-native"]["support_level"] == "experimental"
    finally:
        manager.stop()


def test_executor_manager_ray_external_without_shared_fs_transfers_file_content(tmp_path, monkeypatch):
    _install_fake_ray(monkeypatch)
    source = tmp_path / "source_ligand.smi"
    source.write_text("CCO ligand_1\n", encoding="utf-8")

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_ray_executor(
        name="ray-native",
        mode="external",
        shared_fs=False,
        native=True,
    )
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="ray-native",
            chunks=[{"data_path": FileInputSpec(str(source), fmt="text")}],
            run_chunk=_echo_payload,
            job_payload={
                "_data_context": {
                    "project_path": str(tmp_path),
                    "executor_backend": "ray",
                    "executor_mode": "external",
                    "executor_shared_fs": False,
                }
            },
        )
        job = _wait_for_status(manager, job_id, "completed")
        assert job["chunks_done"] == 1
    finally:
        manager.stop()


def test_executor_manager_capability_matrix_exposes_shared_fs_modes(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.register_ray_executor(name="ray-ext", mode="external", shared_fs=False)
    matrix = manager.get_executor_capability_matrix()

    assert matrix["thread"]["supports_file_input"] is True
    assert matrix["thread"]["local_resource_accounting"] == "none"
    assert matrix["thread"]["support_level"] == "stable"
    assert matrix["ray-ext"]["shared_fs"] is False
    assert matrix["ray-ext"]["local_resource_accounting"] == "none"
    assert matrix["ray-ext"]["support_level"] == "experimental"
    assert matrix["ray-ext"]["supports_file_input"] is True


def test_executor_manager_status_exposes_local_resource_accounting(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=6, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.register_process_pool_executor(name="process")
    manager.register_ray_executor(name="ray-local", mode="local", cpus=3, shared_fs=True)

    snap = manager.get_status()

    assert snap["cpu"]["reserved"] == 3
    assert snap["executors"]["thread"]["local_resource_accounting"] == "none"
    assert snap["executors"]["thread"]["support_level"] == "stable"
    assert snap["executors"]["process"]["local_resource_accounting"] == "dynamic"
    assert snap["executors"]["process"]["locally_constrained"] is True
    assert snap["executors"]["process"]["support_level"] == "experimental"
    assert snap["executors"]["ray-local"]["local_resource_accounting"] == "reserved"
    assert snap["executors"]["ray-local"]["locally_constrained"] is True
    assert snap["executors"]["ray-local"]["support_level"] == "experimental"
    assert snap["loop"]["consecutive_errors"] == 0
    assert snap["loop"]["backoff_s"] == manager.poll_interval
    assert snap["loop"]["last_error"] == ""


def test_executor_manager_uses_fast_polling_only_for_local_active_work(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=2,
        poll_interval=0.1,
        active_poll_interval=0.02,
    )
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.register_hpc_executor(name="hpc")
    try:
        assert manager._current_loop_backoff_s() == pytest.approx(0.1)

        manager.runtime_state.register_running_chunk(
            RunningChunk(
                job_id="local-job",
                chunk_id="local-chunk",
                executor_name="thread",
                handle_id="local-handle",
                cpu_required=0,
            )
        )
        assert manager._current_loop_backoff_s() == pytest.approx(0.02)

        manager.runtime_state.clear_running_chunks()
        manager.runtime_state.register_running_chunk(
            RunningChunk(
                job_id="remote-job",
                chunk_id="remote-chunk",
                executor_name="hpc",
                handle_id="remote-handle",
                cpu_required=0,
            )
        )
        assert manager._current_loop_backoff_s() == pytest.approx(0.1)

        snapshot = manager.loop_runtime_snapshot()
        assert snapshot["adaptive_polling"] is True
        assert snapshot["active_poll_interval_s"] == pytest.approx(0.02)
        assert snapshot["latency_sensitive_work"] is False
    finally:
        manager.runtime_state.clear_running_chunks()
        manager.stop()


def test_executor_manager_status_exposes_hybrid_ray_budget_policy(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=8, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.register_process_pool_executor(name="process_pool", max_workers=4)
    manager.register_ray_executor(name="ray-local", mode="local", cpus=3, shared_fs=True)
    manager.register_ray_executor(name="ray-managed", mode="managed", shared_fs=True)
    manager.register_ray_executor(name="ray-ext", mode="external", shared_fs=False)

    snap = manager.get_status()
    policy = snap["local_budget_policy"]

    assert policy["total_cpu"] == 8
    assert policy["fixed_reserved_cpu"] == 3
    assert policy["dynamic_used_cpu"] == 0
    assert policy["dynamic_available_cpu"] == 5
    assert policy["distributed_runtime_policy"]["local_mode_budgeting"] == "reserved"
    assert policy["distributed_runtime_policy"]["managed_mode_budgeting"] == "none"
    assert policy["distributed_runtime_policy"]["external_mode_budgeting"] == "none"
    assert policy["distributed_runtime_policy"]["reusable_local_process_budgeting"] == "dynamic"
    assert policy["executors"]["process_pool"]["budgeting"] == "dynamic"
    assert policy["executors"]["ray-local"]["budgeting"] == "reserved"
    assert policy["executors"]["ray-local"]["reserved_cpu"] == 3
    assert policy["executors"]["ray-managed"]["budgeting"] == "none"
    assert policy["executors"]["ray-ext"]["budgeting"] == "none"


def test_reusable_process_pools_keep_cpu_required_under_scheduler_control(tmp_path):
    executor_name = "process_pool"
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_process_pool_executor(
        name=executor_name,
        max_workers=8,
    )
    manager.start()

    max_running = 0
    try:
        job_id = manager.submit_job(
            executor_name=executor_name,
            chunks=[{"name": f"chunk_{idx}", "sleep": 0.15, "_cpu_required": 2} for idx in range(4)],
            run_chunk=_process_sleep,
            max_inflight_tasks=4,
        )

        deadline = time.time() + 10.0
        while time.time() < deadline:
            row = manager.get_job(job_id)
            with executor_db.get_session() as session:
                running = session.exec(
                    select(ExecutorJobChunk).where(
                        ExecutorJobChunk.job_id == job_id,
                        ExecutorJobChunk.status == "running",
                    )
                ).all()
                errors = session.exec(
                    select(ExecutorJobChunk.error).where(
                        ExecutorJobChunk.job_id == job_id,
                        ExecutorJobChunk.status == "failed",
                    )
                ).all()
            max_running = max(max_running, len(running))
            if any("Permission denied" in (err or "") for err in errors):
                pytest.skip("Process spawning is blocked in this sandbox environment.")
            if row is not None:
                assert row["running_cpu"] <= 2
                assert row["executor_name"] == executor_name
            if row is not None and row["status"] == "completed":
                break
            time.sleep(0.02)

        final = _wait_for_status(manager, job_id, "completed", timeout_s=10.0)
        assert final["chunks_done"] == 4
        assert max_running <= 1
        assert manager.get_status()["cpu"]["used"] == 0
        assert manager._dispatch_pool.get_total_cpu_required() == 0
    finally:
        manager.stop()


def test_executor_manager_rejects_local_executor_registration_without_headroom(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_ray_executor(name="ray-local", mode="local", cpus=3, shared_fs=True)

    try:
        manager.register_ray_executor(name="ray-local-2", mode="local", cpus=2, shared_fs=True)
    except ValueError as exc:
        assert "available" in str(exc)
    else:
        raise AssertionError("Expected local executor registration without headroom to fail.")


def test_executor_manager_registration_accounts_for_running_local_work(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.runtime_state.register_running_chunk(
        RunningChunk(
            job_id="active",
            chunk_id="active-chunk",
            executor_name="process",
            handle_id="handle",
            cpu_required=3,
        )
    )

    with pytest.raises(ValueError, match="only 1 available"):
        manager.register_ray_executor(name="ray-local", mode="local", cpus=2, shared_fs=True)


def test_executor_manager_process_respects_local_ray_reservation(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_ray_executor(name="ray-local", mode="local", cpus=3, shared_fs=True)
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"name": "needs_two", "sleep": 0.1, "_cpu_required": 2}],
            run_chunk=_process_sleep,
        )

        deadline = time.time() + 1.0
        observed_blocked = False
        while time.time() < deadline:
            with executor_db.get_session() as session:
                chunk = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).first()
            if chunk is not None and chunk.status == "pending" and chunk.started_at is None:
                observed_blocked = True
                break
            row = manager.get_job(job_id)
            if row is not None and row["chunks_failed"] >= 1:
                with executor_db.get_session() as session:
                    errors = session.exec(
                        select(ExecutorJobChunk.error).where(ExecutorJobChunk.job_id == job_id)
                    ).all()
                if any("Permission denied" in (err or "") for err in errors):
                    pytest.skip("Process spawning is blocked in this sandbox environment.")
            time.sleep(0.03)

        assert observed_blocked
        assert manager.get_status()["cpu"]["available"] == 1

        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)
    finally:
        manager.stop()


def test_executor_manager_reports_queue_wait_telemetry(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[
                {"name": "a", "sleep": 0.20, "_cpu_required": 1},
                {"name": "b", "sleep": 0.20, "_cpu_required": 1},
            ],
            run_chunk=_process_sleep,
        )
        deadline = time.time() + 8.0
        job = None
        while time.time() < deadline:
            job = manager.get_job(job_id)
            if job and job["status"] in {"completed", "failed"}:
                break
            time.sleep(0.05)
        if job is None:
            raise AssertionError("Job queue telemetry test did not produce any status.")
        if job["status"] == "failed":
            with executor_db.get_session() as session:
                errors = session.exec(
                    select(ExecutorJobChunk.error).where(ExecutorJobChunk.job_id == job_id)
                ).all()
            if any("Permission denied" in (err or "") for err in errors):
                pytest.skip("Process spawning is blocked in this sandbox environment.")
            raise AssertionError(f"Process job failed unexpectedly: {errors}")
        assert job["status"] == "completed"
        assert job["job_queue_wait_s"] is not None
        assert job["job_queue_wait_s"] >= 0.0
        assert job["chunks_started"] == 2
        assert job["chunk_queue_wait_avg_s"] >= 0.0
        assert job["chunk_queue_wait_max_s"] >= 0.0

        with executor_db.get_session() as session:
            event_payloads = session.exec(
                select(ExecutorJobEvent.payload_json).where(
                    ExecutorJobEvent.job_id == job_id,
                    ExecutorJobEvent.event_type == "chunk_dispatched",
                )
            ).all()
        assert len(event_payloads) == 2
        decoded = [json.loads(item or "{}") for item in event_payloads]
        assert all("queue_wait_s" in row for row in decoded)
    finally:
        manager.stop()


def test_executor_manager_heavy_staging_does_not_block_other_executor_dispatch(tmp_path):
    """
    E2E stress-lite:
    - Heavy job with many staged chunks on one executor.
    - Fast job on a different executor should complete quickly.
    This validates that the central loop remains responsive while staging runs.
    """
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=4, poll_interval=0.02)
    manager.register_thread_executor(name="thread_slow", max_workers=2)
    manager.register_thread_executor(name="thread_fast", max_workers=1)
    manager.start()
    try:
        heavy_job_id = manager.submit_job(
            executor_name="thread_slow",
            chunks=_heavy_chunk_stream(1500, stage_sleep=0.01),
            run_chunk=_echo_payload,
            stage_ref=_stage_delay,
            max_inflight_tasks=64,
        )

        t0 = time.time()
        fast_job_id = manager.submit_job(
            executor_name="thread_fast",
            chunks=[{"value": 999}],
            run_chunk=_echo_payload,
        )
        fast = _wait_for_status(manager, fast_job_id, "completed", timeout_s=3.0)
        dt = time.time() - t0

        assert fast["chunks_done"] == 1
        assert dt < 2.0

        # Heavy job should still be active around this point in normal conditions.
        heavy_row = manager.get_job(heavy_job_id)
        assert heavy_row is not None
        assert heavy_row["status"] in {"pending", "staging", "running", "completed"}

        if heavy_row["status"] != "completed":
            manager.cancel_job(heavy_job_id)
            _wait_for_status(manager, heavy_job_id, "canceled", timeout_s=6.0)
    finally:
        manager.stop()


def test_executor_manager_cancel_running_thread_job_marks_cancel_requested_then_canceled(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    bucket = []
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 7, "run_sleep": 0.25}],
            run_chunk=_run_delay,
            result_handler=_BucketHandler(bucket),
        )

        deadline = time.time() + 4.0
        while time.time() < deadline:
            with executor_db.get_session() as session:
                chunk = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).first()
            if chunk is not None and chunk.status == "running":
                break
            time.sleep(0.02)
        else:
            raise AssertionError("Chunk did not reach running state before cancel.")

        manager.cancel_job(job_id)
        cancel_row = manager.get_job(job_id)
        assert cancel_row is not None
        assert cancel_row["status"] in {"cancel_requested", "canceled"}
        assert cancel_row["cancel_requested"] is True or cancel_row["status"] == "canceled"

        job = _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)
        assert job["cancel_requested"] is False
        assert job["chunks_done"] == 0
        assert job["chunks_failed"] == 0
        assert bucket == []

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
            events = session.exec(
                select(ExecutorJobEvent.event_type).where(
                    ExecutorJobEvent.job_id == job_id
                ).order_by(ExecutorJobEvent.id.asc())
            ).all()

        assert chunk is not None
        assert chunk.status == "canceled"
        assert json.loads(chunk.output_json or "{}") == {}
        assert "job_cancel_requested" in events
        assert "chunk_cancel_requested" in events
        assert "chunk_canceled" in events
        assert "job_canceled" in events
    finally:
        manager.stop()


def test_executor_manager_cancel_queued_during_completion_does_not_interleave(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    entered = threading.Event()
    release = threading.Event()
    bucket = []

    class _BlockingHandler(_BucketHandler):
        def handle(self, chunk_id: str, result):
            entered.set()
            release.wait(timeout=5.0)
            super().handle(chunk_id, result)

    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 7}],
            run_chunk=_thread_double,
            result_handler=_BlockingHandler(bucket),
        )

        assert entered.wait(timeout=4.0), "Completion did not reach the result handler barrier."
        cancel_done = threading.Event()

        def _cancel():
            manager.cancel_job(job_id)
            cancel_done.set()

        cancel_thread = threading.Thread(target=_cancel)
        cancel_thread.start()
        time.sleep(0.05)
        assert not cancel_done.is_set()
        release.set()
        cancel_thread.join(timeout=4.0)
        assert cancel_done.is_set()

        final = _wait_for_status(manager, job_id, "completed", timeout_s=6.0)
        assert final["chunks_done"] == 1
        assert bucket == [14]
        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        assert chunk is not None
        assert chunk.status == "completed"
        assert json.loads(chunk.output_json or "{}") == 14
    finally:
        release.set()
        manager.stop()


def test_executor_manager_result_handler_failure_fails_chunk_without_retry(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 7}],
            run_chunk=_thread_double,
            result_handler=_FailingHandler(),
        )

        job = _wait_for_status(manager, job_id, "failed", timeout_s=6.0)
        assert job["chunks_done"] == 0
        assert job["chunks_failed"] == 1

        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).one()
        assert chunk.status == "failed"
        assert "sink unavailable" in chunk.error
        assert manager._available_cpu == manager.total_cpu
    finally:
        manager.stop()


def test_executor_manager_cancel_pending_dispatch_releases_reserved_cpu(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    adapter = manager._executors["process"]

    submit_started = threading.Event()
    release_submit = threading.Event()
    canceled_handles = []

    def _slow_submit(job_id, chunk_id, payload, fn_ref, progress_cb, submit_context=None):
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        submit_started.set()
        release_submit.wait(timeout=2.0)
        return "late-handle"

    monkeypatch.setattr(adapter, "submit", _slow_submit)
    monkeypatch.setattr(adapter, "cancel", lambda handle_id: canceled_handles.append(handle_id) or True)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"value": 1, "_cpu_required": 1}],
            run_chunk=_process_sleep,
        )

        assert submit_started.wait(timeout=4.0), "Dispatch submit did not start."

        deadline = time.time() + 4.0
        while time.time() < deadline:
            if manager.get_status()["cpu"]["used"] == 1:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("Reserved CPU was not observed during pending dispatch.")

        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled", timeout_s=6.0)
        assert manager._dispatch_pool.snapshot()["abandoned_tasks"] == 1

        deadline = time.time() + 4.0
        while time.time() < deadline:
            status = manager.get_status()
            if status["cpu"]["used"] == 0 and manager._dispatch_pool.get_total_cpu_required() == 0:
                break
            time.sleep(0.02)
        else:
            raise AssertionError(f"Reserved CPU was not released after cancel. status={status}")

        release_submit.set()
        deadline = time.time() + 4.0
        while time.time() < deadline:
            if "late-handle" in canceled_handles and manager._dispatch_pool.snapshot()["abandoned_tasks"] == 0:
                break
            time.sleep(0.02)
        else:
            raise AssertionError(
                f"Late submit handle was not observed and canceled. handles={canceled_handles} "
                f"pool={manager._dispatch_pool.snapshot()}"
            )
    finally:
        release_submit.set()
        manager.stop()


def test_executor_manager_emits_structured_dispatch_timeout_event(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    adapter = manager._executors["process"]
    canceled_handles = []

    def _stuck_submit(job_id, chunk_id, payload, fn_ref, progress_cb, submit_context=None):
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        time.sleep(0.3)
        return "never-used"

    monkeypatch.setattr(adapter, "submit", _stuck_submit)
    monkeypatch.setattr(adapter, "cancel", lambda handle_id: canceled_handles.append(handle_id) or True)
    manager._dispatch_pool._timeout_s = 0.1
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[{"value": 1, "_cpu_required": 1}],
            run_chunk=_process_sleep,
        )
        _wait_for_status(manager, job_id, "failed", timeout_s=6.0)
        _wait_for_event(executor_db, job_id, "dispatch_timeout")
        with executor_db.get_session() as session:
            event = session.exec(
                select(ExecutorJobEvent).where(
                    ExecutorJobEvent.job_id == job_id,
                    ExecutorJobEvent.event_type == "dispatch_timeout",
                )
            ).first()
        assert event is not None
        payload = json.loads(event.payload_json or "{}")
        assert payload["executor_name"] == "process"
        assert payload["timeout_s"] == 0.1
        assert payload["cpu_required"] == 1

        deadline = time.time() + 4.0
        while time.time() < deadline:
            if "never-used" in canceled_handles and manager._dispatch_pool.snapshot()["abandoned_tasks"] == 0:
                break
            time.sleep(0.02)
        else:
            raise AssertionError(
                f"Timed-out submit future was not observed. handles={canceled_handles} "
                f"pool={manager._dispatch_pool.snapshot()}"
            )
    finally:
        manager.stop()


def test_executor_manager_dispatch_failure_does_not_corrupt_remaining_chunks(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    adapter = manager._executors["process"]
    original_submit = adapter.submit
    state = {"failed_once": False}

    def _fail_once_submit(*args, **kwargs):
        if not state["failed_once"]:
            state["failed_once"] = True
            raise RuntimeError("transient dispatch fault")
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(adapter, "submit", _fail_once_submit)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="process",
            chunks=[
                {"name": "a", "sleep": 0.05, "_cpu_required": 1},
                {"name": "b", "sleep": 0.05, "_cpu_required": 1},
                {"name": "c", "sleep": 0.05, "_cpu_required": 1},
            ],
            run_chunk=_process_sleep,
            max_inflight_tasks=3,
        )
        final = _wait_for_status(manager, job_id, "failed", timeout_s=8.0)
        assert final["chunks_failed"] == 1
        assert final["chunks_done"] == 2
        assert manager.get_status()["cpu"]["used"] == 0
        assert manager._dispatch_pool.get_total_cpu_required() == 0

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
            events = session.exec(
                select(ExecutorJobEvent).where(
                    ExecutorJobEvent.job_id == job_id,
                    ExecutorJobEvent.event_type == "dispatch_failed",
                )
            ).all()
        statuses = sorted(row.status for row in rows)
        if any("Permission denied" in str(row.error or "") for row in rows):
            pytest.skip("Process spawning is blocked in this sandbox environment.")
        assert statuses == ["completed", "completed", "failed"]
        assert len(events) == 1
        assert "transient dispatch fault" in str(events[0].message or "")
    finally:
        manager.stop()


def test_executor_manager_cancel_during_setup_marks_job_canceled(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    _setup_gate.clear()  # setup will block here until we release it post-cancel
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo_payload,
            setup_ref=_setup_delay,
        )
        _wait_for_event(executor_db, job_id, "job_setup_started")

        manager.cancel_job(job_id)
        _setup_gate.set()  # let setup finish now; cancel is already registered
        job = _wait_for_status(manager, job_id, "canceled")

        assert job["cancel_requested"] is False
        with executor_db.get_session() as session:
            chunk = session.exec(select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)).first()
            events = session.exec(select(ExecutorJobEvent).where(ExecutorJobEvent.job_id == job_id)).all()

        assert chunk is not None
        assert chunk.status == "canceled"
        event_types = [event.event_type for event in events]
        assert "job_setup_started" in event_types
        assert "job_setup_completed" not in event_types
        assert "job_cancel_requested" in event_types
        assert "job_canceled" in event_types
    finally:
        _setup_gate.set()  # never leave the setup thread blocked on failure
        manager.stop()


def test_executor_manager_cancel_during_staging_marks_chunk_and_job_canceled(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    _stage_gate.clear()
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo_payload,
            stage_ref=_stage_wait_for_cancel,
        )
        _wait_for_event(executor_db, job_id, "chunk_staging_started")

        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled")

        with executor_db.get_session() as session:
            chunk = session.exec(select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)).first()
            events = session.exec(select(ExecutorJobEvent).where(ExecutorJobEvent.job_id == job_id)).all()

        assert chunk is not None
        assert chunk.status == "canceled"
        event_types = [event.event_type for event in events]
        assert "chunk_staging_started" in event_types
        assert "chunk_staging_completed" not in event_types
        assert "job_cancel_requested" in event_types
        assert "job_canceled" in event_types
    finally:
        _stage_gate.set()
        manager.stop()


def test_executor_manager_cancel_during_finalize_marks_job_canceled(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")

    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo_payload,
            finalize_ref=_finalize_delay,
        )
        _wait_for_event(executor_db, job_id, "job_finalize_started")

        manager.cancel_job(job_id)
        job = _wait_for_status(manager, job_id, "canceled")

        assert job["cancel_requested"] is False
        with executor_db.get_session() as session:
            events = session.exec(select(ExecutorJobEvent).where(ExecutorJobEvent.job_id == job_id)).all()

        event_types = [event.event_type for event in events]
        assert "job_finalize_started" in event_types
        assert "job_finalize_completed" not in event_types
        assert "job_cancel_requested" in event_types
        assert "job_canceled" in event_types
    finally:
        manager.stop()


def test_executor_manager_persists_output_spec_in_batches(tmp_path):
    db_path = tmp_path / "project.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        rows = [{"ligand_id": idx, "score": float(idx) * 0.1} for idx in range(1, 8)]
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=rows,
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="custom",
                db_path=str(db_path),
            ),
            output_flush_every=3,
            job_payload={
                "_data_context": {"project_db_path": str(db_path)},
            },
        )
        job = _wait_for_status(manager, job_id, "completed")
        deadline = time.time() + 2.0
        while time.time() < deadline and job["chunks_done"] != 7:
            job = manager.get_job(job_id)
            time.sleep(0.02)
        assert job["chunks_done"] == 7

        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM docking_results")
            total = cur.fetchone()[0]
        finally:
            conn.close()
        assert total == 7

        with executor_db.get_session() as session:
            stored_outputs = session.exec(
                select(
                    ExecutorJobChunk.output_json,
                    ExecutorJobChunk.output_state,
                    ExecutorJobChunk.output_payload_json,
                    ExecutorJobChunk.output_confirmed_at,
                ).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert all(item[0] == "{}" for item in stored_outputs)
        assert all(item[1] == "confirmed" for item in stored_outputs)
        # Retention contract (docs/executor_output_retention_contract_2026-06-08.md):
        # with a sink and store_results=False the payload is NOT duplicated in executor.db.
        assert all(not json.loads(item[2] or "{}") for item in stored_outputs)
        assert all(item[3] is not None for item in stored_outputs)
    finally:
        manager.stop()


def test_executor_manager_exposes_sink_telemetry_in_job_and_runtime_snapshots(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "sleep": 0.05},
                {"value": 2, "sleep": 0.05},
                {"value": 3, "sleep": 0.05},
            ],
            run_chunk=_thread_double,
            store_results=False,
            output_spec=FileOutputSpec(path="sink_metrics.json", root="project", fmt="json"),
            output_flush_every=2,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )

        deadline = time.time() + 4.0
        observed = None
        runtime = None
        while time.time() < deadline:
            observed = manager.get_job(job_id)
            runtime = manager.get_operational_snapshot()
            if (
                observed is not None
                and observed["output_sink"] is not None
                and runtime["sinks"]["active_sinks"] >= 1
                and runtime["sinks"]["buffered_items"] >= 0
            ):
                break
            time.sleep(0.02)

        assert observed is not None
        assert runtime is not None
        assert observed["output_sink"] is not None
        assert "buffered_bytes" in observed["output_sink"]
        assert "max_payload_bytes" in observed["output_sink"]
        assert "max_observed_buffer_bytes" in observed["output_sink"]
        assert "max_pending_chunks" in observed["output_sink"]
        assert "throughput_eps" in observed
        assert "loop_latency_ms" in observed
        assert "sink_writer_flush_count" in observed
        assert runtime["sinks"]["active_sinks"] >= 1
        assert "flush_count" in runtime["sinks"]
        assert "writer_total_bytes_written" in runtime["sinks"]
        assert "pending_bytes_pressure" in runtime["sinks"]
        assert "blocked_by_reason" in runtime["jobs"]
        assert "loop" in runtime
        assert "staging" in runtime
        assert "feeds" in runtime
        assert "operational" in runtime
        assert "persistence" in runtime["operational"]
        assert "sink" in runtime["operational"]
        assert "lag_pending_units" in runtime["operational"]["persistence"]
        assert "pending_bytes_pressure" in runtime["operational"]["sink"]

        final = _wait_for_status(manager, job_id, "completed")
        assert final["chunks_done"] == 3
    finally:
        manager.stop()


def test_executor_manager_healthcheck_reports_runtime_state(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        health = manager.get_healthcheck()
        assert health["status"] == "ok"
        assert health["checks"]["executor_db"]["ok"] is True
        assert health["checks"]["manager_thread"]["ok"] is True
        assert health["checks"]["manager_loop"]["ok"] is True
        assert health["checks"]["manager_loop"]["consecutive_errors"] == 0
        assert health["checks"]["staging_pool"]["ok"] is True
        assert health["checks"]["heartbeat"]["ok"] is True
        assert health["core_health"]["status"] == "ok"
        assert health["persistence_health"]["status"] == "ok"
        assert health["sink_health"]["status"] == "ok"
    finally:
        manager.stop()


def test_executor_manager_persistence_lag_does_not_degrade_core_runtime(tmp_path, monkeypatch):
    import ms_flow.core.executor.runtime_status_service as runtime_status_service

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        monkeypatch.setattr(
            runtime_status_service,
            "build_runtime_health_db_snapshot",
            lambda *args, **kwargs: RuntimeHealthDbSnapshot(
                active_jobs=0,
                heartbeat_age_s=999.0,
                heartbeat_stale=True,
            ),
        )
        health = manager.get_healthcheck()
        assert health["status"] == "ok"
        assert health["core_health"]["status"] == "ok"
        assert health["persistence_health"]["status"] == "degraded"
        assert health["checks"]["heartbeat"]["stale"] is True
    finally:
        manager.stop()


def test_executor_manager_recovers_from_transient_executor_db_outage(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    outage = _InjectedSessionOutage(executor_db, "injected transient executor db outage")
    outage.install(monkeypatch)
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": idx, "sleep": 0.03} for idx in range(24)],
            run_chunk=_thread_double,
            max_inflight_tasks=8,
        )
        outage.fail_next(5)

        deadline = time.time() + 8.0
        observed_error = False
        final = None
        while time.time() < deadline:
            if manager._consecutive_loop_errors > 0:
                observed_error = True
            try:
                row = manager.get_job(job_id)
            except sqlite3.OperationalError:
                time.sleep(0.03)
                continue
            if row is not None and row["status"] == "completed":
                final = row
                break
            time.sleep(0.03)

        assert observed_error
        assert final is not None
        assert final["chunks_done"] == 24
        assert manager._thread is not None and manager._thread.is_alive()

        snap = manager.get_status()
        assert "transient executor db outage" in str(snap["loop"]["last_error"] or "")
        health = manager.get_healthcheck()
        assert health["status"] == "ok"
        assert health["checks"]["manager_loop"]["ok"] is True
        assert "transient executor db outage" in str(health["checks"]["manager_loop"]["last_error"] or "")
    finally:
        manager.stop()


def test_executor_manager_healthcheck_reports_persistent_executor_db_outage(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    outage = _InjectedSessionOutage(executor_db, "injected permanent executor db outage")
    outage.install(monkeypatch)
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": idx, "sleep": 0.05} for idx in range(16)],
            run_chunk=_thread_double,
            max_inflight_tasks=4,
        )
        outage.set_always_fail(True)

        deadline = time.time() + 8.0
        observed_health = None
        while time.time() < deadline:
            health = manager.get_healthcheck()
            loop_check = health["checks"]["manager_loop"]
            if (
                health["checks"]["executor_db"]["ok"] is False
                and loop_check["consecutive_errors"] > 0
                and "permanent executor db outage" in str(loop_check["last_error"] or "")
            ):
                observed_health = health
                break
            time.sleep(0.03)

        assert observed_health is not None
        assert observed_health["status"] == "failed"
        assert observed_health["persistence_health"]["status"] == "failed"
        assert observed_health["core_health"]["status"] == "degraded"
        assert observed_health["checks"]["manager_thread"]["alive"] is True
        assert observed_health["checks"]["manager_loop"]["backoff_s"] >= manager.poll_interval

        outage.set_always_fail(False)
        final = _wait_for_status(manager, job_id, "completed", timeout_s=8.0)
        assert final["chunks_done"] == 16
    finally:
        outage.set_always_fail(False)
        manager.stop()


def test_executor_manager_healthcheck_reports_dispatch_pool_metrics(tmp_path, monkeypatch):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=1, poll_interval=0.02)
    manager.register_process_pool_executor(name="process")
    adapter = manager._executors["process"]
    submit_started = threading.Event()
    release_submit = threading.Event()

    def _slow_submit(job_id, chunk_id, payload, fn_ref, progress_cb, submit_context=None):
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        submit_started.set()
        release_submit.wait(timeout=2.0)
        return "late-handle"

    monkeypatch.setattr(adapter, "submit", _slow_submit)
    manager.start()
    try:
        manager.submit_job(
            executor_name="process",
            chunks=[{"value": 1, "_cpu_required": 1}],
            run_chunk=_process_sleep,
        )
        assert submit_started.wait(timeout=4.0)

        deadline = time.time() + 4.0
        health = None
        while time.time() < deadline:
            health = manager.get_healthcheck()
            dispatch = health["checks"]["dispatch_pool"]
            if int(dispatch["active_tasks"]) >= 1 and dispatch["oldest_pending_age_s"] is not None:
                break
            time.sleep(0.02)
        assert health is not None
        dispatch = health["checks"]["dispatch_pool"]
        assert dispatch["ok"] is True
        assert dispatch["active_tasks"] >= 1
        assert dispatch["max_workers"] >= 1
        assert dispatch["timeout_s"] >= 1.0
        assert dispatch["oldest_pending_age_s"] is not None
        assert dispatch["submitted_total"] >= 1
    finally:
        release_submit.set()
        manager.stop()


def test_executor_manager_persists_dependency_block_reason_and_event_payload(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        blocker_job = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1, "sleep": 0.3}],
            run_chunk=_thread_double,
            max_inflight_tasks=1,
        )
        dependent_job = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 5}],
            run_chunk=_thread_double,
            depends_on=[blocker_job],
        )

        deadline = time.time() + 5.0
        observed = None
        while time.time() < deadline:
            observed = manager.get_job(dependent_job)
            if observed is not None and observed["scheduler_block_reason"] == "waiting_for_dependencies":
                break
            time.sleep(0.02)
        assert observed is not None
        assert observed["scheduler_block_reason"] == "waiting_for_dependencies"
        assert observed["last_scheduler_reason"] == "waiting_for_dependencies"
        assert observed["scheduler_block_category"] == "dependency"
        assert blocker_job in observed["scheduler_block_details"].get("dependencies", [])
        _wait_for_event(executor_db, dependent_job, "job_waiting_for_dependencies")

        with executor_db.get_session() as session:
            job_row = session.exec(
                select(ExecutorJob).where(ExecutorJob.job_id == dependent_job)
            ).first()
            event = session.exec(
                select(ExecutorJobEvent).where(
                    ExecutorJobEvent.job_id == dependent_job,
                    ExecutorJobEvent.event_type == "job_waiting_for_dependencies",
                )
            ).first()
        assert job_row is not None
        assert job_row.scheduler_reason == "waiting_for_dependencies"
        assert event is not None
        assert blocker_job in json.loads(event.payload_json or "{}").get("dependencies", [])

        _wait_for_status(manager, blocker_job, "completed")
        _wait_for_status(manager, dependent_job, "completed")
    finally:
        manager.stop()


def test_executor_manager_cancels_dependent_job_when_dependency_fails(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        blocker_job = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1, "sleep": 0.05, "error": "upstream failed"}],
            run_chunk=_thread_fail,
            max_inflight_tasks=1,
        )
        dependent_job = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 5}],
            run_chunk=_thread_double,
            depends_on=[blocker_job],
        )

        blocker = _wait_for_status(manager, blocker_job, "failed")
        assert blocker["status"] == "failed"
        dependent = _wait_for_status(manager, dependent_job, "canceled")
        assert dependent["status"] == "canceled"

        with executor_db.get_session() as session:
            dependent_row = session.exec(
                select(ExecutorJob).where(ExecutorJob.job_id == dependent_job)
            ).first()
            events = session.exec(
                select(ExecutorJobEvent).where(ExecutorJobEvent.job_id == dependent_job)
            ).all()
        assert dependent_row is not None
        assert "dependency failed or was canceled" in str(dependent_row.error or "").lower()
        assert "job_canceled" in {event.event_type for event in events}
    finally:
        manager.stop()


def test_executor_manager_chunk_fail_fast_aborts_after_consecutive_failures(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "fail": True, "sleep": 0.01},
                {"value": 2, "fail": True, "sleep": 0.01},
                {"value": 3, "fail": False, "sleep": 0.01},
            ],
            run_chunk=_thread_maybe_fail,
            max_inflight_tasks=1,
            chunk_fail_fast_max_consecutive_failures=2,
        )

        final = _wait_for_status(manager, job_id, "failed", timeout_s=6.0)
        assert final["chunks_failed"] == 2
        assert final["chunks_done"] == 0

        event = _wait_for_event(executor_db, job_id, "job_chunk_fail_fast_triggered")
        payload = json.loads(event.payload_json or "{}")
        assert payload["consecutive_chunk_failures"] == 2
    finally:
        manager.stop()


def test_executor_manager_chunk_fail_fast_aborts_on_failed_ratio_after_min_processed(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "fail": False, "sleep": 0.01},
                {"value": 2, "fail": True, "sleep": 0.01},
                {"value": 3, "fail": True, "sleep": 0.01},
                {"value": 4, "fail": False, "sleep": 0.01},
            ],
            run_chunk=_thread_maybe_fail,
            max_inflight_tasks=1,
            chunk_fail_fast_min_processed=3,
            chunk_fail_fast_max_failed_ratio=0.5,
        )

        final = _wait_for_status(manager, job_id, "failed", timeout_s=6.0)
        assert final["chunks_done"] == 1
        assert final["chunks_failed"] == 2

        event = _wait_for_event(executor_db, job_id, "job_chunk_fail_fast_triggered")
        assert "failed ratio reached" in event.message.lower()
        payload = json.loads(event.payload_json or "{}")
        assert payload["processed_for_ratio"] == 3
    finally:
        manager.stop()


def test_executor_manager_chunk_fail_fast_resets_consecutive_failures_after_success(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[
                {"value": 1, "fail": True, "sleep": 0.01},
                {"value": 2, "fail": False, "sleep": 0.01},
                {"value": 3, "fail": True, "sleep": 0.01},
                {"value": 4, "fail": True, "sleep": 0.01},
            ],
            run_chunk=_thread_maybe_fail,
            max_inflight_tasks=1,
            chunk_fail_fast_max_consecutive_failures=2,
        )

        final = _wait_for_status(manager, job_id, "failed", timeout_s=6.0)
        assert final["chunks_done"] == 1
        assert final["chunks_failed"] == 3

        event = _wait_for_event(executor_db, job_id, "job_chunk_fail_fast_triggered")
        payload = json.loads(event.payload_json or "{}")
        assert payload["consecutive_chunk_failures"] == 2
        assert payload["chunks_done"] == 1
    finally:
        manager.stop()


def test_executor_manager_large_output_sink_payload_flushes_without_failing_job(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.configure_output_sink_limits(max_payload_bytes=128)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"size": 1024}],
            run_chunk=_emit_large_result,
            store_results=False,
            output_spec=FileOutputSpec(path="oversized.json", root="project", fmt="json"),
            output_flush_every=1,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )
        final = _wait_for_status(manager, job_id, "completed", timeout_s=6.0)
        assert final["chunks_done"] == 1

        with executor_db.get_session() as session:
            events = session.exec(
                select(ExecutorJobEvent.event_type).where(
                    ExecutorJobEvent.job_id == job_id,
                )
            ).all()
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()

        assert chunk is not None
        assert chunk.output_state == "confirmed"
        assert json.loads(str(chunk.output_sink_info_json or "{}")).get("payload_bytes", 0) > 128
        assert "result_sink_failed" not in events

        payload = json.loads((tmp_path / "oversized.json").read_text(encoding="utf-8"))
        assert len(payload) == 1
        assert len(payload[0]["blob"]) == 1024
    finally:
        manager.stop()


def test_executor_manager_pending_sink_chunk_quota_forces_early_flush(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    manager.configure_output_sink_limits(max_pending_chunks=2)
    manager.start()
    try:
        original_persist = manager._data_bridge.persist_output
        calls = {"n": 0}

        def _counting_persist(spec, data, context):
            calls["n"] += 1
            return original_persist(spec, data, context)

        manager._data_bridge.persist_output = _counting_persist

        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}, {"value": 2}, {"value": 3}],
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=FileOutputSpec(path="quota.json", root="project", fmt="json"),
            output_flush_every=50,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )
        final = _wait_for_status(manager, job_id, "completed", timeout_s=6.0)
        assert final["chunks_done"] == 3
        assert calls["n"] >= 2

        payload = json.loads((tmp_path / "quota.json").read_text(encoding="utf-8"))
        assert len(payload) == 3
    finally:
        manager.stop()


def test_executor_manager_snapshot_exposes_sink_lag_metrics(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=1)
    sink_entered = threading.Event()
    sink_release = threading.Event()
    original_persist = manager._data_bridge.persist_output

    def _blocking_persist(spec, data, context):
        sink_entered.set()
        sink_release.wait(timeout=5.0)
        return original_persist(spec, data, context)

    manager._data_bridge.persist_output = _blocking_persist
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1, "sleep": 0.01}, {"value": 2, "sleep": 0.3}],
            run_chunk=_thread_double,
            store_results=False,
            output_spec=FileOutputSpec(path="sink_lag.json", root="project", fmt="json"),
            output_flush_every=50,
            job_payload={"_data_context": {"project_path": str(tmp_path)}},
        )
        assert sink_entered.wait(timeout=4.0)

        deadline = time.time() + 2.0
        observed = None
        while time.time() < deadline:
            observed = manager.get_job(job_id)
            if observed is not None and observed["sink_lag_chunks"] >= 1:
                break
            time.sleep(0.02)

        assert observed is not None
        assert observed["sink_lag_chunks"] >= 1
        assert observed["sink_lag_bytes"] > 0
        assert observed["sink_oldest_lag_s"] is not None
        assert observed["job_age_s"] >= 0.0
        sink_release.set()
        _wait_for_status(manager, job_id, "completed", timeout_s=6.0)
    finally:
        sink_release.set()
        manager.stop()


def test_executor_manager_output_spec_retries_then_succeeds(tmp_path):
    db_path = tmp_path / "project_retry.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        original_persist = manager._data_bridge.persist_output
        calls = {"n": 0}

        def _flaky_persist(spec, data, context):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("temporary db write error")
            return original_persist(spec, data, context)

        manager._data_bridge.persist_output = _flaky_persist

        rows = [{"ligand_id": idx, "score": float(idx)} for idx in range(1, 5)]
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=rows,
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="custom",
                db_path=str(db_path),
            ),
            output_flush_every=2,
            job_payload={"_data_context": {"project_db_path": str(db_path)}},
        )
        _wait_for_status(manager, job_id, "completed")
        assert calls["n"] >= 2

        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM docking_results")
            total = cur.fetchone()[0]
        finally:
            conn.close()
        assert total == 4
    finally:
        manager.stop()


def test_executor_manager_output_spec_persistent_failure_fails_job(tmp_path):
    db_path = tmp_path / "project_fail.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        def _broken_persist(spec, data, context):
            raise RuntimeError("hard failure")

        manager._data_bridge.persist_output = _broken_persist

        rows = [{"ligand_id": idx, "score": float(idx)} for idx in range(1, 4)]
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=rows,
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="custom",
                db_path=str(db_path),
            ),
            output_flush_every=1,
            job_payload={"_data_context": {"project_db_path": str(db_path)}},
        )
        job = _wait_for_status(manager, job_id, "failed")
        assert job["status"] == "failed"

        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM docking_results")
            total = cur.fetchone()[0]
        finally:
            conn.close()
        assert total == 0
    finally:
        manager.stop()


def test_executor_manager_output_spec_transient_flush_failure_recovers_without_duplicates(tmp_path):
    db_path = tmp_path / "project_retry.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        original_persist = manager._data_bridge.persist_output
        state = {"remaining_failures": 2}

        def _flaky_persist(spec, data, context):
            if state["remaining_failures"] > 0:
                state["remaining_failures"] -= 1
                raise RuntimeError("transient sink fault")
            return original_persist(spec, data, context)

        manager._data_bridge.persist_output = _flaky_persist
        rows = [{"ligand_id": idx, "score": float(idx)} for idx in range(1, 5)]
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=rows,
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="custom",
                db_path=str(db_path),
            ),
            output_flush_every=1,
            job_payload={"_data_context": {"project_db_path": str(db_path)}},
        )
        final = _wait_for_status(manager, job_id, "completed", timeout_s=8.0)
        assert final["chunks_done"] == 4

        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT ligand_id, score FROM docking_results ORDER BY ligand_id")
            persisted = cur.fetchall()
        finally:
            conn.close()
        assert persisted == [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert all(row.output_state == "confirmed" for row in rows)
        handler = manager._job_result_handlers.get(job_id)
        assert handler is None or handler.snapshot()["flush_failures"] == 0
    finally:
        manager.stop()


def test_executor_manager_output_spec_batch_retry_recovers_without_duplicates(tmp_path):
    db_path = tmp_path / "project_batch_retry.db"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
        conn.commit()
    finally:
        conn.close()

    master_db = MasterDB(tmp_path / "projects.db")
    executor_db = ExecutorDB(tmp_path / "executor.db")
    manager = ExecutorManager(executor_db=executor_db, master_db=master_db, total_cpu=2, poll_interval=0.02)
    manager.register_thread_executor(name="thread", max_workers=2)
    manager.start()
    try:
        original_persist = manager._data_bridge.persist_output
        state = {"remaining_failures": 1}

        def _flaky_persist(spec, data, context):
            if isinstance(data, list) and len(data) == 2 and state["remaining_failures"] > 0:
                state["remaining_failures"] -= 1
                raise RuntimeError("transient batched sink fault")
            return original_persist(spec, data, context)

        manager._data_bridge.persist_output = _flaky_persist
        rows = [{"ligand_id": idx, "score": float(idx)} for idx in range(1, 5)]
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=rows,
            run_chunk=_echo_payload,
            store_results=False,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="custom",
                db_path=str(db_path),
            ),
            output_flush_every=2,
            job_payload={"_data_context": {"project_db_path": str(db_path)}},
        )
        final = _wait_for_status(manager, job_id, "completed", timeout_s=8.0)
        assert final["chunks_done"] == 4

        conn = sqlite3.connect(str(db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT ligand_id, score FROM docking_results ORDER BY ligand_id")
            persisted = cur.fetchall()
        finally:
            conn.close()
        assert persisted == [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]

        with executor_db.get_session() as session:
            rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).all()
        assert all(row.output_state == "confirmed" for row in rows)
    finally:
        manager.stop()
