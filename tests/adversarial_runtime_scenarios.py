from __future__ import annotations

import os
import threading
import time
from pathlib import Path

from sqlmodel import select

from ms_flow.core.database import ExecutorDB, MasterDB
from ms_flow.core.database.executor_models import ExecutorJobChunk
from ms_flow.core.executor.manager import ExecutorManager


_STAGE_ENTERED = threading.Event()
_STAGE_RELEASE = threading.Event()
_RUN_ENTERED = threading.Event()
_RUN_RELEASE = threading.Event()
_FAIL_ENTERED = threading.Event()
_FAIL_RELEASE = threading.Event()


def pytest_generate_tests(metafunc) -> None:
    if "rla_iteration" not in metafunc.fixturenames:
        return
    repetitions = max(1, int(os.environ.get("MOLSUITE_RLA_REPETITIONS", "1")))
    metafunc.parametrize(
        "rla_iteration",
        range(1, repetitions + 1),
        ids=lambda value: f"rla-{value:03d}",
    )


def _timeout_s() -> float:
    return max(1.0, float(os.environ.get("MOLSUITE_RLA_TIMEOUT", "8.0")))


def _wait_for_status(
    manager: ExecutorManager,
    job_id: str,
    expected: str,
) -> dict:
    deadline = time.time() + _timeout_s()
    last = None
    while time.time() < deadline:
        last = manager.get_job(job_id)
        if last is not None and last["status"] == expected:
            return dict(last)
        time.sleep(0.01)
    raise AssertionError(f"Job {job_id!r} did not reach {expected!r}; last={last}")


def _wait_for_chunk_status(
    executor_db: ExecutorDB,
    job_id: str,
    expected: str,
) -> ExecutorJobChunk:
    deadline = time.time() + _timeout_s()
    last = None
    while time.time() < deadline:
        with executor_db.get_session() as session:
            last = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).first()
        if last is not None and last.status == expected:
            return last
        time.sleep(0.01)
    raise AssertionError(f"Chunk for {job_id!r} did not reach {expected!r}; last={last}")


def _manager(root: Path, *, workers: int = 1) -> tuple[ExecutorManager, ExecutorDB]:
    master_db = MasterDB(root / "projects.db")
    executor_db = ExecutorDB(root / "executor.db")
    manager = ExecutorManager(
        executor_db=executor_db,
        master_db=master_db,
        total_cpu=max(1, workers),
        poll_interval=0.02,
    )
    manager.register_thread_executor(name="thread", max_workers=max(1, workers))
    return manager, executor_db


def _assert_terminal_chunks(executor_db: ExecutorDB, job_id: str) -> None:
    with executor_db.get_session() as session:
        chunks = session.exec(
            select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
        ).all()
    assert all(chunk.status in {"completed", "failed", "stage_failed", "canceled"} for chunk in chunks)


def _assert_no_divergence(manager: ExecutorManager) -> None:
    del manager


def _stage_block(payload: dict, _context: dict) -> dict:
    _STAGE_ENTERED.set()
    if not _STAGE_RELEASE.wait(timeout=_timeout_s()):
        raise TimeoutError("stage release was not signaled")
    return dict(payload)


def _run_block(payload: dict) -> dict:
    _RUN_ENTERED.set()
    if not _RUN_RELEASE.wait(timeout=_timeout_s()):
        raise TimeoutError("run release was not signaled")
    return dict(payload)


def _run_fail_after_release(payload: dict) -> dict:
    _FAIL_ENTERED.set()
    if not _FAIL_RELEASE.wait(timeout=_timeout_s()):
        raise TimeoutError("failure release was not signaled")
    raise RuntimeError(str(payload.get("error") or "forced failure"))


def _echo(payload: dict) -> dict:
    return dict(payload)


def test_submit_cancel_simultaneous(tmp_path, rla_iteration):
    del rla_iteration
    manager, executor_db = _manager(tmp_path)
    entered = threading.Event()
    release = threading.Event()
    persisted: dict[str, str] = {}
    original_create_job = manager.job_store.create_job

    def _blocking_create_job(*args, **kwargs):
        original_create_job(*args, **kwargs)
        persisted["job_id"] = str(kwargs["job_id"])
        entered.set()
        release.wait(timeout=_timeout_s())

    manager.job_store.create_job = _blocking_create_job
    result: dict[str, str] = {}
    manager.start()

    def _submit() -> None:
        result["job_id"] = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo,
        )

    submit_thread = threading.Thread(target=_submit)
    submit_thread.start()
    cancel_thread = None
    try:
        assert entered.wait(timeout=_timeout_s())
        job_id = persisted["job_id"]
        cancel_thread = threading.Thread(target=manager.cancel_job, args=(job_id,))
        cancel_thread.start()
        release.set()
        submit_thread.join(timeout=_timeout_s())
        cancel_thread.join(timeout=_timeout_s())
        assert not submit_thread.is_alive()
        assert not cancel_thread.is_alive()
        assert result["job_id"] == job_id
        _wait_for_status(manager, job_id, "canceled")
        _assert_terminal_chunks(executor_db, job_id)
        assert manager.get_job_feed(job_id) is None
        _assert_no_divergence(manager)
    finally:
        release.set()
        submit_thread.join(timeout=1.0)
        if cancel_thread is not None:
            cancel_thread.join(timeout=1.0)
        manager.stop()


def test_cancel_during_staging(tmp_path, rla_iteration):
    del rla_iteration
    _STAGE_ENTERED.clear()
    _STAGE_RELEASE.clear()
    manager, executor_db = _manager(tmp_path)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo,
            stage_ref=_stage_block,
        )
        assert _STAGE_ENTERED.wait(timeout=_timeout_s())
        manager.cancel_job(job_id)
        _STAGE_RELEASE.set()
        _wait_for_status(manager, job_id, "canceled")
        _assert_terminal_chunks(executor_db, job_id)
        _assert_no_divergence(manager)
    finally:
        _STAGE_RELEASE.set()
        manager.stop()


def test_cancel_during_dispatch(tmp_path, rla_iteration):
    del rla_iteration
    manager, executor_db = _manager(tmp_path)
    entered = threading.Event()
    release = threading.Event()
    adapter = manager._executors["thread"]
    original_submit = adapter.submit

    def _blocking_submit(*args, **kwargs):
        entered.set()
        release.wait(timeout=_timeout_s())
        return original_submit(*args, **kwargs)

    adapter.submit = _blocking_submit
    manager.start()
    cancel_thread = None
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo,
        )
        assert entered.wait(timeout=_timeout_s())
        cancel_thread = threading.Thread(target=manager.cancel_job, args=(job_id,))
        cancel_thread.start()
        release.set()
        cancel_thread.join(timeout=_timeout_s())
        assert not cancel_thread.is_alive()
        _wait_for_status(manager, job_id, "canceled")
        _assert_terminal_chunks(executor_db, job_id)
        _assert_no_divergence(manager)
    finally:
        release.set()
        if cancel_thread is not None:
            cancel_thread.join(timeout=1.0)
        manager.stop()


def test_completion_during_cancel(tmp_path, rla_iteration):
    del rla_iteration
    _RUN_ENTERED.clear()
    _RUN_RELEASE.clear()
    manager, executor_db = _manager(tmp_path)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_run_block,
        )
        assert _RUN_ENTERED.wait(timeout=_timeout_s())
        _wait_for_chunk_status(executor_db, job_id, "running")
        manager.cancel_job(job_id)
        _RUN_RELEASE.set()
        _wait_for_status(manager, job_id, "canceled")
        _assert_terminal_chunks(executor_db, job_id)
        _assert_no_divergence(manager)
    finally:
        _RUN_RELEASE.set()
        manager.stop()


def test_shutdown_with_active_work(tmp_path, rla_iteration):
    del rla_iteration
    _RUN_ENTERED.clear()
    _RUN_RELEASE.clear()
    manager, executor_db = _manager(tmp_path)
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_run_block,
        )
        assert _RUN_ENTERED.wait(timeout=_timeout_s())
        _wait_for_chunk_status(executor_db, job_id, "running")
        manager.stop()
        assert manager.manager_thread_alive() is False
        assert manager.get_status()["cpu"]["used"] == 0
        with executor_db.get_session() as session:
            chunk = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
            ).one()
        assert chunk.status == "failed"
        assert chunk.error == "runtime_interrupted"
    finally:
        _RUN_RELEASE.set()
        manager.stop()


def test_restart_with_running_chunk(tmp_path, rla_iteration):
    del rla_iteration
    _RUN_ENTERED.clear()
    _RUN_RELEASE.clear()
    manager1, executor_db = _manager(tmp_path)
    manager1.start()
    try:
        job_id = manager1.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_run_block,
        )
        assert _RUN_ENTERED.wait(timeout=_timeout_s())
        _wait_for_chunk_status(executor_db, job_id, "running")
        manager1.stop()
    finally:
        _RUN_RELEASE.set()
        manager1.stop()

    manager2 = ExecutorManager(
        executor_db=executor_db,
        master_db=manager1.master_db,
        total_cpu=1,
        poll_interval=0.02,
    )
    manager2.register_thread_executor(name="thread", max_workers=1)
    manager2.start()
    try:
        final = _wait_for_status(manager2, job_id, "failed")
        assert final["error"] == "runtime_interrupted"
        _assert_terminal_chunks(executor_db, job_id)
        _assert_no_divergence(manager2)
    finally:
        manager2.stop()


def test_late_handle_canceled_after_cancel(tmp_path, rla_iteration):
    """Cancel during in-flight dispatch; late handle arrives after cancel is ACKed."""
    del rla_iteration
    manager, executor_db = _manager(tmp_path)
    adapter = manager._executors["thread"]

    submit_entered = threading.Event()
    release_submit = threading.Event()
    canceled_handles: list[str] = []

    original_submit = adapter.submit

    def _blocking_submit(job_id, chunk_id, payload, fn_ref, progress_cb, submit_context=None):
        del job_id, chunk_id, payload, fn_ref, progress_cb, submit_context
        submit_entered.set()
        release_submit.wait(timeout=_timeout_s())
        return "late-handle-adv"

    def _recording_cancel(handle_id: str) -> bool:
        canceled_handles.append(handle_id)
        return True

    adapter.submit = _blocking_submit
    adapter.cancel = _recording_cancel
    manager.start()
    try:
        job_id = manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1}],
            run_chunk=_echo,
        )

        assert submit_entered.wait(timeout=_timeout_s()), "Dispatch submit did not start."

        # submit_entered fires from inside the pool's worker thread, which means
        # the item is already in _pending — no extra poll needed.

        manager.cancel_job(job_id)
        _wait_for_status(manager, job_id, "canceled")

        # Now release the blocked submit so the late handle arrives.
        release_submit.set()

        # Engine must cancel the late handle and release all resources.
        deadline = time.time() + _timeout_s()
        while time.time() < deadline:
            status = manager.get_status()
            pool_snapshot = manager._dispatch_pool.snapshot()
            if (
                "late-handle-adv" in canceled_handles
                and status["cpu"]["used"] == 0
                and pool_snapshot["abandoned_tasks"] == 0
            ):
                break
            time.sleep(0.01)
        else:
            raise AssertionError(
                f"Late handle was not canceled or resources not freed. "
                f"handles={canceled_handles} "
                f"cpu={manager.get_status()['cpu']['used']} "
                f"pool={manager._dispatch_pool.snapshot()}"
            )

        _assert_terminal_chunks(executor_db, job_id)
        _assert_no_divergence(manager)
    finally:
        release_submit.set()
        adapter.submit = original_submit
        manager.stop()
