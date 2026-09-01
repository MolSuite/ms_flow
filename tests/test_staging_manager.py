import time
from pathlib import Path

from ms_flow.core.executor.staging_manager import StagingManager


def _call_with_context(fn, payload, context):
    return fn(payload, context)


def _stage_echo(payload: dict, context: dict):
    data = dict(payload)
    data["job_id"] = context["job_id"]
    return data


def _stage_sleep(payload: dict, _context: dict):
    time.sleep(float(payload.get("sleep", 0.1)))
    return dict(payload)


def test_staging_manager_tracks_submit_and_completion():
    manager = StagingManager(total_cpu=4, max_workers=2)
    try:
        manager.submit(
            token="chunk:1",
            kind="chunk",
            job_id="job-1",
            chunk_id="chunk-1",
            call_with_optional_context=_call_with_context,
            fn=_stage_echo,
            payload={"value": 3},
            context={"job_id": "job-1"},
        )
        assert manager.active_count() == 1
        assert manager.capacity() == 2
        deadline = time.time() + 2.0
        completed = []
        while time.time() < deadline:
            completed = manager.pop_completed()
            if completed:
                break
            time.sleep(0.01)
        assert len(completed) == 1
        assert completed[0].future.result()["value"] == 3
        assert manager.active_count() == 0
    finally:
        manager.shutdown()


def test_staging_manager_cancel_job_removes_only_matching_tasks():
    manager = StagingManager(total_cpu=4, max_workers=1)
    try:
        manager.submit(
            token="chunk:a",
            kind="chunk",
            job_id="job-a",
            chunk_id="chunk-a",
            call_with_optional_context=_call_with_context,
            fn=_stage_sleep,
            payload={"sleep": 0.2},
            context={"job_id": "job-a"},
        )
        manager.submit(
            token="chunk:b",
            kind="chunk",
            job_id="job-b",
            chunk_id="chunk-b",
            call_with_optional_context=_call_with_context,
            fn=_stage_sleep,
            payload={"sleep": 0.2},
            context={"job_id": "job-b"},
        )
        canceled = manager.cancel_job("job-b")
        assert len(canceled) == 1
        assert manager.active_count() == 1
    finally:
        manager.shutdown()
