"""Retention in executor.db: there is one chunk row per unit of work, and they were never deleted.

What deserves a test is the boundary of the pruning: only finished jobs, only older than the
cutoff, and the job row — the history the user sees — stays.
"""
from datetime import datetime, timedelta

from sqlmodel import select

from ms_flow.core.database.executor import ExecutorStore
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk, ExecutorJobEvent


def _store(tmp_path, jobs) -> ExecutorStore:
    store = ExecutorStore(tmp_path / "executor.db")
    old = datetime.now() - timedelta(days=90)
    with store.get_session() as session:
        for job_id, status, stale in jobs:
            when = old if stale else datetime.now()
            session.add(ExecutorJob(job_id=job_id, status=status, updated_at=when))
            session.add(ExecutorJobChunk(job_id=job_id, chunk_id=f"{job_id}-c", updated_at=when))
            session.add(ExecutorJobEvent(job_id=job_id, event_type="log", message="x"))
        session.commit()
    return store


def test_purge_drops_chunks_and_events_of_old_finished_jobs_only(tmp_path):
    store = _store(tmp_path, [
        ("old-done", "completed", True),
        ("old-running", "running", True),
        ("fresh-done", "completed", False),
    ])

    assert store.purge_finished_jobs(older_than_days=30) == {"chunks": 1, "events": 1}

    with store.get_session() as session:
        assert sorted(r.job_id for r in session.exec(select(ExecutorJobChunk)).all()) == [
            "fresh-done", "old-running",
        ]
        assert sorted(r.job_id for r in session.exec(select(ExecutorJobEvent)).all()) == [
            "fresh-done", "old-running",
        ]
        assert len(session.exec(select(ExecutorJob)).all()) == 3  # the history is left untouched


def test_zero_retention_disables_the_purge(tmp_path):
    store = _store(tmp_path, [("old-done", "completed", True)])
    assert store.purge_finished_jobs(older_than_days=0) == {"chunks": 0, "events": 0}
    with store.get_session() as session:
        assert len(session.exec(select(ExecutorJobChunk)).all()) == 1
