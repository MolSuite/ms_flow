from __future__ import annotations

import pytest
from sqlmodel import select

from ms_flow.core.database import (
    ExecutorStore,
    open_executor_store,
)
from ms_flow.core.database.executor_models import ExecutorJob


def _exercise_job_round_trip(store: ExecutorStore) -> None:
    with store.get_session() as session:
        session.add(ExecutorJob(job_id="job-1", status="pending"))
        session.commit()

    with store.get_session() as session:
        job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == "job-1")).one()
        job.status = "completed"
        session.add(job)
        session.commit()

    with store.get_session() as session:
        status = session.exec(
            select(ExecutorJob.status).where(ExecutorJob.job_id == "job-1")
        ).one()
    assert status == "completed"


def test_executor_store_contract(tmp_path):
    store = open_executor_store(tmp_path / "executor.db")
    try:
        assert isinstance(store, ExecutorStore)
        assert store.backend_name == "sqlite"
        _exercise_job_round_trip(store)
    finally:
        store.dispose()


def test_open_operational_store_rejects_postgresql_dsn():
    with pytest.raises(ValueError, match="SQLite only"):
        open_executor_store("postgresql://localhost/molsuite")
