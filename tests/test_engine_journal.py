"""The engine's journal: what reaches the disk, what a finished job leaves there, who owns the file."""
import os
import sqlite3
import time
from contextlib import closing

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Busy, Engine, ThreadExecutor, content_key


def _boom_on_3(payload):
    if payload == 3:
        raise ValueError("three")
    return payload * 2


def _rows(path, sql):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(sql).fetchall()


def _engine(tmp_path):
    engine = Engine(cpus=4, mem_mb=0)
    engine.register("thread", ThreadExecutor(4))
    engine.attach("project", tmp_path / "executor.db")
    return engine


def test_a_finished_job_leaves_one_row_and_its_failures(tmp_path):
    db = tmp_path / "executor.db"
    with _engine(tmp_path) as engine:
        snap = engine.wait(engine.submit("thread", _boom_on_3, range(5000), name="dock", journal="project"), 30)
        assert snap.status == "completed_with_errors"
        # already on disk when wait() returns
        assert _rows(db, "SELECT name, status, total, done, failed FROM jobs") == \
            [("dock", "completed_with_errors", 5000, 4999, 1)]
        (key, status, error), = _rows(db, "SELECT key, status, error FROM chunks")
        assert key == content_key(3) and status == 2 and "ValueError: three" in error
        engine.wait(engine.submit("thread", _boom_on_3, range(10)), 30)  # no journal: no trace
        assert _rows(db, "SELECT count(*) FROM jobs") == [(1,)]
    assert sum(f.stat().st_size for f in tmp_path.glob("executor.db*")) < 32_000  # 5000 chunks went through


def test_detach_interrupts_and_keeps_what_was_done(tmp_path):
    db = tmp_path / "executor.db"
    with _engine(tmp_path) as engine:
        job = engine.submit("thread", workers.nap, [{"s": 0.01, "i": i} for i in range(2000)], journal="project")
        while engine.status(job).chunks_done < 20:
            time.sleep(0.01)
        engine.detach("project")
        snap = engine.status(job)
        assert snap.status == "interrupted"
        assert _rows(db, "SELECT status, done FROM jobs") == [("interrupted", snap.chunks_done)]
        assert _rows(db, "SELECT count(*), min(status), max(status) FROM chunks") == [(snap.chunks_done, 1, 1)]
        with pytest.raises(ValueError):
            engine.submit("thread", _boom_on_3, [1], journal="project")
        engine.attach("project", db)  # the lock went with the detach
        again = engine.wait(engine.submit("thread", _boom_on_3, [1], journal="project"), 30)
        assert again.status == "completed" and _rows(db, "SELECT n, status FROM jobs ORDER BY n")[1] == (2, "completed")


def test_one_owner_per_journal_and_one_engine_per_state_dir(tmp_path):
    with _engine(tmp_path) as engine, Engine(cpus=1, mem_mb=0, state_dir=tmp_path) as other:
        with pytest.raises(Busy, match=f"pid {os.getpid()}"):
            other.attach("project", tmp_path / "executor.db")
        with pytest.raises(Busy, match=f"pid {os.getpid()}"):
            Engine(cpus=1, mem_mb=0, state_dir=tmp_path)
        assert engine.wait(engine.submit("thread", _boom_on_3, [1], journal="project"), 30).status == "completed"
    Engine(cpus=1, mem_mb=0, state_dir=tmp_path).close()  # both locks are free again


def test_a_journal_that_cannot_be_written_fails_its_jobs(tmp_path):
    with _engine(tmp_path) as engine:
        job = engine.submit("thread", workers.nap, [{"s": 0.01, "i": i} for i in range(2000)], journal="project")
        other = engine.submit("thread", workers.nap, [{"s": 0.01, "i": i} for i in range(20)])
        engine._journals["project"]._records.append(("not a job",))  # the writer chokes on it
        snap = engine.wait(job, 30)
        assert snap.status == "failed" and "journal could not be written" in snap.error
        assert engine.wait(other, 30).status == "completed"
        with pytest.raises(ValueError):
            engine.submit("thread", _boom_on_3, [1], journal="project")
