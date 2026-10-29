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


def test_an_interrupted_job_continues_where_it_stopped(tmp_path):
    db, payloads = tmp_path / "executor.db", [{"s": 0.005, "i": i} for i in range(400)]
    with _engine(tmp_path) as engine:
        job = engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1")
        with pytest.raises(ValueError, match="already running"):
            engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1")
        while engine.status(job).chunks_done < 30:
            time.sleep(0.01)
    before = engine.status(job)  # closing the engine interrupted it
    assert before.status == "interrupted" and before.key == "dock-1" and 30 <= before.chunks_done < 400

    ran = []
    with _engine(tmp_path) as engine:
        (row,) = engine.recorded("project")
        assert (row["job_id"], row["status"], row["done"]) == ("dock-1", "interrupted", before.chunks_done)
        more = payloads + [{"s": 0.005, "i": i} for i in range(400, 450)]  # the source grew meanwhile
        job = engine.submit("thread", workers.nap, more, journal="project", key="dock-1",
                            on_result=lambda key, result: ran.append(key))
        after = engine.wait(job, 60)
        assert (after.status, after.chunks_done, after.chunks_total) == ("completed", 450, 450)
        assert len(ran) == 450 - before.chunks_done  # only what was missing
        assert _rows(db, "SELECT n, status, done FROM jobs") == [(1, "completed", 450)]
        with pytest.raises(ValueError, match="already ended as completed"):
            engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1")

    with closing(sqlite3.connect(db)) as raw:  # what a killed process leaves behind
        raw.execute("UPDATE jobs SET status = 'running', finished_at = NULL")
        raw.commit()
    with _engine(tmp_path) as engine:
        assert engine.recorded("project")[0]["status"] == "interrupted"


def test_a_canceled_job_continues_or_is_discarded(tmp_path):
    db, payloads = tmp_path / "executor.db", [{"s": 0.005, "i": i} for i in range(400)]
    with _engine(tmp_path) as engine:
        job = engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1")
        with pytest.raises(ValueError, match="is running"):
            engine.discard("project", "dock-1")
        while engine.status(job).chunks_done < 30:
            time.sleep(0.01)
        engine.cancel(job)
        before = engine.wait(job, 30)
        assert before.status == "canceled" and 30 <= before.chunks_done < 400
        assert _rows(db, "SELECT count(*) FROM chunks") == [(before.chunks_done,)]  # kept: it may continue

        ran = []
        job = engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1",
                            on_result=lambda key, result: ran.append(key))
        after = engine.wait(job, 60)
        assert (after.status, after.chunks_done) == ("completed", 400)
        assert len(ran) == 400 - before.chunks_done  # only what was missing

        job = engine.submit("thread", workers.nap, payloads, journal="project", key="dock-2")
        while engine.status(job).chunks_done < 30:
            time.sleep(0.01)
        engine.cancel(job)
        engine.wait(job, 30)
        engine.discard("project", "dock-2")
        assert _rows(db, "SELECT job_id FROM jobs") == [("dock-1",)]
        assert _rows(db, "SELECT count(*) FROM chunks") == [(0,)]
        job = engine.submit("thread", workers.nap, payloads[:20], journal="project", key="dock-2")  # free again
        assert engine.wait(job, 30).chunks_done == 20


def test_a_continued_job_counts_what_it_had_done_when_the_source_leaves_it_out(tmp_path):
    payloads, done = [{"s": 0.005, "i": i} for i in range(200)], set()
    with _engine(tmp_path) as engine:
        job = engine.submit("thread", workers.nap, payloads, journal="project", key="dock-1",
                            on_result=lambda key, result: done.add(key))
        while engine.status(job).chunks_done < 30:
            time.sleep(0.01)
        engine.cancel(job)
        before = engine.wait(job, 30)

        left = [p for p in payloads if content_key(p) not in done]  # the caller leaves out what is done
        after = engine.wait(engine.submit("thread", workers.nap, left, journal="project", key="dock-1"), 60)
        assert before.chunks_done == len(done) and len(left) == 200 - len(done)
        assert (after.status, after.chunks_done, after.chunks_total) == ("completed", 200, 200)


@pytest.mark.parametrize("stop", ["cancel", "detach", "close"])
def test_a_result_taken_after_the_job_stopped_is_not_run_again(tmp_path, stop):
    import threading

    taken, first, go = [], threading.Event(), threading.Event()

    def take(pairs):
        first.set()
        go.wait(30)  # the first group holds the delivery thread: the rest of the results queue behind it
        taken.extend(key for key, _ in pairs)

    def submit(engine):
        return engine.submit("thread", lambda payload: payload["i"], [{"_key": str(i), "i": i} for i in range(40)],
                             journal="project", key="job", on_results=take)

    engine = _engine(tmp_path)
    job_id = submit(engine)
    assert first.wait(30)
    time.sleep(0.2)  # every chunk has run by now, and waits to be taken
    stopping = threading.Thread(target={"cancel": lambda: engine.cancel(job_id),
                                        "detach": lambda: engine.detach("project"), "close": engine.close}[stop])
    stopping.start()
    while engine.status(job_id).status == "running":
        time.sleep(0.005)
    go.set()
    stopping.join(30)
    if stop == "close":
        engine = _engine(tmp_path)
    elif stop == "detach":
        engine.attach("project", tmp_path / "executor.db")
    else:
        engine.wait(job_id, 30)
    with engine:
        assert engine.wait(submit(engine), 30).status == "completed"
    assert sorted(taken, key=int) == [str(i) for i in range(40)]  # each one once, over the two runs


def test_what_a_job_used_stays_once_it_ended(tmp_path):
    import json

    from ms_flow.engine.journal import recorded

    with _engine(tmp_path) as engine:
        snap = engine.wait(engine.submit("thread", workers.nap, [{"s": 0.05, "i": i} for i in range(8)],
                                         cpu=2, name="nap", journal="project"), 30)
    usage = json.loads(recorded(tmp_path / "executor.db")[0]["usage"])
    assert usage == snap.usage
    assert usage["peak_cpu"] >= 2 and usage["cpu_s"] >= 2 * 8 * 0.05 * 0.9
    assert usage["peak_gpu"] == usage["gpu_s"] == 0 and usage["vram_mb"] is None


def test_a_version_3_journal_gets_the_usage_column(tmp_path):
    from ms_flow.engine.journal import recorded

    db = tmp_path / "executor.db"
    with closing(sqlite3.connect(db)) as old:
        old.executescript("""
            CREATE TABLE jobs (n INTEGER PRIMARY KEY, job_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
                status TEXT NOT NULL, spec TEXT NOT NULL, total INTEGER, done INTEGER NOT NULL,
                failed INTEGER NOT NULL, error TEXT, started_at REAL, finished_at REAL, source TEXT);
            CREATE TABLE chunks (job INTEGER NOT NULL, key BLOB NOT NULL, status INTEGER NOT NULL,
                attempt INTEGER NOT NULL, error TEXT, input TEXT, PRIMARY KEY (job, key)) WITHOUT ROWID;
            INSERT INTO jobs VALUES (1, 'old', 'old', 'completed', '{}', 1, 1, 0, NULL, 1.0, 2.0, NULL);
            PRAGMA user_version = 3;""")
    assert [row["job_id"] for row in recorded(db)] == ["old"]  # read as it is, before anyone owns it
    with _engine(tmp_path) as engine:
        engine.wait(engine.submit("thread", _boom_on_3, [1], journal="project"), 30)
    old, new = recorded(db)
    assert old["usage"] is None and new["usage"] is not None


def test_a_job_on_this_machines_gpus_keeps_what_the_cards_did(tmp_path, monkeypatch):
    from ms_flow.engine import engine as engine_module

    bin_ = tmp_path / "bin"
    bin_.mkdir()
    (bin_ / "nvidia-smi").write_text("#!/bin/sh\necho '40, 900'\necho '60, 100'\n")
    (bin_ / "nvidia-smi").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setattr(engine_module, "_GPU_EVERY", 0.02)
    with Engine(cpus=4, gpus=1, mem_mb=0) as engine:
        engine.register("gpu", ThreadExecutor(4, uses_tokens=True))
        snap = engine.wait(engine.submit("gpu", workers.nap, [{"s": 0.2, "i": i} for i in range(3)], gpu=1), 30)
    assert snap.usage["peak_gpu"] == 1 and snap.usage["gpu_s"] >= 0.5
    assert snap.usage["vram_mb"] == 1000 and snap.usage["gpu_util"] == 50.0
