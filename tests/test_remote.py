"""`RemoteExecutor` against this machine: the same commands, run by `sh` here instead of over ssh."""
import os
import signal
import sys
import time
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Engine
from ms_flow.remote import RemoteExecutor

pytestmark = pytest.mark.slow

_ROOT = Path(__file__).resolve().parents[1]


def _far(tmp_path, **options):
    return RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable,
                          setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}", poll_s=0.2, part_seconds=0.1, **options)


def _inside(folder) -> list:
    """The processes whose directory is `folder`: a job of the queue and its workers."""
    found = []
    for entry in Path("/proc").iterdir():
        try:
            if entry.name.isdigit() and os.readlink(entry / "cwd").startswith(str(folder)):
                found.append(int(entry.name))
        except OSError:
            pass
    return found


def _until(check, seconds=60):
    deadline = time.monotonic() + seconds
    while not check() and time.monotonic() < deadline:
        time.sleep(0.05)
    return check()


def test_a_job_is_run_as_jobs_of_the_other_machine(tmp_path):
    got, errors = [], []
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, elements_per_job=10, runner="--cpus 2"))
        job = engine.submit("far", workers.die_on_7, [{"idx": i} for i in range(25)], on_results=got.extend,
                            on_error=lambda key, error: errors.append(error))
        ended = engine.wait(job, 120)
    assert (ended.status, ended.chunks_done, ended.chunks_failed) == ("completed_with_errors", 24, 1)
    assert len(got) == 24 and all(result == {"ok": 1} for key, result in got)
    assert len(errors) == 1 and "worker process died" in errors[0]
    assert list((tmp_path / "far").iterdir()) == []  # what ended well leaves nothing there


def test_results_come_while_it_runs_and_a_job_out_of_the_queue_is_sent_again(tmp_path):
    got = []
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, runner="--cpus 1"))
        job = engine.submit("far", workers.nap, [{"s": 0.3, "n": i} for i in range(12)], on_results=got.extend)
        assert _until(lambda: engine.status(job).chunks_done)
        assert engine.status(job).status == "running" and engine.status(job).chunks_done < 12
        (home,) = (tmp_path / "far").iterdir()
        for pid in _inside(home):  # what a queue does to a job whose time ran out
            os.kill(pid, signal.SIGTERM)
        ended = engine.wait(job, 120)
    assert (ended.status, ended.chunks_done, len(got)) == ("completed", 12, 12)


def test_a_canceled_job_leaves_the_queue_and_nothing_behind(tmp_path):
    with Engine(cpus=2) as engine:
        engine.register("far", _far(tmp_path, runner="--cpus 1"))
        job = engine.submit("far", workers.nap, [{"s": 0.3, "n": i} for i in range(40)])
        assert _until(lambda: engine.status(job).chunks_done)
        (home,) = (tmp_path / "far").iterdir()
        assert _inside(home)
        engine.cancel(job)
        assert engine.wait(job, 60).status == "canceled"
        assert _until(lambda: not _inside(home) and not home.exists())


def test_a_job_that_cannot_run_there_fails_with_what_it_said(tmp_path):
    with Engine(cpus=2) as engine:
        engine.register("far", RemoteExecutor(workdir=str(tmp_path / "far"), python=sys.executable, poll_s=0.2,
                                              setup="export PYTHONPATH=/nowhere"))
        ended = engine.wait(engine.submit("far", workers.nap, [{"s": 0, "n": i} for i in range(3)]), 60)
    assert ended.status in ("failed", "completed_with_errors") and ended.chunks_done == 0
    assert len(list((tmp_path / "far").iterdir())) == 1  # kept, to look at its runner.log
