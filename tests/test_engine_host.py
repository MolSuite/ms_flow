"""The engine in a process of its own (`Workspace(engine_host="process")`)."""
import os
import signal
import threading
import time

import pytest

from ms_flow.engine import job, task
from ms_flow.sinks import file_sink


@task()
def where(element, params):
    time.sleep(params.get("nap", 0))
    return f"{element} {os.getpid()}\n"


@job(task=where, output=file_sink("out/where.txt", fmt="text", append=True))
def numbers(params, project):
    yield from range(params.get("n", 3))


def _workspace(tmp_path, monkeypatch):
    from ms_flow.main import Workspace

    monkeypatch.setenv("MOLSUITE_HOME", str(tmp_path / "home"))  # the child reads the environment
    ms = Workspace(app_id="hosted", engine_host="process")
    ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
    return ms


def _rows(tmp_path):
    return [line.split() for line in (tmp_path / "one" / "out" / "where.txt").read_text().splitlines()]


def test_the_engine_runs_in_a_child_and_calls_back(tmp_path, monkeypatch):
    ms = _workspace(tmp_path, monkeypatch)
    try:
        ended = threading.Event()
        seen = []
        job_id = ms.submit(numbers, {"n": 3}, executor="thread",
                           on_terminal=lambda snap: (seen.append(snap.status), ended.set()))
        assert ms.engine.wait(job_id, 60).status == "completed"
        assert ended.wait(10) and seen == ["completed"]
        child = ms.engine.process.pid
        assert child != os.getpid()
        assert sorted(int(n) for n, _ in _rows(tmp_path)) == [0, 1, 2]
        # what the app polls: the journal here, the live jobs there
        assert [job.status for job in ms.jobs()] == ["completed"]
        assert ms.get_project_activity()["jobs_active"] == 0
        assert ms.executors()[0]["state"] == "on"
    finally:
        ms.shutdown()
    assert not _alive(child)  # nothing is left behind


def test_a_child_that_dies_leaves_the_app_standing(tmp_path, monkeypatch):
    from ms_flow._api_impl.engine_host import EngineGone

    ms = _workspace(tmp_path, monkeypatch)
    try:
        job_id = ms.submit(numbers, {"n": 400, "nap": 0.2}, executor="thread")
        key = ms.engine.status(job_id).key
        first = ms.engine.process.pid
        threading.Timer(0.5, os.kill, (first, signal.SIGKILL)).start()
        with pytest.raises(EngineGone):
            ms.engine.wait(job_id, 60)
        assert _until(lambda: ms._engine is None)
        assert ms.jobs()[0].status == "running"  # it died young: none starts until the next use
        # the next use starts another engine, and the job continues where it was
        assert ms.engine.wait(ms.resume(key), 60).status == "completed"
        assert ms.engine.process.pid != first
        assert sorted({int(n) for n, _ in _rows(tmp_path)}) == list(range(400))
    finally:
        ms.shutdown()


def test_a_child_that_dies_is_replaced_and_its_jobs_read_interrupted(tmp_path, monkeypatch):
    from ms_flow.main import Workspace

    monkeypatch.setattr(Workspace, "_restart_after", 0)
    ms = _workspace(tmp_path, monkeypatch)
    try:
        job_id = ms.submit(numbers, {"n": 400, "nap": 0.2}, executor="thread")
        key = ms.engine.status(job_id).key
        first = ms.engine.process.pid
        from ms_flow.engine.journal import recorded

        assert _until(lambda: list(recorded(tmp_path / "one" / "executor.db", None)))  # on record
        os.kill(first, signal.SIGKILL)
        # with no use of the engine, another starts and the app reads the job as interrupted
        assert _until(lambda: [job.status for job in ms.jobs()] == ["interrupted"], 30)
        assert ms._engine is not None and ms._engine.process.pid != first
        assert ms.engine.wait(ms.resume(key), 60).status == "completed"
        assert sorted({int(n) for n, _ in _rows(tmp_path)}) == list(range(400))
    finally:
        ms.shutdown()


def test_the_engines_process_starts_when_a_project_opens(tmp_path, monkeypatch):
    ms = _workspace(tmp_path, monkeypatch)  # no job submitted
    try:
        assert _until(lambda: ms._engine is not None and ms._flow)  # the project is open there
        child = ms._engine.process.pid
        assert ms.engine.process.pid == child  # the first use takes it, it does not start another
        ms.create_or_open_project(name="two", folder=tmp_path / "two", description="", activate=True)
        assert ms.engine.wait(ms.submit(numbers, {}, executor="thread"), 60).status == "completed"
        assert ms.engine.process.pid == child
        assert (tmp_path / "two" / "out" / "where.txt").exists()
    finally:
        ms.shutdown()
    assert not _alive(child)


def test_a_shutdown_while_the_engines_process_starts_leaves_none_behind(tmp_path, monkeypatch):
    ms = _workspace(tmp_path, monkeypatch)
    ms.shutdown()  # right after the project opened: its process may still be starting
    assert ms._engine is None


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _until(check, seconds: float = 10) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if check():
            return True
        time.sleep(0.05)
    return check()


def test_an_app_that_exits_without_shutdown_closes_its_engine(tmp_path):
    import subprocess
    import sys

    script = f"""
from ms_flow.main import Workspace
ms = Workspace(app_id="hosted", engine_host="process")
ms.create_or_open_project(name="one", folder={str(tmp_path / "one")!r}, description="", activate=True)
print(ms.engine.process.pid, flush=True)
"""  # no `shutdown`: the interpreter's exit closes the engine
    env = {**os.environ, "MOLSUITE_HOME": str(tmp_path / "home")}
    done = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr
    assert "process ended" not in done.stdout + done.stderr  # closed, not taken for dead
    assert _until(lambda: not _alive(int(done.stdout.split()[-1])))


def test_a_child_that_stops_answering_is_killed_and_the_call_raises(tmp_path, monkeypatch):
    from ms_flow._api_impl import engine_host
    from ms_flow._api_impl.engine_host import EngineGone

    monkeypatch.setattr(engine_host, "PING_EVERY", 0.2)
    monkeypatch.setattr(engine_host, "HANG_AFTER", 1.0)
    monkeypatch.setenv("MSFLOW_SLOW_MS", "100000")  # only the kill is that slow: it logs anyway
    ms = _workspace(tmp_path, monkeypatch)
    try:
        child = ms.engine.process
        os.kill(child.pid, signal.SIGSTOP)  # alive, and silent
        started = time.monotonic()
        with pytest.raises(EngineGone):
            ms.engine.wait(-1, 30)
        assert time.monotonic() - started < 10
        assert _until(lambda: not child.is_alive())  # killed (a zombie of ours until joined)
        assert "engine process silent, killed, during wait" in (tmp_path / "home" / "logs" / "slow.log").read_text()
    finally:
        ms.shutdown()


def test_a_long_call_to_a_busy_child_that_answers_is_not_killed(tmp_path, monkeypatch):
    from ms_flow._api_impl import engine_host

    monkeypatch.setattr(engine_host, "PING_EVERY", 0.2)
    monkeypatch.setattr(engine_host, "HANG_AFTER", 1.0)
    ms = _workspace(tmp_path, monkeypatch)
    try:
        job_id = ms.submit(numbers, {"n": 2, "nap": 2.5}, executor="thread")  # longer than HANG_AFTER
        assert ms.engine.wait(job_id, 60).status == "completed"
    finally:
        ms.shutdown()


def test_slow_steps_are_logged_when_asked(tmp_path, monkeypatch):
    monkeypatch.setenv("MSFLOW_SLOW_MS", "0")  # everything is that slow
    ms = _workspace(tmp_path, monkeypatch)
    try:
        assert ms.engine.wait(ms.submit(numbers, {"n": 2}, executor="thread"), 60).status == "completed"
    finally:
        ms.shutdown()
    log = (tmp_path / "home" / "logs" / "slow.log").read_text()
    assert "engine process start" in log and "engine call submit" in log
    assert "writer" in log and "of numbers" in log  # written by the engine's process
    assert "engine call wait" not in log


def test_the_engine_package_stays_light_for_workers():
    import subprocess
    import sys

    code = "import sys, ms_flow.engine.flow; assert 'sqlalchemy' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)
