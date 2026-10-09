"""Who plays `process`: one executor, changed between jobs, and never the pool on a login node."""
import sys
import time
from pathlib import Path

import pytest

from benchmarks.refactor_optimization import workers
from ms_flow.engine import Engine, ProcessExecutor, ThreadExecutor, job, task

_ROOT = Path(__file__).resolve().parents[1]


class _Held:
    """An executor that runs nothing: the test says when a chunk ended."""

    capacity, uses_tokens = 1, False

    def __init__(self):
        self.tasks, self.down = [], False

    def bind(self, on_done):
        self.on_done = on_done

    def submit(self, fn, payload, tag):
        self.tasks.append(tag)

    def cancel(self, handle):
        pass

    def shutdown(self):
        self.down = True


def _until(check, seconds=10):
    deadline = time.monotonic() + seconds
    while not check() and time.monotonic() < deadline:
        time.sleep(0.01)
    return check()


def test_an_executor_gives_its_name_to_another_only_between_jobs():
    old, new = _Held(), _Held()
    with Engine(cpus=1) as engine:
        engine.register("process", old)
        running = engine.submit("process", len, ["a"])
        assert _until(lambda: old.tasks)
        with pytest.raises(ValueError, match="have not ended"):
            engine.register("process", new)
        assert engine.executor("process") is old and not old.down
        old.on_done(old.tasks[0], "done", 1, None, 0)
        assert engine.wait(running, 10).status == "completed"
        engine.register("process", new)
        assert engine.executor("process") is new and old.down and not new.down


@task()
def _one(n: int) -> dict:
    return {"n": n}


@job(task=_one)
def _numbers(params, project):
    return range(3)


@pytest.mark.slow
def test_the_workspace_puts_in_process_whoever_the_settings_name(tmp_path, monkeypatch):
    from ms_flow._api_impl import job_api
    from ms_flow.api import Workspace
    from ms_flow.remote import RemoteExecutor

    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: tmp_path)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: tmp_path)
    monkeypatch.setattr(job_api, "_login_node", lambda: True)

    def choose(**values):
        for path, value in values.items():
            ms.settings_manager.set_global_value("resources.process." + path.replace("__", "."), value)
        ms.reload_executors()

    ms = Workspace(app_id="role")
    try:
        ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
        with pytest.raises(RuntimeError, match="login node"):  # no pool there, and the threads are still here
            ms.submit(_numbers)
        assert isinstance(ms.engine.executor("thread"), ThreadExecutor)

        choose(kind="queue", queue__scheduler="none", queue__workdir=str(tmp_path / "far"), queue__python=sys.executable,
               queue__setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}", queue__poll_seconds=0.2)
        assert isinstance(ms.engine.executor("process"), RemoteExecutor)
        done = ms.engine.wait(ms.engine.submit("process", workers.row, [{"idx": i} for i in range(3)]), 120)
        assert (done.status, done.chunks_done) == ("completed", 3)

        choose(kind="process", on_login_node=True)  # said in so many words
        assert isinstance(ms.engine.executor("process"), ProcessExecutor)
    finally:
        ms.shutdown()
