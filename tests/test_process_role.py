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
        assert _until(lambda: ms.remote_jobs() == [])  # asked of that machine: what ended well leaves nothing

        ms.run_process_on(on_login_node=True)  # said in so many words, and in one call
        assert isinstance(ms.engine.executor("process"), ProcessExecutor)
        assert ms.remote_jobs() == []  # nothing is anywhere else

        def unprepared(address, busy):
            raise OSError(f"{address} is not reached")

        ms.host_env = unprepared
        with pytest.raises(OSError, match="pc2 is not reached"):
            ms.run_process_on("ray", hosts=["pc2"])
        assert ms.settings_manager.settings.resources.process.kind == "process"  # as it was, and in place
        assert isinstance(ms.engine.executor("process"), ProcessExecutor)
        with pytest.raises(ValueError, match="scheduler"):
            ms.run_process_on("queue", scheduler="none of them")
        assert isinstance(ms.engine.executor("process"), ProcessExecutor)
    finally:
        ms.shutdown()


def test_a_host_with_no_env_named_gets_it_from_the_app_before_anything_runs(monkeypatch):
    from ms_flow import ray_hosts, remote
    from ms_flow._api_impl.job_api import _process_executor
    from ms_flow.core.settings.models import ResourcesConfig

    class Far:
        def __init__(self, host=None, **said):
            self.host, self.said = host, said

        def jobs_there(self):
            return [{"in_queue": True}, {"in_queue": False}]

        def shutdown(self):
            pass

    asked = []

    def host_env(address, busy):
        asked.append((address, busy() if busy else None))
        return f"/envs/{address}"

    monkeypatch.setattr(remote, "RemoteExecutor", Far)
    monkeypatch.setattr(ray_hosts, "RayOverHosts", lambda hosts, env: (hosts, env))
    queue = ResourcesConfig(process={"kind": "queue", "queue": {"host": "hpc"}})
    assert _process_executor(queue, host_env).said["env"] == "/envs/hpc"
    assert _process_executor(queue).said["env"] == ""  # no app behind: the `python` of that account
    named = ResourcesConfig(process={"kind": "queue", "queue": {"host": "hpc", "env": "/mine"}})
    assert _process_executor(named, host_env).said["env"] == "/mine"
    ray = ResourcesConfig(process={"kind": "ray", "ray": {"hosts": ["pc2", "pc3"]}})
    assert _process_executor(ray, host_env) == (["pc2", "pc3"], ["/envs/pc2", "/envs/pc3"])
    with pytest.raises(ValueError, match="nothing prepares the hosts"):
        _process_executor(ray)
    assert asked == [("hpc", "1 of its jobs are in the queue there"), ("pc2", None), ("pc3", None)]
