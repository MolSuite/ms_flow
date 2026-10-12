"""The machines and the executors kept by name in the settings, and what the workspace makes of them."""
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


def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("MOLSUITE_HOME", str(tmp_path / "home"))


def test_hosts_and_executors_are_kept_by_name(tmp_path, monkeypatch):
    from ms_flow.core.settings.manager import SettingsManager
    from ms_flow.core.settings.models import share

    # 0 is all it has, a negative number leaves that many free, a positive one is how many
    assert [share(asked, 16, 1) for asked in (0, -2, 8, 32, -20)] == [16, 14, 8, 16, 1]

    _home(tmp_path, monkeypatch)
    kept = SettingsManager()
    assert list(kept.settings.hosts) == list(kept.settings.executors) == ["local"]  # this machine, always
    kept.save_host("laptop", address="192.168.1.2", cpus=-2, tools={"vina": "/opt/vina"})
    kept.save_host("pc1", address="192.168.1.3")
    kept.save_host("local", cpus=-2)
    kept.save_executor("lab", kind="ray", hosts=["local", "pc1"])
    kept.save_executor("hpc", kind="queue", hosts=["laptop"], max_jobs=20)

    again = SettingsManager().settings  # as another session reads them
    assert again.hosts["laptop"].tools == {"vina": "/opt/vina"} and again.hosts["local"].cpus == -2
    assert again.executors["lab"].hosts == ["local", "pc1"] and again.executors["hpc"].max_jobs == 20
    assert again.executors["local"].kind == "loky"

    for wrong, why in (
        (lambda: kept.save_host("pc2", adress="192.168.1.4"), "has no adress"),
        (lambda: kept.save_host("pc.2", address="192.168.1.4"), "not a name"),
        (lambda: kept.save_host("pc2"), "has no address"),
        (lambda: kept.save_host("pc2", address="192.168.1.3"), "address of another host"),
        (lambda: kept.save_host("local", address="192.168.1.9"), "this machine"),
        (lambda: kept.save_executor("far", kind="ray", hosts=["nobody"]), "not kept: nobody"),
        (lambda: kept.save_executor("far", kind="queue", hosts=["laptop", "pc1"]), "^`executors.far` takes one host"),
        (lambda: kept.save_executor("far", kind="queue", hosts=["laptop"], scheduler="pbs"), "^executors.far.scheduler: Input"),
        (lambda: kept.save_executor("pool", kind="loky"), "the only one"),
        (lambda: kept.save_executor("local", kind="ray"), "the only one"),
        (lambda: kept.remove_host("pc1"), "a host of lab"),
        (lambda: kept.remove_host("local"), "not removed"),
        (lambda: kept.remove_executor("local"), "not removed"),
        (lambda: kept.remove_executor("nobody"), "no `executors.nobody`"),
    ):
        with pytest.raises(ValueError, match=why):
            wrong()
    assert SettingsManager().settings == again  # nothing of that was kept

    kept.remove_executor("lab")
    kept.remove_host("pc1")
    assert sorted(SettingsManager().settings.hosts) == ["laptop", "local"]


@pytest.mark.slow
def test_the_workspace_runs_on_the_executors_the_settings_describe(tmp_path, monkeypatch):
    from ms_flow._api_impl import job_api
    from ms_flow.api import Workspace
    from ms_flow.remote import RemoteExecutor

    _home(tmp_path, monkeypatch)
    monkeypatch.setattr(job_api, "_login_node", lambda: True)
    ms = Workspace(app_id="role")
    settings = ms.settings_manager
    try:
        ms.create_or_open_project(name="one", folder=tmp_path / "one", description="", activate=True)
        with pytest.raises(RuntimeError, match="login node"):  # no pool there, and the threads are still here
            ms.submit(_numbers)
        assert isinstance(ms.engine.executor("thread"), ThreadExecutor)

        # The queue of this machine, with the environment this test runs in.
        settings.save_host("local", env=sys.prefix, workdir=str(tmp_path / "far"))
        settings.save_executor("far", kind="queue", scheduler="none", poll_seconds=0.2,
                               setup=f"export PYTHONPATH={_ROOT}:{_ROOT / 'src'}")
        ms.engine.register("far", job_api.build_executor("far", settings.settings))
        assert isinstance(ms.engine.executor("far"), RemoteExecutor)
        done = ms.engine.wait(ms.engine.submit("far", workers.row, [{"idx": i} for i in range(3)]), 120)
        assert (done.status, done.chunks_done) == ("completed", 3)
        assert _until(lambda: ms.remote_jobs("far") == [])  # asked of that machine: what ended well leaves nothing
        assert ms.remote_jobs("local") == []  # nothing is anywhere else

        settings.save_host("local", cpus=2)
        settings.save_executor("local", kind="loky", on_login_node=True)  # said in so many words
        ms.reload_executors()
        assert isinstance(ms.engine.executor("local"), ProcessExecutor) and ms.engine.executor("local").capacity == 2
        done = ms.engine.wait(ms.submit(_numbers), 120)
        assert (done.status, done.chunks_done) == ("completed", 3)
    finally:
        ms.shutdown()


def test_an_executor_is_built_from_its_hosts(monkeypatch):
    """Whose address, whose environment and how much of this machine: said by the hosts it names."""
    from ms_flow import engine, ray_hosts, remote
    from ms_flow._api_impl import job_api
    from ms_flow.core.settings.models import Settings

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
    monkeypatch.setattr(ray_hosts, "RayOverHosts", lambda hosts, env, **init: (hosts, env, init))
    monkeypatch.setattr(engine, "RayExecutor", lambda address, **init: (address, init))
    monkeypatch.setattr(job_api, "_here", lambda local: (local.cpus, 1, None))
    settings = Settings(
        hosts={"local": {"cpus": 6}, "hpc": {"address": "login", "workdir": "scratch/jobs"},
               "mine": {"address": "login2", "env": "/mine"}, "pc2": {"address": "pc2"}, "pc3": {"address": "pc3"}},
        executors={"slurm": {"kind": "queue", "hosts": ["hpc"]}, "slurm2": {"kind": "queue", "hosts": ["mine"]},
                   "alone": {"kind": "ray"}, "lab": {"kind": "ray", "hosts": ["local", "pc2", "pc3"]},
                   "others": {"kind": "ray", "hosts": ["pc2"]}})

    def built(name, host_env=host_env):
        return job_api.build_executor(name, settings, host_env)

    assert (built("slurm").host, built("slurm").said["workdir"], built("slurm").said["env"]) == ("login", "scratch/jobs", "/envs/login")
    assert built("slurm", None).said["env"] == ""  # no app behind: the `python` of that account
    assert built("slurm2").said["env"] == "/mine"
    assert built("alone") == (None, {"num_cpus": 6, "num_gpus": 1})  # a Ray of this machine alone
    assert built("lab") == (["pc2", "pc3"], ["/envs/pc2", "/envs/pc3"], {"num_cpus": 6, "num_gpus": 1})
    assert built("others") == (["pc2"], ["/envs/pc2"], {"num_cpus": 0, "num_gpus": 0})  # the head here runs nothing
    with pytest.raises(ValueError, match="nothing prepares it"):
        built("lab", None)
    assert asked == [("login", "1 of its jobs are in the queue there")] * 3 + [("pc2", None), ("pc3", None), ("pc2", None)]
