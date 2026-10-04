"""Tool specs: TOML binding, lookup order, env bootstrap script, project-less installs, feeder gate."""
from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable

from ms_flow.core.executor import tools
from ms_flow.core.executor.provisioning import run_step
from ms_flow.main import MolSuite
from ms_flow.tasking import JobSpec

_TOML = """
[tool]
name = "faketool"
label = "Fake tool"
version = "1.0"

[locate]
paths = ["{prefix}/bin/faketool"]
commands = ["faketool"]
check = ["{exe}", "--ok"]

[env]
conda = ["faketool={version}"]
pip = ["extra pkg"]
"""


def _spec(tmp_path: Path, steps: str = "") -> tools.ToolSpec:
    path = tmp_path / "faketool.toml"
    path.write_text(_TOML + steps)
    return tools.ToolSpec.load(path)


def _fake_exe(path: Path, *, works: bool = True) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"#!/bin/sh\nexit {0 if works else 1}\n")
    path.chmod(0o755)
    return path


def test_spec_binds_per_target(tmp_path):
    spec = _spec(tmp_path)
    assert spec.info["label"] == "Fake tool"

    local = spec.recipe(str(tmp_path / "home"))
    assert local.prefix == f"{tmp_path}/home/faketool/1.0"
    assert local.candidates == (f"{local.prefix}/bin/faketool",)
    assert local.check == ("{exe}", "--ok")  # {exe} survives binding

    remote = spec.recipe("/scratch/tools", prefix="/opt/mine/", extra_paths=["/usr/local/bin/faketool"])
    assert remote.candidates == ("/usr/local/bin/faketool", "/opt/mine/bin/faketool")
    steps = tools.recipe_steps(remote, "login.hpc", "mario")
    assert steps[0].argv[0] == "ssh" and "mario@login.hpc" in steps[0].argv
    assert "/scratch/tools/micromamba" in steps[0].argv[-1]  # bootstrap lands under the remote home
    assert steps[1].argv[-1].endswith("'extra pkg'")  # one quoted remote command line
    assert tools.worker_targets(SimpleNamespace(type="hpc", host="login.hpc", user="mario")) == [("login.hpc", "mario")]


def test_locate_order_and_check(tmp_path, monkeypatch):
    recipe = _spec(tmp_path).recipe(str(tmp_path / "home"), extra_paths=[str(tmp_path / "user/faketool")])
    monkeypatch.setenv("PATH", f"{tmp_path / 'onpath'}:/usr/bin:/bin")
    assert tools.locate(recipe) == (tools.MISSING, "")

    on_path = _fake_exe(tmp_path / "onpath/faketool")
    assert tools.locate(recipe) == (tools.READY, str(on_path))
    managed = _fake_exe(Path(recipe.candidates[1]))
    assert tools.locate(recipe) == (tools.READY, str(managed))
    _fake_exe(tmp_path / "user/faketool", works=False)  # registered but broken: skipped
    assert tools.locate(recipe) == (tools.READY, str(managed))
    user = _fake_exe(tmp_path / "user/faketool")
    assert tools.locate(recipe) == (tools.READY, str(user))
    assert tools.tool_status(recipe) == tools.READY


def test_env_step_uses_the_manager_it_finds(tmp_path, monkeypatch):
    log = tmp_path / "calls"
    manager = tmp_path / "fake-conda"
    manager.write_text(f'#!/bin/sh\necho "$@" >> {log}\n')
    manager.chmod(0o755)
    recipe = _spec(tmp_path).recipe(str(tmp_path / "home"), manager=str(manager))
    run_step(tools.recipe_steps(recipe)[0])
    assert log.read_text().split() == [
        "create", "--yes", "--quiet", "--prefix", recipe.prefix, "--channel", "conda-forge", "faketool=1.0",
    ]

    # No hint: the one on PATH wins before any download is attempted.
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:/usr/bin:/bin")
    _fake_exe(tmp_path / "bin/micromamba").write_text(f'#!/bin/sh\necho micromamba "$@" >> {log}\n')
    run_step(tools.recipe_steps(_spec(tmp_path).recipe(str(tmp_path / "home")))[0])
    assert log.read_text().splitlines()[-1].startswith("micromamba create")


def _plain_spec(tmp_path: Path, *, fail: bool = False, slow: bool = False) -> tools.Recipe:
    """No [env]: the install step just drops a working executable at the managed path."""
    script = "{prefix}/bin/faketool"
    steps = '\n[[install]]\nname = "wait"\nargv = ["sleep", "1"]\n' if slow else ""
    if fail:
        steps += '\n[[install]]\nname = "blow up"\nargv = ["sh", "-c", "echo boom detail; exit 3"]\n'
    steps += (
        '\n[[install]]\nname = "install"\n'
        f'argv = ["sh", "-c", "mkdir -p $(dirname {script}) && printf \'#!/bin/sh\\\\nexit 0\\\\n\' > {script} && chmod +x {script}"]\n'
    )
    path = tmp_path / "plain.toml"
    path.write_text(_TOML.replace('[env]\nconda = ["faketool={version}"]\npip = ["extra pkg"]\n', "") + steps)
    return tools.ToolSpec.load(path).recipe(str(tmp_path / f"home-{fail}-{slow}"))


def test_install_needs_no_project(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    recipe = _plain_spec(tmp_path, slow=True)
    key = tools.install_tool(recipe)
    assert tools.install_tool(recipe) == key  # joins the running install
    assert tools.dependency_state(key) == "running"
    tools.get_install(key).future.result(timeout=30)
    assert tools.dependency_state(key) == "completed"
    assert tools.locate(recipe)[0] == tools.READY

    broken = _plain_spec(tmp_path, fail=True)
    install = tools.get_install(tools.install_tool(broken, SimpleNamespace(name="other")))
    assert install.future.exception(timeout=30) is not None
    assert "boom detail" in install.lines
    assert tools.dependency_state(install.key) == "failed"
    assert tools.dependency_state("not-a-tool") is None


class _NeedsTool(JobSpec):
    name = "needs_tool"
    executor = "thread"
    supported_executors = ("thread",)
    store_results = False

    @staticmethod
    def build_chunks(params: dict, config: dict | None = None) -> Iterable[dict]:
        yield {"exe": params["exe"]}

    @staticmethod
    def run_chunk(payload: dict, progress=None):
        assert Path(payload["exe"]).exists(), "the job ran before its tool was installed"
        return {}


def test_jobs_wait_for_the_install(tmp_path, monkeypatch):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: tmp_path)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: tmp_path)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    ms = MolSuite(app_id="testtools")
    try:
        ms.create_or_open_project(
            name="tools_project", folder=tmp_path / "tools_project", description="", scope="testing", activate=True
        )
        recipe = _plain_spec(tmp_path, slow=True)
        key = tools.install_tool(recipe)
        job_id = ms.submit_job(_NeedsTool.to_job_definition(), params={"exe": recipe.candidates[0]}, depends_on=[key])
        assert ms.wait_for_job(job_id, poll_s=0.05).status == "completed"

        broken = _plain_spec(tmp_path, fail=True, slow=True)
        key = tools.install_tool(broken, SimpleNamespace(name="elsewhere"))
        job_id = ms.submit_job(_NeedsTool.to_job_definition(), params={"exe": broken.candidates[0]}, depends_on=[key])
        assert ms.wait_for_job(job_id, poll_s=0.05).status == "canceled"
    finally:
        ms.shutdown()
