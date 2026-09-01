"""Checks for the executor provisioning helpers (no network, no ssh)."""
from __future__ import annotations

import socket

import pytest

from ms_flow.core.executor import provisioning as pv
from ms_flow.core.settings.models import HPCWorkerConfig, RayWorkerConfig


def test_split_address_forms():
    assert pv.split_address("ray://10.0.0.5:10001") == ("10.0.0.5", 10001)
    assert pv.split_address("10.0.0.5:6379") == ("10.0.0.5", 6379)
    assert pv.split_address("10.0.0.5") == ("10.0.0.5", 6379)
    assert pv.split_address("") == ("", 0)


def test_preflight_fails_fast_on_a_closed_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    error = pv.ray_preflight(f"127.0.0.1:{closed_port}")
    assert error and "unreachable" in error
    assert pv.ray_preflight("") == ""  # local mode: nothing to reach


def test_conda_bootstrap_is_idempotent_and_optional():
    assert pv.conda_bootstrap_command("") == ""
    command = pv.conda_bootstrap_command("amdock", "3.12")
    assert "conda create -y -n amdock python=3.12" in command
    assert command.startswith("conda env list")  # only creates when missing
    assert pv.in_env("ray --version", "amdock").startswith("conda run")
    assert pv.in_env("ray --version", "") == "ray --version"


def test_prepare_steps_cover_every_host_and_skip_local_only_workers():
    worker = RayWorkerConfig(
        name="cluster",
        mode="managed",
        head_ip="10.0.0.1",
        worker_ips=["10.0.0.2", "10.0.0.3"],
        ssh_user="mario",
        conda_env="amdock",
        setup_commands=["pip install -e ."],
    )
    steps = pv.prepare_steps(worker)
    assert [s.name.split(":")[0] for s in steps] == [
        "10.0.0.1",
        "10.0.0.1",
        "10.0.0.2",
        "10.0.0.2",
        "10.0.0.3",
        "10.0.0.3",
    ]
    assert all("BatchMode=yes" in " ".join(step.argv or []) for step in steps)

    local_only = RayWorkerConfig(name="ray", mode="local")
    assert pv.prepare_steps(local_only) == []


def test_cluster_yaml_has_what_ray_up_needs():
    worker = RayWorkerConfig(
        name="cluster",
        mode="managed",
        head_ip="10.0.0.1",
        worker_ips=["10.0.0.2"],
        ssh_user="mario",
        conda_env="amdock",
    )
    text = pv.ray_cluster_yaml(worker)
    assert "type: local" in text
    assert "head_ip: 10.0.0.1" in text
    assert "worker_ips: [10.0.0.2]" in text
    assert "ssh_user: mario" in text
    assert "ray start --head" in text
    assert "--num-cpus=4" in text
    assert "--num-gpus=0" in text
    assert pv.cluster_address(worker) == "10.0.0.1:6379"
    assert pv.cluster_launch_needed(worker) is True
    assert pv.cluster_launch_needed(RayWorkerConfig(name="external", mode="external", worker_ips=["10.0.0.2"])) is False


def test_hpc_test_steps_probe_the_scheduler():
    worker = HPCWorkerConfig(name="hpc", host="cluster.local", user="mario", scheduler="slurm")
    names = [step.name for step in pv.test_steps(worker)]
    assert names == ["ssh cluster.local", "slurm available"]


def test_run_steps_stops_at_the_first_failure():
    seen: list[str] = []
    steps = [
        pv.Step("ok", call=lambda: "fine"),
        pv.Step("boom", argv=["false"]),
        pv.Step("never", call=lambda: "unreachable"),
    ]
    with pytest.raises(pv.StepFailed):
        pv.run_steps(steps, on_line=seen.append)
    assert "unreachable" not in seen
