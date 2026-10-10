"""Checks for the steps that test a configured worker (no network, no ssh)."""
from __future__ import annotations

from ms_flow.core.settings import probes
from ms_flow.core.settings.models import HPCWorkerConfig


def test_split_address_forms():
    assert probes.split_address("ray://10.0.0.5:10001") == ("10.0.0.5", 10001)
    assert probes.split_address("10.0.0.5:6379") == ("10.0.0.5", 6379)
    assert probes.split_address("10.0.0.5") == ("10.0.0.5", 6379)
    assert probes.split_address("") == ("", 0)


def test_hpc_test_steps_probe_the_scheduler():
    worker = HPCWorkerConfig(name="hpc", host="cluster.local", user="mario", scheduler="slurm")
    steps = probes.test_steps(worker)
    assert [step.name for step in steps] == ["ssh cluster.local", "slurm available"]
    assert steps[1].argv[0] == "ssh" and steps[1].argv[-2:] == ["mario@cluster.local", "sinfo --version"]
    assert "BatchMode=yes" in steps[1].argv
