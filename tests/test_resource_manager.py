from pathlib import Path
from types import SimpleNamespace

from ms_flow.core.executor.resource_manager import LocalResourceManager


def test_local_resource_manager_tracks_adapter_reservations():
    manager = LocalResourceManager(total_cpu=8)
    executors = {
        "thread": SimpleNamespace(reserved_cpu=0),
        "ray-local": SimpleNamespace(reserved_cpu=2),
    }

    assert manager.reserved_cpu(executors) == 2


def test_local_resource_manager_reports_accounting_modes():
    manager = LocalResourceManager(total_cpu=8)

    thread = SimpleNamespace(reserved_cpu=0, execution_mode="local", consumes_local_cpu_tokens=False)
    process = SimpleNamespace(reserved_cpu=0, execution_mode="local", consumes_local_cpu_tokens=True)
    ray_local = SimpleNamespace(reserved_cpu=3, execution_mode="local", consumes_local_cpu_tokens=False)
    ray_remote = SimpleNamespace(reserved_cpu=0, execution_mode="external", consumes_local_cpu_tokens=False)

    assert manager.accounting_mode(thread) == "none"
    assert manager.accounting_mode(process) == "dynamic"
    assert manager.accounting_mode(ray_local) == "reserved"
    assert manager.accounting_mode(ray_remote) == "none"

    assert manager.participates_in_local_accounting(thread) is False
    assert manager.participates_in_local_accounting(process) is True
    assert manager.participates_in_local_accounting(ray_local) is True
