from __future__ import annotations

import time
from pathlib import Path

import pytest

from ms_flow.core.executor.local_adapters import (
    LokyProcessExecutorAdapter,
    ThreadExecutorAdapter,
)

from adapter_contracts import (
    AdapterContractCase,
    assert_contract_cancel,
    assert_contract_failure,
    assert_contract_metadata,
    assert_contract_success,
    assert_contract_unknown_handle,
)


def _contract_double(payload: dict):
    return {"value": int(payload["value"]) * 2}


def _contract_fail(payload: dict):
    raise RuntimeError(f"boom:{payload['value']}")


def _contract_sleep(payload: dict):
    time.sleep(float(payload.get("sleep", 0.2)))
    return {"done": True}


def _contract_progress(payload: dict, progress_cb):
    steps = max(1, int(payload.get("steps", 3)))
    sleep_s = float(payload.get("sleep", 0.03))
    for index in range(steps):
        time.sleep(sleep_s)
        progress_cb(((index + 1) / steps) * 100.0)
    return {"steps": steps}


LOKY_INSTALLED = True
try:
    import loky  # noqa: F401
except Exception:
    LOKY_INSTALLED = False


CASES = (
    AdapterContractCase(
        name="thread",
        factory=lambda: ThreadExecutorAdapter(name="thread-contract", max_workers=2),
        backend="thread",
        mode="local",
        integration="builtin",
        support_level="stable",
        shared_fs=True,
        consumes_local_cpu_tokens=False,
    ),
)
if LOKY_INSTALLED:
    CASES = CASES + (
        AdapterContractCase(
            name="process_pool_loky",
            factory=lambda: LokyProcessExecutorAdapter(
                name="process-pool-loky-contract",
                max_workers=2,
                timeout_s=10.0,
                kill_workers_on_shutdown=True,
            ),
            backend="process_pool_loky",
            mode="local",
            integration="builtin",
            support_level="experimental",
            shared_fs=True,
            consumes_local_cpu_tokens=True,
        ),
    )


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_adapter_contract_metadata(case: AdapterContractCase):
    assert_contract_metadata(case)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_adapter_contract_success(case: AdapterContractCase):
    assert_contract_success(case, _contract_double)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_adapter_contract_failure(case: AdapterContractCase):
    assert_contract_failure(case, _contract_fail)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_adapter_contract_unknown_handle(case: AdapterContractCase):
    assert_contract_unknown_handle(case)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_adapter_contract_cancel(case: AdapterContractCase):
    assert_contract_cancel(case, _contract_sleep)
@pytest.mark.skipif(not LOKY_INSTALLED, reason="loky is not installed")
def test_process_pool_loky_adapter_shutdown_cleans_internal_handles():
    adapter = LokyProcessExecutorAdapter(
        name="process-pool-loky-cleanup-shutdown",
        max_workers=2,
        timeout_s=10.0,
        kill_workers_on_shutdown=True,
    )
    handle_id = adapter.submit(
        "job-pool-loky-shutdown",
        "chunk-pool-loky-shutdown",
        {"sleep": 0.5},
        {"module": __name__, "fn": "_contract_sleep"},
        lambda _value: None,
    )
    assert handle_id in adapter._futures
    adapter.shutdown()
    assert adapter._futures == {}


@pytest.mark.skipif(not LOKY_INSTALLED, reason="loky is not installed")
def test_process_pool_loky_adapter_reports_progress():
    adapter = LokyProcessExecutorAdapter(
        name="process-pool-loky-progress",
        max_workers=2,
        timeout_s=10.0,
        kill_workers_on_shutdown=True,
    )
    try:
        handle_id = adapter.submit(
            "job-pool-loky-progress",
            "chunk-pool-loky-progress",
            {"steps": 4, "sleep": 0.02},
            {"module": __name__, "fn": "_contract_progress"},
            lambda _value: None,
        )
        deadline = time.time() + 5.0
        latest_progress = None
        while time.time() < deadline:
            latest_progress = adapter.drain_progress(handle_id)
            state, payload, error = adapter.poll(handle_id)
            if state != "RUNNING":
                assert state == "DONE", error
                assert payload == {"result": {"steps": 4}}
                break
            time.sleep(0.01)
        assert latest_progress is None or latest_progress >= 25.0
    finally:
        adapter.shutdown()
