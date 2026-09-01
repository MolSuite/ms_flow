from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

import pytest

from ms_flow.core.executor.local_adapters import ExecutorAdapterBase
from ms_flow.core.executor.runner_refs import normalize_runner


@dataclass(frozen=True)
class AdapterContractCase:
    name: str
    factory: Callable[[], ExecutorAdapterBase]
    backend: str
    mode: str
    integration: str
    support_level: str
    shared_fs: bool
    consumes_local_cpu_tokens: bool
    submit_context_factory: Callable[[], dict[str, Any]] | None = None
    timeout_s: float = 5.0


def wait_for_terminal_state(
    adapter: ExecutorAdapterBase,
    handle_id: str,
    *,
    timeout_s: float = 5.0,
    poll_interval_s: float = 0.02,
) -> tuple[str, dict | None, str | None]:
    deadline = time.time() + timeout_s
    last_state = ("RUNNING", None, None)
    while time.time() < deadline:
        last_state = adapter.poll(handle_id)
        state, payload, error = last_state
        if state != "RUNNING":
            return state, payload, error
        time.sleep(poll_interval_s)
    return last_state


def assert_contract_metadata(case: AdapterContractCase) -> None:
    adapter = case.factory()
    try:
        metadata = adapter.metadata
        health = adapter.health_snapshot()

        assert adapter.backend_name == case.backend
        assert adapter.execution_mode == case.mode
        assert adapter.has_shared_filesystem is case.shared_fs
        assert adapter.consumes_local_cpu_tokens is case.consumes_local_cpu_tokens
        assert metadata.backend == case.backend
        assert metadata.mode == case.mode
        assert metadata.support_level == case.support_level
        assert metadata.shared_filesystem is case.shared_fs
        assert metadata.consumes_local_cpu_tokens is case.consumes_local_cpu_tokens
        assert health["ok"] is True
        assert health["integration"] == case.integration
    finally:
        adapter.shutdown()


def assert_contract_success(case: AdapterContractCase, worker: Callable[[dict], dict]) -> None:
    adapter = case.factory()
    try:
        handle_id = adapter.submit(
            job_id="job-contract",
            chunk_id="chunk-contract",
            payload={"value": 7},
            fn_ref=normalize_runner(worker),
            progress_cb=lambda _value: None,
            submit_context=case.submit_context_factory() if case.submit_context_factory is not None else None,
        )
        assert isinstance(handle_id, str)
        state, payload, error = wait_for_terminal_state(adapter, handle_id, timeout_s=case.timeout_s)
        if state == "FAILED" and "Permission denied" in str(error or ""):
            pytest.skip("Process spawning is blocked in this sandbox environment.")
        assert state == "DONE"
        assert error is None
        assert payload == {"result": {"value": 14}}
    finally:
        adapter.shutdown()


def assert_contract_failure(case: AdapterContractCase, worker: Callable[[dict], dict]) -> None:
    adapter = case.factory()
    try:
        handle_id = adapter.submit(
            job_id="job-contract-fail",
            chunk_id="chunk-contract-fail",
            payload={"value": 3},
            fn_ref=normalize_runner(worker),
            progress_cb=lambda _value: None,
            submit_context=case.submit_context_factory() if case.submit_context_factory is not None else None,
        )
        state, payload, error = wait_for_terminal_state(adapter, handle_id, timeout_s=case.timeout_s)
        if state == "FAILED" and "Permission denied" in str(error or ""):
            pytest.skip("Process spawning is blocked in this sandbox environment.")
        assert state == "FAILED"
        assert payload is None
        assert error
    finally:
        adapter.shutdown()


def assert_contract_unknown_handle(case: AdapterContractCase) -> None:
    adapter = case.factory()
    try:
        state, payload, error = adapter.poll("unknown-handle")
        assert state == "FAILED"
        assert payload is None
        assert error
    finally:
        adapter.shutdown()


def assert_contract_cancel(case: AdapterContractCase, worker: Callable[[dict], dict]) -> None:
    adapter = case.factory()
    try:
        handle_id = adapter.submit(
            job_id="job-contract-cancel",
            chunk_id="chunk-contract-cancel",
            payload={"sleep": 0.4},
            fn_ref=normalize_runner(worker),
            progress_cb=lambda _value: None,
            submit_context=case.submit_context_factory() if case.submit_context_factory is not None else None,
        )
        canceled = adapter.cancel(handle_id)
        assert isinstance(canceled, bool)

        state, payload, error = wait_for_terminal_state(adapter, handle_id, timeout_s=case.timeout_s)
        if state == "FAILED" and "Permission denied" in str(error or ""):
            pytest.skip("Process spawning is blocked in this sandbox environment.")
        assert state in {"DONE", "FAILED"}
        assert state != "RUNNING"
        if canceled and state == "FAILED":
            assert payload is None
    finally:
        adapter.shutdown()
