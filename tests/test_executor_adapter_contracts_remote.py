from __future__ import annotations

import sys
import tempfile
import time
from subprocess import CompletedProcess
from pathlib import Path

import pytest

from ms_flow.core.data import (
    DataBridge,
    DataContext,
    ExecutorTransportProfile,
    FileArtifact,
    FileInputSpec,
    ProjectOutputDirSpec,
    to_wire_value,
)
from ms_flow.core.executor.hpc_adapter import HPCCommandExecutorAdapter, HPCHandleState
from ms_flow.core.executor.ray_adapter import RayExecutorAdapter

from adapter_contracts import (
    AdapterContractCase,
    assert_contract_cancel,
    assert_contract_failure,
    assert_contract_metadata,
    assert_contract_success,
    assert_contract_unknown_handle,
)
from adapter_test_fakes import install_fake_ray, write_fake_hpc_scheduler


def _contract_double(payload: dict):
    return {"value": int(payload["value"]) * 2}


def _contract_fail(payload: dict):
    raise RuntimeError(f"boom:{payload['value']}")


def _contract_sleep(payload: dict):
    time.sleep(float(payload.get("sleep", 0.2)))
    return {"done": True}


def _consume_file_and_return_artifact(payload: dict):
    text = Path(payload["source"]).read_text(encoding="utf-8")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as handle:
        handle.write(text.upper())
        output_path = handle.name
    return {
        "text": text,
        "artifact_path": FileArtifact(output_path, payload["destination"]),
    }


def _write_output_directory(payload: dict):
    output_dir = Path(payload["output_dir"])
    nested = output_dir / "reports" / "result.json"
    nested.parent.mkdir(parents=True, exist_ok=True)
    nested.write_text('{"ok": true}', encoding="utf-8")
    return {"report_path": str(nested)}


def _poll_done(adapter: RayExecutorAdapter, handle_id: str):
    deadline = time.time() + 3
    while time.time() < deadline:
        status, payload, error = adapter.poll(handle_id)
        if status != "RUNNING":
            return status, payload, error
        time.sleep(0.01)
    raise AssertionError("Ray adapter did not complete in time.")


def _build_case(kind: str, tmp_path: Path, monkeypatch) -> AdapterContractCase:
    if kind == "ray":
        install_fake_ray(monkeypatch)
        return AdapterContractCase(
            name="ray-native",
            factory=lambda: RayExecutorAdapter(name="ray-contract", mode="external", shared_fs=True),
            backend="ray",
            mode="external",
            integration="native",
            support_level="experimental",
            shared_fs=True,
            consumes_local_cpu_tokens=False,
        )
    if kind == "hpc":
        scheduler_script = tmp_path / "fake_hpc_scheduler.py"
        scheduler_state_dir = tmp_path / "fake_hpc_state"
        hpc_wdir = tmp_path / "fake_hpc_wdir"
        write_fake_hpc_scheduler(scheduler_script)
        return AdapterContractCase(
            name="hpc-command",
            factory=lambda: HPCCommandExecutorAdapter(
                name="hpc-contract",
                shared_fs=False,
                submit_command=[
                    sys.executable,
                    str(scheduler_script),
                    "submit",
                    "{submit_script_path}",
                    "{control_dir}",
                    str(scheduler_state_dir),
                ],
                poll_command=[
                    sys.executable,
                    str(scheduler_script),
                    "poll",
                    "{scheduler_job_id}",
                    str(scheduler_state_dir),
                ],
                cancel_command=[
                    sys.executable,
                    str(scheduler_script),
                    "cancel",
                    "{scheduler_job_id}",
                    str(scheduler_state_dir),
                ],
            ),
            backend="hpc",
            mode="external",
            integration="command",
            support_level="stable",
            shared_fs=False,
            consumes_local_cpu_tokens=False,
            submit_context_factory=lambda: {"hpc_wdir": str(hpc_wdir)},
            timeout_s=8.0,
        )
    raise ValueError(f"Unknown adapter contract kind: {kind}")


@pytest.mark.parametrize("kind", ("ray", "hpc"))
def test_remote_adapter_contract_metadata(kind: str, tmp_path: Path, monkeypatch):
    case = _build_case(kind, tmp_path, monkeypatch)
    assert_contract_metadata(case)


@pytest.mark.parametrize("kind", ("ray", "hpc"))
def test_remote_adapter_contract_success(kind: str, tmp_path: Path, monkeypatch):
    case = _build_case(kind, tmp_path, monkeypatch)
    assert_contract_success(case, _contract_double)


@pytest.mark.parametrize("kind", ("ray", "hpc"))
def test_remote_adapter_contract_failure(kind: str, tmp_path: Path, monkeypatch):
    case = _build_case(kind, tmp_path, monkeypatch)
    assert_contract_failure(case, _contract_fail)


@pytest.mark.parametrize("kind", ("ray", "hpc"))
def test_remote_adapter_contract_unknown_handle(kind: str, tmp_path: Path, monkeypatch):
    case = _build_case(kind, tmp_path, monkeypatch)
    assert_contract_unknown_handle(case)


@pytest.mark.parametrize("kind", ("ray", "hpc"))
def test_remote_adapter_contract_cancel(kind: str, tmp_path: Path, monkeypatch):
    case = _build_case(kind, tmp_path, monkeypatch)
    assert_contract_cancel(case, _contract_sleep)


def test_ray_transfers_cached_path_and_materializes_worker_artifact(tmp_path, monkeypatch):
    fake_ray = install_fake_ray(monkeypatch)
    source = tmp_path / "receptor.pdbqt"
    source.write_text("RECEPTOR\n", encoding="utf-8")
    bridge = DataBridge()
    profile = ExecutorTransportProfile(backend="ray", mode="external", shared_fs=False)
    staged = bridge.materialize_payload(
        to_wire_value(
            {
                "source": FileInputSpec(str(source), fmt="text", delivery="path", cache=True),
                "destination": "results/pose.txt",
            }
        ),
        DataContext(project_dir=tmp_path),
        executor_profile=profile,
    )
    adapter = RayExecutorAdapter(
        name="ray-transfer",
        mode="external",
        shared_fs=False,
        gpu_slots_per_device=3,
    )
    try:
        for index in range(2):
            handle = adapter.submit(
                "job",
                f"chunk-{index}",
                staged,
                {"module": __name__, "fn": "_consume_file_and_return_artifact"},
                None,
                {"project_dir": str(tmp_path), "gpu_required": 1},
            )
            status, payload, error = _poll_done(adapter, handle)
            assert (status, error) == ("DONE", None)
            assert payload["result"]["artifact_path"] == "results/pose.txt"
        assert fake_ray._put_calls == 1
        assert fake_ray._futures[-1]._num_gpus == pytest.approx(1 / 3)
        assert (tmp_path / "results" / "pose.txt").read_text(encoding="utf-8") == "RECEPTOR\n"

        escaped = {**staged, "destination": "../escape.txt"}
        handle = adapter.submit(
            "job",
            "chunk-escape",
            escaped,
            {"module": __name__, "fn": "_consume_file_and_return_artifact"},
            None,
            {"project_dir": str(tmp_path)},
        )
        status, _payload, error = _poll_done(adapter, handle)
        assert status == "FAILED"
        assert "escapes the project directory" in str(error)
        assert not (tmp_path.parent / "escape.txt").exists()
    finally:
        adapter.shutdown()


def test_ray_materializes_project_output_directory(tmp_path, monkeypatch):
    install_fake_ray(monkeypatch)
    bridge = DataBridge()
    staged = bridge.materialize_payload(
        to_wire_value({"output_dir": ProjectOutputDirSpec("results/docking")}),
        DataContext(project_dir=tmp_path),
        executor_profile=ExecutorTransportProfile(backend="ray", mode="external", shared_fs=False),
    )
    adapter = RayExecutorAdapter(name="ray-output-dir", mode="external", shared_fs=False)
    try:
        handle = adapter.submit(
            "job",
            "chunk",
            staged,
            {"module": __name__, "fn": "_write_output_directory"},
            None,
            {"project_dir": str(tmp_path)},
        )
        status, payload, error = _poll_done(adapter, handle)
        assert (status, error) == ("DONE", None)
        assert payload["result"]["report_path"] == "results/docking/reports/result.json"
        assert (tmp_path / "results/docking/reports/result.json").read_text() == '{"ok": true}'
    finally:
        adapter.shutdown()


def test_hpc_poll_command_is_rate_limited_but_local_result_is_immediate(
    tmp_path: Path,
    monkeypatch,
):
    control_dir = tmp_path / "control"
    control_dir.mkdir()
    status_path = control_dir / "status.json"
    result_path = control_dir / "result.json"
    status_path.write_text('{"state": "RUNNING"}', encoding="utf-8")

    adapter = HPCCommandExecutorAdapter(
        name="hpc",
        submit_command=["submit"],
        poll_command=["poll"],
        poll_interval_s=5.0,
    )
    adapter._handles["handle"] = HPCHandleState(
        scheduler_job_id="scheduler-1",
        control_dir=control_dir,
        manifest_path=control_dir / "manifest.json",
        submit_script_path=control_dir / "submit.sh",
        status_path=status_path,
        result_path=result_path,
        stdout_path=control_dir / "stdout.log",
        stderr_path=control_dir / "stderr.log",
    )

    poll_calls: list[float] = []

    def _poll_command(*_args, **_kwargs):
        poll_calls.append(time.monotonic())
        return CompletedProcess(args=["poll"], returncode=0, stdout="RUNNING\n", stderr="")

    monkeypatch.setattr(adapter, "_run_command", _poll_command)

    assert adapter.poll("handle") == ("RUNNING", None, None)
    assert adapter.poll("handle") == ("RUNNING", None, None)
    assert len(poll_calls) == 1
    assert adapter.health_snapshot()["poll_interval_s"] == pytest.approx(5.0)

    result_path.write_text(
        '{"ok": true, "result": {"value": 42}}',
        encoding="utf-8",
    )
    assert adapter.poll("handle") == ("DONE", {"result": {"value": 42}}, None)
    assert len(poll_calls) == 1
