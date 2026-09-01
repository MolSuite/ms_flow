import sys
import time
import types
import multiprocessing as mp
import platform
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlmodel import select

from ms_flow.core.database import ExecutorDB
from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobEvent
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import ProjectResourceSpec
from ms_flow.core.settings.models import Settings
from ms_flow.runtime import AppRuntime
from ms_flow.main import MolSuite
from ms_flow.runtime import BaseRuntime
from ms_flow.specs.input import InputSource


def _double_value(payload: dict):
    return {"value": int(payload["value"]) * 2}


def _echo_payload(payload: dict):
    return payload


class _ProjectResourceInput(InputSource):
    def iter_items(self, params: dict[str, object], config: dict[str, object]):
        del params
        yield {"ligands_path": config["project_resources"]["ligands"]["relative_path"]}


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _write_fake_hpc_scheduler(script_path: Path):
    script_path.write_text(
        """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def main(argv: list[str]) -> int:
    command = argv[1]
    if command == "submit":
        script_path = Path(argv[2]).resolve()
        control_dir = Path(argv[3]).resolve()
        state_dir = Path(argv[4]).resolve()
        scheduler_job_id = uuid.uuid4().hex
        proc = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        _save(
            state_dir / f"{scheduler_job_id}.json",
            {
                "pid": proc.pid,
                "control_dir": str(control_dir),
            },
        )
        print(scheduler_job_id)
        return 0

    scheduler_job_id = argv[2]
    state_dir = Path(argv[3]).resolve()
    state_path = state_dir / f"{scheduler_job_id}.json"
    state = _load(state_path)
    if not state:
        print(json.dumps({"state": "FAILED", "error": "Unknown scheduler job id"}))
        return 1

    control_dir = Path(state["control_dir"]).resolve()
    status_path = control_dir / "status.json"
    result_path = control_dir / "result.json"
    pid = int(state.get("pid", 0))

    if command == "poll":
        if result_path.exists():
            result = _load(result_path)
            if result.get("ok"):
                print(json.dumps({"state": "DONE"}))
                return 0
            print(json.dumps({"state": "FAILED", "error": result.get("error", "Execution failed")}))
            return 0
        if status_path.exists():
            status = _load(status_path)
            current = str(status.get("state", "")).upper()
            if current in {"FAILED", "ERROR", "CANCELED", "CANCELLED"}:
                print(json.dumps({"state": current, "error": status.get("error", "")}))
                return 0
        if pid and _is_alive(pid):
            print(json.dumps({"state": "RUNNING"}))
            return 0
        print(json.dumps({"state": "FAILED", "error": "Scheduler process exited without result"}))
        return 0

    if command == "cancel":
        if pid and _is_alive(pid):
            os.kill(pid, signal.SIGTERM)
        status = _load(status_path) if status_path.exists() else {}
        status.update({"state": "CANCELED"})
        _save(status_path, status)
        print("OK")
        return 0

    raise ValueError(f"Unknown command: {command}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
""",
        encoding="utf-8",
    )
    script_path.chmod(0o755)
def test_executor_db_is_created_inside_active_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_runtime"

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_runtime",
            folder=project_dir,
            description="runtime test",
            scope="docking",
            activate=True,
        )
        assert ms.executor_db is not None
        assert ms.executor_db.db_path == project_dir / "executor.db"
        assert (project_dir / "executor.db").exists()
    finally:
        ms.shutdown()


def test_project_context_accepts_legacy_update_at_input_but_exposes_updated_at():
    now = datetime.now()
    context = ProjectContext(
        name="demo",
        path=Path("/tmp/demo"),
        settings=Settings(),
        update_at=now,
    )

    assert context.updated_at == now


def test_open_project_after_create_project_works_on_molsuite(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_alias_runtime"

    ms = MolSuite(app_id="testruntime")
    try:
        created = ms.create_or_open_project(
            name="project_alias_runtime",
            folder=project_dir,
            description="runtime alias test",
            activate=False,
        )
        opened = ms.open_project(created.id)

        assert opened.id == created.id
        assert ms.active_context is not None
        assert ms.active_context.id == created.id
    finally:
        ms.shutdown()


def test_molsuite_reuses_executor_manager_between_project_sessions(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_a"
    project_b_dir = tmp_path / "project_b"

    ms = MolSuite(app_id="testruntime")
    try:
        project_a = ms.create_or_open_project(
            name="project_a",
            folder=project_a_dir,
            description="project a",
            activate=True,
        )
        manager_a = ms.executor_manager

        assert project_a is not None
        assert manager_a is not None
        assert ms.executor_db is not None
        assert ms.executor_db.db_path == project_a_dir / "executor.db"

        ms.close_project()

        assert ms.active_context is None
        assert ms.executor_manager is manager_a
        assert ms.executor_db is None
        assert manager_a._thread is not None and manager_a._thread.is_alive()

        project_b = ms.create_or_open_project(
            name="project_b",
            folder=project_b_dir,
            description="project b",
            activate=True,
        )

        assert project_b.id != project_a.id
        assert ms.executor_manager is manager_a
        assert ms.executor_db is not None
        assert ms.executor_db.db_path == project_b_dir / "executor.db"
    finally:
        ms.shutdown()


def test_close_project_clears_project_scoped_runtime_but_keeps_engine(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_cleanup"

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_cleanup",
            folder=project_dir,
            description="cleanup runtime",
            activate=True,
        )
        manager = ms.executor_manager
        assert manager is not None
        assert ms.project_db is not None
        assert ms.project_store is not None
        assert ms.project_logger is not None

        ms.close_project(cancel_running_tasks=False)

        assert ms.active_context is None
        assert ms.executor_manager is manager
        assert ms.executor_db is None
        assert ms.project_db is None
        assert ms.project_store is None
        assert ms.project_logger is None
        assert manager._thread is not None and manager._thread.is_alive()
    finally:
        ms.shutdown()


def test_register_task_canceller_requires_active_project_and_is_project_scoped(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_canceller_scope"

    ms = MolSuite(app_id="testruntime")
    try:
        with pytest.raises(RuntimeError):
            ms.register_task_canceller("00000000-0000-0000-0000-000000000000", lambda: None)

        project = ms.create_or_open_project(
            name="project_canceller_scope",
            folder=project_dir,
            description="canceller scope",
            activate=True,
        )
        ms.register_task_canceller(project.id, lambda: None)
        assert ms._runtime_state.active_project is not None
        assert len(ms._runtime_state.active_project.task_cancellers) == 1

        ms.close_project(cancel_running_tasks=False)
        assert ms._runtime_state.active_project is None
    finally:
        ms.shutdown()


def test_close_project_aborts_if_drain_fails(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_drain_fail"

    ms = MolSuite(app_id="testruntime")
    try:
        project = ms.create_or_open_project(
            name="project_drain_fail",
            folder=project_dir,
            description="drain fail",
            activate=True,
        )
        manager = ms.executor_manager
        db_path = ms.executor_db.db_path if ms.executor_db is not None else None

        monkeypatch.setattr(
            ms,
            "_wait_for_project_jobs_to_drain",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("drain timeout")),
        )

        with pytest.raises(RuntimeError, match="drain timeout"):
            ms.close_project(cancel_running_tasks=True)

        assert ms.active_context is not None
        assert ms.active_context.id == project.id
        assert ms.executor_manager is manager
        assert ms.executor_db is not None
        assert ms.executor_db.db_path == db_path
    finally:
        ms.shutdown()


def test_open_project_switch_aborts_if_current_project_does_not_drain(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_switch_a"
    project_b_dir = tmp_path / "project_switch_b"

    ms = MolSuite(app_id="testruntime")
    try:
        project_a = ms.create_or_open_project(
            name="project_switch_a",
            folder=project_a_dir,
            description="switch a",
            activate=True,
        )
        project_b = ms.create_or_open_project(
            name="project_switch_b",
            folder=project_b_dir,
            description="switch b",
            activate=False,
        )
        manager = ms.executor_manager
        db_path = ms.executor_db.db_path if ms.executor_db is not None else None

        monkeypatch.setattr(
            ms,
            "_wait_for_project_jobs_to_drain",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("switch blocked")),
        )

        with pytest.raises(RuntimeError, match="switch blocked"):
            ms.open_project(project_b.id)

        assert ms.active_context is not None
        assert ms.active_context.id == project_a.id
        assert ms.executor_manager is manager
        assert ms.executor_db is not None
        assert ms.executor_db.db_path == db_path
    finally:
        ms.shutdown()


def test_project_jobs_and_events_are_isolated_between_executor_dbs(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_iso_a"
    project_b_dir = tmp_path / "project_iso_b"

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_iso_a",
            folder=project_a_dir,
            description="iso a",
            activate=True,
        )
        job_a = ms.run(
            name="job_iso_a",
            input=[{"value": 2}],
            process=_double_value,
            executor="thread",
            store_results=True,
        )
        result_a = ms.wait_for_job(job_a, poll_s=0.05)
        assert result_a.status == "completed"
        db_a_path = ms.executor_db.db_path if ms.executor_db is not None else None

        ms.close_project()
        ms.create_or_open_project(
            name="project_iso_b",
            folder=project_b_dir,
            description="iso b",
            activate=True,
        )
        job_b = ms.run(
            name="job_iso_b",
            input=[{"value": 5}],
            process=_double_value,
            executor="thread",
            store_results=True,
        )
        result_b = ms.wait_for_job(job_b, poll_s=0.05)
        assert result_b.status == "completed"
        db_b_path = ms.executor_db.db_path if ms.executor_db is not None else None
    finally:
        ms.shutdown()

    assert db_a_path is not None and db_b_path is not None and db_a_path != db_b_path

    db_a = ExecutorDB(db_a_path)
    db_b = ExecutorDB(db_b_path)
    try:
        with db_a.get_session() as session:
            jobs_a = session.exec(select(ExecutorJob)).all()
            events_a = session.exec(select(ExecutorJobEvent)).all()
        with db_b.get_session() as session:
            jobs_b = session.exec(select(ExecutorJob)).all()
            events_b = session.exec(select(ExecutorJobEvent)).all()

        assert {job.job_id for job in jobs_a} == {job_a}
        assert {job.job_id for job in jobs_b} == {job_b}
        assert {event.job_id for event in events_a} == {job_a}
        assert {event.job_id for event in events_b} == {job_b}
    finally:
        db_a.dispose()
        db_b.dispose()


def test_switch_project_executes_registered_project_cancellers(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_cancel_a"
    project_b_dir = tmp_path / "project_cancel_b"

    ms = MolSuite(app_id="testruntime")
    try:
        project_a = ms.create_or_open_project(
            name="project_cancel_a",
            folder=project_a_dir,
            description="cancel a",
            activate=True,
        )
        project_b = ms.create_or_open_project(
            name="project_cancel_b",
            folder=project_b_dir,
            description="cancel b",
            activate=False,
        )

        state = {"count": 0}
        ms.register_task_canceller(project_a.id, lambda: state.__setitem__("count", state["count"] + 1))

        opened = ms.open_project(project_b.id)

        assert opened.id == project_b.id
        assert ms.active_context is not None
        assert ms.active_context.id == project_b.id
        assert state["count"] == 1
    finally:
        ms.shutdown()


def test_get_project_activity_reports_jobs_and_cancellers(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_activity"

    ms = MolSuite(app_id="testruntime")
    try:
        project = ms.create_or_open_project(
            name="project_activity",
            folder=project_dir,
            description="activity snapshot",
            activate=True,
        )
        ms.register_task_canceller(project.id, lambda: None)

        job_id = ms.run(
            name="activity_job",
            input=[{"value": 1}, {"value": 2}],
            process=_double_value,
            executor="thread",
            store_results=True,
        )

        deadline = datetime.now() + timedelta(seconds=5)
        snapshot = {}
        while datetime.now() < deadline:
            snapshot = ms.get_project_activity()
            if snapshot["active"]:
                break
            time.sleep(0.02)

        assert snapshot["project_id"] == str(project.id)
        assert snapshot["active"] is True
        assert snapshot["can_switch_project"] is False
        assert snapshot["jobs_active"] >= 1
        assert job_id in snapshot["job_ids"]
        assert snapshot["external_cancellers"] == 1
        switch_status = ms.get_project_switch_status()
        assert switch_status["can_switch"] is False
        assert "jobs_active" in switch_status["block_reasons"]
        assert ms.can_switch_project() is False
    finally:
        ms.shutdown()


def test_hpc_executor_persists_between_project_switches(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_a_dir = tmp_path / "project_hpc_a"
    project_b_dir = tmp_path / "project_hpc_b"
    scheduler_script = tmp_path / "fake_hpc_scheduler.py"
    scheduler_state_dir = tmp_path / "fake_hpc_state"
    _write_fake_hpc_scheduler(scheduler_script)

    ms = MolSuite(app_id="testruntime")
    try:
        project_a = ms.create_or_open_project(
            name="project_hpc_a",
            folder=project_a_dir,
            description="hpc a",
            activate=True,
        )
        project_b = ms.create_or_open_project(
            name="project_hpc_b",
            folder=project_b_dir,
            description="hpc b",
            activate=False,
        )

        ms.register_hpc_executor(
            name="hpc-persistent",
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
        )
        manager = ms.executor_manager
        adapter_before = manager._executors["hpc-persistent"] if manager is not None else None
        job_a = ms.run(
            name="hpc_project_a",
            input=[{"value": 2}],
            process=_double_value,
            executor="hpc-persistent",
            store_results=True,
        )
        result_a = ms.wait_for_job(job_a, poll_s=0.05)
        assert result_a.status == "completed"

        opened = ms.open_project(project_b.id)
        adapter_after = ms.executor_manager._executors["hpc-persistent"] if ms.executor_manager is not None else None
        job_b = ms.run(
            name="hpc_project_b",
            input=[{"value": 7}],
            process=_double_value,
            executor="hpc-persistent",
            store_results=True,
        )
        result_b = ms.wait_for_job(job_b, poll_s=0.05)

        assert opened.id == project_b.id
        assert ms.active_context is not None
        assert ms.active_context.id == project_b.id
        assert ms.executor_manager is manager
        assert adapter_after is adapter_before
        assert result_b.status == "completed"
    finally:
        ms.shutdown()


def test_entity_loader_context_exposes_project_db(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_loader_ctx"

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_loader_ctx",
            folder=project_dir,
            description="loader ctx test",
            scope="docking",
            activate=True,
        )
        loader_ctx = ms.get_entity_loader_context()
        assert loader_ctx.molsuite is ms
        assert loader_ctx.active_context is ms.active_context
        assert loader_ctx.project_db is ms.project_db
        assert loader_ctx.project_store is ms.project_store
        assert ms.advanced.project_data_context().project_db is ms.project_db
        assert ms.advanced.project_data_context().project_store is ms.project_store
        assert ms.advanced.project_store is ms.project_store
        assert ms.get_project_store() is ms.project_store
    finally:
        ms.shutdown()


def test_molsuite_project_resources_are_resolved_from_active_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_resources_runtime"

    ms = MolSuite(
        app_id="testruntime",
        project_resources=(
            ProjectResourceSpec(key="ligands", relative_path="data/ligands"),
            ProjectResourceSpec(key="docking_results", relative_path="results/docking"),
        ),
    )
    try:
        ms.create_or_open_project(
            name="project_resources_runtime",
            folder=project_dir,
            description="resource contract test",
            activate=True,
        )

        resources = ms.get_project_resource_map()
        loader_ctx = ms.get_project_data_context()

        assert set(resources) == {"docking_results", "ligands"}
        assert resources["ligands"]["relative_path"] == "data/ligands"
        assert Path(resources["ligands"]["path"]) == project_dir / "data" / "ligands"
        assert loader_ctx.project_resources == resources
        assert ms.get_project_resource_path("docking_results", "scores.csv", create_parent=True) == (
            project_dir / "results" / "docking" / "scores.csv"
        )
        assert (project_dir / "results" / "docking").is_dir()
    finally:
        ms.shutdown()


def test_molsuite_run_injects_project_resources_into_workflow_config(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_resources_job"

    ms = MolSuite(
        app_id="testruntime",
        project_resources=(ProjectResourceSpec(key="ligands", relative_path="data/ligands"),),
    )
    try:
        ms.create_or_open_project(
            name="project_resources_job",
            folder=project_dir,
            description="resource config job test",
            activate=True,
        )

        job_id = ms.run(
            name="resource_probe",
            input=_ProjectResourceInput(batch_size=1),
            process=_echo_payload,
            store_results=True,
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        outputs = ms.get_job_outputs(job_id)

        assert final.status == "completed"
        assert outputs == [{"items": [{"ligands_path": "data/ligands"}]}]
    finally:
        ms.shutdown()


def test_molsuite_register_hpc_executor_exposes_runtime_api(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_hpc_runtime"
    scheduler_script = tmp_path / "fake_hpc_scheduler.py"
    scheduler_state_dir = tmp_path / "fake_hpc_state"
    _write_fake_hpc_scheduler(scheduler_script)

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_hpc_runtime",
            folder=project_dir,
            description="runtime hpc registration test",
            scope="docking",
            activate=True,
        )
        ms.register_hpc_executor(
            name="hpc",
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
        )

        matrix = ms.executor_manager.get_executor_capability_matrix() if ms.executor_manager is not None else {}
        assert matrix["hpc"]["backend"] == "hpc"
        assert matrix["hpc"]["mode"] == "external"
        assert matrix["hpc"]["support_level"] == "stable"
    finally:
        ms.shutdown()
def test_molsuite_auto_registers_pooled_local_executors(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_auto_local_executors"

    ms = MolSuite(app_id="testruntime")
    try:
        ms.create_or_open_project(
            name="project_auto_local_executors",
            folder=project_dir,
            description="auto local executors test",
            scope="docking",
            activate=True,
        )

        matrix = ms.executor_manager.get_executor_capability_matrix() if ms.executor_manager is not None else {}
        assert "thread" in matrix
        assert "compute" in matrix
        assert matrix["compute"]["mode"] == "local"
        assert matrix["compute"]["local_resource_accounting"] == "dynamic"
        assert "process" not in matrix
        assert "process_pool" not in matrix
        assert "process_pool_loky" not in matrix
        assert ms.executor_manager.compute_backend_status()["backend"] == "loky"
    finally:
        ms.shutdown()


def test_molsuite_settings_can_register_hpc_and_run_job(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "project_hpc_settings"
    scheduler_script = tmp_path / "fake_hpc_scheduler.py"
    scheduler_state_dir = tmp_path / "fake_hpc_state"
    _write_fake_hpc_scheduler(scheduler_script)

    ms = MolSuite(app_id="testruntime")
    try:
        ms.settings_manager.update_setting(
            "workers.hpc",
            {
                "name": "hpc",
                "type": "hpc",
                "shared_fs": False,
                "submit_command": [
                    sys.executable,
                    str(scheduler_script),
                    "submit",
                    "{submit_script_path}",
                    "{control_dir}",
                    str(scheduler_state_dir),
                ],
                "poll_command": [
                    sys.executable,
                    str(scheduler_script),
                    "poll",
                    "{scheduler_job_id}",
                    str(scheduler_state_dir),
                ],
                "cancel_command": [
                    sys.executable,
                    str(scheduler_script),
                    "cancel",
                    "{scheduler_job_id}",
                    str(scheduler_state_dir),
                ],
            },
        )
        ms.create_or_open_project(
            name="project_hpc_settings",
            folder=project_dir,
            description="settings hpc test",
            scope="docking",
            activate=True,
        )

        job_id = ms.run(
            name="hpc_double_job",
            input=[{"value": 2}, {"value": 5}],
            process=_double_value,
            executor="hpc",
            store_results=True,
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        outputs = ms.get_job_outputs(job_id)

        assert final.status == "completed"
        assert final.chunks_done == 2
        assert sorted(item["value"] for item in outputs) == [4, 10]
    finally:
        ms.shutdown()


def test_base_runtime_open_project_accepts_owned_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")

    try:
        owned_context = runtime.molsuite.create_project(
            name="dock-owned",
            folder=tmp_path / "dock-owned",
            description="owned by runtime",
            activate=False,
        )

        opened_context = runtime.open_project(owned_context.id)

        assert opened_context.id == owned_context.id
        assert runtime.active_context is not None
        assert runtime.active_context.app_id == "amdockvs"
    finally:
        runtime.shutdown()


def test_base_runtime_open_project_rejects_foreign_app_project(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = MolSuite(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="dock-foreign",
            folder=tmp_path / "dock-foreign",
            description="foreign app project",
            activate=False,
        )

        with pytest.raises(ValueError, match="otherapp"):
            runtime.open_project(foreign_context.id)

        assert runtime.active_context is None
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_base_runtime_create_or_open_project_ignores_foreign_name_match(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = MolSuite(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="shared-name",
            folder=tmp_path / "otherapp-project",
            description="foreign app project",
            activate=False,
        )

        own_context = runtime.create_or_open_project(
            name="shared-name",
            folder=tmp_path / "amdockvs-project",
            description="owned by runtime",
        )

        assert own_context.id != foreign_context.id
        assert own_context.app_id == "amdockvs"
        assert own_context.path == (tmp_path / "amdockvs-project")
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_base_runtime_create_or_open_project_rejects_foreign_path_match(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = BaseRuntime("amdockvs")
    foreign_ms = MolSuite(app_id="otherapp")

    try:
        foreign_ms.create_project(
            name="otherapp-project",
            folder=tmp_path / "shared-folder",
            description="foreign app project",
            activate=False,
        )

        with pytest.raises(ValueError, match="already belongs to project"):
            runtime.create_or_open_project(
                name="amdockvs-project",
                folder=tmp_path / "shared-folder",
                description="should fail",
            )

        assert runtime.active_context is None
    finally:
        foreign_ms.shutdown()
        runtime.shutdown()


def test_molsuite_open_project_rejects_foreign_project_id(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = MolSuite(app_id="amdockvs")
    foreign_ms = MolSuite(app_id="otherapp")

    try:
        foreign_context = foreign_ms.create_project(
            name="foreign-activate",
            folder=tmp_path / "foreign-activate",
            description="foreign project",
            activate=False,
        )

        with pytest.raises(ValueError, match="otherapp"):
            scoped_ms.open_project(foreign_context.id)

        assert scoped_ms.active_context is None
    finally:
        foreign_ms.shutdown()
        scoped_ms.shutdown()


def test_molsuite_find_project_is_scoped_when_names_repeat(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = MolSuite(app_id="amdockvs")
    foreign_ms = MolSuite(app_id="otherapp")

    try:
        foreign_ms.create_project(
            name="shared-name",
            folder=tmp_path / "other-shared",
            description="foreign project",
            activate=False,
        )
        own_context = scoped_ms.create_project(
            name="shared-name",
            folder=tmp_path / "own-shared",
            description="own project",
            activate=False,
        )

        found = scoped_ms.find_project(name="shared-name")

        assert found is not None
        assert found.id == own_context.id
        assert found.app_id == "amdockvs"
    finally:
        foreign_ms.shutdown()
        scoped_ms.shutdown()


def test_molsuite_scoped_app_id_filters_pagination_at_query_level(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = MolSuite(app_id="amdockvs")

    try:
        now = datetime.now()
        with scoped_ms.master_db.get_session() as session:
            session.add(
                Project(
                    name="amdock-old-1",
                    path=str(tmp_path / "amdock-old-1"),
                    app_id="amdockvs",
                    updated_at=now - timedelta(minutes=4),
                )
            )
            session.add(
                Project(
                    name="amdock-old-2",
                    path=str(tmp_path / "amdock-old-2"),
                    app_id="amdockvs",
                    updated_at=now - timedelta(minutes=3),
                )
            )
            session.add(
                Project(
                    name="other-new-1",
                    path=str(tmp_path / "other-new-1"),
                    app_id="otherapp",
                    updated_at=now - timedelta(minutes=2),
                )
            )
            session.add(
                Project(
                    name="other-new-2",
                    path=str(tmp_path / "other-new-2"),
                    app_id="otherapp",
                    updated_at=now - timedelta(minutes=1),
                )
            )
            session.commit()

        page = scoped_ms.list_projects(page=1, items_per_page=2)

        assert [project.name for project in page] == ["amdock-old-2", "amdock-old-1"]
        assert all(project.app_id == "amdockvs" for project in page)
    finally:
        scoped_ms.shutdown()


def test_molsuite_scoped_app_id_filters_pagination_across_multiple_pages(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    scoped_ms = MolSuite(app_id="amdockvs")

    try:
        now = datetime.now()
        with scoped_ms.master_db.get_session() as session:
            for index in range(6):
                session.add(
                    Project(
                        name=f"amdock-{index}",
                        path=str(tmp_path / f"amdock-{index}"),
                        app_id="amdockvs",
                        updated_at=now - timedelta(minutes=20 - index),
                    )
                )
                session.add(
                    Project(
                        name=f"other-{index}",
                        path=str(tmp_path / f"other-{index}"),
                        app_id="otherapp",
                        updated_at=now - timedelta(minutes=10 - index),
                    )
                )
            session.commit()

        page1 = scoped_ms.list_projects(page=1, items_per_page=2)
        page2 = scoped_ms.list_projects(page=2, items_per_page=2)
        page3 = scoped_ms.list_projects(page=3, items_per_page=2)

        assert [project.name for project in page1] == ["amdock-5", "amdock-4"]
        assert [project.name for project in page2] == ["amdock-3", "amdock-2"]
        assert [project.name for project in page3] == ["amdock-1", "amdock-0"]
        assert all(project.app_id == "amdockvs" for project in page1 + page2 + page3)
    finally:
        scoped_ms.shutdown()


def test_molsuite_scoped_app_id_defaults_project_creation(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = MolSuite(app_id="amdockvs")

    try:
        context = ms.create_project(
            name="scoped-project",
            folder=tmp_path / "scoped-project",
            description="scoped runtime",
            activate=False,
        )

        assert context.app_id == "amdockvs"
        assert context.scope == "amdockvs"
    finally:
        ms.shutdown()


def test_app_runtime_exposes_scoped_project_catalog(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = AppRuntime("amdockvs")

    try:
        catalog = runtime.project_catalog

        assert catalog.app_id == "amdockvs"
        assert runtime.project_catalog is catalog
    finally:
        runtime.shutdown()


def test_app_runtime_exposes_declarative_app_settings(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    setting = AppSettingSpec(
        key="preview_limit",
        default=50,
        value_type="integer",
        minimum=1,
    )
    runtime = AppRuntime("amdockvs", app_settings=(setting,))

    try:
        assert runtime.app_setting_specs() == (setting,)
        assert runtime.get_app_setting("preview_limit") == 50

        runtime.update_app_setting("preview_limit", 75)

        assert runtime.get_app_setting("preview_limit") == 75
    finally:
        runtime.shutdown()


def test_app_runtime_project_resources_follow_manifest_like_contract(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    runtime = AppRuntime(
        "amdockvs",
        project_resources=(
            ProjectResourceSpec(key="ligands", relative_path="data/ligands"),
            ProjectResourceSpec(key="receptors", relative_path="data/receptors"),
        ),
    )

    try:
        runtime.create_project(
            name="amdock-project",
            folder=tmp_path / "amdock-project",
            description="runtime resource test",
        )

        assert tuple(spec.key for spec in runtime.project_resource_specs()) == ("ligands", "receptors")
        assert runtime.get_project_resource_path("ligands") == tmp_path / "amdock-project" / "data" / "ligands"
        assert (tmp_path / "amdock-project" / "data" / "receptors").is_dir()
    finally:
        runtime.shutdown()
