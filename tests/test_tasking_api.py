import sqlite3
import json
import random
import threading
import time
from pathlib import Path

from pydantic import BaseModel
from sqlmodel import select

from ms_flow.query import db_input_for
from ms_flow.core.data import DbOutputSpec
from ms_flow.core.database.executor_models import ExecutorJob, ExecutorJobChunk
from ms_flow.main import MolSuite
from ms_flow.tasking import job, task


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


class _NumberPayload(BaseModel):
    value: int


class _BatchJobParams(BaseModel):
    base: int
    count: int = 3


class _ScoreJobParams(BaseModel):
    start: int = 1
    count: int = 5


class _SlowRestartJobParams(BaseModel):
    base: int = 1
    count: int = 40
    sleep: float = 0.03


_LIFECYCLE_FINALIZED = {}
_LAZY_SUBMIT_GATE: threading.Event | None = None


def _make_bucket_handler(bucket: list):
    from ms_flow.core.executor.manager import CallbackResultHandler

    return CallbackResultHandler(on_result=lambda _chunk_id, result: bucket.append(result))


def _double_worker_plain(payload: dict):
    return {"value": int(payload["value"]) * 2}


def _blocking_worker(payload: dict):
    time.sleep(float(payload.get("sleep", 0.2)))
    return {"value": int(payload.get("value", 0))}


@task(
    name="double_task",
    executor="thread",
    supported_executors=("thread", "process_pool"),
    cpu_required=1,
    input_model=_NumberPayload,
)
def _double_task(payload: dict, progress=None):
    value = int(payload["value"])
    if progress is not None:
        progress(50.0)
    return {"value": value * 2}


@job(
    task=_double_task,
    name="double_job",
    params_model=_BatchJobParams,
    executor="thread",
    supported_executors=("thread", "process_pool"),
    result_handler_factory=_make_bucket_handler,
    store_results=False,
)
def _double_job(params: dict, config: dict):
    count = int(config.get("count", params["count"]))
    base = int(params["base"])
    for idx in range(count):
        yield {"value": base + idx}


def _job_setup(_payload: dict, _context: dict):
    return {"offset": 100}


def _job_stage_chunk(payload: dict, context: dict):
    staged = dict(payload)
    staged["value"] = int(staged["value"]) + int(context.get("setup_data", {}).get("offset", 0))
    return staged


def _job_finalize(_payload: dict, context: dict):
    _LIFECYCLE_FINALIZED[context["job_id"]] = True
    return {"ok": True}


@task(
    name="slow_restart_task",
    executor="thread",
    supported_executors=("thread",),
    cpu_required=1,
)
def _slow_restart_task(payload: dict):
    import time

    time.sleep(float(payload.get("sleep", 0.03)))
    return {"value": int(payload["value"]) * 2}


@job(
    task=_slow_restart_task,
    name="slow_restart_job",
    params_model=_SlowRestartJobParams,
    executor="thread",
    supported_executors=("thread",),
    store_results=True,
)
def _slow_restart_job(params: dict, config: dict):
    del config
    base = int(params["base"])
    count = int(params["count"])
    sleep_s = float(params.get("sleep", 0.03))
    for idx in range(count):
        yield {"value": base + idx, "sleep": sleep_s}


@job(
    task=_double_task,
    name="double_job_lifecycle",
    params_model=_BatchJobParams,
    executor="thread",
    supported_executors=("thread",),
    setup=_job_setup,
    stage_chunk=_job_stage_chunk,
    finalize=_job_finalize,
    result_handler_factory=_make_bucket_handler,
    store_results=False,
)
def _double_job_lifecycle(params: dict, config: dict):
    count = int(config.get("count", params["count"]))
    base = int(params["base"])
    for idx in range(count):
        yield {"value": base + idx}


@task(
    name="score_task",
    executor="thread",
    supported_executors=("thread",),
    cpu_required=1,
)
def _score_task(payload: dict):
    return {"ligand_id": int(payload["ligand_id"]), "score": float(payload["score"])}


@task(
    name="double_task_multi_exec",
    executor="thread",
    supported_executors=("thread", "thread_alt"),
    cpu_required=1,
)
def _double_task_multi_exec(payload: dict):
    return {"value": int(payload["value"]) * 2}


@job(
    task=_double_task_multi_exec,
    name="double_job_multi_exec",
    params_model=_BatchJobParams,
    executor="thread",
    supported_executors=("thread", "thread_alt"),
    store_results=True,
)
def _double_job_multi_exec(params: dict, _config: dict):
    base = int(params["base"])
    count = int(params["count"])
    for idx in range(count):
        yield {"value": base + idx}


@task(
    name="summarize_query_rows_task",
    executor="thread",
    supported_executors=("thread",),
)
def _summarize_query_rows_task(payload: dict):
    molecules = list(payload["molecules"])
    return {
        "count": len(molecules),
        "names": [row["name"] for row in molecules],
    }


@job(
    task=_summarize_query_rows_task,
    name="summarize_query_rows_job",
    executor="thread",
    supported_executors=("thread",),
    store_results=True,
)
def _summarize_query_rows_job(_params: dict, _config: dict):
    yield {
        "molecules": db_input_for(
            "molecules",
            fields=("id", "name"),
            filters={"status": "ready"},
            order=("id",),
        )
    }


@job(
    task=_score_task,
    name="score_job_output_spec",
    params_model=_ScoreJobParams,
    executor="thread",
    supported_executors=("thread",),
    output_spec=DbOutputSpec(
        table="docking_results",
        columns=("ligand_id", "score"),
        db_role="project",
    ),
    output_flush_every=2,
    store_results=False,
)
def _score_job(params: dict, _config: dict):
    start = int(params["start"])
    count = int(params["count"])
    for idx in range(start, start + count):
        yield {"ligand_id": idx, "score": float(idx) * 0.25}


@job(
    task=_double_task,
    name="gated_lazy_submit_job",
    params_model=_BatchJobParams,
    executor="thread",
    supported_executors=("thread",),
    store_results=True,
)
def _gated_lazy_submit_job(params: dict, _config: dict):
    gate = _LAZY_SUBMIT_GATE
    if gate is not None:
        gate.wait(timeout=5.0)
    base = int(params["base"])
    count = int(params["count"])
    for idx in range(count):
        yield {"value": base + idx}


def test_task_and_job_support_with_options():
    tuned_task = _double_task.with_options(cpu_required=4, executor="process_pool")
    assert tuned_task.cpu_required == 4
    assert tuned_task.executor == "process_pool"
    assert tuned_task.supported_executors == ("thread", "process_pool")

    tuned_job = _double_job.with_options(cpu_required=2, executor="process_pool", store_results=True)
    assert tuned_job.cpu_required == 2
    assert tuned_job.executor == "process_pool"
    assert tuned_job.store_results is True


def test_job_build_chunks_and_handler_factory():
    params = _double_job.validate_params({"base": "5", "count": "3"})
    assert params == {"base": 5, "count": 3}

    chunks = list(_double_job.build_chunks({"base": "5", "count": "3"}))
    assert chunks == [{"value": 5}, {"value": 6}, {"value": 7}]

    bucket = []
    handler = _double_job.build_result_handler(bucket)
    assert handler is not None
    handler.handle("chunk", {"value": 10})
    assert bucket == [{"value": 10}]


class _FakeJobRuntime:
    def __init__(self):
        self.submit_calls = []
        self.wait_calls = []

    def submit_job(self, job_def, *, params, config=None, **kwargs):
        self.submit_calls.append(
            {
                "job_def": job_def,
                "params": params,
                "config": config,
                "kwargs": dict(kwargs),
            }
        )
        return "job-123"

    def wait_for_job(self, job_id, *, poll_s=0.25, progress_cb=None):
        self.wait_calls.append(
            {
                "job_id": job_id,
                "poll_s": poll_s,
                "progress_cb": progress_cb,
            }
        )
        return {"job_id": job_id, "status": "completed"}


def test_job_definition_submit_and_run_helpers_delegate_to_runtime():
    runtime = _FakeJobRuntime()

    job_id = _double_job.submit(runtime, params={"base": 2, "count": 2}, priority=7)
    assert job_id == "job-123"
    assert len(runtime.submit_calls) == 1
    assert runtime.submit_calls[0]["job_def"] is _double_job
    assert runtime.submit_calls[0]["params"] == {"base": 2, "count": 2}
    assert runtime.submit_calls[0]["kwargs"]["priority"] == 7

    final = _double_job.run(runtime, params={"base": 3, "count": 1}, priority=3, poll_s=0.5)
    assert final == {"job_id": "job-123", "status": "completed"}
    assert len(runtime.submit_calls) == 2
    assert runtime.submit_calls[1]["params"] == {"base": 3, "count": 1}
    assert runtime.submit_calls[1]["kwargs"]["priority"] == 3
    assert runtime.wait_calls == [{"job_id": "job-123", "poll_s": 0.5, "progress_cb": None}]


def test_job_definition_submit_with_options_applies_job_overrides_before_dispatch():
    runtime = _FakeJobRuntime()

    job_id = _double_job.submit_with_options(
        runtime,
        params={"base": 4, "count": 2},
        job_options={"executor": "process_pool", "store_results": True},
        executor_name="process_pool",
    )

    assert job_id == "job-123"
    assert len(runtime.submit_calls) == 1
    submitted_job = runtime.submit_calls[0]["job_def"]
    assert submitted_job is not _double_job
    assert submitted_job.executor == "process_pool"
    assert submitted_job.store_results is True
    assert runtime.submit_calls[0]["kwargs"]["executor_name"] == "process_pool"


def test_job_definition_scalability_warnings_flag_antiscalable_combinations():
    graph_output = DbOutputSpec(
        table="molecules",
        columns=("value",),
        db_role="project",
        mode="graph",
    )
    tuned_job = _double_job.with_options(
        executor="compute",
        supported_executors=("thread", "compute"),
        output_spec=graph_output,
        output_flush_every=1,
        store_results=True,
    )

    warnings = tuned_job.scalability_warnings(
        total_chunks=128,
        max_inflight_tasks=16,
        sink_max_buffer_factor=64,
        sink_max_buffer_bytes=300 * 1024 * 1024,
        sink_max_payload_bytes=8 * 1024 * 1024,
    )
    codes = {item.code for item in warnings}

    assert "graph_flush_every_1" in codes
    assert "store_results_with_sink" in codes
    assert "sink_quotas_unreasonable" in codes


def test_molsuite_submits_job_definition(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project"

    ms = MolSuite(app_id="testtasking")
    bucket = []
    try:
        ms.create_or_open_project(
            name="tasking_project",
            folder=project_dir,
            description="tasking api test",
            scope="testing",
            activate=True,
        )

        job_id = ms.submit_job(
            _double_job,
            params={"base": "5", "count": "3"},
            result_handler_args=(bucket,),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 3
        assert bucket == [{"value": 10}, {"value": 12}, {"value": 14}]
        assert ms.get_job_outputs(job_id) == []
    finally:
        ms.shutdown()


def test_molsuite_completes_an_explicitly_empty_job(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "empty_tasking_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="empty_tasking_project",
            folder=project_dir,
            description="empty tasking api test",
            scope="testing",
            activate=True,
        )
        job_id = ms.submit_job(
            _double_job,
            params={"base": 5, "count": 0},
            total_chunks=0,
            result_handler_args=([],),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)

        assert final.status == "completed"
        assert final.chunks_total == 0
        assert final.chunks_done == 0
    finally:
        ms.shutdown()


def test_submit_job_emits_configuration_warning_events_for_antiscalable_runtime_combo(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_guardrails_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="tasking_guardrails_project",
            folder=project_dir,
            description="tasking guardrails test",
            scope="testing",
            activate=True,
        )
        job_id = ms.executor_manager.submit_job(
            executor_name="compute",
            chunks=[{"value": 1}],
            run_chunk=_double_worker_plain,
            job_payload={
                "_data_context": {
                    "project_path": str(project_dir),
                    "project_db_path": str(project_dir / "project.db"),
                }
            },
            total_chunks=128,
            max_inflight_tasks=16,
            output_spec=DbOutputSpec(
                table="guardrail_results",
                columns=("value",),
                db_role="project",
            ),
            output_flush_every=1,
            store_results=True,
        )
        ms.executor_manager._flush_events()

        with ms.executor_db.get_session() as session:
            job = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
        assert job is not None
        payload = json.loads(job.payload_json or "{}")
        codes = {item["code"] for item in payload.get("_guardrail_warnings", [])}
        assert "flush_every_1_large_payload_budget" in codes
        assert "store_results_with_sink" in codes

        deadline = time.time() + 3.0
        warning_events = []
        while time.time() < deadline:
            warning_events = [
                item for item in ms.get_job_events(job_id)
                if item.get("type") == "job_configuration_warning"
            ]
            if warning_events:
                break
            time.sleep(0.05)

        assert warning_events
        messages = {item.get("message") for item in warning_events}
        assert any("`output_flush_every=1`" in str(message) for message in messages)
        assert any("`output_spec` together with `store_results=True`" in str(message) for message in messages)
    finally:
        ms.shutdown()


def test_molsuite_run_job_definition_blocks_until_completion(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_run"

    ms = MolSuite(app_id="testtasking")
    bucket = []
    try:
        ms.create_or_open_project(
            name="tasking_project_run",
            folder=project_dir,
            description="tasking run api test",
            scope="testing",
            activate=True,
        )

        final = _double_job.run(
            ms,
            params={"base": "6", "count": "2"},
            result_handler_args=(bucket,),
            poll_s=0.05,
        )
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2
        assert bucket == [{"value": 12}, {"value": 14}]
    finally:
        ms.shutdown()


def test_molsuite_submit_job_does_not_block_on_lazy_chunker(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_lazy_submit"

    global _LAZY_SUBMIT_GATE
    gate = threading.Event()
    _LAZY_SUBMIT_GATE = gate

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="tasking_project_lazy_submit",
            folder=project_dir,
            description="tasking lazy submit test",
            scope="testing",
            activate=True,
        )

        result: dict[str, str] = {}
        error: list[Exception] = []

        def _submit():
            try:
                result["job_id"] = ms.submit_job(
                    _gated_lazy_submit_job,
                    params={"base": 1, "count": 2},
                )
            except Exception as exc:
                error.append(exc)

        submit_thread = threading.Thread(target=_submit, daemon=True)
        submit_thread.start()
        submit_thread.join(timeout=0.5)

        assert not submit_thread.is_alive(), "submit_job() must not block while the lazy feed waits for data."
        assert not error
        job_id = result["job_id"]

        row = None
        deadline = time.time() + 2.0
        while time.time() < deadline:
            row = ms.executor_manager.get_job(job_id) if ms.executor_manager is not None else None
            if row is not None:
                break
            time.sleep(0.05)
        assert row is not None
        assert row["status"] in {"pending", "staging", "running"}

        gate.set()
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2
    finally:
        _LAZY_SUBMIT_GATE = None
        ms.shutdown()


def test_molsuite_submit_job_with_dependency_defers_chunk_build_until_upstream_completes(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_dependency_deferred_chunks"

    global _LAZY_SUBMIT_GATE
    gate = threading.Event()
    _LAZY_SUBMIT_GATE = gate

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="tasking_project_dependency_deferred_chunks",
            folder=project_dir,
            description="tasking dependency deferred chunk build test",
            scope="testing",
            activate=True,
        )

        assert ms.executor_manager is not None
        blocker_job = ms.executor_manager.submit_job(
            executor_name="thread",
            chunks=[{"value": 1, "sleep": 0.3}],
            run_chunk=_blocking_worker,
            max_inflight_tasks=1,
        )

        result: dict[str, str] = {}
        error: list[Exception] = []

        def _submit():
            try:
                result["job_id"] = ms.submit_job(
                    _gated_lazy_submit_job,
                    params={"base": 1, "count": 2},
                    depends_on=[blocker_job],
                )
            except Exception as exc:
                error.append(exc)

        submit_thread = threading.Thread(target=_submit, daemon=True)
        submit_thread.start()
        submit_thread.join(timeout=0.5)

        assert not submit_thread.is_alive(), "submit_job() must not block when the chunker is deferred by dependencies."
        assert not error

        dependent_job = result["job_id"]
        dependent_row = None
        deadline = time.time() + 2.0
        while time.time() < deadline:
            dependent_row = ms.executor_manager.get_job(dependent_job)
            if dependent_row is not None and dependent_row["scheduler_block_reason"] == "waiting_for_dependencies":
                break
            time.sleep(0.05)
        assert dependent_row is not None
        assert dependent_row["scheduler_block_reason"] == "waiting_for_dependencies"

        ms.wait_for_job(blocker_job, poll_s=0.05)
        gate.set()
        final = ms.wait_for_job(dependent_job, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2
    finally:
        _LAZY_SUBMIT_GATE = None
        ms.shutdown()


def test_molsuite_submit_job_alias_works(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_submit_alias"

    ms = MolSuite(app_id="testtasking")
    bucket = []
    try:
        ms.create_or_open_project(
            name="tasking_project_submit_alias",
            folder=project_dir,
            description="tasking submit alias test",
            scope="testing",
            activate=True,
        )

        job_id = ms.submit_job(
            _double_job,
            params={"base": "5", "count": "2"},
            result_handler_args=(bucket,),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2
        assert final["origin_id"] == "testtasking"
        assert bucket == [{"value": 10}, {"value": 12}]
    finally:
        ms.shutdown()


def test_molsuite_submits_lifecycle_job_definition(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    _LIFECYCLE_FINALIZED.clear()
    project_dir = tmp_path / "tasking_project_lifecycle"

    ms = MolSuite(app_id="testtasking")
    bucket = []
    try:
        ms.create_or_open_project(
            name="tasking_project_lifecycle",
            folder=project_dir,
            description="tasking lifecycle api test",
            scope="testing",
            activate=True,
        )

        job_id = ms.submit_job(
            _double_job_lifecycle,
            params={"base": 1, "count": 2},
            result_handler_args=(bucket,),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2
        assert bucket == [{"value": 202}, {"value": 204}]
        assert _LIFECYCLE_FINALIZED.get(job_id) is True
    finally:
        ms.shutdown()


def test_molsuite_submits_output_spec_job_definition(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_output_spec"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="tasking_project_output_spec",
            folder=project_dir,
            description="tasking output spec test",
            scope="testing",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None
        project_db_path = ms.project_db.db_path

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE docking_results (ligand_id INTEGER, score REAL)")
            conn.commit()
        finally:
            conn.close()

        job_id = ms.submit_job(
            _score_job,
            params={"start": 1, "count": 6},
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 6
        assert ms.get_job_outputs(job_id) == []

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM docking_results")
            total = cur.fetchone()[0]
        finally:
            conn.close()
        assert total == 6
    finally:
        ms.shutdown()


def test_molsuite_runtime_healthcheck_reports_inactive_and_active_states(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "health_project"

    ms = MolSuite(app_id="testtasking")
    try:
        inactive = ms.get_runtime_healthcheck()
        assert inactive["status"] == "inactive"
        assert inactive["checks"]["runtime"]["ok"] is False
        assert inactive["core_health"]["status"] == "inactive"
        assert inactive["persistence_health"]["status"] == "inactive"
        assert inactive["sink_health"]["status"] == "inactive"

        ms.create_or_open_project(
            name="health_project",
            folder=project_dir,
            description="runtime healthcheck test",
            scope="testing",
            activate=True,
        )

        active = ms.get_runtime_healthcheck()
        assert active["status"] == "ok"
        assert active["checks"]["executor_db"]["ok"] is True
        assert active["checks"]["manager_thread"]["ok"] is True
        assert active["core_health"]["status"] == "ok"
        assert active["persistence_health"]["status"] == "ok"
        assert active["sink_health"]["status"] == "ok"
        assert active["project_id"] == str(ms.active_context.id)
    finally:
        ms.shutdown()


def test_molsuite_applies_operational_limits_from_settings(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "limits_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.settings_manager.update_setting("general.poll_interval", 0.05)
        ms.settings_manager.update_setting("operational_limits.staging_max_workers", 3)
        ms.settings_manager.update_setting("operational_limits.max_inline_chunk_payload_bytes", 2048)
        ms.settings_manager.update_setting("operational_limits.max_spool_payload_bytes", 8192)
        ms.settings_manager.update_setting("operational_limits.default_max_inflight_tasks", 4)
        ms.settings_manager.update_setting("operational_limits.default_max_inflight_items", 12)
        ms.settings_manager.update_setting("operational_limits.output_sink_max_payload_bytes", 4096)
        ms.settings_manager.update_setting("operational_limits.output_sink_max_pending_chunks", 17)
        ms.settings_manager.update_setting("operational_limits.output_sink_max_pending_bytes", 16384)

        ms.create_or_open_project(
            name="limits_project",
            folder=project_dir,
            description="operational limits test",
            scope="testing",
            activate=True,
        )
        assert ms.executor_manager is not None

        runtime = ms.executor_manager.get_operational_snapshot()
        assert runtime["limits"]["max_inline_chunk_payload_bytes"] == 2048
        assert runtime["limits"]["max_spool_payload_bytes"] == 8192
        assert runtime["limits"]["staging_max_workers"] == 3
        assert runtime["limits"]["output_sink_max_payload_bytes"] == 4096
        assert runtime["limits"]["output_sink_max_pending_chunks"] == 17
        assert runtime["limits"]["output_sink_max_pending_bytes"] == 16384
        assert runtime["loop"]["poll_interval_s"] == 0.05
        assert runtime["staging"]["capacity"] == 3

        job_id = ms.submit_job(
            _double_job,
            params={"base": 1, "count": 2},
            result_handler_args=([],),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"

        assert ms.executor_db is not None
        with ms.executor_db.get_session() as session:
            row = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
        assert row is not None
        payload = json.loads(row.payload_json)
        assert payload["_dispatch_policy"]["max_inflight_tasks"] == 4
        assert payload["_dispatch_policy"]["max_inflight_items"] == 12
    finally:
        ms.shutdown()


def test_molsuite_applies_explicit_operational_profile(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "profile_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.settings_manager.update_setting("operational_limits.operational_profile", "throughput")
        ms.create_or_open_project(
            name="profile_project",
            folder=project_dir,
            description="operational profile test",
            scope="testing",
            activate=True,
        )

        job_id = ms.submit_job(
            _double_job,
            params={"base": 1, "count": 2},
            result_handler_args=([],),
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"

        assert ms.executor_db is not None
        with ms.executor_db.get_session() as session:
            row = session.exec(select(ExecutorJob).where(ExecutorJob.job_id == job_id)).first()
        assert row is not None
        payload = json.loads(row.payload_json)
        policy = payload["_operational_profile"]
        assert policy["profile"] == "throughput"
        assert policy["max_inflight_tasks"] == 64
        assert policy["max_inflight_items"] == 2048
        assert policy["output_flush_every"] == 1000
    finally:
        ms.shutdown()


def test_molsuite_task_can_read_db_input_without_sqlmodel_or_file_specs(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "tasking_project_db_input"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="tasking_project_db_input",
            folder=project_dir,
            description="tasking db input api test",
            scope="testing",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None
        project_db_path = ms.project_db.db_path

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
            cur.execute("INSERT INTO molecules (id, name, status) VALUES (1, 'MolA', 'ready')")
            cur.execute("INSERT INTO molecules (id, name, status) VALUES (2, 'MolB', 'ready')")
            cur.execute("INSERT INTO molecules (id, name, status) VALUES (3, 'MolC', 'archived')")
            conn.commit()
        finally:
            conn.close()

        job_id = ms.submit_job(_summarize_query_rows_job, params={})
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 1
        assert ms.get_job_outputs(job_id) == [{"count": 2, "names": ["MolA", "MolB"]}]
    finally:
        ms.shutdown()


def test_molsuite_exposes_executor_status_and_capabilities(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "executor_runtime_api"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="executor_runtime_api",
            folder=project_dir,
            description="executor runtime api test",
            scope="testing",
            activate=True,
        )
        assert ms.executor_manager is not None
        ms.executor_manager.register_thread_executor(name="thread_alt", max_workers=1)

        status = ms.get_executor_status()
        matrix = ms.get_executor_capability_matrix()

        assert "cpu" in status
        assert "executors" in status
        assert "thread" in status["executors"]
        assert "thread_alt" in status["executors"]
        assert matrix["thread"]["backend"] == "thread"
        assert matrix["thread"]["support_level"] == "stable"
        assert matrix["thread_alt"]["mode"] == "local"
        assert matrix["thread_alt"]["support_level"] == "stable"
        assert status["executors"]["thread"]["support_level"] == "stable"
    finally:
        ms.shutdown()


def test_molsuite_can_resubmit_declarative_job_with_runtime_overrides(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "resubmit_job_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="resubmit_job_project",
            folder=project_dir,
            description="resubmit declarative job test",
            scope="testing",
            activate=True,
        )
        assert ms.executor_manager is not None
        ms.executor_manager.register_thread_executor(name="thread_alt", max_workers=1)

        original_job_id = ms.submit_job(_double_job_multi_exec, params={"base": 2, "count": 2})
        original = ms.wait_for_job(original_job_id, poll_s=0.05)
        assert original["status"] == "completed"

        replay_job_id = ms.resubmit_job(
            original_job_id,
            executor_name="thread_alt",
            cpu_required=3,
            priority=5,
            queue_policy="priority",
        )
        replay = ms.wait_for_job(replay_job_id, poll_s=0.05)

        assert replay["status"] == "completed"
        assert replay["executor_name"] == "thread_alt"
        assert ms.get_job_outputs(replay_job_id) == [{"value": 4}, {"value": 6}]

        assert ms.executor_db is not None
        with ms.executor_db.get_session() as session:
            job_row = session.exec(
                select(ExecutorJob).where(ExecutorJob.job_id == replay_job_id)
            ).first()
            chunk_rows = session.exec(
                select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == replay_job_id)
            ).all()

        assert job_row is not None
        assert job_row.executor_name == "thread_alt"
        assert job_row.queue_policy == "priority"
        assert job_row.priority == 5
        assert {int(row.cpu_required) for row in chunk_rows} == {3}
    finally:
        ms.shutdown()


def test_molsuite_can_cancel_job_via_public_api(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "cancel_job_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="cancel_job_project",
            folder=project_dir,
            description="cancel job public api test",
            scope="testing",
            activate=True,
        )

        job_id = ms.submit_job(_slow_restart_job, params={"base": 1, "count": 50, "sleep": 0.03})
        deadline = time.time() + 3.0
        while time.time() < deadline:
            row = ms.executor_manager.get_job(job_id) if ms.executor_manager is not None else None
            if row is not None and row.status in {"pending", "running", "staging"}:
                break
            time.sleep(0.03)

        ms.cancel_job(job_id)
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final.status == "canceled"
    finally:
        ms.shutdown()


def test_molsuite_resubmit_job_requires_persisted_chunker_ref(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "resubmit_inline_project"

    ms = MolSuite(app_id="testtasking")
    try:
        ms.create_or_open_project(
            name="resubmit_inline_project",
            folder=project_dir,
            description="resubmit inline source test",
            scope="testing",
            activate=True,
        )

        job_id = ms.run(
            name="inline_stream_job",
            input=[{"value": 1}, {"value": 2}],
            process=_double_worker_plain,
            executor="thread",
            supported_executors=("thread",),
            store_results=True,
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final.status == "completed"

        try:
            ms.resubmit_job(job_id)
        except ValueError as exc:
            assert "_chunker_ref" in str(exc)
        else:
            raise AssertionError("Expected resubmit_job() to reject inline-source jobs.")
    finally:
        ms.shutdown()
