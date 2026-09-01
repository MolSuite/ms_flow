import sqlite3
from pathlib import Path

import pytest

from pydantic import BaseModel

from ms_flow.api import batch_job, streaming_job
from ms_flow.job_templates import build_streaming_job_definition
from ms_flow.core.data import DbOutputSpec
from ms_flow.main import MolSuite


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


class _TemplateParams(BaseModel):
    start: int = 1
    count: int = 4


_TEMPLATE_FINALIZED = {}


def _template_run_chunk(payload: dict):
    return {
        "ligand_id": int(payload["ligand_id"]),
        "score": float(payload["score"]),
    }


def _template_chunker(params: dict, _config: dict):
    start = int(params["start"])
    count = int(params["count"])
    for ligand_id in range(start, start + count):
        yield {"ligand_id": ligand_id, "score": float(ligand_id) * 0.25}


def _template_setup(_payload: dict, _context: dict):
    return {"factor": 2.0}


def _template_stage(payload: dict, context: dict):
    staged = dict(payload)
    factor = float(context.get("setup_data", {}).get("factor", 1.0))
    staged["score"] = float(staged["score"]) * factor
    return staged


def _template_finalize(_payload: dict, context: dict):
    _TEMPLATE_FINALIZED[context["job_id"]] = True
    return {"ok": True}


def _template_run_batch(payload: dict):
    items = list(payload["items"])
    return {
        "count": len(items),
        "ligand_ids": [int(item["ligand_id"]) for item in items],
    }


def _template_batcher(params: dict, _config: dict):
    start = int(params["start"])
    count = int(params["count"])
    batch_size = int(params.get("batch_size", 2))
    batch = []
    for ligand_id in range(start, start + count):
        batch.append({"ligand_id": ligand_id, "score": float(ligand_id) * 0.25})
        if len(batch) >= batch_size:
            yield {"items": list(batch)}
            batch.clear()
    if batch:
        yield {"items": list(batch)}


def test_build_streaming_job_definition_runs_with_incremental_db_output(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    _TEMPLATE_FINALIZED.clear()
    project_dir = tmp_path / "template_project"

    ms = MolSuite(app_id="testjobs")
    try:
        ms.create_or_open_project(
            name="template_project",
            folder=project_dir,
            description="template job integration test",
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

        job_def = build_streaming_job_definition(
            name="reference_streaming_job",
            task_name="reference_streaming_task",
            run_chunk=_template_run_chunk,
            chunker=_template_chunker,
            params_model=_TemplateParams,
            setup=_template_setup,
            stage_chunk=_template_stage,
            finalize=_template_finalize,
            output_spec=DbOutputSpec(
                table="docking_results",
                columns=("ligand_id", "score"),
                db_role="project",
            ),
            output_flush_every=2,
            store_results=False,
            executor="thread",
            supported_executors=("thread",),
        )

        job_id = ms.submit_job(job_def, params={"start": 1, "count": 4})
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final.status == "completed"
        assert final.chunks_done == 4
        assert _TEMPLATE_FINALIZED.get(job_id) is True
        assert ms.get_job_outputs(job_id) == []

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT ligand_id, score FROM docking_results ORDER BY ligand_id ASC")
            rows = cur.fetchall()
        finally:
            conn.close()
        assert rows == [
            (1, 0.5),
            (2, 1.0),
            (3, 1.5),
            (4, 2.0),
        ]
    finally:
        ms.shutdown()


def test_streaming_job_helper_sets_simple_defaults():
    job_def = streaming_job(
        name="simple_streaming_job",
        run_chunk=_template_run_chunk,
        chunker=_template_chunker,
    )

    assert job_def.name == "simple_streaming_job"
    assert job_def.task.name == "simple_streaming_job_task"
    assert job_def.executor == "compute"
    assert job_def.supported_executors == ("compute",)
    assert job_def.output_spec is None


def test_batch_job_helper_sets_simple_defaults_and_preserves_batches():
    job_def = batch_job(
        name="simple_batch_job",
        run_batch=_template_run_batch,
        batcher=_template_batcher,
    )

    assert job_def.name == "simple_batch_job"
    assert job_def.task.name == "simple_batch_job_task"
    assert job_def.executor == "compute"
    assert job_def.supported_executors == ("compute",)
    assert job_def.output_spec is None
    assert list(job_def.build_chunks({"start": 1, "count": 5, "batch_size": 2})) == [
        {
            "items": [
                {"ligand_id": 1, "score": 0.25},
                {"ligand_id": 2, "score": 0.5},
            ]
        },
        {
            "items": [
                {"ligand_id": 3, "score": 0.75},
                {"ligand_id": 4, "score": 1.0},
            ]
        },
        {
            "items": [
                {"ligand_id": 5, "score": 1.25},
            ]
        },
    ]


def test_streaming_job_reports_invalid_stage_fail_policy_with_job_name():
    with pytest.raises(ValueError, match=r"JobDefinition 'bad_job'.*stage_fail_policy='oops'"):
        streaming_job(
            name="bad_job",
            run_chunk=_template_run_chunk,
            chunker=_template_chunker,
            stage_fail_policy="oops",
        )
