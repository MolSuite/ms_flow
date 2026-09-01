import json
import sqlite3
from pathlib import Path

from ms_flow.api import (
    MolSuite,
    WorkflowSpec,
    inline_items,
    project_file_out,
    project_table,
    project_table_out,
    workflow,
)


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _score_ligand_batches(payload: dict):
    return [
        {
            "ligand_id": int(item["ligand_id"]),
            "score": float(item["ligand_id"]) * 1.5,
        }
        for item in payload["items"]
    ]


def _summarize_items(payload: dict):
    values = [int(item["value"]) for item in payload["items"]]
    return {"count": len(values), "total": sum(values)}


def test_molsuite_run_executes_project_table_workflow(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "workflow_project"

    ms = MolSuite(app_id="testworkflow")
    try:
        ms.create_or_open_project(
            name="workflow_project",
            folder=project_dir,
            description="workflow api test",
            scope="testing",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None
        project_db_path = ms.project_db.db_path

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE ligands (ligand_id INTEGER, active INTEGER)")
            cur.execute("CREATE TABLE ligand_scores (ligand_id INTEGER, score REAL)")
            cur.executemany(
                "INSERT INTO ligands (ligand_id, active) VALUES (?, ?)",
                [(1, 1), (2, 1), (3, 0), (4, 1)],
            )
            conn.commit()
        finally:
            conn.close()

        job_id = ms.run(
            name="ligand_scores_workflow",
            input=project_table(
                "ligands",
                fields=("ligand_id",),
                filters={"active": 1},
                order=("ligand_id",),
                batch_size=2,
            ),
            process=_score_ligand_batches,
            output=project_table_out("ligand_scores", columns=("ligand_id", "score")),
            executor="thread",
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT ligand_id, score FROM ligand_scores ORDER BY ligand_id ASC")
            rows = cur.fetchall()
        finally:
            conn.close()
        assert rows == [
            (1, 1.5),
            (2, 3.0),
            (4, 6.0),
        ]
    finally:
        ms.shutdown()


def test_molsuite_run_accepts_workflow_spec_with_inline_items_and_file_output(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "workflow_inline_project"

    ms = MolSuite(app_id="testworkflow")
    try:
        ms.create_or_open_project(
            name="workflow_inline_project",
            folder=project_dir,
            description="workflow api inline test",
            scope="testing",
            activate=True,
        )

        spec = workflow(
            name="inline_summary_workflow",
            input=inline_items([{"value": 1}, {"value": 2}, {"value": 3}], batch_size=3),
            process=_summarize_items,
            output=project_file_out("summary.json"),
            executor="thread",
        )
        assert isinstance(spec, WorkflowSpec)

        job_id = ms.run(spec)
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 1

        payload = json.loads((project_dir / "summary.json").read_text(encoding="utf-8"))
        assert payload == [{"count": 3, "total": 6}]
    finally:
        ms.shutdown()
