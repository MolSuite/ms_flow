import sqlite3
import time
from pathlib import Path

import pytest

from sqlmodel import select

from ms_flow.api import MolSuite, table_sink, workflow
from ms_flow.core.database.executor_models import ExecutorJobChunk
from ms_flow.query import db_stream


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _calculate_descriptors(payload: dict):
    items = payload.get("items", [payload])
    results = []
    for item in items:
        smiles = str(item["smiles"])
        results.append({
            "ligand_id": int(item["id"]),
            "heavy_atoms": len(smiles),
        })
    return results


def _summarize_batch(payload: dict):
    items = list(payload["items"])
    return {
        "count": len(items),
        "ligand_ids": [int(item["ligand_id"]) for item in items],
    }


def test_run_stream_processes_project_stream_into_table_sink(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "amdockvs_simple_api"

    ms = MolSuite(app_id="amdockvs")
    try:
        ms.create_or_open_project(
            name="amdockvs_simple_api",
            folder=project_dir,
            description="simple api test",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None

        conn = sqlite3.connect(str(ms.project_db.db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, smiles TEXT, active INTEGER)")
            cur.execute(
                "CREATE TABLE amdockvs_ligand_descriptors (ligand_id INTEGER, heavy_atoms INTEGER)"
            )
            cur.executemany(
                "INSERT INTO ligands (id, smiles, active) VALUES (?, ?, ?)",
                [
                    (1, "CCO", 1),
                    (2, "CCCC", 1),
                    (3, "N", 0),
                ],
            )
            conn.commit()
        finally:
            conn.close()

        source = db_stream(
            ms,
            "ligands",
            fields=("id", "smiles"),
            filters={"active": 1},
            order=("id",),
            batch_size=1,
        )
        job_id = ms.run(
            name="descriptor_job",
            input=source,
            process=_calculate_descriptors,
            output=table_sink(
                "amdockvs_ligand_descriptors",
                columns=("ligand_id", "heavy_atoms"),
            ),
            params={"batch_size": 1},
            flush_every=2,
            executor="thread",
            store_results=False,
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)

        assert final.status == "completed"
        assert final.chunks_done == 2
        assert final["origin_id"] == "amdockvs"

        conn = sqlite3.connect(str(ms.project_db.db_path))
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT ligand_id, heavy_atoms FROM amdockvs_ligand_descriptors ORDER BY ligand_id ASC"
            )
            rows = cur.fetchall()
        finally:
            conn.close()
        assert rows == [(1, 3), (2, 4)]
    finally:
        ms.shutdown()


def test_run_batch_groups_iterable_items_with_default_items_key(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "batch_simple_api"

    ms = MolSuite(app_id="amdockvs")
    try:
        ms.create_or_open_project(
            name="batch_simple_api",
            folder=project_dir,
            description="simple batch api test",
            activate=True,
        )

        job_id = ms.run(
            name="descriptor_batches",
            input=[
                {"ligand_id": 1},
                {"ligand_id": 2},
                {"ligand_id": 3},
                {"ligand_id": 4},
                {"ligand_id": 5},
            ],
            process=_summarize_batch,
            params={"batch_size": 2},
            executor="thread",
            store_results=True,
        )
        final = ms.wait_for_job(job_id, poll_s=0.05)
        outputs = ms.get_job_outputs(job_id)

        assert final.status == "completed"
        assert final.chunks_done == 3
        assert [output["count"] for output in outputs] == [2, 2, 1]
        assert [output["ligand_ids"] for output in outputs] == [[1, 2], [3, 4], [5]]
    finally:
        ms.shutdown()


def test_run_batch_load_respects_runtime_inflight_window(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "simple_api_batch_inflight"

    ms = MolSuite(app_id="amdockvs")
    try:
        ms.settings_manager.update_setting("operational_limits.default_max_inflight_tasks", 3)
        ms.create_or_open_project(
            name="simple_api_batch_inflight",
            folder=project_dir,
            description="simple api batch inflight test",
            activate=True,
        )

        job_id = ms.run(
            name="batched_inflight_job",
            input=[{"ligand_id": idx} for idx in range(1, 25)],
            process=_summarize_batch,
            params={"batch_size": 4},
            executor="thread",
            store_results=True,
        )

        max_live = 0
        deadline = time.time() + 6.0
        while time.time() < deadline:
            with ms.executor_db.get_session() as session:
                rows = session.exec(
                    select(ExecutorJobChunk).where(ExecutorJobChunk.job_id == job_id)
                ).all()
            live = sum(1 for row in rows if row.status in {"pending", "running"})
            max_live = max(max_live, live)
            row = ms.executor_manager.get_job(job_id) if ms.executor_manager is not None else None
            if row is not None and row["status"] == "completed":
                break
            time.sleep(0.02)

        final = ms.wait_for_job(job_id, poll_s=0.05)
        outputs = ms.get_job_outputs(job_id)

        assert final.status == "completed"
        assert final.chunks_done == 6
        assert len(outputs) == 6
        assert max_live <= 3
    finally:
        ms.shutdown()


def test_run_rejects_mixing_spec_with_inline_workflow_arguments(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "simple_api_spec_mix"

    ms = MolSuite(app_id="amdockvs")
    try:
        ms.create_or_open_project(
            name="simple_api_spec_mix",
            folder=project_dir,
            description="simple api spec mix test",
            activate=True,
        )

        spec = workflow(
            name="descriptor_job",
            input=[{"ligand_id": 1}],
            process=lambda payload: payload,
        )

        with pytest.raises(ValueError, match="does not accept inline workflow parameters"):
            ms.run(
                spec=spec,
                input=[{"ligand_id": 2}],
                process=lambda payload: payload,
            )
    finally:
        ms.shutdown()
