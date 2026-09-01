import json
import sqlite3
from pathlib import Path

from ms_flow.api import MolSuite, file_sink, graph_sink, streaming_job, table_sink


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _sink_run_chunk(payload: dict):
    return {"ligand_id": int(payload["ligand_id"]), "score": float(payload["score"])}


def _sink_chunker(params: dict, _config: dict):
    start = int(params["start"])
    count = int(params["count"])
    for ligand_id in range(start, start + count):
        yield {"ligand_id": ligand_id, "score": float(ligand_id) * 0.5}


def test_table_sink_helper_builds_project_db_output_spec():
    sink = table_sink("results", columns=("ligand_id", "score"))

    assert sink.table == "results"
    assert sink.columns == ("ligand_id", "score")
    assert sink.db_role == "project"


def test_file_sink_helper_builds_project_json_output_spec():
    sink = file_sink("results.json")

    assert sink.path == "results.json"
    assert sink.root == "project"
    assert sink.fmt == "json"


def test_graph_sink_helper_builds_project_graph_output_spec():
    sink = graph_sink(
        nodes=(
            {"name": "molecules", "table": "molecules", "columns": ("name", "kind")},
            {"name": "ligands", "table": "ligands", "columns": ("molecule_id", "active")},
        ),
        relations=(
            {
                "source_node": "ligands",
                "source_ref_field": "molecule_ref",
                "target_node": "molecules",
                "fk_field": "molecule_id",
            },
        ),
    )

    assert sink.db_role == "project"
    assert sink.mode == "graph"
    assert sink.meta["graph"]["nodes"][0]["name"] == "molecules"
    assert sink.meta["graph"]["relations"][0]["fk_field"] == "molecule_id"


def test_streaming_job_persists_json_file_sink(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "sink_project"

    ms = MolSuite(app_id="testsinks")
    try:
        ms.create_or_open_project(
            name="sink_project",
            folder=project_dir,
            description="output helper test",
            scope="testing",
            activate=True,
        )

        job_def = streaming_job(
            name="sink_file_job",
            run_chunk=_sink_run_chunk,
            chunker=_sink_chunker,
            output=file_sink("results.json"),
            flush_every=2,
            store_results=False,
        )

        job_id = ms.submit_job(job_def, params={"start": 1, "count": 4})
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 4

        written = json.loads((project_dir / "results.json").read_text(encoding="utf-8"))
        assert sorted(written, key=lambda item: int(item["ligand_id"])) == [
            {"ligand_id": 1, "score": 0.5},
            {"ligand_id": 2, "score": 1.0},
            {"ligand_id": 3, "score": 1.5},
            {"ligand_id": 4, "score": 2.0},
        ]
    finally:
        ms.shutdown()


def test_streaming_job_persists_table_sink(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "table_sink_project"

    ms = MolSuite(app_id="testsinks")
    try:
        ms.create_or_open_project(
            name="table_sink_project",
            folder=project_dir,
            description="table sink helper test",
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

        job_def = streaming_job(
            name="sink_table_job",
            run_chunk=_sink_run_chunk,
            chunker=_sink_chunker,
            output=table_sink("docking_results", columns=("ligand_id", "score")),
            flush_every=2,
            store_results=False,
        )

        job_id = ms.submit_job(job_def, params={"start": 1, "count": 3})
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 3

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT ligand_id, score FROM docking_results ORDER BY ligand_id ASC")
            rows = cur.fetchall()
        finally:
            conn.close()
        assert rows == [(1, 0.5), (2, 1.0), (3, 1.5)]
    finally:
        ms.shutdown()


def _graph_chunker(_params: dict, _config: dict):
    yield {"index": 1}
    yield {"index": 2}


def _graph_run_chunk(payload: dict):
    index = int(payload["index"])
    return {
        "molecules": [
            {"$ref": f"mol_rec_{index}", "name": f"rec-{index}", "kind": "protein"},
            {"$ref": f"mol_lig_{index}", "name": f"lig-{index}", "kind": "small_molecule"},
        ],
        "receptors": [
            {"$ref": f"rec_{index}", "molecule_ref": f"mol_rec_{index}", "source": "import"},
        ],
        "ligands": [
            {"$ref": f"lig_{index}", "molecule_ref": f"mol_lig_{index}", "active": index % 2},
        ],
        "complexes": [
            {
                "$ref": f"cx_{index}",
                "receptor_ref": f"rec_{index}",
                "ligand_ref": f"lig_{index}",
                "label": f"complex-{index}",
            },
        ],
    }


def test_streaming_job_persists_graph_sink(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "graph_sink_project"

    ms = MolSuite(app_id="testsinks")
    try:
        ms.create_or_open_project(
            name="graph_sink_project",
            folder=project_dir,
            description="graph sink helper test",
            scope="testing",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None
        project_db_path = ms.project_db.db_path

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, kind TEXT)")
            cur.execute("CREATE TABLE receptors (id INTEGER PRIMARY KEY, molecule_id INTEGER, source TEXT)")
            cur.execute("CREATE TABLE ligands (id INTEGER PRIMARY KEY, molecule_id INTEGER, active INTEGER)")
            cur.execute("CREATE TABLE complexes (id INTEGER PRIMARY KEY, receptor_id INTEGER, ligand_id INTEGER, label TEXT)")
            conn.commit()
        finally:
            conn.close()

        job_def = streaming_job(
            name="sink_graph_job",
            run_chunk=_graph_run_chunk,
            chunker=_graph_chunker,
            output=graph_sink(
                nodes=(
                    {"name": "molecules", "table": "molecules", "columns": ("name", "kind")},
                    {"name": "receptors", "table": "receptors", "columns": ("molecule_id", "source")},
                    {"name": "ligands", "table": "ligands", "columns": ("molecule_id", "active")},
                    {"name": "complexes", "table": "complexes", "columns": ("receptor_id", "ligand_id", "label")},
                ),
                relations=(
                    {
                        "source_node": "receptors",
                        "source_ref_field": "molecule_ref",
                        "target_node": "molecules",
                        "fk_field": "molecule_id",
                    },
                    {
                        "source_node": "ligands",
                        "source_ref_field": "molecule_ref",
                        "target_node": "molecules",
                        "fk_field": "molecule_id",
                    },
                    {
                        "source_node": "complexes",
                        "source_ref_field": "receptor_ref",
                        "target_node": "receptors",
                        "fk_field": "receptor_id",
                    },
                    {
                        "source_node": "complexes",
                        "source_ref_field": "ligand_ref",
                        "target_node": "ligands",
                        "fk_field": "ligand_id",
                    },
                ),
            ),
            flush_every=1,
            store_results=False,
        )

        job_id = ms.submit_job(job_def, params={})
        final = ms.wait_for_job(job_id, poll_s=0.05)
        assert final["status"] == "completed"
        assert final["chunks_done"] == 2

        conn = sqlite3.connect(str(project_db_path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, name, kind FROM molecules ORDER BY id ASC")
            molecules = cur.fetchall()
            cur.execute("SELECT id, molecule_id, source FROM receptors ORDER BY id ASC")
            receptors = cur.fetchall()
            cur.execute("SELECT id, molecule_id, active FROM ligands ORDER BY id ASC")
            ligands = cur.fetchall()
            cur.execute("SELECT id, receptor_id, ligand_id, label FROM complexes ORDER BY id ASC")
            complexes = cur.fetchall()
        finally:
            conn.close()

        # Two chunks persist in parallel, so autoincrement ids depend on which
        # chunk commits first (nondeterministic). Assert content + referential
        # integrity by resolving FKs through the molecule name, not by literal id.
        mol_by_id = {mid: (name, kind) for mid, name, kind in molecules}
        assert {(name, kind) for _, name, kind in molecules} == {
            ("rec-1", "protein"),
            ("lig-1", "small_molecule"),
            ("rec-2", "protein"),
            ("lig-2", "small_molecule"),
        }
        assert {mol_by_id[m][0] for _, m, _ in receptors} == {"rec-1", "rec-2"}
        assert all(source == "import" for _, _, source in receptors)
        assert {mol_by_id[m][0]: active for _, m, active in ligands} == {"lig-1": 1, "lig-2": 0}
        rec_mol = {rid: mol_by_id[m][0] for rid, m, _ in receptors}
        lig_mol = {lid: mol_by_id[m][0] for lid, m, _ in ligands}
        assert {(rec_mol[r], lig_mol[l], label) for _, r, l, label in complexes} == {
            ("rec-1", "lig-1", "complex-1"),
            ("rec-2", "lig-2", "complex-2"),
        }
    finally:
        ms.shutdown()
