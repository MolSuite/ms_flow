import sqlite3
from contextlib import closing
from pathlib import Path

from ms_flow.api import MolSuite, file_sink, graph_sink, job, table_sink, task


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _sql(db_path, *statements):
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        return [conn.execute(statement).fetchall() for statement in statements]


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


GRAPH = graph_sink(
    nodes=(
        {"name": "molecules", "table": "molecules", "columns": ("name", "kind")},
        {"name": "receptors", "table": "receptors", "columns": ("molecule_id", "source")},
        {"name": "ligands", "table": "ligands", "columns": ("molecule_id", "active")},
        {"name": "complexes", "table": "complexes", "columns": ("receptor_id", "ligand_id", "label")},
    ),
    relations=(
        {"source_node": "receptors", "source_ref_field": "molecule_ref", "target_node": "molecules",
         "fk_field": "molecule_id"},
        {"source_node": "ligands", "source_ref_field": "molecule_ref", "target_node": "molecules",
         "fk_field": "molecule_id"},
        {"source_node": "complexes", "source_ref_field": "receptor_ref", "target_node": "receptors",
         "fk_field": "receptor_id"},
        {"source_node": "complexes", "source_ref_field": "ligand_ref", "target_node": "ligands",
         "fk_field": "ligand_id"},
    ),
)


def test_graph_sink_helper_builds_project_graph_output_spec():
    assert GRAPH.db_role == "project"
    assert GRAPH.mode == "graph"
    assert GRAPH.meta["graph"]["nodes"][0]["name"] == "molecules"
    assert GRAPH.meta["graph"]["relations"][0]["fk_field"] == "molecule_id"


@task()
def _score(payload: dict):
    return {"ligand_id": int(payload["ligand_id"]), "score": float(payload["score"])}


@job(task=_score, output=table_sink("docking_results", columns=("ligand_id", "score")))
def _table_job(params, project):
    for ligand_id in range(params["start"], params["start"] + params["count"]):
        yield {"ligand_id": ligand_id, "score": float(ligand_id) * 0.5}


@task()
def _complex(index: int):
    return {
        "molecules": [
            {"$ref": f"mol_rec_{index}", "name": f"rec-{index}", "kind": "protein"},
            {"$ref": f"mol_lig_{index}", "name": f"lig-{index}", "kind": "small_molecule"},
        ],
        "receptors": [{"$ref": f"rec_{index}", "molecule_ref": f"mol_rec_{index}", "source": "import"}],
        "ligands": [{"$ref": f"lig_{index}", "molecule_ref": f"mol_lig_{index}", "active": index % 2}],
        "complexes": [
            {"$ref": f"cx_{index}", "receptor_ref": f"rec_{index}", "ligand_ref": f"lig_{index}",
             "label": f"complex-{index}"},
        ],
    }


@job(task=_complex, output=GRAPH)
def _graph_job(params, project):
    yield from (1, 2)


def test_jobs_persist_through_table_and_graph_sinks(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = MolSuite(app_id="testsinks")
    try:
        ms.create_or_open_project(name="sinks", folder=tmp_path / "sinks", description="", scope="testing",
                                  activate=True)
        db = ms.project_db.db_path
        _sql(db,
             "CREATE TABLE docking_results (ligand_id INTEGER, score REAL)",
             "CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, kind TEXT)",
             "CREATE TABLE receptors (id INTEGER PRIMARY KEY, molecule_id INTEGER, source TEXT)",
             "CREATE TABLE ligands (id INTEGER PRIMARY KEY, molecule_id INTEGER, active INTEGER)",
             "CREATE TABLE complexes (id INTEGER PRIMARY KEY, receptor_id INTEGER, ligand_id INTEGER, label TEXT)")

        final = ms.engine.wait(ms.submit(_table_job, {"start": 1, "count": 3}), 60)
        assert (final.status, final.chunks_done) == ("completed", 3)
        assert _sql(db, "SELECT ligand_id, score FROM docking_results ORDER BY ligand_id") == [
            [(1, 0.5), (2, 1.0), (3, 1.5)]]

        final = ms.engine.wait(ms.submit(_graph_job), 60)
        assert (final.status, final.chunks_done) == ("completed", 2)
        # Ids depend on which element is written first: follow the keys through the names.
        assert set(_sql(db, "SELECT r.source, mr.name, l.active, ml.name, c.label FROM complexes c "
                            "JOIN receptors r ON r.id = c.receptor_id JOIN molecules mr ON mr.id = r.molecule_id "
                            "JOIN ligands l ON l.id = c.ligand_id JOIN molecules ml ON ml.id = l.molecule_id")[0]) == {
            ("import", "rec-1", 1, "lig-1", "complex-1"), ("import", "rec-2", 0, "lig-2", "complex-2")}
        assert _sql(db, "SELECT count(*) FROM molecules") == [[(4,)]]
    finally:
        ms.shutdown()
