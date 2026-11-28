import sqlite3
from contextlib import closing
from pathlib import Path

from ms_flow.api import Workspace, file_sink, graph_sink, job, table_sink, task


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _sql(db_path, *statements):
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        return [conn.execute(statement).fetchall() for statement in statements]


def test_table_sink_helper_builds_project_db_output_spec():
    sink = table_sink("results", columns=("guest_id", "score"))

    assert sink.table == "results"
    assert sink.columns == ("guest_id", "score")
    assert sink.db_role == "project"


def test_file_sink_helper_builds_project_json_output_spec():
    sink = file_sink("results.json")

    assert sink.path == "results.json"
    assert sink.root == "project"
    assert sink.fmt == "json"


GRAPH = graph_sink(
    nodes=(
        {"name": "specimens", "table": "specimens", "columns": ("name", "kind")},
        {"name": "anchors", "table": "anchors", "columns": ("specimen_id", "source")},
        {"name": "guests", "table": "guests", "columns": ("specimen_id", "active")},
        {"name": "complexes", "table": "complexes", "columns": ("anchor_id", "guest_id", "label")},
    ),
    relations=(
        {"source_node": "anchors", "source_ref_field": "specimen_ref", "target_node": "specimens",
         "fk_field": "specimen_id"},
        {"source_node": "guests", "source_ref_field": "specimen_ref", "target_node": "specimens",
         "fk_field": "specimen_id"},
        {"source_node": "complexes", "source_ref_field": "anchor_ref", "target_node": "anchors",
         "fk_field": "anchor_id"},
        {"source_node": "complexes", "source_ref_field": "guest_ref", "target_node": "guests",
         "fk_field": "guest_id"},
    ),
)


def test_graph_sink_helper_builds_project_graph_output_spec():
    assert GRAPH.db_role == "project"
    assert GRAPH.mode == "graph"
    assert GRAPH.meta["graph"]["nodes"][0]["name"] == "specimens"
    assert GRAPH.meta["graph"]["relations"][0]["fk_field"] == "specimen_id"


@task()
def _score(payload: dict):
    return {"guest_id": int(payload["guest_id"]), "score": float(payload["score"])}


@job(task=_score, output=table_sink("coupling_results", columns=("guest_id", "score")))
def _table_job(params, project):
    for guest_id in range(params["start"], params["start"] + params["count"]):
        yield {"guest_id": guest_id, "score": float(guest_id) * 0.5}


@task()
def _complex(index: int):
    return {
        "specimens": [
            {"$ref": f"spc_rec_{index}", "name": f"rec-{index}", "kind": "large"},
            {"$ref": f"spc_gst_{index}", "name": f"gst-{index}", "kind": "small"},
        ],
        "anchors": [{"$ref": f"rec_{index}", "specimen_ref": f"spc_rec_{index}", "source": "import"}],
        "guests": [{"$ref": f"gst_{index}", "specimen_ref": f"spc_gst_{index}", "active": index % 2}],
        "complexes": [
            {"$ref": f"cx_{index}", "anchor_ref": f"rec_{index}", "guest_ref": f"gst_{index}",
             "label": f"pair-{index}"},
        ],
    }


@job(task=_complex, output=GRAPH)
def _graph_job(params, project):
    yield from (1, 2)


def test_jobs_persist_through_table_and_graph_sinks(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = Workspace(app_id="testsinks")
    try:
        ms.create_or_open_project(name="sinks", folder=tmp_path / "sinks", description="", scope="testing",
                                  activate=True)
        db = ms.project_db.db_path
        _sql(db,
             "CREATE TABLE coupling_results (guest_id INTEGER, score REAL)",
             "CREATE TABLE specimens (id INTEGER PRIMARY KEY, name TEXT, kind TEXT)",
             "CREATE TABLE anchors (id INTEGER PRIMARY KEY, specimen_id INTEGER, source TEXT)",
             "CREATE TABLE guests (id INTEGER PRIMARY KEY, specimen_id INTEGER, active INTEGER)",
             "CREATE TABLE complexes (id INTEGER PRIMARY KEY, anchor_id INTEGER, guest_id INTEGER, label TEXT)")

        final = ms.engine.wait(ms.submit(_table_job, {"start": 1, "count": 3}), 60)
        assert (final.status, final.chunks_done) == ("completed", 3)
        assert _sql(db, "SELECT guest_id, score FROM coupling_results ORDER BY guest_id") == [
            [(1, 0.5), (2, 1.0), (3, 1.5)]]

        final = ms.engine.wait(ms.submit(_graph_job), 60)
        assert (final.status, final.chunks_done) == ("completed", 2)
        # Ids depend on which element is written first: follow the keys through the names.
        assert set(_sql(db, "SELECT r.source, mr.name, l.active, ml.name, c.label FROM complexes c "
                            "JOIN anchors r ON r.id = c.anchor_id JOIN specimens mr ON mr.id = r.specimen_id "
                            "JOIN guests l ON l.id = c.guest_id JOIN specimens ml ON ml.id = l.specimen_id")[0]) == {
            ("import", "rec-1", 1, "gst-1", "pair-1"), ("import", "rec-2", 0, "gst-2", "pair-2")}
        assert _sql(db, "SELECT count(*) FROM specimens") == [[(4,)]]
    finally:
        ms.shutdown()
