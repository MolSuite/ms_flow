import sqlite3
from pathlib import Path

from ms_flow.core.data import DataBridge, DataContext
from ms_flow.query import QuerySpec, db_input_for, db_rows, db_stream
from ms_flow.main import MolSuite


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def test_query_spec_compiles_filters_order_and_limit():
    spec = QuerySpec(
        table="molecules",
        fields=("id", "name"),
        filters={"score__gte": 2.5, "name__contains": "Mol"},
        order=("-score", "id"),
        limit=10,
        offset=5,
    )

    sql, params = spec.compile()
    assert sql == (
        "SELECT id, name FROM molecules "
        "WHERE score >= ? AND name LIKE ? "
        "ORDER BY score DESC, id ASC LIMIT ? OFFSET ?"
    )
    assert params == (2.5, "%Mol%", 10, 5)


def test_db_input_for_builds_query_driven_db_input_spec():
    spec = db_input_for(
        "molecules",
        fields=("id", "name"),
        filters={"id__in": [1, 2]},
        order=("id",),
        limit=2,
    )

    assert spec.query == "SELECT id, name FROM molecules WHERE id IN (?, ?) ORDER BY id ASC LIMIT ?"
    assert spec.params == (1, 2, 2)
    assert spec.db_role == "project"


def test_project_rows_and_stream_use_active_project_without_sqlmodel(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    project_dir = tmp_path / "data_api_project"

    ms = MolSuite(app_id="testdataapi")
    try:
        ms.create_or_open_project(
            name="data_api_project",
            folder=project_dir,
            description="data api test",
            scope="testing",
            activate=True,
        )
        assert ms.project_db is not None
        assert ms.project_db.db_path is not None

        conn = sqlite3.connect(str(ms.project_db.db_path))
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
            cur.execute("INSERT INTO molecules (id, name, score) VALUES (1, 'MolA', 1.5)")
            cur.execute("INSERT INTO molecules (id, name, score) VALUES (2, 'MolB', 2.5)")
            cur.execute("INSERT INTO molecules (id, name, score) VALUES (3, 'LigC', 3.5)")
            conn.commit()
        finally:
            conn.close()

        rows = db_rows(
            ms,
            "molecules",
            fields=("id", "name"),
            filters={"score__gte": 2.0},
            order=("id",),
        )
        assert rows == [{"id": 2, "name": "MolB"}, {"id": 3, "name": "LigC"}]

        streamed = list(
            db_stream(
                ms,
                QuerySpec(
                    table="molecules",
                    fields=("id", "name"),
                    filters={"name__contains": "M"},
                    order=("id",),
                ),
                batch_size=1,
            )
        )
        assert streamed == [{"id": 1, "name": "MolA"}, {"id": 2, "name": "MolB"}]

        bridge = DataBridge()
        materialized = bridge.resolve_input(
            db_input_for(
                "molecules",
                fields=("id", "name"),
                filters={"id": 2},
                limit=1,
            ),
            DataContext(project_db_path=ms.project_db.db_path),
        )
        assert materialized == [{"id": 2, "name": "MolB"}]
    finally:
        ms.shutdown()
