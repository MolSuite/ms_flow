from __future__ import annotations

from pathlib import Path
from typing import Optional

from sqlalchemy import JSON
from sqlmodel import Field, SQLModel

from ms_flow.core.database import ProjectStore
from ms_flow.core.database.project_records import ProjectCommitKey, ProjectGraphInsertRequest


def test_database_layer_does_not_import_data_backends():
    database_dir = Path(__file__).resolve().parents[1] / "src" / "ms_flow" / "core" / "database"
    offenders = [
        path.relative_to(database_dir)
        for path in database_dir.rglob("*.py")
        if "core.data.backends" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


def test_project_store_is_public_api_not_backend_subclass():
    assert [base.__name__ for base in ProjectStore.__bases__] == ["object"]


class _TestMolecule(SQLModel, table=True):
    __tablename__ = "test_project_store_molecules"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    selected: bool = False
    score: float = 0.0


class _GraphParent(SQLModel, table=True):
    __tablename__ = "test_project_store_graph_parents"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class _GraphChild(SQLModel, table=True):
    __tablename__ = "test_project_store_graph_children"

    id: Optional[int] = Field(default=None, primary_key=True)
    parent_id: int
    name: str
    extra: Optional[dict] = Field(default=None, sa_type=JSON)


def test_project_store_crud_and_filters(tmp_path):
    project_dir = tmp_path / "project_store_case"
    project_db = ProjectStore()
    project_db.connect(project_dir)
    store = ProjectStore(project_db)

    first = store.insert(_TestMolecule(name="mol-a", selected=True, score=1.5))
    assert first.id is not None

    store.insert_batch(
        [
            _TestMolecule(name="mol-b", selected=False, score=2.5),
            _TestMolecule(name="mol-c", selected=False, score=3.5),
        ]
    )

    all_rows = store.select(_TestMolecule, order=("id",))
    assert [row.name for row in all_rows] == ["mol-a", "mol-b", "mol-c"]
    assert store.count(_TestMolecule) == 3

    selected_rows = store.select(_TestMolecule, filters={"selected": True})
    assert len(selected_rows) == 1
    assert selected_rows[0].name == "mol-a"

    first_high_score = store.first(_TestMolecule, filters={"score__gt": 2.0}, order=("score",))
    assert first_high_score is not None
    assert first_high_score.name == "mol-b"

    deleted = store.delete(_TestMolecule, filters={"selected": False})
    assert deleted == 2
    assert store.count(_TestMolecule) == 1


def test_project_store_high_level_filters_rows_and_keyset(tmp_path):
    project_dir = tmp_path / "project_store_high_level"
    project_db = ProjectStore()
    project_db.connect(project_dir)
    store = ProjectStore(project_db)

    store.insert_batch(
        [
            _TestMolecule(name="mol-a", selected=True, score=1.0),
            _TestMolecule(name="mol-b", selected=True, score=3.0),
            _TestMolecule(name="mol-c", selected=False, score=2.0),
            _TestMolecule(name="mol-d", selected=True, score=4.0),
        ]
    )

    top_selected = store.select_rows(
        _TestMolecule,
        fields=("id", "name", "score"),
        filters={"selected": True, "score__gt": 1.5},
        order=("-score",),
        limit=2,
    )
    assert [row["name"] for row in top_selected] == ["mol-d", "mol-b"]
    assert all("selected" not in row for row in top_selected)

    streamed = list(
        store.stream_rows(
            _TestMolecule,
            fields=("id", "name"),
            filters={"selected": True},
            order=("id",),
            yield_per=2,
        )
    )
    assert [row["name"] for row in streamed] == ["mol-a", "mol-b", "mol-d"]

    page1, cursor = store.page_after(
        _TestMolecule,
        cursor_field="id",
        after=None,
        limit=2,
    )
    assert len(page1) == 2
    assert cursor is not None

    page2, cursor2 = store.page_after(
        _TestMolecule,
        cursor_field="id",
        after=cursor,
        limit=10,
    )
    assert len(page2) >= 1
    assert cursor2 is not None

    keyset_rows = list(
        store.stream_keyset(
            _TestMolecule,
            cursor_field="id",
            batch_size=2,
            fields=("id", "name"),
        )
    )
    assert [row["name"] for row in keyset_rows] == ["mol-a", "mol-b", "mol-c", "mol-d"]

    updated = store.update_rows(
        _TestMolecule,
        filters={"name__in": ["mol-a", "mol-c"]},
        values={"selected": False},
    )
    assert updated == 2
    assert store.count(_TestMolecule, filters={"selected": True}) == 2


def test_project_store_ops_caches_graph_model_resolution(tmp_path, monkeypatch):
    project_dir = tmp_path / "project_store_graph_cache"
    project_db = ProjectStore()
    project_db.connect(project_dir)
    store = project_db

    SQLModel.metadata.create_all(project_db.engine, tables=[_GraphParent.__table__, _GraphChild.__table__])

    model_calls = {"count": 0}
    original_resolve_model = store._ops._resolve_model

    def _counting_resolve_model(model_path: str):
        model_calls["count"] += 1
        return original_resolve_model(model_path)

    monkeypatch.setattr(store._ops, "_resolve_model", _counting_resolve_model)

    graph_meta = {
        "graph": {
            "nodes": [
                {
                    "name": "parents",
                    "table": _GraphParent.__tablename__,
                    "model_path": "test_project_store:_GraphParent",
                    "columns": ["name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
                {
                    "name": "children",
                    "table": _GraphChild.__tablename__,
                    "model_path": "test_project_store:_GraphChild",
                    "columns": ["parent_id", "name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
            ],
            "relations": [
                {
                    "source_node": "children",
                    "source_ref_field": "parent_ref",
                    "target_node": "parents",
                    "fk_field": "parent_id",
                    "target_ref_field": "$ref",
                }
            ],
        }
    }

    result1 = store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p1", "name": "parent-1"}],
                "children": [{"$ref": "c1", "parent_ref": "p1", "name": "child-1"}],
            },
            commit_key=ProjectCommitKey(sink_key="sink-graph", commit_key="commit-1"),
        )
    )
    result2 = store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p2", "name": "parent-2"}],
                "children": [{"$ref": "c2", "parent_ref": "p2", "name": "child-2"}],
            },
            commit_key=ProjectCommitKey(sink_key="sink-graph", commit_key="commit-2"),
        )
    )

    assert result1.rows_written == 2
    assert result2.rows_written == 2
    assert model_calls["count"] == 2


def test_insert_graph_leaf_skips_returning_but_resolves_fk(tmp_path):
    """A leaf/source node takes the executemany path (no RETURNING); a FK-target
    node keeps RETURNING so the child still resolves its FK to the real parent id.
    Guards the leaf-skip optimization in insert_graph.
    """
    project_dir = tmp_path / "project_store_graph_leaf"
    store = ProjectStore()
    store.connect(project_dir)
    SQLModel.metadata.create_all(store.engine, tables=[_GraphParent.__table__, _GraphChild.__table__])

    graph_meta = {
        "graph": {
            "nodes": [
                {
                    "name": "parents",
                    "table": _GraphParent.__tablename__,
                    "model_path": "test_project_store:_GraphParent",
                    "columns": ["name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
                {
                    "name": "children",
                    "table": _GraphChild.__tablename__,
                    "model_path": "test_project_store:_GraphChild",
                    "columns": ["parent_id", "name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
            ],
            "relations": [
                {
                    "source_node": "children",
                    "source_ref_field": "parent_ref",
                    "target_node": "parents",
                    "fk_field": "parent_id",
                    "target_ref_field": "$ref",
                }
            ],
        }
    }
    result = store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p1", "name": "parent-1"}],
                "children": [
                    {"$ref": "c1", "parent_ref": "p1", "name": "child-1"},
                    {"$ref": "c2", "parent_ref": "p1", "name": "child-2"},
                ],
            },
        )
    )
    assert result.rows_written == 3

    with store.get_session() as session:
        conn = session.connection()
        parent_id = conn.exec_driver_sql(
            f"SELECT id FROM {_GraphParent.__tablename__} WHERE name = 'parent-1'"
        ).fetchone()[0]
        child_parent_ids = [
            row[0]
            for row in conn.exec_driver_sql(
                f"SELECT parent_id FROM {_GraphChild.__tablename__} ORDER BY name"
            ).fetchall()
        ]
    store.disconnect()

    # Both children (leaf executemany path) resolved their FK to the parent's
    # real RETURNING-captured id.
    assert child_parent_ids == [parent_id, parent_id]


def test_insert_graph_leaf_serializes_json_columns(tmp_path):
    """A leaf node with a JSON column must serialize dict/list values on the
    executemany path, matching Core's typed insert. Regression for the leaf-skip
    optimization binding a raw dict -> "type 'dict' is not supported".
    """
    project_dir = tmp_path / "project_store_graph_leaf_json"
    store = ProjectStore()
    store.connect(project_dir)
    SQLModel.metadata.create_all(store.engine, tables=[_GraphParent.__table__, _GraphChild.__table__])

    graph_meta = {
        "graph": {
            "nodes": [
                {"name": "parents", "table": _GraphParent.__tablename__,
                 "model_path": "test_project_store:_GraphParent", "columns": ["name"],
                 "pk_field": "id", "ref_field": "$ref"},
                {"name": "children", "table": _GraphChild.__tablename__,
                 "model_path": "test_project_store:_GraphChild",
                 "columns": ["parent_id", "name", "extra"], "pk_field": "id", "ref_field": "$ref"},
            ],
            "relations": [
                {"source_node": "children", "source_ref_field": "parent_ref",
                 "target_node": "parents", "fk_field": "parent_id", "target_ref_field": "$ref"}
            ],
        }
    }
    store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p1", "name": "parent-1"}],
                "children": [
                    {"$ref": "c1", "parent_ref": "p1", "name": "with-dict",
                     "extra": {"component_class": "ligand", "selector": "JXM:A:1"}},
                    {"$ref": "c2", "parent_ref": "p1", "name": "with-none", "extra": None},
                ],
            },
        )
    )

    with store.get_session() as session:
        conn = session.connection()
        rows = conn.exec_driver_sql(
            f"SELECT name, extra FROM {_GraphChild.__tablename__} ORDER BY name"
        ).fetchall()
    store.disconnect()

    by_name = {name: extra for name, extra in rows}
    # dict was serialized to JSON text (not bound raw); None -> JSON 'null', exactly
    # what Core's JSON bind processor does (so the leaf path matches the RETURNING
    # path and reads back as None via the ORM json result processor).
    assert by_name["with-dict"] == '{"component_class": "ligand", "selector": "JXM:A:1"}'
    assert by_name["with-none"] == "null"


def test_project_store_ops_caches_graph_table_reflection(tmp_path, monkeypatch):
    project_dir = tmp_path / "project_store_graph_table_cache"
    project_db = ProjectStore()
    project_db.connect(project_dir)
    store = project_db

    SQLModel.metadata.create_all(project_db.engine, tables=[_GraphParent.__table__, _GraphChild.__table__])

    table_calls = {"count": 0}
    original_resolve_table = store._ops._resolve_graph_table

    def _counting_resolve_table(conn, node, model):
        table_calls["count"] += 1
        return original_resolve_table(conn, node, model)

    monkeypatch.setattr(store._ops, "_resolve_graph_table", _counting_resolve_table)

    graph_meta = {
        "graph": {
            "nodes": [
                {
                    "name": "parents",
                    "table": _GraphParent.__tablename__,
                    "columns": ["name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
                {
                    "name": "children",
                    "table": _GraphChild.__tablename__,
                    "columns": ["parent_id", "name"],
                    "pk_field": "id",
                    "ref_field": "$ref",
                },
            ],
            "relations": [
                {
                    "source_node": "children",
                    "source_ref_field": "parent_ref",
                    "target_node": "parents",
                    "fk_field": "parent_id",
                    "target_ref_field": "$ref",
                }
            ],
        }
    }

    result1 = store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p1", "name": "parent-1"}],
                "children": [{"$ref": "c1", "parent_ref": "p1", "name": "child-1"}],
            },
            commit_key=ProjectCommitKey(sink_key="sink-graph", commit_key="commit-1"),
        )
    )
    result2 = store.insert_graph(
        request=ProjectGraphInsertRequest(
            graph_meta=graph_meta,
            payload={
                "parents": [{"$ref": "p2", "name": "parent-2"}],
                "children": [{"$ref": "c2", "parent_ref": "p2", "name": "child-2"}],
            },
            commit_key=ProjectCommitKey(sink_key="sink-graph", commit_key="commit-2"),
        )
    )

    assert result1.rows_written == 2
    assert result2.rows_written == 2
    assert table_calls["count"] == 2
