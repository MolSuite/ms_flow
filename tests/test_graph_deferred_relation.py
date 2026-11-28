"""A FK pointing "backwards" (parent -> child) is closed with an UPDATE at the end.

Without `deferred` the case had only two bad outcomes: declare the relation and have the
topological order detect a cycle, or invent a stable per-parent index in the child table so the
row can be pointed at before its id exists — exactly the `bs_index` DemoApp used to drag around,
with its UNIQUE and its slot reservation. The test pins both halves: the direct relation still
resolves the child's FK, and the deferred one leaves the parent pointing at the right child
(not the first, not the last).
"""
import sqlite3
from pathlib import Path

import pytest

from ms_flow.api import Workspace, graph_sink, job, task


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


@task()
def _run_chunk(payload: dict):
    index = int(payload["index"])
    # Three sites per specimen; the active one is the middle one, so a "take the first" or
    # "take the last" bug cannot pass the test by accident.
    return {
        "specimens": [
            {"$ref": f"spc_{index}", "name": f"rec-{index}", "active_site_ref": f"site_{index}_b"},
        ],
        "spots": [
            {"$ref": f"site_{index}_{tag}", "specimen_ref": f"spc_{index}", "label": f"{index}-{tag}"}
            for tag in ("a", "b", "c")
        ],
    }


def _sink():
    return graph_sink(
        nodes=(
            {"name": "specimens", "table": "specimens", "columns": ("name", "active_site_id")},
            {"name": "spots", "table": "spots", "columns": ("specimen_id", "label")},
        ),
        relations=(
            {
                "source_node": "spots",
                "source_ref_field": "specimen_ref",
                "target_node": "specimens",
                "fk_field": "specimen_id",
            },
            {
                "source_node": "specimens",
                "source_ref_field": "active_site_ref",
                "target_node": "spots",
                "fk_field": "active_site_id",
                "deferred": True,
            },
        ),
    )


@job(task=_run_chunk, output=_sink())
def _deferred_graph_job(params, project):
    yield {"index": 1}
    yield {"index": 2}


def test_deferred_relation_points_the_parent_at_its_own_child(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = Workspace(app_id="testdeferred")
    try:
        ms.create_or_open_project(
            name="deferred_project",
            folder=tmp_path / "deferred_project",
            description="deferred graph relation",
            scope="testing",
            activate=True,
        )
        db_path = ms.project_db.db_path
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE specimens (id INTEGER PRIMARY KEY, name TEXT, active_site_id INTEGER)")
            conn.execute("CREATE TABLE spots (id INTEGER PRIMARY KEY, specimen_id INTEGER, label TEXT)")
            conn.commit()
        finally:
            conn.close()

        assert ms.engine.wait(ms.submit(_deferred_graph_job), 60).status == "completed"

        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute(
                "SELECT m.name, s.label, s.specimen_id = m.id "
                "FROM specimens m JOIN spots s ON s.id = m.active_site_id "
                "ORDER BY m.name"
            ).fetchall()
            total_sites = conn.execute("SELECT COUNT(*) FROM spots").fetchone()[0]
        finally:
            conn.close()

        assert total_sites == 6
        # The active one is the middle one and belongs to its own parent.
        assert rows == [("rec-1", "1-b", 1), ("rec-2", "2-b", 1)]
    finally:
        ms.shutdown()


def test_a_backwards_relation_without_deferred_is_still_a_cycle():
    from ms_flow.core.database.project_store_ops import _ProjectStoreSqliteOps

    relations = [dict(relation) for relation in _sink().meta["graph"]["relations"]]
    with pytest.raises(Exception, match="cycle"):
        _ProjectStoreSqliteOps._graph_topological_order(
            ["specimens", "spots"], relations
        )
