"""A FK pointing "backwards" (parent -> child) is closed with an UPDATE at the end.

Without `deferred` the case had only two bad outcomes: declare the relation and have the
topological order detect a cycle, or invent a stable per-parent index in the child table so the
row can be pointed at before its id exists — exactly the `bs_index` AMDock used to drag around,
with its UNIQUE and its slot reservation. The test pins both halves: the direct relation still
resolves the child's FK, and the deferred one leaves the parent pointing at the right child
(not the first, not the last).
"""
import sqlite3
from pathlib import Path

import pytest

from ms_flow.api import MolSuite, graph_sink, streaming_job


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def _chunker(_params: dict, _config: dict):
    yield {"index": 1}
    yield {"index": 2}


def _run_chunk(payload: dict):
    index = int(payload["index"])
    # Three sites per molecule; the active one is the middle one, so a "take the first" or
    # "take the last" bug cannot pass the test by accident.
    return {
        "molecules": [
            {"$ref": f"mol_{index}", "name": f"rec-{index}", "active_site_ref": f"site_{index}_b"},
        ],
        "binding_sites": [
            {"$ref": f"site_{index}_{tag}", "molecule_ref": f"mol_{index}", "label": f"{index}-{tag}"}
            for tag in ("a", "b", "c")
        ],
    }


def _sink():
    return graph_sink(
        nodes=(
            {"name": "molecules", "table": "molecules", "columns": ("name", "active_site_id")},
            {"name": "binding_sites", "table": "binding_sites", "columns": ("molecule_id", "label")},
        ),
        relations=(
            {
                "source_node": "binding_sites",
                "source_ref_field": "molecule_ref",
                "target_node": "molecules",
                "fk_field": "molecule_id",
            },
            {
                "source_node": "molecules",
                "source_ref_field": "active_site_ref",
                "target_node": "binding_sites",
                "fk_field": "active_site_id",
                "deferred": True,
            },
        ),
    )


def test_deferred_relation_points_the_parent_at_its_own_child(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    ms = MolSuite(app_id="testdeferred")
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
            conn.execute("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT, active_site_id INTEGER)")
            conn.execute("CREATE TABLE binding_sites (id INTEGER PRIMARY KEY, molecule_id INTEGER, label TEXT)")
            conn.commit()
        finally:
            conn.close()

        job_id = ms.submit_job(
            streaming_job(
                name="deferred_graph_job",
                run_chunk=_run_chunk,
                chunker=_chunker,
                output=_sink(),
                flush_every=1,
                store_results=False,
            ),
            params={},
        )
        assert ms.wait_for_job(job_id, poll_s=0.05)["status"] == "completed"

        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute(
                "SELECT m.name, s.label, s.molecule_id = m.id "
                "FROM molecules m JOIN binding_sites s ON s.id = m.active_site_id "
                "ORDER BY m.name"
            ).fetchall()
            total_sites = conn.execute("SELECT COUNT(*) FROM binding_sites").fetchone()[0]
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
            ["molecules", "binding_sites"], relations
        )
