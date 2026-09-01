"""A defaulted column the row does not carry is written with its default, not NULL.

The sink writes dicts, not model instances: `default_factory` never runs. Previously, when each
row was flattened column by column, a missing key became an **explicit** NULL that also overrode
the column's default — a NOT NULL `created_at` blew up the whole insert. Both write paths are
covered, because there are two: `bulk` (executemany over tuples) and leaf-node `insert` of a graph.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel

from ms_flow.core.database import ProjectStore
from ms_flow.sinks import graph_sink, table_sink


class _DefaultsRow(SQLModel, table=True):
    __tablename__ = "test_sink_column_defaults_rows"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    created_at: datetime = Field(default_factory=datetime.now)
    tag: str = "auto"


def _store(tmp_path) -> ProjectStore:
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    return ProjectStore(project_db)


def test_bulk_write_fills_missing_defaults(tmp_path):
    store = _store(tmp_path)

    store.persist_output_spec(
        table_sink(model=_DefaultsRow, write_mode="bulk"),
        [{"name": "a"}, {"name": "b", "tag": "explicit"}],
    )

    rows = {row.name: row for row in store.select(_DefaultsRow)}
    assert rows["a"].created_at is not None
    assert rows["a"].tag == "auto"
    assert rows["b"].tag == "explicit"


def test_graph_write_fills_missing_defaults(tmp_path):
    store = _store(tmp_path)

    store.persist_output_spec(
        graph_sink(nodes=({"name": "rows", "model": _DefaultsRow},)),
        {"rows": [{"name": "c"}]},
    )

    row = next(iter(store.select(_DefaultsRow)))
    assert row.created_at is not None
    assert row.tag == "auto"
