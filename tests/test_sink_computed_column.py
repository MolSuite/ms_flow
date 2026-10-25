"""A generated column (`Computed`) is left to the database: a sink that listed it in its INSERT
failed with "cannot INSERT into generated column"."""
from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, Computed, Integer
from sqlmodel import Field, SQLModel

from ms_flow.core.database import ProjectStore
from ms_flow.sinks import graph_sink, table_sink


class _ComputedRow(SQLModel, table=True):
    __tablename__ = "test_sink_computed_rows"

    id: Optional[int] = Field(default=None, primary_key=True)
    value: int = 0
    doubled: Optional[int] = Field(default=None, sa_column=Column(Integer, Computed("value * 2", persisted=False)))


def test_sinks_never_insert_a_generated_column(tmp_path):
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    store = ProjectStore(project_db)

    store.persist_output_spec(table_sink(model=_ComputedRow, write_mode="bulk"), [{"value": 1}])
    store.persist_output_spec(graph_sink(nodes=({"name": "rows", "model": _ComputedRow},)), {"rows": [{"value": 2}]})

    assert sorted(row.doubled for row in store.select(_ComputedRow)) == [2, 4]
