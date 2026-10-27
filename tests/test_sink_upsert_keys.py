"""An upsert sink's default columns carry its conflict keys.

The model default leaves primary keys out, so a composite-PK upsert used to emit rows without
the keys its ON CONFLICT names and failed with "missing conflict key".
"""
from __future__ import annotations

from sqlmodel import Field, SQLModel

from ms_flow.core.database import ProjectStore
from ms_flow.sinks import graph_sink, table_sink


class _PairRow(SQLModel, table=True):
    __tablename__ = "test_sink_upsert_keys_rows"

    shard_id: int = Field(primary_key=True)
    family: str = Field(primary_key=True)
    n_records: int = 0


def _store(tmp_path) -> ProjectStore:
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    return ProjectStore(project_db)


def test_upsert_default_columns_include_the_conflict_keys(tmp_path):
    store = _store(tmp_path)
    spec = table_sink(model=_PairRow, write_mode="upsert", conflict_keys=("shard_id", "family"))
    assert spec.columns[:2] == ("shard_id", "family")

    store.persist_output_spec(spec, [{"shard_id": 1, "family": "ad4", "n_records": 3}])
    store.persist_output_spec(spec, [{"shard_id": 1, "family": "ad4", "n_records": 5}])
    assert [(row.shard_id, row.family, row.n_records) for row in store.select(_PairRow)] == [(1, "ad4", 5)]


def test_graph_upsert_node_defaults_include_the_conflict_keys():
    spec = graph_sink(nodes=({"name": "rows", "model": _PairRow, "write_mode": "upsert",
                              "conflict_keys": ("shard_id", "family")},))
    node = spec.meta["graph"]["nodes"][0]
    assert node["columns"][:2] == ["shard_id", "family"]
