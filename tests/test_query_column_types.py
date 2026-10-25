"""A column whose type converts values (a TypeDecorator: a name stored as an integer code)
reads and filters through QuerySpec as it does through the ORM, and every sink binds it."""
from __future__ import annotations

from typing import Optional

from sqlalchemy import Integer, text
from sqlalchemy.types import TypeDecorator
from sqlmodel import Field, SQLModel

from ms_flow.core.database import ProjectStore
from ms_flow.query import QuerySpec, db_count, db_rows, db_stream
from ms_flow.sinks import graph_sink, table_sink

_CODES = {"small": 1, "big": 2}


class _Code(TypeDecorator):
    impl = Integer
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return None if value is None else _CODES[value]

    def process_result_value(self, value, dialect):
        return None if value is None else {v: k for k, v in _CODES.items()}[value]


class _CodedRow(SQLModel, table=True):
    __tablename__ = "test_query_coded_rows"

    id: Optional[int] = Field(default=None, primary_key=True)
    size: str = Field(default="small", sa_type=_Code)


def test_a_converting_column_round_trips_through_sinks_and_queries(tmp_path):
    project_db = ProjectStore()
    project_db.connect(tmp_path / "project")
    store = ProjectStore(project_db)
    store.persist_output_spec(table_sink(model=_CodedRow, write_mode="bulk"), [{"size": "small"}])
    store.persist_output_spec(graph_sink(nodes=({"name": "rows", "model": _CodedRow},)), {"rows": [{"size": "big"}, {"size": "small"}]})
    upsert = {"name": "rows", "model": _CodedRow, "columns": ["id", "size"], "write_mode": "upsert", "conflict_keys": ["id"]}
    store.persist_output_spec(graph_sink(nodes=(upsert,)), {"rows": [{"id": 3, "size": "big"}]})

    with store.get_session() as session:
        stored = [r[0] for r in session.connection().execute(text("SELECT size FROM test_query_coded_rows ORDER BY id"))]
    assert stored == [1, 2, 2]

    table = "test_query_coded_rows"
    assert [r["size"] for r in db_rows(store, QuerySpec(table=table, order=("id",)))] == ["small", "big", "big"]
    assert db_count(store, QuerySpec(table=table, filters={"size": "big"})) == 2
    assert db_count(store, QuerySpec(table=table, filters={"size__in": ["small"]})) == 1
    assert db_count(store, QuerySpec(table=table, filters={"size__ne": "big"})) == 1
    assert [r["size"] for r in db_stream(store, QuerySpec(table=table, filters={"size": "big"}), batch_size=1)] == ["big", "big"]
    big_ids = QuerySpec(table=table, fields=("id",), filters={"size": "big"})
    assert db_count(store, QuerySpec(table=table, filters={"id__in_subquery": big_ids})) == 2
