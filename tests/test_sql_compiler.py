"""Single SQL compiler + single engine (phase 1.1/1.2 of the data-access plan)."""

from __future__ import annotations

from ms_flow.core.data import DataBridge, DataContext, DbInputSpec
from ms_flow.core.database.base import get_sqlite_engine
from ms_flow.query import QuerySpec


def test_subquery_operator_compiles_without_materializing_ids():
    coupled = QuerySpec(
        table="couplingresult",
        fields=["guest_specimen_id"],
        filters={"anchor_id": 7},
    )
    pending = QuerySpec(
        table="specimen",
        fields=["id", "name"],
        filters={"kind": "guest", "id__not_in_subquery": coupled},
        order=["id"],
        limit=100,
    )

    sql, params = pending.compile()
    assert sql == (
        "SELECT id, name FROM specimen "
        "WHERE kind = ? AND id NOT IN "
        "(SELECT __sq.guest_specimen_id FROM "
        "(SELECT guest_specimen_id FROM couplingresult WHERE anchor_id = ?) AS __sq "
        "WHERE __sq.guest_specimen_id IS NOT NULL) "
        "ORDER BY id ASC LIMIT ?"
    )
    assert params == ("guest", 7, 100)


def test_not_in_subquery_is_null_safe(tmp_path):
    """A NULL in the subquery must not empty the result (`NOT IN` semantics)."""
    engine = get_sqlite_engine(tmp_path / "nulls.db")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE specimen (id INTEGER PRIMARY KEY, kind TEXT)")
        conn.exec_driver_sql("INSERT INTO specimen VALUES (1, 'guest'), (2, 'guest')")
        conn.exec_driver_sql("CREATE TABLE couplingresult (guest_specimen_id INTEGER, anchor_id INTEGER)")
        conn.exec_driver_sql("INSERT INTO couplingresult VALUES (1, 7), (NULL, 7)")

    coupled = QuerySpec(table="couplingresult", fields=["guest_specimen_id"], filters={"anchor_id": 7})
    pending = QuerySpec(table="specimen", fields=["id"], filters={"id__not_in_subquery": coupled})
    sql, params = pending.compile()
    with engine.connect() as conn:
        assert [row[0] for row in conn.exec_driver_sql(sql, params)] == [2]


def test_backend_and_project_store_share_the_same_pragmas(tmp_path):
    db_path = tmp_path / "sample.db"
    engine = get_sqlite_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE specimens (id INTEGER PRIMARY KEY, name TEXT)")
        conn.exec_driver_sql("INSERT INTO specimens (id, name) VALUES (1, 'MolA'), (2, 'MolB')")

    # two concurrent pool connections, both with the pragmas applied
    with engine.connect() as first, engine.connect() as second:
        for conn in (first, second):
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
            assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000

    assert get_sqlite_engine(db_path) is engine

    rows = DataBridge().resolve_input(
        DbInputSpec(table="specimens", columns=("id", "name"), where={"id__gt": 1},
                    db_role="custom", db_path=str(db_path)),
        DataContext(project_dir=tmp_path),
    )
    assert rows == [{"id": 2, "name": "MolB"}]


def test_model_filters_accept_subquery_specs(tmp_path):
    """`stream_keyset` resolves membership with a subquery, without materialising ids."""
    from sqlmodel import Field, SQLModel

    from ms_flow.core.database import ProjectStore

    class Spc(SQLModel, table=True):
        __tablename__ = "spc"
        id: int | None = Field(default=None, primary_key=True)
        kind: str = "guest"

    class Member(SQLModel, table=True):
        __tablename__ = "member"
        id: int | None = Field(default=None, primary_key=True)
        set_id: int = 0
        specimen_id: int | None = None

    store = ProjectStore(tmp_path / "sub.db", auto_setup=True)
    engine = store.engine
    SQLModel.metadata.create_all(engine, tables=[Spc.__table__, Member.__table__])
    with engine.begin() as conn:
        conn.exec_driver_sql("INSERT INTO spc (id, kind) VALUES (1,'guest'),(2,'guest'),(3,'guest')")
        # NULL is the classic NOT IN trap
        conn.exec_driver_sql("INSERT INTO member (id, set_id, specimen_id) VALUES (1,5,1),(2,5,2),(3,5,NULL)")

    members = QuerySpec(table="member", fields=["specimen_id"], filters={"set_id": 5})
    inside = [row["id"] for row in store.stream_keyset(Spc, filters={"id__in_subquery": members}, fields=["id"])]
    outside = [row["id"] for row in store.stream_keyset(Spc, filters={"id__not_in_subquery": members}, fields=["id"])]
    assert inside == [1, 2]
    assert outside == [3]
