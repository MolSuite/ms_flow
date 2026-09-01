"""Single SQL compiler + single engine (phase 1.1/1.2 of the data-access plan)."""

from __future__ import annotations

from ms_flow.core.data import DataBridge, DataContext, DbInputSpec
from ms_flow.core.database.base import get_sqlite_engine
from ms_flow.query import QuerySpec


def test_subquery_operator_compiles_without_materializing_ids():
    docked = QuerySpec(
        table="dockingresult",
        fields=["ligand_molecule_id"],
        filters={"receptor_id": 7},
    )
    pending = QuerySpec(
        table="molecule",
        fields=["id", "name"],
        filters={"kind": "ligand", "id__not_in_subquery": docked},
        order=["id"],
        limit=100,
    )

    sql, params = pending.compile()
    assert sql == (
        "SELECT id, name FROM molecule "
        "WHERE kind = ? AND id NOT IN "
        "(SELECT __sq.ligand_molecule_id FROM "
        "(SELECT ligand_molecule_id FROM dockingresult WHERE receptor_id = ?) AS __sq "
        "WHERE __sq.ligand_molecule_id IS NOT NULL) "
        "ORDER BY id ASC LIMIT ?"
    )
    assert params == ("ligand", 7, 100)


def test_not_in_subquery_is_null_safe(tmp_path):
    """A NULL in the subquery must not empty the result (`NOT IN` semantics)."""
    engine = get_sqlite_engine(tmp_path / "nulls.db")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE molecule (id INTEGER PRIMARY KEY, kind TEXT)")
        conn.exec_driver_sql("INSERT INTO molecule VALUES (1, 'ligand'), (2, 'ligand')")
        conn.exec_driver_sql("CREATE TABLE dockingresult (ligand_molecule_id INTEGER, receptor_id INTEGER)")
        conn.exec_driver_sql("INSERT INTO dockingresult VALUES (1, 7), (NULL, 7)")

    docked = QuerySpec(table="dockingresult", fields=["ligand_molecule_id"], filters={"receptor_id": 7})
    pending = QuerySpec(table="molecule", fields=["id"], filters={"id__not_in_subquery": docked})
    sql, params = pending.compile()
    with engine.connect() as conn:
        assert [row[0] for row in conn.exec_driver_sql(sql, params)] == [2]


def test_backend_and_project_store_share_the_same_pragmas(tmp_path):
    db_path = tmp_path / "sample.db"
    engine = get_sqlite_engine(db_path)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE molecules (id INTEGER PRIMARY KEY, name TEXT)")
        conn.exec_driver_sql("INSERT INTO molecules (id, name) VALUES (1, 'MolA'), (2, 'MolB')")

    # two concurrent pool connections, both with the pragmas applied
    with engine.connect() as first, engine.connect() as second:
        for conn in (first, second):
            assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
            assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000

    assert get_sqlite_engine(db_path) is engine

    rows = DataBridge().resolve_input(
        DbInputSpec(table="molecules", columns=("id", "name"), where={"id__gt": 1},
                    db_role="custom", db_path=str(db_path)),
        DataContext(project_dir=tmp_path),
    )
    assert rows == [{"id": 2, "name": "MolB"}]


def test_model_filters_accept_subquery_specs(tmp_path):
    """`stream_keyset` resolves membership with a subquery, without materialising ids."""
    from sqlmodel import Field, SQLModel

    from ms_flow.core.database import ProjectStore

    class Mol(SQLModel, table=True):
        __tablename__ = "mol"
        id: int | None = Field(default=None, primary_key=True)
        kind: str = "ligand"

    class Member(SQLModel, table=True):
        __tablename__ = "member"
        id: int | None = Field(default=None, primary_key=True)
        set_id: int = 0
        molecule_id: int | None = None

    store = ProjectStore(tmp_path / "sub.db", auto_setup=True)
    engine = store.engine
    SQLModel.metadata.create_all(engine, tables=[Mol.__table__, Member.__table__])
    with engine.begin() as conn:
        conn.exec_driver_sql("INSERT INTO mol (id, kind) VALUES (1,'ligand'),(2,'ligand'),(3,'ligand')")
        # NULL is the classic NOT IN trap
        conn.exec_driver_sql("INSERT INTO member (id, set_id, molecule_id) VALUES (1,5,1),(2,5,2),(3,5,NULL)")

    members = QuerySpec(table="member", fields=["molecule_id"], filters={"set_id": 5})
    inside = [row["id"] for row in store.stream_keyset(Mol, filters={"id__in_subquery": members}, fields=["id"])]
    outside = [row["id"] for row in store.stream_keyset(Mol, filters={"id__not_in_subquery": members}, fields=["id"])]
    assert inside == [1, 2]
    assert outside == [3]
