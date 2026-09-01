import sqlite3

import pytest

from ms_flow.core.database import ProjectStore
from ms_flow.query import QuerySpec, db_pages, db_stream

ROWS = 1000


@pytest.fixture
def store(tmp_path):
    db_path = tmp_path / "project.db"
    store = ProjectStore()
    store.set_db_path(str(db_path))
    store.setup()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE mols (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
        conn.executemany(
            "INSERT INTO mols (id, name, score) VALUES (?, ?, ?)",
            [(i, f"M{i}", float(ROWS - i)) for i in range(1, ROWS + 1)],
        )
        conn.commit()
    finally:
        conn.close()
    return store


def _spec(**kwargs) -> QuerySpec:
    base = {"table": "mols", "fields": ("id", "name"), "order": ("id",)}
    base.update(kwargs)
    return QuerySpec(**base)


def test_pages_return_the_same_rows_as_one_open_cursor(store):
    spec = _spec()
    assert list(db_pages(store, spec, page_size=97)) == list(db_stream(store, spec))


def test_each_page_is_its_own_short_read(store, monkeypatch):
    reads = []
    original = ProjectStore.read_rows
    monkeypatch.setattr(
        ProjectStore,
        "read_rows",
        lambda self, query: reads.append(query.limit) or original(self, query),
    )
    rows = list(db_pages(store, _spec(), page_size=100))
    assert len(rows) == ROWS
    # 10 full pages + one that comes back empty; never a single read of the whole table.
    assert reads == [100] * 11


def test_filters_and_limit_survive_paging(store):
    rows = list(db_pages(store, _spec(filters={"score__lte": 500.0}, limit=250), page_size=40))
    assert len(rows) == 250
    assert rows[0]["id"] == 500 and rows[-1]["id"] == 749


def test_unpageable_specs_fall_back_to_the_cursor(store):
    for spec in (
        _spec(order=("-score",)),                       # order unrelated to the key
        _spec(offset=10),                               # offset and keyset do not mix
        QuerySpec(query="SELECT id, name FROM mols ORDER BY id"),  # raw SQL
    ):
        assert list(db_pages(store, spec, page_size=64)) == list(db_stream(store, spec))
