from __future__ import annotations

import sqlite3

import pytest

from ms_flow.core.database import ProjectStore


@pytest.fixture
def project(tmp_path):
    store = ProjectStore.open_at(tmp_path / "project.db")
    yield store
    store.dispose()


def _app_table(db_path):
    """A table the way an app declares it: ms_flow never sees this schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE app_items (id INTEGER PRIMARY KEY,"
        " structure_payload_id INTEGER REFERENCES ms_payloads(id))"
    )
    conn.execute("CREATE INDEX ix_app_items_payload ON app_items(structure_payload_id)")
    return conn


def test_payload_tables_exist_with_autoincrement_and_enforced_fk(project):
    conn = _app_table(project.db_path)
    sql = dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall())
    assert "AUTOINCREMENT" in sql["ms_volumes"] and "AUTOINCREMENT" in sql["ms_payloads"]

    conn.execute("INSERT INTO ms_volumes (series, state, size, created_at) VALUES ('s', 'active', 0, '')")
    conn.execute(
        'INSERT INTO ms_payloads (volume_id, "offset", length, raw_length, codec, crc32, kind)'
        " VALUES (1, 32, 0, 0, 0, 0, '')"
    )
    conn.execute("INSERT INTO app_items (structure_payload_id) VALUES (1)")
    with pytest.raises(sqlite3.IntegrityError):  # a reference to bytes that don't exist
        conn.execute("INSERT INTO app_items (structure_payload_id) VALUES (99)")
    with pytest.raises(sqlite3.IntegrityError):  # a payload still referenced
        conn.execute("DELETE FROM ms_payloads WHERE id = 1")
    conn.close()
