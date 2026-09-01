import sqlite3
from pathlib import Path

import pytest

from ms_flow.core.database import ExecutorDB, MasterDB


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}


def test_master_db_applies_versioned_migrations_and_records_version(tmp_path):
    db_path = tmp_path / "legacy_master.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                name TEXT,
                path TEXT,
                description TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    db = MasterDB(db_path)
    db.dispose()

    conn = sqlite3.connect(str(db_path))
    try:
        project_columns = _columns(conn, "projects")
        version_columns = _columns(conn, "_schema_versions")
        index_columns = _columns(conn, "project_job_index")
        version_row = conn.execute(
            "SELECT version FROM _schema_versions WHERE schema_name = 'master'"
        ).fetchone()
    finally:
        conn.close()

    assert {"tags", "favorite", "scope", "app_id"} <= project_columns
    assert "scheduler_reason" in index_columns
    assert {"schema_name", "version", "updated_at"} <= version_columns
    assert version_row == (6,)


def test_master_db_migration_preserves_rows_and_is_idempotent(tmp_path):
    db_path = tmp_path / "legacy_master_rows.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                name TEXT,
                path TEXT,
                description TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO projects (id, name, path, description, created_at, updated_at)
            VALUES ('p1', 'Legacy Project', '/tmp/legacy', 'legacy row', '2026-01-01', '2026-01-01')
            """
        )
        conn.commit()
    finally:
        conn.close()

    db = MasterDB(db_path)
    db.reconnect()
    db.dispose()

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT name, path, description, tags, favorite, scope, app_id "
            "FROM projects WHERE id = 'p1'"
        ).fetchone()
        version_row = conn.execute(
            "SELECT version FROM _schema_versions WHERE schema_name = 'master'"
        ).fetchone()
    finally:
        conn.close()

    assert row == ("Legacy Project", "/tmp/legacy", "legacy row", "", 0, "full", "")
    assert version_row == (6,)


def test_executor_db_rejects_superseded_operational_schema(tmp_path):
    db_path = tmp_path / "legacy_executor.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE executor_jobs (
                id TEXT PRIMARY KEY,
                job_id TEXT,
                status TEXT,
                progress REAL,
                payload_json TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="Unsupported legacy operational schema"):
        ExecutorDB(db_path)
