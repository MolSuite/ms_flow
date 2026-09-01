import sqlite3
from pathlib import Path

import pytest
from sqlmodel import select

from ms_flow.core.database import (
    EXECUTOR_TABLE_NAMES,
    MASTER_TABLE_NAMES,
    ExecutorDB,
    MasterDB,
    ProjectStore,
)
from ms_flow.core.database.master_models import Project


def _list_tables(db_path: Path) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    return {row[0] for row in rows}


def test_master_and_executor_create_expected_tables(tmp_path):
    master_path = tmp_path / "projects.db"
    executor_path = tmp_path / "executor.db"

    master_db = MasterDB(master_path)
    executor_db = ExecutorDB(executor_path)

    assert master_db.db_path.exists()
    assert executor_db.db_path.exists()

    master_tables = _list_tables(master_db.db_path)
    assert MASTER_TABLE_NAMES.issubset(master_tables)
    assert EXECUTOR_TABLE_NAMES.isdisjoint(master_tables)

    executor_tables = _list_tables(executor_db.db_path)
    assert EXECUTOR_TABLE_NAMES.issubset(executor_tables)
    assert MASTER_TABLE_NAMES.isdisjoint(executor_tables)


def test_executor_db_defaults_to_requested_path(tmp_path):
    master_path = tmp_path / "global_data" / "projects.db"
    master_db = MasterDB(master_path)
    executor_db = ExecutorDB(master_path.parent / "executor.db")

    assert executor_db.db_path == master_path.parent / "executor.db"
    assert executor_db.db_path.exists()
    master_db.dispose()


def test_executor_session_requires_setup_path(tmp_path):
    executor_db = ExecutorDB(tmp_path / "executor.db")
    executor_db.dispose()
    with pytest.raises(RuntimeError):
        executor_db.get_session()


def test_project_db_does_not_create_master_or_executor_tables(tmp_path):
    MasterDB(tmp_path / "projects.db")
    ExecutorDB(tmp_path / "executor.db")

    project_dir = tmp_path / "project-a"
    project_db = ProjectStore()
    project_db.connect(project_dir)

    assert project_db.db_path == project_dir / "project.db"
    assert project_db.db_path.exists()
    project_tables = _list_tables(project_db.db_path)
    assert MASTER_TABLE_NAMES.isdisjoint(project_tables)
    assert EXECUTOR_TABLE_NAMES.isdisjoint(project_tables)


def test_master_db_project_model_includes_app_id(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")

    with master_db.get_session() as session:
        session.add(
            Project(
                name="dock-project",
                path=str(tmp_path / "dock-project"),
                app_id="amdockvs",
                scope="docking",
            )
        )
        session.commit()
        row = session.exec(select(Project)).first()

    assert row is not None
    assert row.app_id == "amdockvs"
