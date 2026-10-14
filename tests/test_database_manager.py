import sqlite3
from pathlib import Path

from sqlmodel import select

from ms_flow.core.database import (
    MASTER_TABLE_NAMES,
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


def test_master_and_project_keep_their_tables_apart(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")
    assert MASTER_TABLE_NAMES.issubset(_list_tables(master_db.db_path))

    project_db = ProjectStore()
    project_db.connect(tmp_path / "project-a")
    assert project_db.db_path == tmp_path / "project-a" / "project.db"
    assert MASTER_TABLE_NAMES.isdisjoint(_list_tables(project_db.db_path))
