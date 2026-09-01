from pathlib import Path

from ms_flow.core.database import MasterDB
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.manager import ProjectManager


def test_project_manager_filters_by_app_id(tmp_path):
    master_db = MasterDB(tmp_path / "projects.db")

    with master_db.get_session() as session:
        session.add(Project(name="dock-1", path=str(tmp_path / "dock-1"), app_id="amdockvs"))
        session.add(Project(name="dock-2", path=str(tmp_path / "dock-2"), app_id="amdockvs"))
        session.add(Project(name="other-1", path=str(tmp_path / "other-1"), app_id="otherapp"))
        session.commit()

    manager = ProjectManager(master_db, app_id_filter="amdockvs")

    rows = manager.repository.get_projects_paginated(page=1, items_per_page=10)

    assert manager.repository.get_total_projects() == 2
    assert {row.name for row in rows} == {"dock-1", "dock-2"}
