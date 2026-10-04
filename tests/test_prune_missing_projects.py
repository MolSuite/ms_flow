import uuid

from ms_flow.core.database import MasterDB
from ms_flow.core.project.repository import ProjectRepository


def test_delete_missing_projects_drops_only_vanished_folders(tmp_path):
    repo = ProjectRepository(MasterDB(tmp_path / "projects.db"))
    kept = tmp_path / "kept"
    kept.mkdir()
    repo.create_project_record(uuid.uuid4(), "kept", kept, "", app_id="a")
    repo.create_project_record(uuid.uuid4(), "gone", tmp_path / "gone", "", app_id="b")

    assert repo.delete_missing_projects() == 1
    assert repo.get_project_by_path(kept).name == "kept"
    assert repo.delete_missing_projects() == 0
