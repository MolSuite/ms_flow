from pathlib import Path

import pytest

from ms_flow.core.database import MasterDB
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.manager import ProjectManager
from ms_flow.core.settings.manager import SettingsManager


def _patch_fake_home(monkeypatch, fake_home: Path):
    monkeypatch.setattr("ms_flow.core.settings.manager.Path.home", lambda: fake_home)
    monkeypatch.setattr("ms_flow.core.settings.models.Path.home", lambda: fake_home)


def test_project_manager_create_bootstraps_min_layout(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    settings_manager = SettingsManager()
    project_manager = ProjectManager(master_db=MasterDB(tmp_path / "projects.db"))
    project_dir = tmp_path / "proj-a"

    context = project_manager.create_project(
        name="proj-a",
        folder=project_dir,
        sm=settings_manager,
        description="test project",
        scope="full",
    )

    assert context.path == project_dir
    assert (project_dir / "config.toml").exists()
    assert (project_dir / "logs").is_dir()
    assert (project_dir / "data").is_dir()
    assert (project_dir / "results").is_dir()
    assert (project_dir / "tmp").is_dir()


def test_project_manager_load_raises_if_folder_missing(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    settings_manager = SettingsManager()
    project_manager = ProjectManager(master_db=MasterDB(tmp_path / "projects.db"))
    project = Project(
        name="missing",
        path=str(tmp_path / "missing-project"),
        description="",
        scope="full",
    )

    with pytest.raises(RuntimeError):
        project_manager.load_project(project, settings_manager)


def test_project_manager_delete_path_removes_directory(tmp_path):
    project_manager = ProjectManager(master_db=MasterDB(tmp_path / "projects.db"))
    project_dir = tmp_path / "to-delete"
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "dummy.txt").write_text("ok")

    project_manager.delete_project_path(project_dir)
    assert not project_dir.exists()


def test_project_manager_load_ensures_required_layout(tmp_path, monkeypatch):
    _patch_fake_home(monkeypatch, tmp_path)
    settings_manager = SettingsManager()
    project_manager = ProjectManager(master_db=MasterDB(tmp_path / "projects.db"))
    project_dir = tmp_path / "existing-project"
    project_dir.mkdir(parents=True, exist_ok=True)

    project = Project(
        name="existing-project",
        path=str(project_dir),
        description="",
        scope="full",
    )
    context = project_manager.load_project(project, settings_manager)

    assert context.path == project_dir
    assert (project_dir / "logs").is_dir()
    assert (project_dir / "data").is_dir()
    assert (project_dir / "results").is_dir()
    assert (project_dir / "tmp").is_dir()
