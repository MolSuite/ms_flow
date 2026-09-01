from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
import shutil
from typing import Literal, Union

from ms_flow.core.database.master_models import Project
from ms_flow.core.events import project_closed, project_opened
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.repository import ProjectRepository
from ms_flow.core.settings.manager import SettingsManager

SettingsProfile = Literal["default", "global"]

class ProjectManager:
    REQUIRED_DIRS = ("logs", "data", "results", "tmp")

    def __init__(self, master_db, app_id_filter: str | None = None):
        # The PM does not hold the active state, the Facade does.
        # The PM only knows how to run actions on projects.
        if master_db is None:
            raise ValueError("ProjectManager requires a MasterDB instance.")
        self.master_db = master_db
        self.repository = ProjectRepository(master_db, app_id_filter=app_id_filter)

    def _global_repository(self) -> ProjectRepository:
        return ProjectRepository(self.master_db)

    def _ensure_project_layout(self, folder: Path, extra_dirs: list[str] | None = None):
        all_dirs = list(self.REQUIRED_DIRS)
        if extra_dirs:
            all_dirs.extend(extra_dirs)
        for dir_name in all_dirs:
            folder.joinpath(dir_name).mkdir(parents=True, exist_ok=True)

    def _assert_unique_path(self, folder: Path | str, exclude_id=None):
        self.repository.assert_unique_path(folder, exclude_id=exclude_id)

    def get_project_by_id(self, project_id) -> Project:
        return self.repository.get_project_by_id(project_id)

    def get_project_by_id_global(self, project_id) -> Project:
        return self._global_repository().get_project_by_id(project_id)

    def touch_project(self, project_id):
        self.repository.touch_project(project_id)

    def find_project(self, *, name: str | None = None, folder: Path | str | None = None) -> Project | None:
        if not name and folder is None:
            raise ValueError("You must provide at least name or folder.")

        normalized_folder = None
        if folder is not None:
            normalized_folder = str(Path(folder).expanduser().resolve())

        if normalized_folder is not None:
            try:
                return self.repository.get_project_by_path(normalized_folder)
            except ValueError:
                pass

        if name:
            try:
                return self.repository.get_project_by_name(name)
            except ValueError:
                pass
        return None

    def find_project_global(self, *, name: str | None = None, folder: Path | str | None = None) -> Project | None:
        if not name and folder is None:
            raise ValueError("You must provide at least name or folder.")

        repository = self._global_repository()
        normalized_folder = None
        if folder is not None:
            normalized_folder = str(Path(folder).expanduser().resolve())

        if normalized_folder is not None:
            try:
                return repository.get_project_by_path(normalized_folder)
            except ValueError:
                pass

        if name:
            try:
                return repository.get_project_by_name(name)
            except ValueError:
                pass
        return None

    def create_project(self, name: str, folder: Path, sm: SettingsManager, base: SettingsProfile = "global",
                       description: str = "", scope: str = "full", app_id: str = "",
                       tags: list[str] | None = None, extra_dirs: list[str] | None = None) -> (ProjectContext):
        """Create a new project from scratch."""
        try:
            self._assert_unique_path(folder)
            folder.mkdir(parents=True, exist_ok=True)
            self._ensure_project_layout(folder, extra_dirs=extra_dirs)

            # Configure the project settings (creates a local config.toml if missing)
            sm.set_project(folder, base=base)

            context = ProjectContext(
                id=uuid.uuid4(),
                name=name,
                path=folder,
                app_id=app_id,
                scope=scope,
                settings=sm.settings,
                description=description,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            self.repository.create_project_record(
                project_id=context.id,
                name=name,
                folder=folder,
                description=description,
                app_id=app_id,
                scope=scope,
                tags=tags,
            )

            # Tell the app a project has been created/opened
            project_opened.send(self, context=context)
            return context
        except Exception as exc:
            raise RuntimeError(f"Could not create project '{name}': {exc}") from exc

    def load_project(self, project: Project, sm: SettingsManager, extra_dirs: list[str] | None = None) -> ProjectContext:
        """Load an existing project using the data registered in the Master DB."""
        try:
            folder = Path(project.path).expanduser().resolve()
            if not folder.exists():
                raise FileNotFoundError(f"Project folder not found: {folder}")

            self._ensure_project_layout(folder, extra_dirs=extra_dirs)
            sm.set_project(folder)

            context = ProjectContext(
                id=project.id,
                name=project.name,
                path=folder,
                app_id=project.app_id or "",
                scope=project.scope or "full",
                settings=sm.settings,
                description=project.description or "",
                created_at=project.created_at,
                updated_at=project.updated_at,
            )

            project_opened.send(self, context=context)
            return context
        except Exception as exc:
            raise RuntimeError(f"Could not load project '{project.name}': {exc}") from exc

    def load_project_by_id(self, project_id, sm: SettingsManager, extra_dirs: list[str] | None = None) -> ProjectContext:
        project = self.get_project_by_id(project_id)
        return self.load_project(project=project, sm=sm, extra_dirs=extra_dirs)

    def close_project(self, context: ProjectContext):
        """Signal that the current project context is closing."""
        project_closed.send(self, context=context)

    def delete_project_path(self, folder: Union[Path, str]):
        """Delete the project's folder on disk."""
        target = Path(folder).expanduser().resolve()
        if not target.exists():
            return
        if not target.is_dir():
            raise ValueError(f"The project path is not a folder: {target}")
        shutil.rmtree(target)

    def delete_projects(self, project_ids, delete_files: bool = True):
        projects_to_delete = []
        normalized_ids = []

        for project_id in project_ids:
            try:
                project = self.repository.get_project_by_id(project_id)
            except ValueError:
                continue
            normalized_ids.append(project.id)
            projects_to_delete.append(project)

        if delete_files:
            for project in projects_to_delete:
                self.delete_project_path(project.path)

        self.repository.delete_projects(normalized_ids)
