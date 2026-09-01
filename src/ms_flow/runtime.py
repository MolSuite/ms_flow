from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional
from uuid import UUID

from ms_flow.core.project.catalog import AppProjectCatalog
from ms_flow.main import MolSuite
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import ProjectResource, ProjectResourceContract, ProjectResourceSpec


class BaseRuntime:
    """
    Base class for application-specific runtimes.
    Provides common project-management methods and lifecycle hooks.
    """

    def __init__(
        self,
        app_id: str,
        logger_name: str | None = None,
        project_resources: Iterable[ProjectResourceSpec | dict | object] | None = None,
        app_settings=None,
    ):
        self.app_id = app_id
        self.molsuite = MolSuite(
            app_id=app_id,
            host_namespace=app_id,
            project_resources=project_resources,
            app_settings=app_settings,
        )
        self.logger = self.molsuite.get_app_logger(logger_name or app_id)

    def shutdown(self):
        """Shut down the MolSuite engine."""
        self.molsuite.shutdown()

    def list_projects(self, page: int = 1, items_per_page: int = 20):
        """List projects filtered by this application's app_id."""
        return self.molsuite.list_projects(page=page, items_per_page=items_per_page)

    def create_project(
        self,
        name: str,
        folder: Path | str,
        description: str = "",
        tags: list[str] | None = None,
        extra_dirs: list[str] | None = None
    ) -> ProjectContext:
        """Create a new project for this application."""
        context = self.molsuite.create_project(
            name=name,
            folder=folder,
            description=description,
            tags=tags,
            activate=True,
            extra_dirs=extra_dirs,
        )
        self.on_project_activated(context)
        return context

    def _validate_project_app_id(self, project_id: UUID | str):
        project = self.molsuite.project_manager.get_project_by_id_global(project_id)
        project_app_id = (project.app_id or "").strip()
        runtime_app_id = self.app_id.strip()
        if project_app_id != runtime_app_id:
            raise ValueError(
                f"Project '{project.name}' belongs to app_id='{project_app_id or '<empty>'}' "
                f"and cannot be opened from the '{runtime_app_id}' runtime."
            )
        return project

    def open_project(self, project_id: UUID | str, extra_dirs: list[str] | None = None) -> ProjectContext:
        """Open and activate an existing project."""
        self._validate_project_app_id(project_id)
        context = self.molsuite.open_project(project_id, extra_dirs=extra_dirs)
        self.on_project_activated(context)
        return context

    def close_project(self):
        """Deactivate the current project."""
        self.molsuite.close_project()

    def create_or_open_project(
        self,
        name: str,
        folder: Path | str,
        description: str = "",
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        """Find a project by name/path, or create it if it does not exist."""
        context = self.molsuite.create_or_open_project(
            name=name,
            folder=folder,
            description=description,
            activate=True,
            extra_dirs=extra_dirs,
        )
        self.on_project_activated(context)
        return context

    def run(self, *args, **kwargs) -> str:
        """Shortcut to run a declarative workflow on the active runtime."""
        return self.molsuite.run(*args, **kwargs)

    def submit_job(self, job, **kwargs) -> str:
        return self.molsuite.submit_job(job, **kwargs)

    @property
    def active_context(self) -> Optional[ProjectContext]:
        """Context of the currently active project."""
        return self.molsuite.active_context

    def _require_active_project(self) -> ProjectContext:
        """Ensure there is an active project, or raise."""
        if self.molsuite.active_context is None:
            raise RuntimeError(f"There is no active project for {self.app_id}.")
        return self.molsuite.active_context

    def get_project_store(self):
        """Return the data store of the active project."""
        self._require_active_project()
        return self.molsuite.advanced.project_store

    @property
    def artifacts(self):
        self._require_active_project()
        return self.molsuite.artifacts

    def get_project_resource_contract(self) -> ProjectResourceContract:
        return self.molsuite.get_project_resource_contract()

    def list_project_resources(self, project_id: UUID | str | None = None) -> dict[str, ProjectResource]:
        return self.molsuite.list_project_resources(project_id=project_id)

    def get_project_resource(self, key: str, project_id: UUID | str | None = None) -> ProjectResource:
        return self.molsuite.get_project_resource(key, project_id=project_id)

    def get_project_resource_path(
        self,
        key: str,
        *parts: str | Path,
        project_id: UUID | str | None = None,
        create_parent: bool = False,
    ) -> Path:
        return self.molsuite.get_project_resource_path(
            key,
            *parts,
            project_id=project_id,
            create_parent=create_parent,
        )

    def get_project_resource_map(self, project_id: UUID | str | None = None) -> dict[str, dict[str, str]]:
        return self.molsuite.get_project_resource_map(project_id=project_id)

    def project_resource_specs(self) -> tuple[ProjectResourceSpec, ...]:
        return self.molsuite.project_resource_specs()

    def app_setting_specs(self):
        return self.molsuite.app_setting_specs()

    def get_app_setting(self, key: str):
        return self.molsuite.get_app_setting(key)

    def update_app_setting(self, key: str, value, *, save_global_too: bool = False) -> None:
        self.molsuite.update_app_setting(key, value, save_global_too=save_global_too)

    def on_project_activated(self, context: ProjectContext):
        """
        Hook run after creating or opening a project.
        Override it in a subclass to initialise layouts or configs.
        """
        pass


class AppRuntime(BaseRuntime):
    """Public runtime facade for a concrete app."""

    def __init__(
        self,
        app_id: str,
        logger_name: str | None = None,
        project_resources: Iterable[ProjectResourceSpec | dict | object] | None = None,
        app_settings=None,
    ):
        super().__init__(
            app_id=app_id,
            logger_name=logger_name,
            project_resources=project_resources,
            app_settings=app_settings,
        )
        self._project_catalog: AppProjectCatalog | None = None

    @property
    def project_catalog(self) -> AppProjectCatalog:
        if self._project_catalog is None:
            self._project_catalog = AppProjectCatalog(self.app_id)
        return self._project_catalog

    def shutdown(self):
        if self._project_catalog is not None:
            self._project_catalog.shutdown()
            self._project_catalog = None
        super().shutdown()


__all__ = ["AppRuntime", "BaseRuntime"]
