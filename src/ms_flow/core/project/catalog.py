from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from ms_flow.core.apps import AppManifest, AppRegistry
from ms_flow.core.database import MasterDB
from ms_flow.core.project.manager import ProjectManager
from ms_flow.core.project.repository import ProjectRepository
from ms_flow.core.settings.manager import SettingsManager
from ms_flow.logger import LoggingManager


class _ProjectCatalogSupport:
    def __init__(
        self,
        *,
        app_id_filter: str | None = None,
        logger_name: str = "molsuite.catalog",
        host_namespace: str = "molsuite",
        discover_apps: bool = True,
        package_prefix: str = "ms_",
        app_modules: list[str] | None = None,
        manifests: list[AppManifest] | None = None,
        app_registry: AppRegistry | None = None,
    ):
        self.app_id_filter = (app_id_filter or "").strip() or None
        self.host_namespace = str(host_namespace).strip() or "molsuite"
        self.settings_manager = SettingsManager()
        logging_cfg = self.settings_manager.settings.logging
        self.logging_manager = LoggingManager(
            global_log_dir=self.settings_manager.settings.projects_db.parent / "logs",
            max_bytes=logging_cfg.max_file_size_mb * 1024 * 1024,
            backup_count=logging_cfg.backup_count,
            retention_days=logging_cfg.retention_days,
            queue_size=logging_cfg.queue_size,
            app_level=logging_cfg.app_level,
            executor_level=logging_cfg.executor_level,
            project_level=logging_cfg.project_level,
            console_level=logging_cfg.console_level,
            root_namespace=self.host_namespace,
        )
        self.logging_manager.start()
        self.logger = self.logging_manager.get_app_logger(logger_name)
        self.master_db = MasterDB(self.settings_manager.settings.projects_db)
        self.project_manager = ProjectManager(self.master_db, app_id_filter=self.app_id_filter)
        self.app_registry = app_registry or AppRegistry()
        if manifests:
            for manifest in manifests:
                self.app_registry.register(manifest)
        if app_modules:
            self.app_registry.discover_modules(app_modules)
        if discover_apps:
            self.app_registry.discover_prefixed_apps(package_prefix)
            self.app_registry.discover_workspace_apps()

    def shutdown(self):
        try:
            self.logging_manager.stop()
        except Exception:
            pass


class _ProjectCatalogService:
    def __init__(
        self,
        *,
        support: _ProjectCatalogSupport | None = None,
        app_id_filter: str | None = None,
        logger_name: str = "molsuite.catalog",
        host_namespace: str = "molsuite",
        discover_apps: bool = True,
        package_prefix: str = "ms_",
        app_modules: list[str] | None = None,
        manifests: list[AppManifest] | None = None,
        app_registry: AppRegistry | None = None,
    ):
        self._owns_support = support is None
        self._support = support or _ProjectCatalogSupport(
            app_id_filter=app_id_filter,
            logger_name=logger_name,
            host_namespace=host_namespace,
            discover_apps=discover_apps,
            package_prefix=package_prefix,
            app_modules=app_modules,
            manifests=manifests,
            app_registry=app_registry,
        )

    @property
    def app_id_filter(self) -> str | None:
        return self._support.app_id_filter

    @property
    def settings_manager(self) -> SettingsManager:
        return self._support.settings_manager

    @property
    def logger(self):
        return self._support.logger

    @property
    def project_manager(self) -> ProjectManager:
        return self._support.project_manager

    @property
    def project_repository(self):
        return self._support.project_manager.repository

    @property
    def app_registry(self) -> AppRegistry:
        return self._support.app_registry

    def shutdown(self):
        if self._owns_support:
            self._support.shutdown()


class ProjectCatalog(_ProjectCatalogService):
    def list_projects(self, page: int = 1, items_per_page: int = 20):
        return self.project_repository.get_projects_paginated(page, items_per_page)

    def get_total_projects(self) -> int:
        return self.project_repository.get_total_projects()

    def set_sort_mode(self, sort_mode: str):
        self.project_repository.set_sort_mode(sort_mode)

    def set_filter_mode(self, filter_mode: str):
        self.project_repository.set_filter_mode(filter_mode)

    def set_search(self, field: str, query: str):
        self.project_repository.set_search(field, query)

    def get_project(self, project_id):
        return self.project_repository.get_project_by_id(project_id)

    def parse_tags(self, raw_tags: str | None) -> list[str]:
        return ProjectRepository.parse_tags(raw_tags)

    def get_app_manifest(self, app_id: str):
        return self.app_registry.get(app_id)

    def list_apps(self):
        manifests = self.app_registry.list_manifests()
        if self.app_id_filter is None:
            return manifests
        return [manifest for manifest in manifests if manifest.app_id == self.app_id_filter]


class AppProjectCatalog(ProjectCatalog):
    def __init__(self, app_id: str, **kwargs):
        normalized_app_id = str(app_id).strip()
        if not normalized_app_id:
            raise ValueError("AppProjectCatalog requires a non-empty app_id.")
        super().__init__(app_id_filter=normalized_app_id, **kwargs)
        self.app_id = normalized_app_id


class ProjectCatalogEditor(_ProjectCatalogService):
    @staticmethod
    def _manifest_extra_dirs(manifest: AppManifest) -> list[str]:
        return [spec.relative_path for spec in getattr(manifest, "project_resources", ()) if spec.relative_path]

    def _resolve_manifest(self, app_id: str | None = None) -> AppManifest:
        effective_app_id = (app_id or "").strip() or self.app_id_filter
        if not effective_app_id:
            raise ValueError("You must provide an app_id to create the project.")

        manifest = self.app_registry.get(effective_app_id)
        if manifest is None:
            raise ValueError(f"App '{effective_app_id}' not found.")
        if self.app_id_filter is not None and manifest.app_id != self.app_id_filter:
            raise ValueError(f"This view only allows projects of '{self.app_id_filter}'.")
        return manifest

    def create_project(
        self,
        *,
        name: str,
        folder: Path | str,
        description: str,
        tags: list[str] | None = None,
        base_settings: str = "global",
        app_id: str | None = None,
    ):
        manifest = self._resolve_manifest(app_id)
        folder_path = Path(folder).expanduser().resolve()
        context = self.project_manager.create_project(
            name=name,
            folder=folder_path,
            sm=self.settings_manager,
            base=base_settings,
            description=description,
            scope=manifest.scope_id,
            app_id=manifest.app_id,
            tags=tags,
            extra_dirs=self._manifest_extra_dirs(manifest),
        )
        self.logger.info(
            "Project created in catalog: id=%s app_id=%s path=%s",
            context.id,
            manifest.app_id,
            folder_path,
        )
        return context

    def update_project(
        self,
        *,
        project_id,
        name: str,
        folder: Path | str,
        description: str,
        tags: list[str],
        app_id: str | None = None,
        scope: str | None = None,
    ):
        self.project_repository.update_project(
            project_id=project_id,
            name=name,
            folder=folder,
            description=description,
            tags=tags,
            app_id=app_id,
            scope=scope,
        )

    def toggle_favorite(self, project_ids):
        self.project_repository.toggle_favorite(project_ids)

    def delete_projects(self, project_ids, delete_files: bool = True):
        self.project_manager.delete_projects(project_ids, delete_files=delete_files)


class ProjectLauncher(_ProjectCatalogService):
    def launch_project(self, project_id) -> subprocess.Popen:
        project = self.project_repository.get_project_by_id(project_id)
        if self.app_id_filter is not None and project.app_id != self.app_id_filter:
            raise RuntimeError("The project does not belong to this app.")

        manifest = self.app_registry.resolve_for_project(project)
        if manifest is None:
            raise RuntimeError(f"No registered app found for project '{project.name}'.")

        src_root = Path(__file__).resolve().parents[3]
        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH", "")
        pythonpath_parts = [str(src_root)]
        if manifest.source_root is not None:
            pythonpath_parts.insert(0, str(manifest.source_root))
        if existing_pythonpath:
            pythonpath_parts.append(existing_pythonpath)
        env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(pythonpath_parts))

        command = [
            sys.executable,
            "-m",
            manifest.package_name,
        ]
        command.extend(["--project-id", str(project.id)])

        self.logger.info("Launching app '%s' for project=%s", manifest.app_id, project.id)
        return subprocess.Popen(command, cwd=str(src_root), env=env)


class ProjectCatalogBackend:
    def __init__(
        self,
        *,
        app_id_filter: str | None = None,
        logger_name: str = "molsuite.catalog",
        host_namespace: str = "molsuite",
        discover_apps: bool = True,
        package_prefix: str = "ms_",
        app_modules: list[str] | None = None,
        manifests: list[AppManifest] | None = None,
        app_registry: AppRegistry | None = None,
    ):
        self._support = _ProjectCatalogSupport(
            app_id_filter=app_id_filter,
            logger_name=logger_name,
            host_namespace=host_namespace,
            discover_apps=discover_apps,
            package_prefix=package_prefix,
            app_modules=app_modules,
            manifests=manifests,
            app_registry=app_registry,
        )
        self.catalog = ProjectCatalog(support=self._support)
        self.editor = ProjectCatalogEditor(support=self._support)
        self.launcher = ProjectLauncher(support=self._support)

    @property
    def app_id_filter(self) -> str | None:
        return self._support.app_id_filter

    def list_projects(self, page: int = 1, items_per_page: int = 20):
        return self.catalog.list_projects(page, items_per_page)

    def get_total_projects(self) -> int:
        return self.catalog.get_total_projects()

    def set_sort_mode(self, sort_mode: str):
        self.catalog.set_sort_mode(sort_mode)

    def set_filter_mode(self, filter_mode: str):
        self.catalog.set_filter_mode(filter_mode)

    def set_search(self, field: str, query: str):
        self.catalog.set_search(field, query)

    def get_project(self, project_id):
        return self.catalog.get_project(project_id)

    def parse_tags(self, raw_tags: str | None) -> list[str]:
        return self.catalog.parse_tags(raw_tags)

    def get_app_manifest(self, app_id: str):
        return self.catalog.get_app_manifest(app_id)

    def list_apps(self):
        return self.catalog.list_apps()

    def create_project(self, **kwargs):
        return self.editor.create_project(**kwargs)

    def update_project(self, **kwargs):
        return self.editor.update_project(**kwargs)

    def toggle_favorite(self, project_ids):
        self.editor.toggle_favorite(project_ids)

    def delete_projects(self, project_ids, delete_files: bool = True):
        self.editor.delete_projects(project_ids, delete_files=delete_files)

    def launch_project(self, project_id):
        return self.launcher.launch_project(project_id)

    def shutdown(self):
        self._support.shutdown()
