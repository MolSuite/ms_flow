from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from ms_flow.core.database import ProjectStore
from ms_flow.core.database.master_models import Project
from ms_flow.core.project import ActiveProjectRuntime, ProjectDataContext, ProjectRuntimeState
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import ProjectResource
from ms_flow.core.project.manager import SettingsProfile


class WorkspaceProjectRuntimeMixin:
    def get_project_resource_contract(self):
        return self._project_resource_contract

    def project_resource_specs(self):
        return self._project_resource_contract.specs

    def _resolve_project_root(self, project_id: str | UUID | None = None) -> Path | None:
        if project_id is not None:
            project = self._validate_project_access(self._normalize_id(project_id))
            return Path(project.path).expanduser().resolve()
        if self.active_context is None:
            return None
        return Path(self.active_context.path).expanduser().resolve()

    def list_project_resources(self, project_id: str | UUID | None = None) -> dict[str, ProjectResource]:
        project_root = self._resolve_project_root(project_id=project_id)
        if project_root is None:
            return {}
        return self._project_resource_contract.resolve(project_root)

    def get_project_resource(self, key: str, project_id: str | UUID | None = None) -> ProjectResource:
        project_root = self._resolve_project_root(project_id=project_id)
        if project_root is None:
            raise RuntimeError(f"There is no active project to resolve resource '{key}'.")
        return self._project_resource_contract.resolve_one(project_root, key)

    def get_project_resource_path(
        self,
        key: str,
        *parts: str | Path,
        project_id: str | UUID | None = None,
        create_parent: bool = False,
    ) -> Path:
        resource = self.get_project_resource(key, project_id=project_id)
        path = resource.path.joinpath(*(str(part) for part in parts))
        if create_parent:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_project_resource_map(self, project_id: str | UUID | None = None) -> dict[str, dict[str, str]]:
        return {
            key: resource.to_mapping()
            for key, resource in self.list_project_resources(project_id=project_id).items()
        }

    def _merge_project_dirs(self, extra_dirs: list[str] | None = None) -> list[str]:
        names: list[str] = []
        for item in tuple(extra_dirs or ()):
            normalized = str(item).strip()
            if normalized and normalized not in names:
                names.append(normalized)
        return names

    def _resolve_project_db_path(self, project_path: Path) -> Path:
        return Path(project_path).expanduser().resolve() / "project.db"

    def _shutdown_runtime_stack(self):
        self._close_project_runtime()
        if self._engine is not None:
            self._engine.close()
            self._engine = self._aside = None

    def _close_project_runtime(self):
        active_project = self._runtime_state.active_project
        if self._flow is not None:  # before the project goes: what its jobs delivered is written first
            self._flow = None
            self._engine.detach("project")
        try:
            if active_project is not None and active_project.project_db is not None:
                active_project.project_db.disconnect()
        except Exception:
            pass
        self.logging_manager.clear_project_logging()
        self.project_logger = None
        self._runtime_state = ProjectRuntimeState()

    def _require_runtime(self):
        if not self._runtime_state.initialized:
            raise RuntimeError("Runtime is not initialised. Activate a project first.")

    def _require_active_project_runtime(self) -> ActiveProjectRuntime:
        runtime = self._runtime_state.active_project
        if runtime is None or runtime.context is None:
            raise RuntimeError("There is no active project.")
        return runtime

    def list_projects(self, page: int = 1, items_per_page: int = 20) -> list[Project]:
        return self.project_manager.repository.get_projects_paginated(page, items_per_page)

    def open_project(
        self,
        project_id: str | UUID,
        *,
        cancel_running_tasks_on_switch: bool = True,
        touch_master: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        normalized_id = self._normalize_id(project_id)
        self._validate_project_access(normalized_id)

        if self.active_context:
            if self.active_context.id == normalized_id:
                return self.active_context
            self.close_project(cancel_running_tasks=cancel_running_tasks_on_switch)

        context = self.project_manager.load_project_by_id(
            normalized_id,
            sm=self.settings_manager,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )
        self._activate_context(context, touch_master=touch_master)
        self.app_logger.info(
            "Project activated: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )
        return context

    def create_project(
        self,
        name: str,
        folder: Path | str,
        description: str = "",
        tags: list[str] | None = None,
        base_settings: SettingsProfile = "global",
        scope: str = "full",
        activate: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        folder = Path(folder).expanduser().resolve()
        normalized_scope = str(scope).strip()
        effective_scope = normalized_scope or "full"
        if effective_scope == "full":
            effective_scope = self.app_id
        context = self.project_manager.create_project(
            name=name,
            folder=folder,
            sm=self.settings_manager,
            base=base_settings,
            description=description,
            scope=effective_scope,
            app_id=self.app_id,
            tags=tags,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )
        if not activate:
            return context

        if self.active_context and self.active_context.id != context.id:
            self.close_project(cancel_running_tasks=True)
        self._activate_context(context, touch_master=True)
        self.app_logger.info(
            "Project created and activated: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )
        return context

    def find_project(
        self,
        *,
        name: str | None = None,
        folder: Path | str | None = None,
    ) -> Project | None:
        return self.project_manager.find_project(name=name, folder=folder)

    def create_or_open_project(
        self,
        *,
        name: str,
        folder: Path | str,
        description: str = "",
        tags: list[str] | None = None,
        base_settings: SettingsProfile = "global",
        scope: str = "full",
        cancel_running_tasks_on_switch: bool = True,
        activate: bool = True,
        extra_dirs: list[str] | None = None,
    ) -> ProjectContext:
        folder_path = Path(folder).expanduser().resolve()
        existing_at_path = self.project_manager.find_project_global(folder=folder_path)
        if existing_at_path is not None:
            existing_app_id = (existing_at_path.app_id or "").strip()
            if existing_app_id != self.app_id:
                raise ValueError(
                    f"Path '{folder_path}' already belongs to project '{existing_at_path.name}' "
                    f"of app_id='{existing_app_id or '<empty>'}'."
                )
            if activate:
                return self.open_project(
                    existing_at_path.id,
                    cancel_running_tasks_on_switch=cancel_running_tasks_on_switch,
                    touch_master=True,
                    extra_dirs=self._merge_project_dirs(extra_dirs),
                )
            return self.project_manager.load_project(
                project=existing_at_path,
                sm=self.settings_manager,
                extra_dirs=self._merge_project_dirs(extra_dirs),
            )
        existing = self.project_manager.find_project(name=name)
        if existing is not None:
            if activate:
                return self.open_project(
                    existing.id,
                    cancel_running_tasks_on_switch=cancel_running_tasks_on_switch,
                    touch_master=True,
                    extra_dirs=self._merge_project_dirs(extra_dirs),
                )
            return self.project_manager.load_project(
                project=existing,
                sm=self.settings_manager,
                extra_dirs=self._merge_project_dirs(extra_dirs),
            )
        return self.create_project(
            name=name,
            folder=folder_path,
            description=description,
            tags=tags,
            base_settings=base_settings,
            scope=scope,
            activate=activate,
            extra_dirs=self._merge_project_dirs(extra_dirs),
        )

    def register_task_canceller(self, project_id: str | UUID, canceller: Callable[[], None]):
        normalized_id = self._normalize_id(project_id)
        runtime = self._require_active_project_runtime()
        if runtime.context.id != normalized_id:
            raise RuntimeError("Cancellers can only be registered for the active project.")
        runtime.task_cancellers.append(canceller)

    def get_app_logger(self, name: str = "core"):
        return self.logging_manager.get_app_logger(name)

    def get_executor_logger(self, name: str = "core"):
        return self.logging_manager.get_executor_logger(name)

    def get_project_logger(self, name: str = "core"):
        return self.logging_manager.get_project_logger(name)

    def get_entity_loader_context(self) -> ProjectDataContext:
        return self.advanced.entity_loader_context()

    def get_project_data_context(self) -> ProjectDataContext:
        return self.advanced.project_data_context()

    def refresh_master_db(self):
        self.master_db.reconnect()
        self.app_logger.debug("Master DB reconnected")

    def close_project(self, cancel_running_tasks: bool = True):
        """Close the active project. Its jobs that have not ended are interrupted, not lost:
        submitted again under the same key in a later session, each continues where it stopped.

        With `cancel_running_tasks=False`, raises instead when one is still going on.
        """
        if not self.active_context:
            self.settings_manager.clear_project()
            self.logging_manager.clear_project_logging()
            return

        context = self.active_context
        if cancel_running_tasks:
            self._cancel_project_tasks(context.id)
        elif self.get_project_activity()["jobs_active"]:
            raise RuntimeError("The project cannot be closed while active jobs remain.")

        self.project_manager.close_project(context)
        self._close_project_runtime()
        self.settings_manager.clear_project()
        self.app_logger.info(
            "Project closed: id=%s name=%s",
            context.id,
            context.name,
            extra={"project_id": str(context.id)},
        )

    def get_project_activity(self, project_id: str | UUID | None = None) -> dict[str, Any]:
        """What is still going on in a project: its jobs that have not ended, and the external
        tasks registered with `register_task_canceller`. Only the active project has any.
        """
        context = self.active_context
        mine = context is not None and (project_id is None or context.id == self._normalize_id(project_id))
        # ponytail: every recorded job of the engine counts, as the project's journal is the
        # only one this facade attaches.
        jobs = [
            job
            for job in (self._engine.jobs() if mine and self._flow is not None else ())
            if job.key is not None and job.status in ("waiting", "running")
        ]
        cancellers = len(self._runtime_state.active_project.task_cancellers) if mine else 0
        active = bool(jobs or cancellers)
        return {
            "project_id": str(project_id or context.id) if project_id or context else None,
            "active": active,
            "can_switch_project": not active,
            "jobs_active": len(jobs),
            "job_ids": [job.id for job in jobs],
            "statuses": sorted({job.status for job in jobs}),
            "external_cancellers": cancellers,
        }

    def _cancel_project_tasks(self, project_id: UUID):
        runtime = self._runtime_state.active_project
        if runtime is None or runtime.context is None or runtime.context.id != project_id:
            return
        cancellers = list(runtime.task_cancellers or [])
        runtime.task_cancellers = []
        errors = []
        for cancel in cancellers:
            try:
                cancel()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError(f"Cancellation failed for {len(errors)} task(s).")

    def delete_projects(self, project_ids: list[str | UUID], delete_files: bool = True):
        normalized_ids = []
        for project_id in project_ids:
            try:
                normalized_ids.append(self._normalize_id(project_id))
            except ValueError:
                continue

        if not normalized_ids:
            return

        if self.active_context and self.active_context.id in normalized_ids:
            self.close_project()

        self.project_manager.delete_projects(normalized_ids, delete_files=delete_files)
        self.app_logger.info("Projects deleted: count=%s delete_files=%s", len(normalized_ids), delete_files)

    def _activate_context(self, context: ProjectContext, touch_master: bool):
        project_db = ProjectStore()
        project_db.connect(context.path)
        active_project = ActiveProjectRuntime(context=context, project_db=project_db, project_store=project_db)
        self._runtime_state = ProjectRuntimeState(active_project=active_project)
        self.logging_manager.set_project_logging(context.path)
        active_project.project_logger = self.logging_manager.get_project_logger("core")
        self.project_logger.info(
            "Project context active: id=%s path=%s",
            context.id,
            context.path,
            extra={"project_id": str(context.id)},
        )
        self.app_logger.info(
            "Project runtime active for project=%s",
            context.id,
            extra={"project_id": str(context.id)},
        )
        if touch_master:
            self.project_manager.touch_project(context.id)

    def shutdown(self):
        try:
            self.close_project(cancel_running_tasks=True)
        except Exception:
            pass
        self._shutdown_runtime_stack()
        try:
            self.logging_manager.stop()
        except Exception:
            pass


__all__ = ["WorkspaceProjectRuntimeMixin"]
