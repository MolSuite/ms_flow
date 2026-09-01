from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import UUID

from ms_flow._api_impl import (
    MolSuiteAdvancedAccess,
    MolSuiteJobApiMixin,
    MolSuiteProjectRuntimeMixin,
)
from ms_flow.artifacts import ArtifactRegistry
from ms_flow.core.app_settings import coerce_app_setting_specs
from ms_flow.core.database import ExecutorDB, ExecutorStore, MasterDB, ProjectStore
from ms_flow.core.project import ProjectResourceContract
from ms_flow.core.project import ProjectRuntimeState
from ms_flow.core.project.context import ProjectContext
from ms_flow.core.project.resources import coerce_project_resource_specs
from ms_flow.core.project.manager import ProjectManager
from ms_flow.core.settings.manager import SettingsManager
from ms_flow.logger import LoggingManager


class MolSuite(MolSuiteJobApiMixin, MolSuiteProjectRuntimeMixin):
    """
    Main core facade.

    - Lightweight global bootstrap.
    - Per-process persistent runtime.
    - Swappable active project.
    """

    def __init__(
        self,
        *,
        app_id: str,
        host_namespace: str | None = None,
        project_resources=None,
        app_settings=None,
    ):
        self.app_id = str(app_id).strip()
        if not self.app_id:
            raise ValueError("MolSuite requires a non-empty app_id.")
        self.host_namespace = str(host_namespace or "").strip() or self.app_id
        self.settings_manager = SettingsManager()
        legacy_specs = coerce_app_setting_specs(app_settings)
        if legacy_specs:
            self.settings_manager.register_app_settings(self.app_id, legacy_specs)
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
        self.app_logger = self.logging_manager.get_app_logger("core")
        self.executor_logger = None

        self._master_db_path = self.settings_manager.settings.projects_db
        self._executor_db_relative = Path("executor.db")

        self.master_db = MasterDB(self._master_db_path)
        self.project_manager = ProjectManager(self.master_db, app_id_filter=self.app_id)
        self._project_resource_contract = ProjectResourceContract(
            app_id=self.app_id,
            specs=coerce_project_resource_specs(project_resources),
        )

        self._runtime_state = ProjectRuntimeState()
        self.project_logger = None
        self._advanced = MolSuiteAdvancedAccess(self)

    @property
    def executor_db(self) -> ExecutorDB | None:
        runtime = self._runtime_state.active_project
        return runtime.executor_db if runtime is not None else None

    def app_setting_specs(self):
        return self.settings_manager.app_setting_specs(self.app_id)

    def get_app_setting(self, key: str):
        return self.settings_manager.get_app_setting(self.app_id, key)

    def update_app_setting(self, key: str, value, *, save_global_too: bool = False) -> None:
        self.settings_manager.update_app_setting(
            self.app_id,
            key,
            value,
            save_global_too=save_global_too,
        )

    @property
    def executor_access(self) -> ExecutorStore | None:
        return self.executor_db

    @property
    def project_db(self) -> ProjectStore | None:
        runtime = self._runtime_state.active_project
        return runtime.project_db if runtime is not None else None

    @property
    def project_store(self) -> ProjectStore | None:
        runtime = self._runtime_state.active_project
        return runtime.project_store if runtime is not None else None

    @property
    def executor_manager(self):
        return self._runtime_state.executor_manager

    @property
    def active_context(self) -> Optional[ProjectContext]:
        runtime = self._runtime_state.active_project
        return runtime.context if runtime is not None else None

    @property
    def project_logger(self):
        runtime = self._runtime_state.active_project
        return runtime.project_logger if runtime is not None else None

    @project_logger.setter
    def project_logger(self, value):
        runtime = self._runtime_state.active_project
        if runtime is not None:
            runtime.project_logger = value

    @property
    def advanced(self) -> MolSuiteAdvancedAccess:
        """Explicit access to advanced surfaces outside the public happy path."""
        return self._advanced

    @property
    def artifacts(self) -> ArtifactRegistry:
        if self.project_db is None:
            raise RuntimeError("There is no active project to access the artifact registry.")
        return ArtifactRegistry(self.project_db, app_id=self.app_id)

    @staticmethod
    def _normalize_id(project_id: str | UUID) -> UUID:
        try:
            return UUID(str(project_id))
        except ValueError as exc:
            raise ValueError("Invalid project id.") from exc

    def _validate_project_access(self, project_id: str | UUID):
        project = self.project_manager.get_project_by_id_global(project_id)
        project_app_id = (project.app_id or "").strip()
        if project_app_id != self.app_id:
            raise ValueError(
                f"Project '{project.name}' belongs to app_id='{project_app_id or '<empty>'}' "
                f"and cannot be activated from app '{self.app_id}'."
            )
        return project


if __name__ == "__main__":
    ms = MolSuite(app_id="molsuite")
    print(ms)
