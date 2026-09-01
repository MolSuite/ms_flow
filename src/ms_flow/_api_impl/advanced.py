from __future__ import annotations

from typing import TYPE_CHECKING

from ms_flow.core.project import ProjectDataContext

if TYPE_CHECKING:
    from ms_flow.core.database import ExecutorDB, ProjectStore
    from ms_flow.core.executor.manager import ExecutorManager
    from ms_flow.core.project.context import ProjectContext
    from ms_flow.main import MolSuite


class MolSuiteAdvancedAccess:
    """
    Explicit door to engine internals.

    The happy path should use `molsuite.api`.
    If explicit typing/imports are needed, the advanced public surface lives in
    `molsuite.advanced.MolSuiteAdvancedAccess`.

    This adapter exists for advanced scenarios that genuinely need to touch
    `ExecutorManager`, `ProjectStore` or the active runtime's DB handles.
    """

    def __init__(self, molsuite: "MolSuite"):
        self._molsuite = molsuite

    @property
    def active_context(self) -> "ProjectContext | None":
        return self._molsuite.active_context

    @property
    def executor_db(self) -> "ExecutorDB | None":
        return self._molsuite.executor_db

    @property
    def executor_manager(self) -> "ExecutorManager | None":
        return self._molsuite.executor_manager

    @property
    def project_store(self) -> "ProjectStore":
        self._molsuite._require_runtime()
        store = self._molsuite.project_store
        if store is None:
            raise RuntimeError("ProjectStore is not initialised. Activate a project first.")
        return store

    def project_data_context(self) -> ProjectDataContext:
        return ProjectDataContext(
            molsuite=self._molsuite,
            active_context=self._molsuite.active_context,
            project_store_handle=self.project_store,
            project_resources=self._molsuite.get_project_resource_map(),
        )

    def entity_loader_context(self) -> ProjectDataContext:
        return self.project_data_context()

    def runtime_healthcheck(self) -> dict[str, object]:
        return self._molsuite.get_runtime_healthcheck()


__all__ = ["MolSuiteAdvancedAccess"]
