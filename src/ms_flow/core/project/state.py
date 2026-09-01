from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ActiveProjectRuntime:
    """
    Resources bound strictly to the active project.

    They do not include the process-wide persistent engine. Only the context and
    the handles that must be opened/closed when entering or leaving a project.
    """

    context: Any = None
    executor_db: Any = None
    project_db: Any = None
    project_store: Any = None
    project_logger: Any = None
    executor_db_path: Path | None = None
    task_cancellers: list[Any] = field(default_factory=list)


@dataclass
class ProjectRuntimeState:
    """
    Heavy runtime associated with the active project.

    It groups the components that only make sense while a project is open, so
    `MolSuite` acts as a coordinator instead of a scattered bag of runtime handles.
    """

    executor_manager: Any = None
    active_project: ActiveProjectRuntime | None = None

    @property
    def initialized(self) -> bool:
        return (
            self.active_project is not None
            and self.active_project.executor_db is not None
            and self.active_project.project_db is not None
            and self.executor_manager is not None
        )
