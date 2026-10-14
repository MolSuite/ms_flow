from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ActiveProjectRuntime:
    """
    Resources bound strictly to the active project.

    They do not include the process-wide persistent engine. Only the context and
    the handles that must be opened/closed when entering or leaving a project.
    """

    context: Any = None
    project_db: Any = None
    project_store: Any = None
    project_logger: Any = None
    task_cancellers: list[Any] = field(default_factory=list)


@dataclass
class ProjectRuntimeState:
    """What only makes sense while a project is open."""

    active_project: ActiveProjectRuntime | None = None

    @property
    def initialized(self) -> bool:
        return self.active_project is not None and self.active_project.project_db is not None
