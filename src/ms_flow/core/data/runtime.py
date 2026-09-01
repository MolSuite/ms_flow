from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


@dataclass(frozen=True)
class DataContext:
    project_dir: Optional[Path] = None
    project_db_path: Optional[Path] = None
    executor_db_path: Optional[Path] = None
    extras: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "DataContext":
        project_dir = raw.get("project_dir") or raw.get("project_path")
        project_db_path = raw.get("project_db_path")
        executor_db_path = raw.get("executor_db_path")
        extras = {
            k: v
            for k, v in dict(raw).items()
            if k not in {"project_dir", "project_path", "project_db_path", "executor_db_path"}
        }
        return cls(
            project_dir=Path(project_dir).expanduser().resolve() if project_dir else None,
            project_db_path=Path(project_db_path).expanduser().resolve() if project_db_path else None,
            executor_db_path=Path(executor_db_path).expanduser().resolve() if executor_db_path else None,
            extras=extras,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "project_dir": str(self.project_dir) if self.project_dir else "",
            "project_db_path": str(self.project_db_path) if self.project_db_path else "",
            "executor_db_path": str(self.executor_db_path) if self.executor_db_path else "",
            **dict(self.extras),
        }


@dataclass(frozen=True)
class ExecutorTransportProfile:
    backend: str = "local"
    mode: str = "local"
    shared_fs: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "ExecutorTransportProfile":
        backend = str(raw.get("executor_backend") or raw.get("backend") or "local").strip().lower()
        mode = str(raw.get("executor_mode") or raw.get("mode") or "local").strip().lower()
        shared_fs = as_bool(raw.get("executor_shared_fs"), default=True)
        return cls(backend=backend, mode=mode, shared_fs=shared_fs)


@dataclass(frozen=True)
class ResolvedHandle:
    strategy: str
    spec_kind: str
    details: dict[str, Any] = field(default_factory=dict)
