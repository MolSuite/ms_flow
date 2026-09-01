from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping


def _normalize_key(raw_key: str) -> str:
    key = str(raw_key or "").strip()
    if not key:
        raise ValueError("Project resource key must not be empty.")
    if any(ch.isspace() for ch in key):
        raise ValueError(f"Invalid project resource key '{raw_key}'.")
    return key


def _normalize_relative_path(raw_path: str | Path) -> str:
    text = str(raw_path or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("Project resource relative_path must not be empty.")
    path = Path(text)
    if path.is_absolute():
        raise ValueError(f"Project resource path must be relative: '{raw_path}'.")
    parts = [part for part in path.parts if part not in ("", ".")]
    if not parts:
        raise ValueError(f"Project resource path must not be empty: '{raw_path}'.")
    if any(part == ".." for part in parts):
        raise ValueError(f"Project resource path must not escape project root: '{raw_path}'.")
    return "/".join(parts)


@dataclass(frozen=True)
class ProjectResourceSpec:
    key: str
    relative_path: str
    description: str = ""
    kind: str = "artifact_dir"

    def __post_init__(self):
        object.__setattr__(self, "key", _normalize_key(self.key))
        object.__setattr__(self, "relative_path", _normalize_relative_path(self.relative_path))
        object.__setattr__(self, "description", str(self.description or "").strip())
        object.__setattr__(self, "kind", str(self.kind or "artifact_dir").strip() or "artifact_dir")


@dataclass(frozen=True)
class ProjectResource:
    key: str
    relative_path: str
    path: Path
    description: str = ""
    kind: str = "artifact_dir"

    def to_mapping(self) -> dict[str, str]:
        return {
            "key": self.key,
            "relative_path": self.relative_path,
            "path": str(self.path),
            "description": self.description,
            "kind": self.kind,
        }


@dataclass(frozen=True)
class ProjectResourceContract:
    app_id: str
    specs: tuple[ProjectResourceSpec, ...] = ()

    def __post_init__(self):
        normalized_app_id = str(self.app_id or "").strip()
        if not normalized_app_id:
            raise ValueError("ProjectResourceContract requires a non-empty app_id.")
        object.__setattr__(self, "app_id", normalized_app_id)
        normalized_specs = tuple(coerce_project_resource_spec(item) for item in (self.specs or ()))
        seen: set[str] = set()
        for spec in normalized_specs:
            if spec.key in seen:
                raise ValueError(f"Duplicate project resource key '{spec.key}' for app '{normalized_app_id}'.")
            seen.add(spec.key)
        object.__setattr__(self, "specs", normalized_specs)

    def required_dirs(self) -> list[str]:
        dirs: list[str] = []
        for spec in self.specs:
            if spec.relative_path not in dirs:
                dirs.append(spec.relative_path)
        return dirs

    def resolve(self, project_root: str | Path) -> dict[str, ProjectResource]:
        root = Path(project_root).expanduser().resolve()
        return {
            spec.key: ProjectResource(
                key=spec.key,
                relative_path=spec.relative_path,
                path=(root / spec.relative_path).resolve(),
                description=spec.description,
                kind=spec.kind,
            )
            for spec in self.specs
        }

    def resolve_one(self, project_root: str | Path, key: str) -> ProjectResource:
        resources = self.resolve(project_root)
        normalized_key = _normalize_key(key)
        try:
            return resources[normalized_key]
        except KeyError as exc:
            known = ", ".join(sorted(resources.keys()))
            raise KeyError(
                f"Unknown project resource '{normalized_key}' for app '{self.app_id}'. Known: {known or '<none>'}."
            ) from exc


def coerce_project_resource_spec(raw: ProjectResourceSpec | Mapping[str, object] | object) -> ProjectResourceSpec:
    if isinstance(raw, ProjectResourceSpec):
        return raw
    source = raw if isinstance(raw, Mapping) else None
    key = (
        (source or {}).get("key", "")
        if source is not None
        else getattr(raw, "key", "")
    )
    relative_path = (
        (source or {}).get("relative_path", "")
        if source is not None
        else getattr(raw, "relative_path", "")
    )
    description = (
        (source or {}).get("description", "")
        if source is not None
        else getattr(raw, "description", "")
    )
    kind = (
        (source or {}).get("kind", "artifact_dir")
        if source is not None
        else getattr(raw, "kind", "artifact_dir")
    )
    return ProjectResourceSpec(
        key=str(key),
        relative_path=str(relative_path),
        description=str(description or ""),
        kind=str(kind or "artifact_dir"),
    )


def coerce_project_resource_specs(
    raw_specs: Iterable[ProjectResourceSpec | Mapping[str, object] | object] | None,
) -> tuple[ProjectResourceSpec, ...]:
    return tuple(coerce_project_resource_spec(item) for item in (raw_specs or ()))

