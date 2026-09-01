from __future__ import annotations

import enum
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from types import UnionType
from typing import Any, Literal, Union, get_args, get_origin

import toml
from pydantic import BaseModel


_MISSING = object()
_NO_DEFAULT = object()


def deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in overrides.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def get_path_value(source: Any, path: str, default: Any = _NO_DEFAULT) -> Any:
    current = source
    for part in (item for item in str(path).split(".") if item):
        if isinstance(current, BaseModel):
            if not hasattr(current, part):
                if default is _NO_DEFAULT:
                    raise KeyError(path)
                return default
            current = getattr(current, part)
        elif isinstance(current, dict):
            if part not in current:
                if default is _NO_DEFAULT:
                    raise KeyError(path)
                return default
            current = current[part]
        else:
            if default is _NO_DEFAULT:
                raise KeyError(path)
            return default
    return current


def set_path_value(target: dict[str, Any], path: str, value: Any) -> None:
    parts = [item for item in str(path).split(".") if item]
    if not parts:
        raise ValueError("Configuration path must not be empty.")
    current = target
    for part in parts[:-1]:
        child = current.setdefault(part, {})
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child
    current[parts[-1]] = value


def delete_path_value(target: dict[str, Any], path: str) -> None:
    parts = [item for item in str(path).split(".") if item]
    if not parts:
        return
    parents: list[tuple[dict[str, Any], str]] = []
    current = target
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            return
        parents.append((current, part))
        current = child
    current.pop(parts[-1], None)
    for parent, key in reversed(parents):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            parent.pop(key, None)


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin not in (Union, UnionType):
        return annotation, False
    args = tuple(item for item in get_args(annotation) if item is not type(None))
    nullable = len(args) != len(get_args(annotation))
    return (args[0] if len(args) == 1 else annotation), nullable


def _is_model_type(annotation: Any) -> bool:
    annotation, _nullable = _unwrap_optional(annotation)
    try:
        return isinstance(annotation, type) and issubclass(annotation, BaseModel)
    except TypeError:
        return False


def _field_constraints(field) -> tuple[int | float | None, int | float | None]:
    minimum = maximum = None
    for metadata in field.metadata:
        if getattr(metadata, "ge", None) is not None:
            minimum = metadata.ge
        elif getattr(metadata, "gt", None) is not None:
            minimum = metadata.gt
        if getattr(metadata, "le", None) is not None:
            maximum = metadata.le
        elif getattr(metadata, "lt", None) is not None:
            maximum = metadata.lt
    return minimum, maximum


def _field_choices(field) -> tuple[Any, ...]:
    """Values the UI should offer as a combo box: a Literal/Enum annotation, or a
    ``json_schema_extra={"choices": [...]}`` for sets only known at runtime (themes,
    installed backends) that must not harden into a type."""
    extra = field.json_schema_extra if isinstance(field.json_schema_extra, dict) else {}
    declared = extra.get("choices")
    if declared:
        return tuple(declared)
    annotation, _nullable = _unwrap_optional(field.annotation)
    origin = get_origin(annotation)
    if origin is Literal:
        return tuple(get_args(annotation))
    try:
        if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
            return tuple(item.value for item in annotation)
    except TypeError:
        pass
    return ()


@dataclass(frozen=True)
class ConfigurationEntry:
    path: str
    name: str
    description: str
    annotation: Any
    default: Any
    section: str
    minimum: int | float | None = None
    maximum: int | float | None = None
    choices: tuple[Any, ...] = ()
    nullable: bool = False


def configuration_entries(model_type: type[BaseModel], default_model: BaseModel) -> tuple[ConfigurationEntry, ...]:
    entries: list[ConfigurationEntry] = []

    def visit(current_type: type[BaseModel], current_default: BaseModel, prefix: str = "") -> None:
        for field_name, field in current_type.model_fields.items():
            annotation, nullable = _unwrap_optional(field.annotation)
            path = f"{prefix}.{field_name}" if prefix else field_name
            default_value = getattr(current_default, field_name)
            if _is_model_type(field.annotation) and isinstance(default_value, BaseModel):
                visit(annotation, default_value, path)
                continue
            extra = field.json_schema_extra if isinstance(field.json_schema_extra, dict) else {}
            if extra.get("settings_hidden"):
                # State the app manages (layouts, visible columns): persisted in the same
                # file, but not a setting anyone should type into a grid.
                continue
            minimum, maximum = _field_constraints(field)
            title = str(field.title or field_name.replace("_", " ").title())
            section_key = path.split(".", 1)[0]
            section = "Core" if "." not in path else section_key.replace("_", " ").title()
            entries.append(
                ConfigurationEntry(
                    path=path,
                    name=title,
                    description=str(field.description or ""),
                    annotation=annotation,
                    default=default_value,
                    section=section,
                    minimum=minimum,
                    maximum=maximum,
                    choices=_field_choices(field),
                    nullable=nullable,
                )
            )

    visit(model_type, default_model)
    return tuple(entries)


class PydanticConfiguration:
    """One package configuration with default, global and project layers."""

    def __init__(
        self,
        *,
        config_id: str,
        display_name: str,
        model_type: type[BaseModel],
        default_path: str | Path,
        global_path: str | Path,
        project_relative_path: str | Path | None = None,
        description: str = "",
        icon_name: str | None = None,
    ) -> None:
        self.config_id = str(config_id).strip()
        self.display_name = str(display_name).strip() or self.config_id
        if not self.config_id:
            raise ValueError("Configuration id must not be empty.")
        self.model_type = model_type
        self.default_path = Path(default_path).expanduser().resolve()
        self.global_path = Path(global_path).expanduser().resolve()
        self.project_relative_path = (
            Path(project_relative_path) if project_relative_path is not None else Path(".molsuite/config") / f"{self.config_id}.toml"
        )
        if self.project_relative_path.is_absolute() or ".." in self.project_relative_path.parts:
            raise ValueError("Project configuration path must stay inside the project root.")
        self.description = str(description or "").strip()
        self.icon_name = icon_name
        self.project_root: Path | None = None
        self.project_path: Path | None = None
        self._default = self._load_default()
        self._global_overrides = self._load_overrides(self.global_path)
        self._project_overrides: dict[str, Any] = {}
        self._global = self._validate_layers(self._global_overrides)
        self._effective = self._global.model_copy(deep=True)
        self._entries = configuration_entries(self.model_type, self._default)

    @staticmethod
    def _load_overrides(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        data = toml.load(path)
        if not isinstance(data, dict):
            raise ValueError(f"Configuration file must contain a TOML table: {path}")
        return data

    def _load_default(self) -> BaseModel:
        if not self.default_path.exists():
            raise FileNotFoundError(f"Packaged default configuration not found: {self.default_path}")
        return self.model_type.model_validate(toml.load(self.default_path))

    def _validate_layers(
        self,
        global_overrides: dict[str, Any],
        project_overrides: dict[str, Any] | None = None,
    ) -> BaseModel:
        data = self._default.model_dump(mode="python")
        data = deep_merge(data, global_overrides)
        if project_overrides is not None:
            data = deep_merge(data, project_overrides)
        return self.model_type.model_validate(data)

    def _refresh(self) -> None:
        self._global = self._validate_layers(self._global_overrides)
        self._effective = self._validate_layers(self._global_overrides, self._project_overrides)

    @staticmethod
    def _save_overrides(path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            toml.dump(data, handle)
        temporary.replace(path)

    @property
    def has_project(self) -> bool:
        return self.project_root is not None

    def set_project_root(self, project_root: str | Path | None) -> None:
        if project_root is None:
            self.project_root = None
            self.project_path = None
            self._project_overrides = {}
        else:
            self.project_root = Path(project_root).expanduser().resolve()
            self.project_path = (self.project_root / self.project_relative_path).resolve()
            self._project_overrides = self._load_overrides(self.project_path)
        self._refresh()

    def entries(self) -> tuple[ConfigurationEntry, ...]:
        return self._entries

    def get_value(self, path: str) -> Any:
        return get_path_value(self._effective, path)

    def get_default_value(self, path: str) -> Any:
        return get_path_value(self._default, path)

    def get_global_value(self, path: str) -> Any:
        return get_path_value(self._global, path)

    def get_source(self, path: str) -> str:
        if get_path_value(self._project_overrides, path, _MISSING) is not _MISSING:
            return "project"
        if get_path_value(self._global_overrides, path, _MISSING) is not _MISSING:
            return "global"
        return "default"

    def set_value(self, path: str, value: Any) -> None:
        target = deepcopy(self._project_overrides if self.has_project else self._global_overrides)
        set_path_value(target, path, value)
        if self.has_project:
            candidate = self._validate_layers(self._global_overrides, target)
        else:
            candidate = self._validate_layers(target)
        normalized = get_path_value(candidate.model_dump(mode="json"), path)
        set_path_value(target, path, normalized)
        if self.has_project:
            assert self.project_path is not None
            self._save_overrides(self.project_path, target)
            self._project_overrides = target
        else:
            self._save_overrides(self.global_path, target)
            self._global_overrides = target
        self._refresh()

    def reset_value(self, path: str, target: Literal["global", "default"]) -> None:
        if target == "global":
            if not self.has_project:
                raise ValueError("Reset to global is only available for an active project.")
            overrides = deepcopy(self._project_overrides)
            delete_path_value(overrides, path)
            assert self.project_path is not None
            self._validate_layers(self._global_overrides, overrides)
            self._save_overrides(self.project_path, overrides)
            self._project_overrides = overrides
        elif target == "default":
            if self.has_project:
                overrides = deepcopy(self._project_overrides)
                default_value = get_path_value(self._default.model_dump(mode="json"), path)
                set_path_value(overrides, path, default_value)
                assert self.project_path is not None
                self._validate_layers(self._global_overrides, overrides)
                self._save_overrides(self.project_path, overrides)
                self._project_overrides = overrides
            else:
                overrides = deepcopy(self._global_overrides)
                delete_path_value(overrides, path)
                self._validate_layers(overrides)
                self._save_overrides(self.global_path, overrides)
                self._global_overrides = overrides
        else:
            raise ValueError(f"Unknown reset target: {target}")
        self._refresh()


__all__ = [
    "ConfigurationEntry",
    "PydanticConfiguration",
    "configuration_entries",
    "deep_merge",
    "delete_path_value",
    "get_path_value",
    "set_path_value",
]
