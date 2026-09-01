from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


_SUPPORTED_VALUE_TYPES = {"boolean", "integer", "number", "string"}


def _normalize_identifier(raw: object, *, label: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError(f"{label} must not be empty.")
    if any(character.isspace() for character in value) or "." in value:
        raise ValueError(f"Invalid {label.lower()} '{raw}'.")
    return value


@dataclass(frozen=True)
class AppSettingSpec:
    """Declarative app setting metadata for persistence and future UI editors."""

    key: str
    default: Any
    value_type: str
    label: str = ""
    description: str = ""
    section: str = "General"
    minimum: int | float | None = None
    maximum: int | float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", _normalize_identifier(self.key, label="App setting key"))
        value_type = str(self.value_type or "").strip().lower()
        if value_type not in _SUPPORTED_VALUE_TYPES:
            supported = ", ".join(sorted(_SUPPORTED_VALUE_TYPES))
            raise ValueError(f"Unsupported app setting value_type '{self.value_type}'. Expected one of: {supported}.")
        object.__setattr__(self, "value_type", value_type)
        object.__setattr__(self, "label", str(self.label or self.key).strip())
        object.__setattr__(self, "description", str(self.description or "").strip())
        object.__setattr__(self, "section", str(self.section or "General").strip() or "General")
        self.validate(self.default)

    def validate(self, value: Any) -> Any:
        if self.value_type == "boolean":
            valid_type = isinstance(value, bool)
        elif self.value_type == "integer":
            valid_type = isinstance(value, int) and not isinstance(value, bool)
        elif self.value_type == "number":
            valid_type = isinstance(value, (int, float)) and not isinstance(value, bool)
        else:
            valid_type = isinstance(value, str)

        if not valid_type:
            raise ValueError(
                f"App setting '{self.key}' expects {self.value_type}, got {type(value).__name__}."
            )
        if self.minimum is not None and value < self.minimum:
            raise ValueError(f"App setting '{self.key}' must be >= {self.minimum}.")
        if self.maximum is not None and value > self.maximum:
            raise ValueError(f"App setting '{self.key}' must be <= {self.maximum}.")
        return value


@dataclass(frozen=True)
class AppSettingsContract:
    app_id: str
    specs: tuple[AppSettingSpec, ...] = ()

    def __post_init__(self) -> None:
        app_id = _normalize_identifier(self.app_id, label="App id")
        specs = coerce_app_setting_specs(self.specs)
        seen: set[str] = set()
        for spec in specs:
            if spec.key in seen:
                raise ValueError(f"Duplicate app setting key '{spec.key}' for app '{app_id}'.")
            seen.add(spec.key)
        object.__setattr__(self, "app_id", app_id)
        object.__setattr__(self, "specs", specs)

    def defaults(self) -> dict[str, Any]:
        return {spec.key: spec.default for spec in self.specs}

    def get(self, key: str) -> AppSettingSpec:
        normalized_key = _normalize_identifier(key, label="App setting key")
        for spec in self.specs:
            if spec.key == normalized_key:
                return spec
        known = ", ".join(spec.key for spec in self.specs)
        raise KeyError(
            f"Unknown app setting '{normalized_key}' for app '{self.app_id}'. Known: {known or '<none>'}."
        )

    def validate_values(self, values: Mapping[str, Any]) -> None:
        known = {spec.key: spec for spec in self.specs}
        for key, value in values.items():
            if key not in known:
                raise ValueError(f"Unknown app setting '{key}' for app '{self.app_id}'.")
            known[key].validate(value)


def coerce_app_setting_spec(raw: AppSettingSpec | Mapping[str, Any] | object) -> AppSettingSpec:
    if isinstance(raw, AppSettingSpec):
        return raw
    source = raw if isinstance(raw, Mapping) else None

    def read(name: str, default: Any = None) -> Any:
        return (source or {}).get(name, default) if source is not None else getattr(raw, name, default)

    return AppSettingSpec(
        key=read("key", ""),
        default=read("default"),
        value_type=read("value_type", ""),
        label=read("label", ""),
        description=read("description", ""),
        section=read("section", "General"),
        minimum=read("minimum"),
        maximum=read("maximum"),
    )


def coerce_app_setting_specs(
    raw_specs: Iterable[AppSettingSpec | Mapping[str, Any] | object] | None,
) -> tuple[AppSettingSpec, ...]:
    return tuple(coerce_app_setting_spec(item) for item in (raw_specs or ()))


__all__ = [
    "AppSettingSpec",
    "AppSettingsContract",
    "coerce_app_setting_spec",
    "coerce_app_setting_specs",
]
