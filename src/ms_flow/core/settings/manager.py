from copy import deepcopy
from pathlib import Path
from typing import Any, Literal, Optional

import toml
from pydantic import ValidationError

from ms_flow.core.app_settings import AppSettingSpec, AppSettingsContract
from ms_flow.core.configuration import (
    configuration_entries,
    delete_path_value,
    get_path_value,
    set_path_value,
)
from ms_flow.core.events import setting_changed
from ms_flow.core.settings.models import Settings

_MISSING = object()


class SettingsManager:
    config_id = "ms_flow"
    display_name = "Molsuite Flow"
    description = "Orchestration, logging, executor and resource settings."
    icon_name = None

    def __init__(self):
        self.global_dir = Path.home() / ".molsuite"
        self.global_path = self.global_dir / "config.toml"
        self.project_path: Optional[Path] = None
        self.default_path = Path(__file__).with_name("defaults.toml")

        self._default = self._load_packaged_defaults()
        loaded_global = self._load_overrides(self.global_path)
        self._global_overrides = self._prune_equal(
            loaded_global,
            self._default.model_dump(mode="json"),
        )
        self._local_overrides: dict[str, Any] | None = None
        self._app_settings_contracts: dict[str, AppSettingsContract] = {}
        self._global = self._compose(self._global_overrides)
        self._local: Optional[Settings] = None
        self._effective: Settings = self._build_effective_settings()
        if loaded_global != self._global_overrides or not self.global_path.exists():
            self._save_overrides(self.global_path, self._global_overrides)

    def _load_packaged_defaults(self) -> Settings:
        base = Settings().model_dump(mode="python")
        packaged = toml.load(self.default_path)
        return Settings.model_validate(self._deep_merge_dicts(base, packaged))

    @staticmethod
    def _deep_merge_dicts(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in overrides.items():
            base_value = merged.get(key)
            if isinstance(base_value, dict) and isinstance(value, dict):
                merged[key] = SettingsManager._deep_merge_dicts(base_value, value)
                continue
            merged[key] = value
        return merged

    @classmethod
    def _prune_equal(cls, overrides: dict[str, Any], base: dict[str, Any]) -> dict[str, Any]:
        """Convert legacy full snapshots into sparse overrides."""
        result: dict[str, Any] = {}
        for key, value in overrides.items():
            base_value = base.get(key, _MISSING)
            if isinstance(value, dict) and isinstance(base_value, dict):
                nested = cls._prune_equal(value, base_value)
                if nested:
                    result[key] = nested
            elif base_value is _MISSING or value != base_value:
                result[key] = deepcopy(value)
        return result

    @staticmethod
    def _load_overrides(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        data = toml.load(path) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Configuration file must contain a TOML table: {path}")
        return data

    def _compose(self, *layers: dict[str, Any]) -> Settings:
        data = self._default.model_dump(mode="python")
        for layer in layers:
            data = self._deep_merge_dicts(data, layer)
            # executor_db is derived from projects_db unless that same layer explicitly
            # chooses it. This preserves the existing Settings validator contract.
            if "projects_db" in layer and "executor_db" not in layer:
                data["executor_db"] = None
        return Settings.model_validate(data)

    def _build_effective_settings(self) -> Settings:
        layers = [self._global_overrides]
        if self._local_overrides is not None:
            layers.append(self._local_overrides)
        effective = self._compose(*layers)
        self._validate_registered_app_settings(effective)
        return effective

    def _refresh_effective(self):
        self._global = self._compose(self._global_overrides)
        self._local = (
            self._compose(self._global_overrides, self._local_overrides)
            if self._local_overrides is not None
            else None
        )
        self._effective = self._build_effective_settings()

    def _validate_registered_app_settings(self, settings: Settings) -> None:
        for app_id, contract in self._app_settings_contracts.items():
            contract.validate_values(settings.applications.get(app_id, {}))

    def register_app_settings(
        self,
        app_id: str,
        specs: tuple[AppSettingSpec, ...] | list[AppSettingSpec] | None,
    ) -> tuple[AppSettingSpec, ...]:
        contract = AppSettingsContract(app_id=app_id, specs=tuple(specs or ()))
        # An application using the newer Pydantic configuration providers does
        # not declare values through this legacy flat contract.  Treat that as
        # "not registered" instead of validating historical values against an
        # empty schema, so older user configuration files remain loadable.
        if not contract.specs:
            self.remove_app_settings(contract.app_id)
            return ()
        defaults = self._default.model_dump(mode="python")
        applications = defaults.setdefault("applications", {})
        applications[contract.app_id] = contract.defaults()
        candidate_default = Settings.model_validate(defaults)

        previous = self._app_settings_contracts.get(contract.app_id)
        previous_default = self._default
        self._app_settings_contracts[contract.app_id] = contract
        try:
            self._default = candidate_default
            self._refresh_effective()
        except Exception:
            if previous is None:
                self._app_settings_contracts.pop(contract.app_id, None)
            else:
                self._app_settings_contracts[contract.app_id] = previous
            self._default = previous_default
            self._refresh_effective()
            raise
        return contract.specs

    def remove_app_settings(self, app_id: str) -> dict[str, Any]:
        """Remove an obsolete flat application section from every loaded layer."""
        normalized_app_id = str(app_id or "").strip()
        if not normalized_app_id:
            raise ValueError("App id must not be empty.")

        def strip(data: dict[str, Any]) -> dict[str, Any]:
            applications = data.get("applications")
            if not isinstance(applications, dict):
                return {}
            removed = dict(applications.pop(normalized_app_id, {}) or {})
            if not applications:
                data.pop("applications", None)
            return removed

        default_data = self._default.model_dump(mode="python")
        strip(default_data)
        self._default = Settings.model_validate(default_data)
        global_removed = strip(self._global_overrides)
        self._save_overrides(self.global_path, self._global_overrides)

        local_removed: dict[str, Any] = {}
        if self._local_overrides is not None and self.project_path is not None:
            local_removed = strip(self._local_overrides)
            self._save_overrides(self.project_path, self._local_overrides)

        self._app_settings_contracts.pop(normalized_app_id, None)
        self._refresh_effective()
        return local_removed or global_removed

    def app_setting_specs(self, app_id: str) -> tuple[AppSettingSpec, ...]:
        contract = self._app_settings_contracts.get(str(app_id or "").strip())
        return contract.specs if contract is not None else ()

    def get_app_setting(self, app_id: str, key: str) -> Any:
        normalized_app_id = str(app_id or "").strip()
        try:
            contract = self._app_settings_contracts[normalized_app_id]
        except KeyError as exc:
            raise KeyError(f"No app settings registered for '{normalized_app_id}'.") from exc
        spec = contract.get(key)
        value = self._effective.applications.get(normalized_app_id, {}).get(spec.key, spec.default)
        return spec.validate(value)

    def update_app_setting(
        self,
        app_id: str,
        key: str,
        value: Any,
        save_global_too: bool = False,
    ) -> None:
        normalized_app_id = str(app_id or "").strip()
        try:
            contract = self._app_settings_contracts[normalized_app_id]
        except KeyError as exc:
            raise KeyError(f"No app settings registered for '{normalized_app_id}'.") from exc
        spec = contract.get(key)
        spec.validate(value)
        self.update_setting(
            f"applications.{normalized_app_id}.{spec.key}",
            value,
            save_global_too=save_global_too,
        )

    # --- PROJECT WORKFLOW ---

    def set_project(self, folder_path: Path, base: Literal["default", "global"] = "global"):
        """
        Step 2: called when creating/opening a project.
        Project files contain overrides only. An empty file inherits the global layer.
        """
        folder_path = Path(folder_path).expanduser().resolve()
        self.project_path = folder_path / "config.toml"

        if self.project_path.exists():
            loaded_local = self._load_overrides(self.project_path)
            self._local_overrides = self._prune_equal(
                loaded_local,
                self._global.model_dump(mode="json"),
            )
            if loaded_local != self._local_overrides:
                self._save_overrides(self.project_path, self._local_overrides)
        else:
            self._local_overrides = (
                self._prune_equal(
                    self._default.model_dump(mode="json"),
                    self._global.model_dump(mode="json"),
                )
                if base == "default"
                else {}
            )
            self._save_overrides(self.project_path, self._local_overrides)
        self._refresh_effective()

    def clear_project(self):
        """Clear the local context so the global configuration is used again."""
        self.project_path = None
        self._local_overrides = None
        self._local = None
        self._refresh_effective()

    # --- ACCESS AND MODIFICATION ---

    @property
    def settings(self) -> Settings:
        """Main attribute to read the current config (Default < Global < Local)."""
        return self._effective

    @property
    def has_project(self) -> bool:
        return self._local_overrides is not None and self.project_path is not None

    def entries(self):
        return tuple(
            entry
            for entry in configuration_entries(Settings, self._default)
            if not entry.path.startswith("applications")
        )

    def custom_editors(self):
        """Sections that need a dedicated editor widget instead of scalar rows.
        ``workers`` is a polymorphic, dynamically-keyed map (local + Ray/HPC executors)
        the generic walker skips. Returns (kind, path, title) tuples — no Qt here."""
        return (("workers", "workers", "Workers / Executors"),)

    def get_value(self, path: str) -> Any:
        return get_path_value(self._effective, path)

    def get_default_value(self, path: str) -> Any:
        return get_path_value(self._default, path)

    def get_global_value(self, path: str) -> Any:
        return get_path_value(self._global, path)

    def get_source(self, path: str) -> str:
        if self._local_overrides is not None and get_path_value(
            self._local_overrides, path, _MISSING
        ) is not _MISSING:
            return "project"
        if get_path_value(self._global_overrides, path, _MISSING) is not _MISSING:
            return "global"
        return "default"

    def set_value(self, path: str, value: Any) -> None:
        self.update_setting(path, value)

    def set_global_value(self, path: str, value: Any) -> None:
        """Persist a user-level value without changing the active project layer."""
        try:
            overrides = deepcopy(self._global_overrides)
            set_path_value(overrides, path, value)
            candidate = self._compose(overrides)
            self._validate_registered_app_settings(candidate)
            set_path_value(overrides, path, get_path_value(candidate.model_dump(mode="json"), path))
            if self._local_overrides is not None:
                self._validate_registered_app_settings(
                    self._compose(overrides, self._local_overrides)
                )
            self._global_overrides = overrides
            self._save_overrides(self.global_path, overrides)
            self._refresh_effective()
            setting_changed.send(self, key=path, value=value, scope="global")
        except ValidationError as exc:
            raise ValueError(f"Invalid value for {path}: {exc}") from exc

    def reset_value(self, path: str, target: Literal["global", "default"]) -> None:
        if target == "global":
            if not self.has_project:
                raise ValueError("Reset to global is only available for an active project.")
            overrides = deepcopy(self._local_overrides or {})
            delete_path_value(overrides, path)
            candidate = self._compose(self._global_overrides, overrides)
            self._validate_registered_app_settings(candidate)
            self._local_overrides = overrides
            assert self.project_path is not None
            self._save_overrides(self.project_path, overrides)
            self._refresh_effective()
            return
        if target == "default":
            if self.has_project:
                self.update_setting(path, self.get_default_value(path))
            else:
                overrides = deepcopy(self._global_overrides)
                delete_path_value(overrides, path)
                self._global_overrides = overrides
                self._save_overrides(self.global_path, overrides)
                self._refresh_effective()
            return
        raise ValueError(f"Unknown reset target: {target}")

    def update_setting(self, key: str, value: Any, save_global_too: bool = False):
        """
        Step 3: change at runtime, validate and save.
        With a project open it saves locally; if asked, in the global file too.
        """
        try:
            if self._local_overrides is not None and self.project_path:
                overrides = deepcopy(self._local_overrides)
                set_path_value(overrides, key, value)
                candidate = self._compose(self._global_overrides, overrides)
                self._validate_registered_app_settings(candidate)
                set_path_value(overrides, key, get_path_value(candidate.model_dump(mode="json"), key))
                self._local_overrides = overrides
                self._save_overrides(self.project_path, overrides)
                active_scope = "project"
            else:
                overrides = deepcopy(self._global_overrides)
                set_path_value(overrides, key, value)
                candidate = self._compose(overrides)
                self._validate_registered_app_settings(candidate)
                set_path_value(overrides, key, get_path_value(candidate.model_dump(mode="json"), key))
                self._global_overrides = overrides
                self._save_overrides(self.global_path, overrides)
                active_scope = "global"

            if save_global_too:
                global_overrides = deepcopy(self._global_overrides)
                set_path_value(global_overrides, key, value)
                candidate_global = self._compose(global_overrides)
                self._validate_registered_app_settings(candidate_global)
                set_path_value(
                    global_overrides,
                    key,
                    get_path_value(candidate_global.model_dump(mode="json"), key),
                )
                self._global_overrides = global_overrides
                self._save_overrides(self.global_path, global_overrides)
                setting_changed.send(self, key=key, value=value, scope="global")

            self._refresh_effective()
            setting_changed.send(self, key=key, value=value, scope=active_scope)

        except ValidationError as e:
            raise ValueError(f"Invalid value for {key}: {e}")

    @staticmethod
    def _save_overrides(path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        with open(temp_path, "w") as f:
            toml.dump(data, f)
        temp_path.replace(path)


if __name__ == '__main__':
    settings = SettingsManager()
    print(settings.settings)
