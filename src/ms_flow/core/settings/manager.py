from pathlib import Path
from typing import Any, Literal, Optional

import toml
from pydantic import ValidationError

from ms_flow.core.app_settings import AppSettingSpec, AppSettingsContract
from ms_flow.core.configuration import configuration_entries, get_path_value
from ms_flow.core.events import setting_changed
from ms_flow.core.settings.models import Settings


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

        # Capas internas
        self._default = self._load_packaged_defaults()
        self._global: Settings = self._load_global_initial()
        self._local: Optional[Settings] = None
        self._app_settings_contracts: dict[str, AppSettingsContract] = {}
        self._effective: Settings = self._build_effective_settings()

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

    @staticmethod
    def _set_by_path(data: dict[str, Any], key_path: str, value: Any):
        parts = [part.strip() for part in key_path.split(".") if part.strip()]
        if not parts:
            raise ValueError("The key cannot be empty.")

        cursor = data
        for part in parts[:-1]:
            nested = cursor.get(part)
            if nested is None:
                nested = {}
                cursor[part] = nested
            elif not isinstance(nested, dict):
                raise ValueError(
                    f"Invalid path '{key_path}'. '{part}' is not a nested object."
                )
            cursor = nested
        cursor[parts[-1]] = value

    def _load_settings_file(self, path: Path, base_model: Settings) -> Settings:
        with open(path, "r") as f:
            raw_data = toml.load(f) or {}

        base_data = base_model.model_dump(mode="python")
        merged_data = self._deep_merge_dicts(base_data, raw_data)
        if "executor_db" not in raw_data:
            merged_data["executor_db"] = None
        return Settings.model_validate(merged_data)

    def _build_effective_settings(self) -> Settings:
        data = self._default.model_dump(mode="python")
        data = self._deep_merge_dicts(data, self._global.model_dump(mode="python"))
        if self._local is not None:
            data = self._deep_merge_dicts(data, self._local.model_dump(mode="python"))
        effective = Settings.model_validate(data)
        self._validate_registered_app_settings(effective)
        return effective

    def _refresh_effective(self):
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
        self._app_settings_contracts[contract.app_id] = contract
        try:
            self._validate_registered_app_settings(self._global)
            if self._local is not None:
                self._validate_registered_app_settings(self._local)
        except Exception:
            if previous is None:
                self._app_settings_contracts.pop(contract.app_id, None)
            else:
                self._app_settings_contracts[contract.app_id] = previous
            raise

        self._default = candidate_default
        self._refresh_effective()
        return contract.specs

    def remove_app_settings(self, app_id: str) -> dict[str, Any]:
        """Remove an obsolete flat application section from every loaded layer."""
        normalized_app_id = str(app_id or "").strip()
        if not normalized_app_id:
            raise ValueError("App id must not be empty.")

        def stripped(model: Settings) -> tuple[Settings, dict[str, Any]]:
            data = model.model_dump(mode="python")
            applications = data.setdefault("applications", {})
            removed = dict(applications.pop(normalized_app_id, {}) or {})
            return Settings.model_validate(data), removed

        self._default, _default_removed = stripped(self._default)
        self._global, global_removed = stripped(self._global)
        self._save_to_disk(self._global, self.global_path)

        local_removed: dict[str, Any] = {}
        if self._local is not None and self.project_path is not None:
            self._local, local_removed = stripped(self._local)
            self._save_to_disk(self._local, self.project_path)

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

    def _load_global_initial(self) -> Settings:
        """Load the global settings at start-up, creating them from Default if missing."""
        if self.global_path.exists():
            return self._load_settings_file(self.global_path, self._default)

        # Create the global file for the first time
        self.global_dir.mkdir(parents=True, exist_ok=True)
        self._save_to_disk(self._default, self.global_path)
        return self._default.model_copy()

    # --- PROJECT WORKFLOW ---

    def set_project(self, folder_path: Path, base: Literal["default", "global"] = "global"):
        """
        Step 2: called when creating/opening a project.
        If there is no local config.toml, it creates one from the user's choice.
        """
        folder_path = Path(folder_path).expanduser().resolve()
        self.project_path = folder_path / "config.toml"

        if self.project_path.exists():
            self._local = self._load_settings_file(self.project_path, self._global)
        else:
            # Create the initial copy following the user's preference
            base_model = self._default if base == "default" else self._global
            self._local = base_model.model_copy()
            self._save_to_disk(self._local, self.project_path)
        self._refresh_effective()

    def clear_project(self):
        """Clear the local context so the global configuration is used again."""
        self.project_path = None
        self._local = None
        self._refresh_effective()

    # --- ACCESS AND MODIFICATION ---

    @property
    def settings(self) -> Settings:
        """Main attribute to read the current config (Default < Global < Local)."""
        return self._effective

    @property
    def has_project(self) -> bool:
        return self._local is not None and self.project_path is not None

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
        effective = self.get_value(path)
        default = self.get_default_value(path)
        if effective == default:
            return "default"
        global_value = self.get_global_value(path)
        if self.has_project and effective != global_value:
            return "project"
        if global_value != default:
            return "global"
        return "default"

    def set_value(self, path: str, value: Any) -> None:
        self.update_setting(path, value)

    def reset_value(self, path: str, target: Literal["global", "default"]) -> None:
        if target == "global":
            if not self.has_project:
                raise ValueError("Reset to global is only available for an active project.")
            self.update_setting(path, self.get_global_value(path))
            return
        if target == "default":
            self.update_setting(path, self.get_default_value(path))
            return
        raise ValueError(f"Unknown reset target: {target}")

    def update_setting(self, key: str, value: Any, save_global_too: bool = False):
        """
        Step 3: change at runtime, validate and save.
        With a project open it saves locally; if asked, in the global file too.
        """
        try:
            # 1) Apply the change to the active layer (project or global)
            if self._local and self.project_path:
                local_data = self._local.model_dump(mode="python")
                self._set_by_path(local_data, key, value)
                candidate_local = Settings.model_validate(local_data)
                self._validate_registered_app_settings(candidate_local)
                self._local = candidate_local
                self._save_to_disk(self._local, self.project_path)
                active_scope = "project"
            else:
                global_data = self._global.model_dump(mode="python")
                self._set_by_path(global_data, key, value)
                candidate_global = Settings.model_validate(global_data)
                self._validate_registered_app_settings(candidate_global)
                self._global = candidate_global
                self._save_to_disk(self._global, self.global_path)
                active_scope = "global"

            # 2) Optional: persist in the global file as well
            if save_global_too:
                global_data = self._global.model_dump(mode="python")
                self._set_by_path(global_data, key, value)
                candidate_global = Settings.model_validate(global_data)
                self._validate_registered_app_settings(candidate_global)
                self._global = candidate_global
                self._save_to_disk(self._global, self.global_path)
                setting_changed.send(self, key=key, value=value, scope="global")

            # 3) Refresh the effective composition and notify observers
            self._refresh_effective()
            setting_changed.send(self, key=key, value=value, scope=active_scope)

        except ValidationError as e:
            raise ValueError(f"Invalid value for {key}: {e}")

    def _save_to_disk(self, model: Settings, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        with open(temp_path, "w") as f:
            toml.dump(model.model_dump(mode="json"), f)
        temp_path.replace(path)


if __name__ == '__main__':
    settings = SettingsManager()
    print(settings.settings)
