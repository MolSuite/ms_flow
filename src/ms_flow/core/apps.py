from __future__ import annotations

import importlib
import importlib.util
import os
import pkgutil
import sys
import contextlib
from dataclasses import dataclass, field
from pathlib import Path

from ms_flow.core.app_settings import AppSettingSpec, coerce_app_setting_specs
from ms_flow.core.project.resources import ProjectResourceSpec, coerce_project_resource_specs


@dataclass(frozen=True)
class AppManifest:
    app_id: str
    scope_id: str
    name: str
    version: str
    description: str = ""
    entry_module: str = ""
    package_name: str = ""
    project_resources: tuple[ProjectResourceSpec, ...] = field(default_factory=tuple)
    source_root: Path | None = None
    settings: tuple[AppSettingSpec, ...] = field(default_factory=tuple)


class AppRegistry:
    def __init__(self):
        self._manifests: dict[str, AppManifest] = {}

    def register(self, manifest: AppManifest | object, *, source_root: Path | str | None = None) -> AppManifest:
        raw = manifest if isinstance(manifest, dict) else None
        app_id = str((raw or {}).get("app_id", "") if raw is not None else getattr(manifest, "app_id", "")).strip()
        if not app_id:
            raise ValueError("Invalid app manifest: empty app_id.")
        scope_id = str(
            (raw or {}).get("scope_id", "") if raw is not None else getattr(manifest, "scope_id", "")
        ).strip()
        if not scope_id:
            raise ValueError("Invalid app manifest: empty scope_id.")
        effective_source_root = (
            Path(source_root).expanduser().resolve()
            if source_root is not None
            else (
                Path(getattr(manifest, "source_root")).expanduser().resolve()
                if getattr(manifest, "source_root", None)
                else None
            )
        )
        stored = AppManifest(
            app_id=app_id,
            scope_id=scope_id,
            name=str((raw or {}).get("name", "") if raw is not None else getattr(manifest, "name", "")).strip(),
            version=str((raw or {}).get("version", "") if raw is not None else getattr(manifest, "version", "")).strip(),
            description=str(
                (raw or {}).get("description", "") if raw is not None else getattr(manifest, "description", "")
            ).strip(),
            entry_module=(
                str(
                    ((raw or {}).get("entry_module", "") if raw is not None else getattr(manifest, "entry_module", ""))
                    or f"{(raw or {}).get('package_name', '') if raw is not None else getattr(manifest, 'package_name', '')}.app"
                )
            ).strip(),
            package_name=str(
                (raw or {}).get("package_name", "") if raw is not None else getattr(manifest, "package_name", "")
            ).strip(),
            project_resources=coerce_project_resource_specs(
                (raw or {}).get("project_resources", ())
                if raw is not None
                else getattr(manifest, "project_resources", ())
            ),
            settings=coerce_app_setting_specs(
                (raw or {}).get("settings", ())
                if raw is not None
                else getattr(manifest, "settings", ())
            ),
            source_root=effective_source_root,
        )
        self._manifests[stored.app_id] = stored
        return stored

    def discover_modules(self, module_paths: list[str]) -> list[AppManifest]:
        loaded: list[AppManifest] = []
        for module_path in module_paths:
            module = importlib.import_module(module_path)
            manifest = getattr(module, "manifest", None)
            if manifest is None:
                continue
            module_file = getattr(module, "__file__", None)
            source_root = Path(module_file).resolve().parents[1] if module_file else None
            loaded.append(self.register(manifest, source_root=source_root))
        return loaded

    def discover_prefixed_apps(self, package_prefix: str = "ms_") -> list[AppManifest]:
        candidates: list[str] = []
        for mod in pkgutil.iter_modules():
            package_name = str(mod.name)
            if not package_name.startswith(package_prefix):
                continue
            # Suite packages: they are libraries, not apps with a manifest.
            if package_name in {"ms_flow", "ms_components", "ms_contactmap"}:
                continue
            manifest_module = f"{package_name}.manifest"
            if importlib.util.find_spec(manifest_module) is None:
                continue
            candidates.append(manifest_module)
        return self.discover_modules(sorted(set(candidates)))

    def discover_manifest_files(self, manifest_paths: list[Path | str]) -> list[AppManifest]:
        loaded: list[AppManifest] = []
        for manifest_path in manifest_paths:
            resolved = Path(manifest_path).expanduser().resolve()
            if not resolved.exists() or resolved.name != "manifest.py":
                continue
            package_name = resolved.parent.name
            source_root = resolved.parents[1]
            module_name = f"{package_name}.manifest"
            inserted = False
            source_root_text = str(source_root)
            if source_root_text not in sys.path:
                sys.path.insert(0, source_root_text)
                inserted = True
            try:
                module = importlib.import_module(module_name)
                manifest = getattr(module, "manifest", None)
                if manifest is None:
                    continue
                loaded.append(self.register(manifest, source_root=source_root))
            except Exception:
                # Opportunistic discovery: a neighbouring app with uninstalled dependencies
                # must not take down the catalog of the app that does work.
                continue
            finally:
                if inserted:
                    with contextlib.suppress(ValueError):
                        sys.path.remove(source_root_text)
        return loaded

    def discover_workspace_apps(self, workspace_root: Path | str | None = None) -> list[AppManifest]:
        if workspace_root is None:
            workspace_root = os.environ.get("MS_FLOW_WORKSPACE_ROOT") or Path(__file__).resolve().parents[4]
        root = Path(workspace_root).expanduser().resolve()
        if not root.exists():
            return []
        manifest_paths = []
        for manifest_path in root.glob("*/src/*/manifest.py"):
            if manifest_path.parent.name.startswith("molsuite"):
                continue
            manifest_paths.append(manifest_path)
        return self.discover_manifest_files(sorted(set(manifest_paths)))

    def get(self, app_id: str) -> AppManifest | None:
        return self._manifests.get(app_id)

    def list_manifests(self) -> list[AppManifest]:
        return sorted(self._manifests.values(), key=lambda item: item.name.lower())

    def resolve_for_project(self, project) -> AppManifest | None:
        app_id = (getattr(project, "app_id", "") or "").strip()
        if app_id:
            return self.get(app_id)

        project_scope = (getattr(project, "scope", "") or "").strip()
        if not project_scope:
            return None

        for manifest in self._manifests.values():
            if manifest.scope_id == project_scope:
                return manifest
        return None
