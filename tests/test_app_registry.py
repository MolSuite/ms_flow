import sys
import types
from pathlib import Path

from ms_flow.core.app_settings import AppSettingSpec
from ms_flow.core.apps import AppManifest, AppRegistry
from ms_flow.core.database.master_models import Project
from ms_flow.core.project.resources import ProjectResourceSpec


def _install_manifest_module(module_name: str):
    module = types.ModuleType(module_name)
    module.manifest = AppManifest(
        app_id="testdock",
        scope_id="docking",
        name="Test Dock",
        version="0.1.0",
        entry_module="testdock.app",
        package_name="testdock",
    )
    sys.modules[module_name] = module
    return module


def test_app_registry_discovers_manifest_module():
    module_name = "tests_fake_manifest_module"
    _install_manifest_module(module_name)
    registry = AppRegistry()
    try:
        loaded = registry.discover_modules([module_name])
    finally:
        sys.modules.pop(module_name, None)

    assert len(loaded) == 1
    manifest = registry.get("testdock")
    assert manifest is not None
    assert manifest.scope_id == "docking"
    assert manifest.entry_module == "testdock.app"

def test_app_registry_resolves_project_by_app_id():
    module_name = "tests_fake_manifest_module_resolve"
    _install_manifest_module(module_name)
    registry = AppRegistry()
    try:
        registry.discover_modules([module_name])
    finally:
        sys.modules.pop(module_name, None)

    project = Project(name="dock", path="/tmp/dock", app_id="testdock", scope="docking")
    manifest = registry.resolve_for_project(project)

    assert manifest is not None
    assert manifest.app_id == "testdock"


def test_app_registry_normalizes_project_resources():
    registry = AppRegistry()

    manifest = registry.register(
        AppManifest(
            app_id="testdock",
            scope_id="docking",
            name="Test Dock",
            version="0.1.0",
            entry_module="testdock.app",
            package_name="testdock",
            project_resources=(
                {"key": "ligands", "relative_path": "data/ligands", "description": "Ligand files"},
                ProjectResourceSpec(key="results", relative_path="results/docking"),
            ),
        )
    )

    assert tuple(spec.key for spec in manifest.project_resources) == ("ligands", "results")
    assert manifest.project_resources[0].relative_path == "data/ligands"
    assert manifest.project_resources[0].description == "Ligand files"


def test_app_registry_normalizes_app_settings():
    registry = AppRegistry()

    manifest = registry.register(
        AppManifest(
            app_id="testdock",
            scope_id="docking",
            name="Test Dock",
            version="0.1.0",
            settings=(
                {
                    "key": "preview_limit",
                    "default": 50,
                    "value_type": "integer",
                    "label": "Preview limit",
                    "minimum": 1,
                },
            ),
        )
    )

    assert manifest.settings == (
        AppSettingSpec(
            key="preview_limit",
            default=50,
            value_type="integer",
            label="Preview limit",
            minimum=1,
        ),
    )


def test_app_registry_discovers_manifest_file_and_records_source_root(tmp_path):
    source_root = tmp_path / "src"
    package_dir = source_root / "dockapp"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "manifest.py").write_text(
        "\n".join(
            [
                "from ms_flow.core.apps import AppManifest",
                "manifest = AppManifest(",
                "    app_id='dockapp',",
                "    scope_id='docking',",
                "    name='Dock App',",
                "    version='0.1.0',",
                "    entry_module='dockapp.app',",
                "    package_name='dockapp',",
                ")",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    registry = AppRegistry()
    loaded = registry.discover_manifest_files([package_dir / "manifest.py"])

    assert len(loaded) == 1
    manifest = registry.get("dockapp")
    assert manifest is not None
    assert manifest.source_root == source_root.resolve()


def test_app_registry_discovers_workspace_apps_from_sibling_sources(tmp_path):
    workspace_root = tmp_path / "workspace"
    source_root = workspace_root / "DockApp" / "src"
    package_dir = source_root / "dockapp"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "manifest.py").write_text(
        "\n".join(
            [
                "from ms_flow.core.apps import AppManifest",
                "manifest = AppManifest(",
                "    app_id='dockapp',",
                "    scope_id='docking',",
                "    name='Dock App',",
                "    version='0.1.0',",
                "    entry_module='dockapp.app',",
                "    package_name='dockapp',",
                ")",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    registry = AppRegistry()
    loaded = registry.discover_workspace_apps(workspace_root)

    assert len(loaded) == 1
    manifest = registry.get("dockapp")
    assert manifest is not None
    assert manifest.source_root == source_root.resolve()
